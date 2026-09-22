"""workspace_publish — publish a vexa-born workspace to GitHub (counterpart of attach/swap).

Proves the publish lifecycle on REAL git over a LOCAL bare repo as the push target (no network;
the GitHub creation call is an injected fake, mirroring how workspace_attach tests inject CloneFn):
  create+push full history → re-publish is a plain push → divergence fails loud (no force push) →
  attached workspaces are refused → tokens never persist and never leak into errors (P15).
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import shared.adapters as adapters
import control_plane.workspace_publish as workspace_publish

from control_plane.workspace_attach import swap_workspace
from control_plane.workspace_publish import (
    PUBLISH_REMOTE,
    PublishError,
    RepoExistsError,
    publish_workspace,
    published_remote_url,
)

TOKEN = "ghp_SECRET_token_123"


def _run(cwd: Path, *a: str) -> str:
    return subprocess.run(["git", *a], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def _workspace(root: Path, subject: str, commits: int = 2) -> Path:
    """A vexa-born (seeded-style) active workspace with real history at <root>/<subject>."""
    ws = root / subject
    ws.mkdir(parents=True)
    _run(ws, "init", "-q", "-b", "main")
    _run(ws, "config", "user.email", "t@t")
    _run(ws, "config", "user.name", "t")
    for i in range(commits):
        (ws / "CLAUDE.md").write_text(f"root v{i}\n")
        _run(ws, "add", "-A")
        _run(ws, "commit", "-q", "-m", f"c{i}")
    return ws


def _bare(path: Path) -> Path:
    path.mkdir(parents=True)
    _run(path, "init", "-q", "--bare", "-b", "main")
    return path


def test_publish_creates_repo_and_pushes_full_history(tmp_path):
    """The create path: the injected creator is called with the caller's args and its returned URL is
    pushed to — full history, head sha returned, repo_url token-free."""
    root = tmp_path / "workspaces"
    ws = _workspace(root, "u1", commits=3)
    bare = _bare(tmp_path / "remote.git")
    calls: list[tuple] = []

    def fake_create(name, private, token, org):
        calls.append((name, private, token, org))
        return str(bare)

    res = publish_workspace(root, "u1", token=TOKEN, repo_name="my-workspace",
                            private=True, create_repo=fake_create)

    assert calls == [("my-workspace", True, TOKEN, None)]
    assert res.created is True and res.pushed_ref == "main"
    assert res.head_sha == _run(ws, "rev-parse", "HEAD")
    # FULL history landed on the remote
    assert _run(bare, "rev-parse", "main") == res.head_sha
    assert _run(bare, "rev-list", "--count", "main") == "3"


def test_publish_to_remote_url_skips_creation(tmp_path):
    """remote_url given → no creation call, plain push to the pre-created (empty) repo."""
    root = tmp_path / "workspaces"
    _workspace(root, "u1")
    bare = _bare(tmp_path / "pre.git")

    def never_create(*a):  # pragma: no cover - must not run
        raise AssertionError("create_repo must not be called when remote_url is given")

    res = publish_workspace(root, "u1", token=TOKEN, remote_url=str(bare), create_repo=never_create)
    assert res.created is False
    assert _run(bare, "rev-parse", "main") == res.head_sha


def test_republish_same_remote_is_plain_push(tmp_path):
    """Idempotent-ish: publish, commit more, publish again to the same remote — a fast-forward push."""
    root = tmp_path / "workspaces"
    ws = _workspace(root, "u1")
    bare = _bare(tmp_path / "remote.git")
    publish_workspace(root, "u1", token=TOKEN, remote_url=str(bare))

    (ws / "more.md").write_text("more\n")
    _run(ws, "add", "-A")
    _run(ws, "commit", "-q", "-m", "more")

    res = publish_workspace(root, "u1", token=TOKEN, remote_url=str(bare))
    assert _run(bare, "rev-parse", "main") == res.head_sha == _run(ws, "rev-parse", "HEAD")


def test_divergence_fails_loud_never_force(tmp_path):
    """The remote grew history the workspace doesn't have → a clear error; the remote's commit
    survives (NO force push, ever)."""
    root = tmp_path / "workspaces"
    _workspace(root, "u1")
    bare = _bare(tmp_path / "remote.git")
    # seed the remote with foreign history
    other = tmp_path / "other"
    other.mkdir()
    _run(other, "init", "-q", "-b", "main")
    _run(other, "config", "user.email", "o@o")
    _run(other, "config", "user.name", "o")
    (other / "X").write_text("foreign\n")
    _run(other, "add", "-A")
    _run(other, "commit", "-q", "-m", "foreign")
    _run(other, "push", "-q", str(bare), "main")
    foreign_sha = _run(bare, "rev-parse", "main")

    with pytest.raises(PublishError):
        publish_workspace(root, "u1", token=TOKEN, remote_url=str(bare))
    assert _run(bare, "rev-parse", "main") == foreign_sha  # remote untouched


def test_token_never_persisted_and_errors_redacted(tmp_path):
    """P15: after a publish the workspace's git config carries NO token; the dedicated remote exists
    token-free and origin was never touched; a push failure's message is token-redacted."""
    root = tmp_path / "workspaces"
    ws = _workspace(root, "u1")
    _run(ws, "remote", "add", "origin", "https://example.com/keep.git")
    bare = _bare(tmp_path / "remote.git")

    publish_workspace(root, "u1", token=TOKEN, remote_url=f"file://{bare}")

    cfg = (ws / ".git" / "config").read_text()
    assert TOKEN not in cfg
    assert _run(ws, "remote", "get-url", "origin") == "https://example.com/keep.git"
    assert _run(ws, "remote", "get-url", PUBLISH_REMOTE) == f"file://{bare}"

    # a failing push (bogus remote) surfaces a token-free error
    with pytest.raises(PublishError) as ei:
        publish_workspace(root, "u1", token=TOKEN, remote_url=str(tmp_path / "nope.git"))
    assert TOKEN not in str(ei.value)


def test_github_push_uses_ephemeral_askpass_and_never_places_pat_in_git_argv(monkeypatch, tmp_path):
    """A GitHub PAT is supplied as Basic-auth password through a one-shot askpass helper only.

    In particular, neither a process listing nor the persisted remote command may contain it.
    """
    real_run = subprocess.run
    monkeypatch.setenv("GIT_TRACE_CURL", "1")
    monkeypatch.setenv("GIT_CURL_VERBOSE", "1")
    calls: list[tuple[list[str], dict[str, str]]] = []
    askpass_path: Path | None = None
    hooks_path: Path | None = None

    def fake_run(argv, *, cwd, env, capture_output, text):
        nonlocal askpass_path, hooks_path
        args = [str(arg) for arg in argv]
        copied_env = {str(key): str(value) for key, value in env.items()}
        calls.append((args, copied_env))

        assert TOKEN not in "\0".join(args)
        assert TOKEN not in "\0".join(copied_env.values())
        if "push" in args:
            assert "GIT_TRACE_CURL" not in copied_env
            assert "GIT_CURL_VERBOSE" not in copied_env
            askpass_path = Path(copied_env["GIT_ASKPASS"])
            assert copied_env["GIT_CONFIG_COUNT"] == "3"
            assert copied_env["GIT_CONFIG_KEY_0"] == "credential.helper"
            assert copied_env["GIT_CONFIG_VALUE_0"] == ""
            assert copied_env["GIT_CONFIG_KEY_1"] == "http.followRedirects"
            assert copied_env["GIT_CONFIG_VALUE_1"] == "false"
            assert copied_env["GIT_CONFIG_KEY_2"] == "core.hooksPath"
            hooks_path = Path(copied_env["GIT_CONFIG_VALUE_2"])
            assert hooks_path.is_dir() and list(hooks_path.iterdir()) == []
            username = real_run(
                [str(askpass_path), "Username for 'https://github.com':"],
                env=copied_env,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            password = real_run(
                [str(askpass_path), "Password for 'https://x-access-token@github.com':"],
                env=copied_env,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            assert username == "x-access-token"
            assert password == TOKEN

        stdout = "deadbeef\n" if "rev-parse" in args else ""
        return subprocess.CompletedProcess(args, 0, stdout, "")

    monkeypatch.setattr(adapters.subprocess, "run", fake_run)

    assert adapters.push_with_token(
        tmp_path, "https://github.com/acme/minutes.git", "main", TOKEN
    ) == "deadbeef"
    assert calls
    assert askpass_path is not None and not askpass_path.exists()
    assert hooks_path is not None and not hooks_path.exists()


@pytest.mark.parametrize(
    "remote_url",
    [
        "http://github.com/acme/minutes.git",
        "https://github.example/acme/minutes.git",
        "https://github.com.evil.example/acme/minutes.git",
        "https://user@github.com/acme/minutes.git",
        "https://github.com:443/acme/minutes.git",
        "https://github.com/acme/minutes.git?redirect=evil",
        "https://github.com/acme/minutes.git#fragment",
        "https://github.com/acme%2fother/minutes.git",
        "https://github.com/../minutes.git",
    ],
)
def test_github_pat_rejects_every_noncanonical_remote_before_git_runs(
    monkeypatch, tmp_path, remote_url
):
    def unexpected_run(*args, **kwargs):  # pragma: no cover - validation must precede git
        raise AssertionError("git ran before the authenticated remote was validated")

    monkeypatch.setattr(adapters.subprocess, "run", unexpected_run)
    with pytest.raises(adapters.GitPushError, match="exact GitHub HTTPS"):
        adapters.push_with_token(tmp_path, remote_url, "main", TOKEN)


def test_create_failure_errors_are_token_free(tmp_path):
    """Creator failures (already-exists and generic) surface redacted, actionable errors."""
    root = tmp_path / "workspaces"
    _workspace(root, "u1")

    def exists(*a):
        raise RepoExistsError("a repository named 'w' already exists under your account — pick "
                              "another name, or pass its URL as remote_url to push into it")

    with pytest.raises(RepoExistsError) as ei:
        publish_workspace(root, "u1", token=TOKEN, repo_name="w", create_repo=exists)
    assert "already exists" in str(ei.value) and TOKEN not in str(ei.value)


def test_github_repo_creation_disables_redirects_and_reads_a_bounded_response(monkeypatch):
    clone_url = "https://github.com/acme/minutes.git"
    body = (f'{{"clone_url":"{clone_url}"}}').encode()

    class Response:
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, size=None):
            assert size == workspace_publish.MAX_INTERNAL_JSON_BYTES + 1
            return body

    def safe_open(request, *, timeout):
        assert request.full_url == "https://api.github.com/user/repos"
        assert timeout == 15
        return Response()

    def unexpected_urlopen(*args, **kwargs):  # pragma: no cover - bearer requests cannot redirect
        raise AssertionError("redirect-following urlopen was used")

    monkeypatch.setattr(workspace_publish, "open_no_redirect", safe_open, raising=False)
    monkeypatch.setattr(workspace_publish.urllib.request, "urlopen", unexpected_urlopen)

    assert workspace_publish._github_create_repo("minutes", True, TOKEN, None) == clone_url


def test_github_repo_creation_rejects_a_noncanonical_clone_url(monkeypatch):
    body = b'{"clone_url":"https://github.com.evil.example/acme/minutes.git"}'

    class Response:
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, size=None):
            return body

    monkeypatch.setattr(workspace_publish, "open_no_redirect", lambda *args, **kwargs: Response())

    with pytest.raises(PublishError, match="invalid clone URL"):
        workspace_publish._github_create_repo("minutes", True, TOKEN, None)


def test_attached_workspace_is_refused(tmp_path):
    """Vexa-born only: an ATTACHED external repo already has a home — publish refuses it."""
    root = tmp_path / "workspaces"
    ws = _workspace(root, "u1")
    origin = tmp_path / "external"
    origin.mkdir()
    _run(origin, "init", "-q", "-b", "main")
    _run(origin, "config", "user.email", "t@t")
    _run(origin, "config", "user.name", "t")
    (origin / "CLAUDE.md").write_text("CUSTOM ROOT")
    _run(origin, "add", "-A")
    _run(origin, "commit", "-q", "-m", "seed")
    swap_workspace(root, "u1", str(origin), "main")   # active workspace is now the attached repo

    with pytest.raises(PublishError) as ei:
        publish_workspace(root, "u1", token=TOKEN, repo_name="w",
                          create_repo=lambda *a: (_ for _ in ()).throw(AssertionError))
    assert "attached" in str(ei.value)


def test_bad_inputs_are_value_errors(tmp_path):
    """Missing token / bad repo_name / no workspace / no commits fail loud with clear messages."""
    root = tmp_path / "workspaces"
    with pytest.raises(ValueError):
        publish_workspace(root, "u1", token="  ", repo_name="w")   # no token
    with pytest.raises(PublishError):
        publish_workspace(root, "u1", token=TOKEN, repo_name="w")  # no workspace yet
    _workspace(root, "u2")
    with pytest.raises(ValueError):
        publish_workspace(root, "u2", token=TOKEN, repo_name="bad name!")  # invalid repo name
    with pytest.raises(ValueError):
        publish_workspace(root, "u2", token=TOKEN)  # neither repo_name nor remote_url


@pytest.mark.parametrize("org", ["../user", "acme/repos?private=false", "-leading", "trailing-"])
def test_publish_rejects_noncanonical_github_org_before_repo_creation(tmp_path, org):
    root = tmp_path / "workspaces"
    _workspace(root, "u1")
    bare = _bare(tmp_path / "remote.git")
    called = False

    def creator(*args):
        nonlocal called
        called = True
        return str(bare)

    with pytest.raises(ValueError, match="invalid GitHub org"):
        publish_workspace(root, "u1", token=TOKEN, repo_name="minutes", org=org, create_repo=creator)
    assert called is False

# ── published_remote_url — the read-side probe the terminal renders the published state from ────────


def test_published_remote_url_reflects_publish_state(tmp_path):
    """None before a publish; the token-free remote URL (``.git`` stripped, like PublishResult) after."""
    root = tmp_path / "workspaces"
    ws = _workspace(root, "u1")
    assert published_remote_url(ws) is None                     # never published

    bare = _bare(tmp_path / "remote.git")
    publish_workspace(root, "u1", token=TOKEN, remote_url=str(bare))

    url = published_remote_url(ws)
    assert url == str(bare)[: -len(".git")]                     # the display URL of the publish remote
    assert TOKEN not in url                                     # P15: never a credential in the read path


def test_published_remote_url_strips_embedded_credentials(tmp_path):
    """Defense in depth (P15): even a credential somehow persisted in the remote URL never reaches the
    client — user:token@ is stripped, and the URL is the human (no ``.git``) form."""
    root = tmp_path / "workspaces"
    ws = _workspace(root, "u1")
    _run(ws, "remote", "add", PUBLISH_REMOTE, f"https://x-access-token:{TOKEN}@github.com/u/repo.git")
    assert published_remote_url(ws) == "https://github.com/u/repo"


def test_published_remote_url_quiet_on_non_repo(tmp_path):
    """A state probe, not an operation: a missing dir / non-repo is simply 'not published' (None)."""
    assert published_remote_url(tmp_path / "nope") is None
    plain = tmp_path / "plain"
    plain.mkdir()
    assert published_remote_url(plain) is None


def test_publish_ws_dir_targets_explicit_workspace(tmp_path):
    """`ws_dir` publishes THAT workspace (an own parked slot / shared dir the API resolved), not the
    subject's seed dir — the slug-aware endpoint path."""
    root = tmp_path / "workspaces"
    _workspace(root, "u1", commits=1)                       # the seed dir — must NOT be pushed
    other = _workspace(root / ".attached" / "u1", "acme-1", commits=2)
    (other / "kg").mkdir(); (other / "kg" / "x.md").write_text("acme\n")
    _run(other, "add", "-A"); _run(other, "commit", "-q", "-m", "acme content")
    bare = _bare(tmp_path / "remote.git")

    res = publish_workspace(root, "u1", token=TOKEN, repo_name="acme",
                            create_repo=lambda n, p, t, o: str(bare), ws_dir=other)
    assert res.created is True
    assert _run(bare, "rev-parse", "main") == _run(other, "rev-parse", "HEAD")
    assert published_remote_url(other)                       # the explicit dir carries the publish remote
    assert published_remote_url(root / "u1") is None         # the seed dir was untouched


def test_publish_ws_dir_refuses_attached_clone(tmp_path):
    """An explicit target with an `origin` remote is an ATTACHED external clone — refused (its home is
    that repo; publish is for vexa-born workspaces)."""
    root = tmp_path / "workspaces"
    other = _workspace(root / ".attached" / "u1", "clone-1", commits=1)
    _run(other, "remote", "add", "origin", "https://github.com/me/upstream.git")
    with pytest.raises(PublishError, match="attached from an external repo"):
        publish_workspace(root, "u1", token=TOKEN, repo_name="x",
                          create_repo=lambda n, p, t, o: "unused", ws_dir=other)
