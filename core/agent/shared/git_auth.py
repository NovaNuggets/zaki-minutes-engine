"""Ephemeral, origin-bound Git credentials for Agent workspace operations.

GitHub personal access tokens are bearer credentials.  They must not appear in a process argv,
environment value, or persisted git remote.  Network git operations therefore receive credentials
through a short-lived ``GIT_ASKPASS`` helper backed by an owner-only token file.
"""
from __future__ import annotations

import contextlib
import os
import re
import tempfile
import urllib.parse
from collections.abc import Iterator, Mapping

from shared.gitenv import scrubbed_git_env


_GITHUB_COMPONENT_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _local_git_target(remote_url: str) -> bool:
    """Return whether ``remote_url`` is a local path/file URL that cannot consume a PAT."""
    parsed = urllib.parse.urlsplit(remote_url)
    if not parsed.scheme:
        # SCP-like targets are network remotes, not local paths.
        return not re.match(r"^[^/\\]+@[^/:\\]+:", remote_url)
    return (
        parsed.scheme.lower() == "file"
        and parsed.netloc in ("", "localhost")
        and not parsed.query
        and not parsed.fragment
    )


def require_github_https_remote(remote_url: str) -> str:
    """Validate the exact HTTPS GitHub repository shape allowed to receive a PAT.

    The accepted form is ``https://github.com/{owner}/{repo}[.git]``.  Credentials, ports,
    redirects/alternate hosts, query strings, fragments, encoded delimiters and path tricks are
    rejected before git is launched.
    """
    if any(ord(char) < 0x20 or char == "\\" for char in remote_url) or "%" in remote_url:
        raise ValueError("authenticated git remotes must be exact GitHub HTTPS repository URLs")
    parsed = urllib.parse.urlsplit(remote_url)
    if (
        parsed.scheme.lower() != "https"
        or parsed.netloc.lower() != "github.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("authenticated git remotes must be exact GitHub HTTPS repository URLs")
    parts = parsed.path.split("/")
    if (
        len(parts) != 3
        or parts[0]
        or any(part in (".", "..") for part in parts[1:])
        or not all(_GITHUB_COMPONENT_RE.fullmatch(part) for part in parts[1:])
    ):
        raise ValueError("authenticated git remotes must be exact GitHub HTTPS repository URLs")
    repo = parts[2][:-4] if parts[2].endswith(".git") else parts[2]
    if not repo or not _GITHUB_COMPONENT_RE.fullmatch(repo):
        raise ValueError("authenticated git remotes must be exact GitHub HTTPS repository URLs")
    return remote_url


def _write_private(path: str, data: bytes, mode: int) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("could not write ephemeral git credential")
            view = view[written:]
    finally:
        os.close(fd)


@contextlib.contextmanager
def git_credential_env(remote_url: str, token: str | None) -> Iterator[Mapping[str, str]]:
    """Yield a scrubbed git environment with an ephemeral GitHub askpass credential.

    Local targets deliberately ignore ``token`` so offline repositories remain usable without ever
    exposing the bearer value.  Every other token-authenticated target must be the exact GitHub HTTPS
    origin validated above.
    """
    base = scrubbed_git_env(GIT_ASKPASS="true", GIT_TERMINAL_PROMPT="0")
    for key in tuple(base):
        if key == "GIT_CURL_VERBOSE" or key.startswith("GIT_TRACE"):
            base.pop(key, None)
    if not token or _local_git_target(remote_url):
        yield base
        return

    require_github_https_remote(remote_url)
    with tempfile.TemporaryDirectory(prefix="vexa-git-auth-") as temp_dir:
        os.chmod(temp_dir, 0o700)
        token_file = os.path.join(temp_dir, "token")
        askpass = os.path.join(temp_dir, "askpass")
        hooks = os.path.join(temp_dir, "hooks")
        os.mkdir(hooks, 0o700)
        _write_private(token_file, token.encode("utf-8"), 0o600)
        _write_private(
            askpass,
            b"#!/bin/sh\n"
            b"case \"$1\" in\n"
            b"  *Username*) printf '%s\\n' 'x-access-token' ;;\n"
            b"  *Password*) exec /bin/cat \"$VEXA_GIT_TOKEN_FILE\" ;;\n"
            b"  *) exit 1 ;;\n"
            b"esac\n",
            0o700,
        )
        yield {
            **base,
            "GIT_ASKPASS": askpass,
            # Reset ambient credential helpers so they cannot pre-empt the one-shot askpass values,
            # and refuse transport redirects so the PAT never changes origin after validation.
            "GIT_CONFIG_COUNT": "3",
            "GIT_CONFIG_KEY_0": "credential.helper",
            "GIT_CONFIG_VALUE_0": "",
            "GIT_CONFIG_KEY_1": "http.followRedirects",
            "GIT_CONFIG_VALUE_1": "false",
            "GIT_CONFIG_KEY_2": "core.hooksPath",
            "GIT_CONFIG_VALUE_2": hooks,
            "VEXA_GIT_TOKEN_FILE": token_file,
        }
