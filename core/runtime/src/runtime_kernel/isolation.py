"""isolation.py — POSIX tenant isolation for the PROCESS backend (the lite deployment).

Docker/k8s workers get tenant isolation from the mount table (one bind per mount — see mounts.py):
another tenant's workspace simply isn't in the container. Lite workers are CHILD PROCESSES sharing one
filesystem, so the wall must be the kernel's instead: every dispatch runs as a PER-SUBJECT uid, private
tiers are ``0700``-owned by that uid, and each shared workspace gets its OWN gid (allocated once,
persisted in a root-owned registry at the store root) that member workers join as a supplementary
group. ``_global`` stays root-owned world-readable — enforced read-only for everyone.

Split for testability:
  * :func:`plan_process_isolation` — PURE: env → the uid/gid/dir plan (or None + reason when
    unavailable). ProcessBackend refuses an Agent spawn when it returns None. Unit-tested offline.
  * :func:`apply_process_isolation` — effects: allocate gids, chown/chmod the plan's dirs. Idempotent
    and cheap when ownership already matches (a full ``chown -R`` runs only on a mismatched tree).
  * :func:`preexec_for` — the ``subprocess.Popen(preexec_fn=…)`` that drops the child to the plan's
    uid/gid/groups. Runs in the forked child, pre-exec.

Requires euid 0 (lite's runtime runs as root inside its container) and a NUMERIC subject (gateway
user ids). Anything else is logged here and rejected by ProcessBackend for Agent workloads.
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import stat
from dataclasses import dataclass, field
from typing import Callable, Mapping, Optional

from .mounts import mount_set

logger = logging.getLogger("runtime_kernel.isolation")

UID_BASE = 100000   # per-subject uid = UID_BASE + int(subject)
MAX_LITE_SUBJECT_ID = 99999  # reserve 200000+ for shared gids and 10M+ for meeting-bot uids
GID_BASE = 200000   # per-shared-workspace gids, allocated sequentially from here
GID_REGISTRY = ".vexa-shared-gids.json"   # root-owned, at the store root


@dataclass(frozen=True)
class ProcessIsolation:
    """One dispatch's POSIX plan: run as ``uid``/``gid`` (+ shared-workspace ``groups``); ``private``
    dirs are owned ``uid`` mode 0700; ``shared`` dirs owned root:<gid> mode 2770; ``home`` is a
    per-subject writable HOME (harness config/creds land there, not in /root)."""

    uid: int
    gid: int
    store_root: str
    home: str
    private: tuple[str, ...] = ()
    shared: tuple[tuple[str, str], ...] = ()   # (path, workspace slug/id) — gid resolved at apply
    groups: tuple[int, ...] = field(default=(), compare=False)  # filled by apply (registry-backed)


def plan_process_isolation(env: Mapping[str, str], *, euid: Optional[int] = None) -> Optional[ProcessIsolation]:
    """Env → the isolation plan, or ``None`` (with ONE loud log naming why) when unavailable."""
    if euid is None:
        euid = os.geteuid()
    subject = (env.get("VEXA_OWNER") or "").strip()
    mounts = mount_set(env)
    root = env.get("VEXA_WORKSPACE_MOUNT_TARGET") or env.get("VEXA_WORKSPACES_DIR") or ""
    if not root:
        # the process backend serves bots too (no workspace env) — nothing to isolate, not an error
        return None if not mounts else _unavailable("no workspace store root in the dispatch env")
    if euid != 0:
        return _unavailable("runtime is not root — cannot setuid workers (run lite's runtime as root)")
    if not subject.isdigit() or str(int(subject)) != subject:
        return _unavailable(f"subject {subject!r} is not numeric — no deterministic uid mapping")
    subject_id = int(subject)
    if subject_id > MAX_LITE_SUBJECT_ID:
        return _unavailable(
            f"subject {subject!r} exceeds Lite's reserved uid range (max {MAX_LITE_SUBJECT_ID})"
        )
    uid = UID_BASE + subject_id
    private: list[str] = []
    shared: list[tuple[str, str]] = []
    for m in mounts:
        path, role = m.get("path") or "", m.get("role") or "private"
        if not path:
            continue
        if role in ("private", "system"):
            private.append(path)
        elif role == "shared":
            shared.append((path, str(m.get("slug") or os.path.basename(path.rstrip("/")))))
        # role == "global": root-owned world-readable — enforced ro by ownership, nothing to do
    home = os.path.join(root, ".home", subject)
    return ProcessIsolation(uid=uid, gid=uid, store_root=root, home=home,
                            private=tuple(private), shared=tuple(shared))


def _unavailable(reason: str) -> None:
    logger.warning("workspace isolation UNAVAILABLE for this dispatch (%s) — Agent spawn must fail "
                   "closed; fix the condition because there is no supported opt-out.", reason)
    return None


def _require_safe_workspace_directory(path: str, root: str) -> str:
    """Return an absolute real directory below ``root`` or fail before any root ownership change."""
    root_abs = os.path.abspath(root)
    path_abs = os.path.abspath(path)
    try:
        within_root = os.path.commonpath((root_abs, path_abs)) == root_abs
    except ValueError:
        within_root = False
    if not within_root:
        raise OSError(f"workspace isolation path escapes store root: {path}")
    if not os.path.lexists(path_abs):
        raise OSError(f"workspace isolation path is missing: {path}")
    try:
        info = os.lstat(path_abs)
    except OSError as exc:
        raise OSError(f"workspace isolation path is unreadable: {path}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise OSError(f"workspace isolation path is not a real directory: {path}")
    if os.path.realpath(path_abs) != path_abs:
        raise OSError(f"workspace isolation path crosses a symlink: {path}")
    return path_abs


def _ensure_safe_workspace_directory(path: str, root: str, mode: int) -> str:
    """Create one direct child below an already-safe root, rejecting pre-existing symlinks."""
    if not os.path.lexists(path):
        os.mkdir(path, mode)
    safe = _require_safe_workspace_directory(path, root)
    os.chmod(safe, mode)
    return safe


# ── effects ────────────────────────────────────────────────────────────────────────────────────────

def _load_registry(root: str) -> dict[str, int]:
    try:
        with open(os.path.join(root, GID_REGISTRY), encoding="utf-8") as f:
            data = json.load(f)
        return {str(k): int(v) for k, v in data.items()} if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _shared_gid(root: str, ws_id: str) -> int:
    """The workspace's gid — allocated once, persisted root-owned (0600) at the store root."""
    reg = _load_registry(root)
    if ws_id in reg:
        return reg[ws_id]
    gid = max(reg.values(), default=GID_BASE - 1) + 1
    reg[ws_id] = gid
    path = os.path.join(root, GID_REGISTRY)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(reg, f)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)
    return gid


def _chown_tree(path: str, uid: int, gid: int) -> None:
    os.chown(path, uid, gid)
    for base, dirs, files in os.walk(path):
        for name in dirs + files:
            p = os.path.join(base, name)
            try:
                os.lchown(p, uid, gid)
            except OSError:
                logger.warning("chown failed under %s: %s", path, p)


def _sweep_default_deny(root: str) -> None:
    """DEFAULT-DENY for tenants that never dispatched since isolation shipped: every tenant-owned dir
    in the store whose mode is still open gets 0700 (owner unchanged — the owner's own next dispatch
    chowns it properly). Top-level only per tier (no recursion — 0700 on the top seals the tree), so
    the sweep is O(#tenants) stat calls per dispatch. Skips dirs already sealed (0700) or already
    group-managed (a shared workspace some member's dispatch set to 2770)."""
    special = {".attached", ".system", ".home", "_global", GID_REGISTRY}
    tiers = [root, os.path.join(root, ".attached"), os.path.join(root, ".system")]
    for tier in tiers:
        try:
            entries = list(os.scandir(tier))
        except OSError:
            continue
        for e in entries:
            if e.name in special or not e.is_dir(follow_symlinks=False):
                continue
            mode = stat.S_IMODE(e.stat(follow_symlinks=False).st_mode)
            if mode in (0o700, 0o2770):
                continue
            os.chmod(e.path, 0o700)


def apply_process_isolation(plan: ProcessIsolation) -> ProcessIsolation:
    """Materialize the plan: store-root traversal perms, DEFAULT-DENY sweep over every tenant dir,
    per-subject HOME, private 0700 trees, shared 2770 group trees. Idempotent — a tree whose top
    already matches is left alone (cheap steady state). Returns the plan with the shared-workspace
    ``groups`` resolved."""
    root = _require_safe_workspace_directory(plan.store_root, plan.store_root)
    # Validate every caller-derived mount before the first chmod/chown. A direct outside-root path
    # is as dangerous as a symlink: both could retarget root's recursive ownership operation.
    for path in (*plan.private, *(shared_path for shared_path, _ in plan.shared)):
        _require_safe_workspace_directory(path, root)
    # store root + tier parents: traversable but not listable/enterable across tenants
    os.chmod(root, 0o755)
    for p, mode in (
        (os.path.join(root, ".attached"), 0o711),
        (os.path.join(root, ".system"), 0o711),
        (os.path.join(root, ".home"), 0o711),
    ):
        _ensure_safe_workspace_directory(p, root, mode)
    # seal EVERY tenant dir, not just this dispatch's — a never-dispatched tenant's data must not
    # sit world-readable while it waits for its owner's first isolated dispatch
    _sweep_default_deny(root)
    # the .attached/<subject> parent dir is the subject's too (their slots live under it)
    subj_attached = os.path.join(root, ".attached", str(plan.uid - UID_BASE))
    private = list(plan.private)
    if os.path.lexists(subj_attached):
        private.append(_require_safe_workspace_directory(subj_attached, root))
    home_parent = _require_safe_workspace_directory(os.path.dirname(plan.home), root)
    if home_parent != os.path.join(root, ".home"):
        raise OSError(f"workspace isolation path has an invalid home parent: {plan.home}")
    if not os.path.lexists(plan.home):
        os.mkdir(plan.home, 0o700)
    _require_safe_workspace_directory(plan.home, root)
    private.append(plan.home)
    for path in private:
        path = _require_safe_workspace_directory(path, root)
        st = os.lstat(path)
        if st.st_uid != plan.uid or stat.S_IMODE(st.st_mode) != 0o700:
            _chown_tree(path, plan.uid, plan.gid)
            os.chmod(path, 0o700)
    groups: list[int] = []
    for path, ws_id in plan.shared:
        path = _require_safe_workspace_directory(path, root)
        gid = _shared_gid(root, ws_id)
        groups.append(gid)
        st = os.lstat(path)
        if st.st_gid != gid or stat.S_IMODE(st.st_mode) != 0o2770:
            _chown_tree(path, 0, gid)
            os.chmod(path, 0o2770)   # setgid: new files inherit the workspace group
    # Stage only the operator-selected subscription file into the subject HOME. Lite mounts this at
    # a configurable in-container path; compose's legacy default remains the root-home file. The
    # source path itself is scrubbed from the child environment by ProcessBackend.
    creds_src = (
        os.environ.get("HOST_CLAUDE_CREDENTIALS")
        or os.path.expanduser("~/.claude/.credentials.json")
    )
    if os.path.isfile(creds_src):
        dot = os.path.join(plan.home, ".claude")
        try:
            os.mkdir(dot, 0o700)
        except FileExistsError:
            pass
        directory_fd = None
        staged_name = f".credentials.{secrets.token_hex(12)}"
        try:
            directory_fd = os.open(
                dot,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
            os.fchown(directory_fd, plan.uid, plan.gid)
            os.fchmod(directory_fd, 0o700)
            staged_fd = os.open(
                staged_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory_fd,
            )
            try:
                with open(creds_src, "rb") as source, os.fdopen(staged_fd, "wb") as staged:
                    while chunk := source.read(64 * 1024):
                        staged.write(chunk)
                    staged.flush()
                    os.fsync(staged.fileno())
                    os.fchown(staged.fileno(), plan.uid, plan.gid)
                    os.fchmod(staged.fileno(), 0o400)
            except Exception:
                try:
                    os.close(staged_fd)
                except OSError:
                    pass
                raise
            os.replace(
                staged_name,
                ".credentials.json",
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
            staged_name = ""
        except OSError as e:
            logger.warning("could not stage claude credentials into %s: %s", dot, e)
        finally:
            if staged_name and directory_fd is not None:
                try:
                    os.unlink(staged_name, dir_fd=directory_fd)
                except OSError:
                    pass
            if directory_fd is not None:
                os.close(directory_fd)
    return ProcessIsolation(uid=plan.uid, gid=plan.gid, store_root=root, home=plan.home,
                            private=plan.private, shared=plan.shared, groups=tuple(groups))


def preexec_for(plan: ProcessIsolation) -> Callable[[], None]:
    """The Popen ``preexec_fn`` dropping the forked child to the plan's identity (groups → gid → uid,
    in that order — after setuid the process can no longer change groups)."""
    def _drop() -> None:
        os.setgroups(list(plan.groups))
        os.setgid(plan.gid)
        os.setuid(plan.uid)
    return _drop


def child_env_for(plan: ProcessIsolation, env: dict[str, str]) -> dict[str, str]:
    """Env adjustments for the dropped child: a writable per-subject HOME (harness config/session
    files), and TMPDIR under it so scratch files never collide across subjects in /tmp."""
    tmp = os.path.join(plan.home, "tmp")
    os.makedirs(tmp, exist_ok=True)
    os.chown(tmp, plan.uid, plan.gid)
    return {**env, "HOME": plan.home, "TMPDIR": tmp}
