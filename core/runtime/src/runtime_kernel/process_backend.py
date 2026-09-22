"""ProcessBackend — runs a workload as a child process (single-host / no Docker). The leanest real
backend; satisfies the runtime.v1 lifecycle. (docker/k8s backends are ported from 0.11 when needed.)

Output capture: each workload's stdout+stderr goes to a per-workload log file under
``PROCESS_LOG_DIR`` (default ``<tempdir>/vexa-workloads``) — the process analog of ``docker logs``.
A workload that exits nonzero gets its log tail surfaced at ERROR level through the runtime's own
logs the first time the exit is observed, so a crashed worker (e.g. an ImportError at startup) is
diagnosable from the runtime service logs instead of vanishing into /dev/null."""
from __future__ import annotations

import hashlib
import logging
import os
import re
import signal
import subprocess
import tempfile
from typing import Optional

from .backend import WorkloadHandle
from .models import Resources
from .isolation import apply_process_isolation, child_env_for, plan_process_isolation, preexec_for
from .mounts import mount_set
from .profiles import Runnable

log = logging.getLogger("runtime_kernel.process")

# How much of a failed workload's log lands in the runtime log line (the full file stays on disk).
_TAIL_BYTES = 4096

# A workload child starts from process mechanics only. Everything product-, tenant-, provider-, or
# operator-specific must cross the authenticated WorkloadSpec.env projection boundary explicitly.
# An allowlist is essential: a deny-list silently leaks the next AWS/GitHub/model credential added
# to the root supervisor environment.
_SAFE_AMBIENT_KEYS = frozenset({
    "PATH",
    "LANG",
    "LANGUAGE",
    "TZ",
    "TERM",
    "COLORTERM",
    "NO_COLOR",
    "TMPDIR",
    "DISPLAY",
    "PULSE_SERVER",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "NODE_EXTRA_CA_CERTS",
    "PYTHONUTF8",
    "PYTHONIOENCODING",
})


def _is_safe_ambient_key(key: str) -> bool:
    return key in _SAFE_AMBIENT_KEYS or key.startswith("LC_")


def _child_process_env(workload_env: dict[str, str]) -> dict[str, str]:
    """Build a minimum ambient environment, then layer the authenticated explicit projection."""
    child = {
        key: value
        for key, value in os.environ.items()
        if _is_safe_ambient_key(key)
    }
    child.update(workload_env)
    return child


def _log_dir() -> str:
    return os.environ.get("PROCESS_LOG_DIR") or os.path.join(tempfile.gettempdir(), "vexa-workloads")


_SAFE_LOG_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def _workload_log_path(log_dir: str, workload_id: str) -> str:
    """Keep caller-chosen runtime.v1 ids out of filesystem path resolution."""
    if _SAFE_LOG_ID.fullmatch(workload_id):
        leaf = f"{workload_id}.log"
    else:
        digest = hashlib.sha256(workload_id.encode("utf-8")).hexdigest()
        leaf = f"workload-{digest}.log"
    return os.path.join(log_dir, leaf)


def _tail(path: str, limit: int = _TAIL_BYTES) -> str:
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - limit))
            return f.read().decode("utf-8", errors="replace").strip()
    except OSError:
        return ""


class ProcessBackend:
    name = "process"

    def __init__(self) -> None:
        # Per-workload capture state: workloadId → (log path | None, failure already reported?).
        # Process-local, like the kernel's handle map — absent after a restart, which is fine:
        # exit codes are unobservable without a live handle anyway.
        self._capture: dict[str, dict] = {}

    def start(
        self,
        workload_id: str,
        runnable: Runnable,
        env: dict[str, str],
        *,
        resources: Optional[Resources] = None,
    ) -> WorkloadHandle:
        if not runnable.command:
            raise ValueError("process backend requires a command")
        # Workspace mount set (WP-A1.1): the lite/process backend shares the HOST filesystem — there is
        # nothing to bind, so tenant isolation is POSIX instead (runtime_kernel.isolation): the worker
        # drops to a per-subject uid, private tiers are 0700-owned, shared workspaces get per-workspace
        # gids. Agent workloads fail closed if that wall is unavailable; meeting bots have no
        # workspace plan and cross their own dedicated-uid launcher boundary instead.
        mounts = mount_set(env)
        if len(mounts) > 1:
            log.info("workload %s: %d active workspace mounts: %s",
                     workload_id, len(mounts), ", ".join(m.get("slug", "?") for m in mounts))
        preexec = None
        child_env = _child_process_env(env)
        try:
            iso = plan_process_isolation(env)
            if iso is None and runnable.broker_model_credentials:
                raise RuntimeError("Agent process isolation is unavailable; refusing root spawn")
            if iso is not None:
                iso = apply_process_isolation(iso)
                preexec = preexec_for(iso)
                child_env = child_env_for(iso, child_env)
                log.info("workload %s: POSIX-isolated as uid %d (%d shared group(s))",
                         workload_id, iso.uid, len(iso.groups))
        except OSError as e:
            if runnable.broker_model_credentials:
                raise RuntimeError(
                    "Agent process isolation setup failed; refusing root spawn"
                ) from e
            # Workspaceless profiles (meeting bots) have a separate launcher wall. Preserve their
            # ability to start while making the absent POSIX workspace layer explicit.
            log.error("workload %s: optional process isolation setup failed (%s)", workload_id, e)
            preexec = None
            child_env = _child_process_env(env)
        # Capture the child's output to a per-workload file (both streams interleaved, like
        # `docker logs`). Fail-open: if the log dir is unwritable we fall back to DEVNULL rather
        # than refusing to start the workload.
        log_path: Optional[str] = None
        out_fh = None
        try:
            log_dir = _log_dir()
            if os.path.islink(log_dir):
                raise OSError("log directory must not be a symlink")
            os.makedirs(log_dir, mode=0o700, exist_ok=True)
            os.chmod(log_dir, 0o700)
            log_path = _workload_log_path(log_dir, workload_id)
            flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(log_path, flags, 0o600)
            try:
                os.fchmod(fd, 0o600)
                out_fh = os.fdopen(fd, "ab")
            except Exception:
                os.close(fd)
                raise
        except OSError as e:
            log.warning("workload %s: cannot capture output (%s) — falling back to DEVNULL", workload_id, e)
            log_path = None
        try:
            proc = subprocess.Popen(
                runnable.command,
                env=child_env,
                stdout=out_fh if out_fh is not None else subprocess.DEVNULL,
                stderr=subprocess.STDOUT if out_fh is not None else subprocess.DEVNULL,
                start_new_session=True,
                preexec_fn=preexec,   # None = shared-trust (isolation unavailable — logged loudly)
            )
        finally:
            if out_fh is not None:
                out_fh.close()  # the child holds its own fd; ours would only leak
        self._capture[workload_id] = {"log_path": log_path, "reported": False}
        return WorkloadHandle(id=workload_id, impl=proc)

    def exit_code(self, h: WorkloadHandle) -> Optional[int]:
        code = h._impl.poll()  # type: ignore[attr-defined]
        if code is not None and code != 0:
            self._report_failure(h.id, code)
        return code

    def _report_failure(self, workload_id: str, code: int) -> None:
        """Log the failed workload's output tail — once per workload (exit_code is polled)."""
        state = self._capture.get(workload_id)
        if state is None or state["reported"]:
            return
        state["reported"] = True
        log_path = state["log_path"]
        tail = _tail(log_path) if log_path else ""
        log.error(
            "workload %s exited %d — output tail (full log: %s):\n%s",
            workload_id, code, log_path or "not captured", tail or "<no output captured>",
        )

    def _suppress_report(self, workload_id: str) -> None:
        """A backend-initiated stop makes the nonzero (signal) exit expected — not an error to tail."""
        state = self._capture.get(workload_id)
        if state is not None:
            state["reported"] = True

    def _signal_group(self, h: WorkloadHandle, sig: signal.Signals) -> None:
        """Signal the start_new_session process group, including children after leader exit."""
        self._suppress_report(h.id)
        try:
            os.killpg(h._impl.pid, sig)  # type: ignore[attr-defined]
        except ProcessLookupError:
            pass

    def terminate(self, h: WorkloadHandle) -> None:
        self._signal_group(h, signal.SIGTERM)

    def kill(self, h: WorkloadHandle) -> None:
        self._signal_group(h, signal.SIGKILL)

    def cleanup(self, h: WorkloadHandle) -> None:
        self.kill(h)
        try:
            h._impl.wait(timeout=2)  # type: ignore[attr-defined]
        except Exception:
            pass
        self._capture.pop(h.id, None)
