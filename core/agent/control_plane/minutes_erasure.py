"""Agent-owned, fail-closed erasure of Minutes-derived meeting state.

The Minutes service owns the source meeting and calls this seam only after owner resolution.  Agent
still owns every mutation here: its Redis tombstone, runtime workload, workspace derivatives and
Brain provenance rows.  The HTTP layer projects this module's content-free receipt verbatim.
"""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time
from typing import Callable, Optional, Protocol

import yaml

from shared.adapters import workspace_write_lock
from shared.gitenv import scrubbed_git_env
from shared.meeting_retention import PROCESSED_SCOPE, retention_fence_key


_COUNT_FIELDS = ("unit_streams", "workspace_documents", "brain_records")
_TERMINAL_WORKLOAD_STATES = frozenset({"absent", "completed", "failed", "stopped"})
_MAX_OWNER_WORKSPACES = 256
_MAX_WORKSPACE_SCAN_FILES = 20_000
_MAX_PROVENANCE_FILE_BYTES = 1024 * 1024
_PROVENANCE_SUFFIXES = frozenset({".md", ".json"})
_SCAN_SKIP_DIRECTORIES = frozenset({".git", ".claude", "node_modules"})
_MARKDOWN_FRONTMATTER = re.compile(r"\A---\r?\n(.*?)\r?\n---(?:\r?\n|\Z)", re.DOTALL)


class ErasurePending(RuntimeError):
    """A durable tombstone exists, but one or more stores could not yet prove a complete purge."""


class ErasureNotFound(RuntimeError):
    """The row was already bound to another owner; expose the same surface as an absent row."""


@dataclass(frozen=True)
class ErasureBegin:
    owner_matches: bool
    completed: Optional[dict]
    unit_streams: int


class ErasureState(Protocol):
    def begin(self, *, user_id: int, meeting_id: str) -> ErasureBegin: ...

    def drain_processing(self, *, user_id: int, meeting_id: str) -> None: ...

    def remember_count(
        self, *, meeting_id: str, user_id: int, field: str, observed: int
    ) -> int: ...

    def purge_carriers(self, *, meeting_id: str) -> None: ...

    def complete(self, *, meeting_id: str, user_id: int, counts: dict) -> dict: ...


class BrainMeetingEraser(Protocol):
    """Permanently tombstone and purge one meeting in a de-duplicated Brain transaction.

    The governed Minutes writer must check this meeting tombstone plus the owner account tombstone in
    the same Brain store and insertion transaction. A Redis owner binding alone cannot close the race
    between registration and the later Brain write.
    """

    def erase_meeting(
        self,
        *,
        user_id: int,
        meeting_id: str,
        write_origin: str,
        source_spoke: str,
        source_item_ids: tuple[str, ...],
        idempotency_key: str,
    ) -> int: ...


def _redis_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _state_key(meeting_id: str) -> str:
    return f"zaki:agent:minutes-erasure:{meeting_id}"


def _processing_key(user_id: int) -> str:
    return f"zaki:agent:minutes-processing:{user_id}"


def _processing_state(value: dict, *, user_id: int) -> dict[str, str] | None:
    if not value:
        return None
    try:
        stored = {
            _redis_text(key) or "": _redis_text(item) or ""
            for key, item in value.items()
        }
    except Exception:
        raise ErasurePending("Minutes processing claim is invalid") from None
    if (
        set(stored) != {"token", "user_id", "state", "meeting_id"}
        or not stored["token"]
        or stored["user_id"] != str(user_id)
        or stored["state"] not in {"active", "cancelled"}
        or (
            stored["meeting_id"] != ""
            and (
                not stored["meeting_id"].isdigit()
                or stored["meeting_id"].startswith("0")
                or int(stored["meeting_id"]) > 2**63 - 1
            )
        )
    ):
        raise ErasurePending("Minutes processing claim is invalid")
    return stored


def _carrier_keys(meeting_id: str) -> tuple[str, str, str, str, str]:
    return (
        f"unit:agent-meet-{meeting_id}:out",
        f"unit:agent-meet-{meeting_id}:in",
        f"proc:meeting:{meeting_id}",
        f"proc:meeting:{meeting_id}:on",
        f"proc:meeting:{meeting_id}:cursor",
    )


class RedisMinutesErasureState:
    """Durable Redis owner binding, processed tombstone and stable receipt census."""

    _WATCH_RETRIES = 8

    def __init__(
        self,
        redis_client,
        *,
        processing_drain_timeout: float = 5.0,
        processing_poll_interval: float = 0.01,
    ) -> None:
        if processing_drain_timeout <= 0 or processing_poll_interval <= 0:
            raise ValueError("Minutes processing drain policy is invalid")
        self._redis = redis_client
        self._processing_drain_timeout = processing_drain_timeout
        self._processing_poll_interval = processing_poll_interval

    def begin(self, *, user_id: int, meeting_id: str) -> ErasureBegin:
        from redis.exceptions import WatchError

        state_key = _state_key(meeting_id)
        fence_key = retention_fence_key(meeting_id)
        carriers = _carrier_keys(meeting_id)
        active_key = "active_meetings"
        processing_key = _processing_key(user_id)
        for _attempt in range(self._WATCH_RETRIES):
            try:
                with self._redis.pipeline(transaction=True) as pipe:
                    pipe.watch(state_key, fence_key, *carriers, active_key, processing_key)
                    owner = _redis_text(pipe.hget(state_key, "user_id"))
                    if owner is not None and owner != str(user_id):
                        pipe.unwatch()
                        return ErasureBegin(owner_matches=False, completed=None, unit_streams=0)
                    stored_unit = _redis_text(pipe.hget(state_key, "unit_streams"))
                    unit_count = int(stored_unit) if stored_unit is not None else int(pipe.exists(carriers[0]))
                    state = _redis_text(pipe.hget(state_key, "state"))
                    completed = self._receipt_from_pipe(pipe, meeting_id) if state == "complete" else None
                    processing = _processing_state(
                        pipe.hgetall(processing_key),
                        user_id=user_id,
                    )
                    # The claim starts before the Minutes read discovers a meeting id. Conservatively
                    # cancel this owner's sole claim even when it is still unbound or names another
                    # row; otherwise a pre-read holder could outlive a completed meeting receipt.
                    cancel_processing = processing is not None

                    pipe.multi()
                    pending = {"state": state or "pending"}
                    if owner is None:
                        pending["user_id"] = str(user_id)
                    if stored_unit is None:
                        pending["unit_streams"] = str(unit_count)
                    pipe.hset(state_key, mapping=pending)
                    pipe.persist(state_key)
                    pipe.hset(fence_key, PROCESSED_SCOPE, "1")
                    pipe.persist(fence_key)
                    pipe.delete(*carriers)
                    pipe.srem(active_key, meeting_id)
                    if cancel_processing:
                        pipe.hset(processing_key, "state", "cancelled")
                        pipe.persist(processing_key)
                    pipe.execute()
                    return ErasureBegin(
                        owner_matches=True,
                        completed=completed,
                        unit_streams=unit_count,
                    )
            except WatchError:
                continue
            except (TypeError, ValueError):
                raise ErasurePending("durable erasure state is invalid") from None
            except ErasurePending:
                raise
            except Exception:
                # Redis client/pipeline errors may retain connection details or carrier keys.
                raise ErasurePending("durable erasure state is unavailable") from None
        raise ErasurePending("durable erasure state is busy")

    def drain_processing(self, *, user_id: int, meeting_id: str) -> None:
        """Wait for the cancelled holder to leave; timeout remains retryable and fail closed."""

        deadline = time.monotonic() + self._processing_drain_timeout
        processing_key = _processing_key(user_id)
        while True:
            try:
                processing = _processing_state(
                    self._redis.hgetall(processing_key),
                    user_id=user_id,
                )
            except ErasurePending:
                raise
            except Exception:
                raise ErasurePending("Minutes processing drain is unavailable") from None
            if processing is None:
                return
            if processing["state"] != "cancelled":
                raise ErasurePending("Minutes processing cancellation is unconfirmed")
            if time.monotonic() >= deadline:
                raise ErasurePending("Minutes processing drain is pending")
            time.sleep(self._processing_poll_interval)

    @staticmethod
    def _receipt_from_pipe(pipe, meeting_id: str) -> dict:
        deleted: dict[str, int] = {}
        for field in _COUNT_FIELDS:
            raw = _redis_text(pipe.hget(_state_key(meeting_id), field))
            if raw is None:
                raise ErasurePending("completed erasure receipt is incomplete")
            try:
                count = int(raw)
            except ValueError:
                raise ErasurePending("completed erasure receipt is invalid") from None
            if count < 0:
                raise ErasurePending("completed erasure receipt is invalid")
            deleted[field] = count
        return {"meeting_id": meeting_id, "tombstoned": True, "deleted": deleted}

    def remember_count(
        self, *, meeting_id: str, user_id: int, field: str, observed: int
    ) -> int:
        from redis.exceptions import WatchError

        if field not in _COUNT_FIELDS or type(observed) is not int or observed < 0:
            raise ErasurePending("erasure count is invalid")
        state_key = _state_key(meeting_id)
        for _attempt in range(self._WATCH_RETRIES):
            try:
                with self._redis.pipeline(transaction=True) as pipe:
                    pipe.watch(state_key)
                    if _redis_text(pipe.hget(state_key, "user_id")) != str(user_id):
                        pipe.unwatch()
                        raise ErasureNotFound
                    existing = _redis_text(pipe.hget(state_key, field))
                    if existing is not None:
                        pipe.unwatch()
                        try:
                            stable = int(existing)
                        except ValueError:
                            raise ErasurePending("durable erasure count is invalid") from None
                        if stable < 0:
                            raise ErasurePending("durable erasure count is invalid")
                        return stable
                    pipe.multi()
                    pipe.hsetnx(state_key, field, str(observed))
                    pipe.persist(state_key)
                    pipe.execute()
                    return observed
            except WatchError:
                continue
            except (ErasureNotFound, ErasurePending):
                raise
            except Exception:
                raise ErasurePending("durable erasure state is unavailable") from None
        raise ErasurePending("durable erasure state is busy")

    def purge_carriers(self, *, meeting_id: str) -> None:
        fence_key = retention_fence_key(meeting_id)
        try:
            with self._redis.pipeline(transaction=True) as pipe:
                pipe.hset(fence_key, PROCESSED_SCOPE, "1")
                pipe.persist(fence_key)
                pipe.delete(*_carrier_keys(meeting_id))
                pipe.srem("active_meetings", meeting_id)
                pipe.execute()
        except Exception:
            raise ErasurePending("durable erasure carriers are unavailable") from None

    def complete(self, *, meeting_id: str, user_id: int, counts: dict) -> dict:
        from redis.exceptions import WatchError

        if set(counts) != set(_COUNT_FIELDS) or any(
            type(counts.get(field)) is not int or counts[field] < 0 for field in _COUNT_FIELDS
        ):
            raise ErasurePending("erasure count is invalid")
        state_key = _state_key(meeting_id)
        fence_key = retention_fence_key(meeting_id)
        carriers = _carrier_keys(meeting_id)
        active_key = "active_meetings"
        processing_key = _processing_key(user_id)
        for _attempt in range(self._WATCH_RETRIES):
            try:
                with self._redis.pipeline(transaction=True) as pipe:
                    pipe.watch(state_key, fence_key, *carriers, active_key, processing_key)
                    if _redis_text(pipe.hget(state_key, "user_id")) != str(user_id):
                        pipe.unwatch()
                        raise ErasureNotFound
                    if (
                        _redis_text(pipe.hget(fence_key, PROCESSED_SCOPE)) != "1"
                        or int(pipe.exists(*carriers)) != 0
                        or bool(pipe.sismember(active_key, meeting_id))
                    ):
                        pipe.unwatch()
                        raise ErasurePending("Agent carriers are not empty")
                    processing = _processing_state(
                        pipe.hgetall(processing_key),
                        user_id=user_id,
                    )
                    if processing is not None:
                        pipe.unwatch()
                        raise ErasurePending("Minutes processing drain is pending")
                    for field in _COUNT_FIELDS:
                        if _redis_text(pipe.hget(state_key, field)) != str(counts[field]):
                            pipe.unwatch()
                            raise ErasurePending("durable erasure count changed")
                    pipe.multi()
                    pipe.hset(state_key, "state", "complete")
                    pipe.persist(state_key)
                    pipe.persist(fence_key)
                    pipe.execute()
                    return {"meeting_id": meeting_id, "tombstoned": True, "deleted": dict(counts)}
            except WatchError:
                continue
            except (ErasureNotFound, ErasurePending):
                raise
            except Exception:
                raise ErasurePending("durable erasure state is unavailable") from None
        raise ErasurePending("durable erasure state is busy")


def _real_directory_identity(root: Path, candidate: Path) -> tuple[int, int]:
    """Return a stable directory identity only for a direct, non-symlink child tree of ``root``."""

    try:
        info = candidate.stat(follow_symlinks=False)
    except FileNotFoundError:
        raise ErasurePending("workspace changed during erasure") from None
    except OSError:
        raise ErasurePending("workspace identity is unavailable") from None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ErasurePending("workspace identity is not containment-safe")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError:
        raise ErasurePending("workspace identity is unavailable") from None
    direct = candidate.absolute()
    if resolved != direct or resolved == root or root not in resolved.parents:
        raise ErasurePending("workspace identity is not containment-safe")
    return info.st_dev, info.st_ino


def _owner_workspace_dirs(root: Path, user_id: int) -> list[Path]:
    """Return only this owner's real private active and parked workspace directories.

    A tenant root is an authorization boundary, not a path hint.  A symlink at the active slot, the
    attached owner directory, or one of its workspace slots is therefore a retryable integrity failure;
    it is never followed to another sibling tenant.
    """

    root = root.resolve(strict=True)
    subject = str(user_id)
    candidates: list[Path] = []
    active = root / subject
    if active.exists() or active.is_symlink():
        _real_directory_identity(root, active)
        candidates.append(active)
    attached = root / ".attached" / subject
    if attached.exists() or attached.is_symlink():
        _real_directory_identity(root, attached)
        try:
            with os.scandir(attached) as entries:
                for index, entry in enumerate(entries, start=1):
                    if index > _MAX_OWNER_WORKSPACES:
                        raise ErasurePending("workspace census is too large")
                    if entry.is_symlink():
                        raise ErasurePending("workspace identity is not containment-safe")
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    candidate = attached / entry.name
                    _real_directory_identity(root, candidate)
                    candidates.append(candidate)
        except ErasurePending:
            raise
        except OSError:
            raise ErasurePending("workspace census is unavailable") from None
    if len(candidates) > _MAX_OWNER_WORKSPACES:
        raise ErasurePending("workspace census is too large")
    return sorted(candidates, key=lambda path: str(path))


def _workspace_targets(workspace: Path, meeting_id: str) -> tuple[Path, Path]:
    root = workspace.resolve()
    directory = root / "kg" / "entities" / "meeting"
    resolved_directory = directory.resolve(strict=False)
    if resolved_directory != root and root not in resolved_directory.parents:
        raise ErasurePending("workspace meeting directory is not containment-safe")
    return directory / f"{meeting_id}.md", directory / f"{meeting_id}.envelope.json"


def _provenance_metadata(path: Path, *, meeting_id: str) -> dict | None:
    """Read bounded structured provenance only when the target meeting tokens are present."""

    no_follow = getattr(os, "O_NOFOLLOW", None)
    if no_follow is None:
        raise ErasurePending("workspace provenance no-follow read is unavailable")
    descriptor: int | None = None
    try:
        descriptor = os.open(path, os.O_RDONLY | no_follow)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ErasurePending("workspace provenance artifact is not a regular file")
        if info.st_size > _MAX_PROVENANCE_FILE_BYTES:
            raise ErasurePending("workspace provenance artifact is too large")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, _MAX_PROVENANCE_FILE_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > _MAX_PROVENANCE_FILE_BYTES:
                raise ErasurePending("workspace provenance artifact is too large")
        raw = b"".join(chunks)
    except ErasurePending:
        raise
    except OSError:
        raise ErasurePending("workspace provenance census is unavailable") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
    needles = (
        f"meeting:{meeting_id}".encode(),
        f"transcript:{meeting_id}".encode(),
        f"summary:{meeting_id}".encode(),
    )
    if not any(needle in raw for needle in needles):
        return None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise ErasurePending("workspace provenance artifact is invalid") from None
    try:
        if path.suffix.lower() == ".json":
            value = json.loads(text)
        else:
            match = _MARKDOWN_FRONTMATTER.match(text)
            value = yaml.safe_load(match.group(1)) if match else None
    except (TypeError, ValueError, yaml.YAMLError):
        raise ErasurePending("workspace provenance artifact is invalid") from None
    if not isinstance(value, dict):
        return None
    nested = value.get("provenance")
    if isinstance(nested, dict):
        value = {**value, **nested}
    return value


def _matches_minutes_provenance(metadata: dict | None, meeting_id: str) -> bool:
    if not isinstance(metadata, dict):
        return False
    raw_items = metadata.get("source_item_ids", metadata.get("source_item_id"))
    if isinstance(raw_items, str):
        source_items = {raw_items}
    elif isinstance(raw_items, list) and all(isinstance(item, str) for item in raw_items):
        source_items = set(raw_items)
    else:
        source_items = set()
    return (
        metadata.get("write_origin") == "meeting_ingest"
        and metadata.get("source_spoke") == "minutes"
        and metadata.get("meeting_id") == f"meeting:{meeting_id}"
        and bool(source_items & {f"transcript:{meeting_id}", f"summary:{meeting_id}"})
    )


def _workspace_provenance_targets(workspace: Path, meeting_id: str) -> list[Path]:
    targets = set(_workspace_targets(workspace, meeting_id))
    scanned = 0
    try:
        for directory, names, files in os.walk(workspace, topdown=True, followlinks=False):
            base = Path(directory)
            kept: list[str] = []
            for name in names:
                child = base / name
                if name in _SCAN_SKIP_DIRECTORIES:
                    continue
                if child.is_symlink():
                    raise ErasurePending("workspace provenance census contains a symlink")
                kept.append(name)
            names[:] = kept
            for name in files:
                path = base / name
                if path.suffix.lower() not in _PROVENANCE_SUFFIXES:
                    continue
                scanned += 1
                if scanned > _MAX_WORKSPACE_SCAN_FILES:
                    raise ErasurePending("workspace provenance census is too large")
                if path.is_symlink():
                    raise ErasurePending("workspace provenance census contains a symlink")
                if _matches_minutes_provenance(
                    _provenance_metadata(path, meeting_id=meeting_id), meeting_id,
                ):
                    targets.add(path)
    except ErasurePending:
        raise
    except OSError:
        raise ErasurePending("workspace provenance census is unavailable") from None
    return sorted(targets, key=lambda path: str(path))


def _git_stdout(workspace: Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=str(workspace),
            env=scrubbed_git_env(),
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise ErasurePending("workspace history audit is unavailable") from None
    if result.returncode != 0:
        raise ErasurePending("workspace history audit is unavailable")
    return result.stdout.strip()


def _prove_targets_absent_from_git(workspace: Path, targets: list[Path]) -> None:
    git_dir = workspace / ".git"
    if not git_dir.exists() and not git_dir.is_symlink():
        return
    if git_dir.is_symlink():
        raise ErasurePending("workspace history is not containment-safe")
    relative: list[str] = []
    root = workspace.resolve(strict=True)
    for target in targets:
        try:
            relative.append(str(target.relative_to(root)))
        except ValueError:
            raise ErasurePending("workspace history target is not containment-safe") from None
    # A configured remote is an independent durable history store.  This process has no governed
    # credential/force-rewrite contract, so it must not claim remote erasure merely from a local delete.
    if _git_stdout(workspace, "remote"):
        raise ErasurePending("workspace remote erasure is unconfirmed")
    if not relative:
        return
    if _git_stdout(workspace, "ls-files", "--stage", "--", *relative):
        raise ErasurePending("workspace index still contains meeting artifacts")
    if _git_stdout(workspace, "rev-list", "--all", "--reflog", "--objects", "--", *relative):
        raise ErasurePending("workspace history still contains meeting artifacts")


def _open_workspace_target_parent(
    workspace: Path, target: Path,
) -> tuple[list[int], str] | None:
    """Open every target parent with O_NOFOLLOW; return None only when a parent is absent."""

    root = workspace.absolute()
    try:
        relative = target.absolute().relative_to(root)
    except ValueError:
        raise ErasurePending("workspace target is not containment-safe") from None
    parts = relative.parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ErasurePending("workspace target is not containment-safe")
    no_follow = getattr(os, "O_NOFOLLOW", None)
    if no_follow is None:
        raise ErasurePending("workspace no-follow deletion is unavailable")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | no_follow
    opened: list[int] = []
    try:
        opened.append(os.open(root, flags))
        for part in parts[:-1]:
            opened.append(os.open(part, flags, dir_fd=opened[-1]))
    except FileNotFoundError:
        for descriptor in reversed(opened):
            os.close(descriptor)
        return None
    except OSError:
        for descriptor in reversed(opened):
            os.close(descriptor)
        raise ErasurePending("workspace target parent is not containment-safe") from None
    return opened, parts[-1]


def _close_descriptors(descriptors: list[int]) -> None:
    for descriptor in reversed(descriptors):
        os.close(descriptor)


def _workspace_target_exists(workspace: Path, target: Path) -> bool:
    opened = _open_workspace_target_parent(workspace, target)
    if opened is None:
        return False
    descriptors, name = opened
    try:
        try:
            os.stat(name, dir_fd=descriptors[-1], follow_symlinks=False)
        except FileNotFoundError:
            return False
        return True
    except OSError:
        raise ErasurePending("workspace target census is unavailable") from None
    finally:
        _close_descriptors(descriptors)


def _unlink_workspace_target(workspace: Path, target: Path) -> None:
    opened = _open_workspace_target_parent(workspace, target)
    if opened is None:
        return
    descriptors, name = opened
    try:
        try:
            os.unlink(name, dir_fd=descriptors[-1])
        except FileNotFoundError:
            return
        except OSError:
            raise ErasurePending("workspace purge is incomplete") from None
    finally:
        _close_descriptors(descriptors)


class AgentMinutesErasure:
    """Coordinate the Agent-owned half of one meeting's erasure receipt."""

    def __init__(
        self,
        *,
        state: ErasureState,
        workspaces_root: str | Path,
        stop_workload: Callable[[str], str],
        brain_eraser: BrainMeetingEraser,
        live_registry: object | None = None,
    ) -> None:
        self._state = state
        self._root = Path(workspaces_root)
        self._stop_workload = stop_workload
        self._brain = brain_eraser
        self._live = live_registry

    def bind_live_registry(self, live_registry: object) -> None:
        """Bind create_app's authoritative in-process registry when one was not injected explicitly."""

        if self._live is None:
            self._live = live_registry

    def erase(self, *, user_id: int, meeting_id: str) -> dict:
        try:
            return self._erase(user_id=user_id, meeting_id=meeting_id)
        except (ErasureNotFound, ErasurePending):
            raise
        except Exception:
            # Store, lock, filesystem, live-registry, and Brain adapters may retain PII in native
            # exception text. The internal API only exposes this content-free retry surface.
            raise ErasurePending("Agent Minutes erasure requires retry") from None

    def _erase(self, *, user_id: int, meeting_id: str) -> dict:
        begun = self._state.begin(user_id=user_id, meeting_id=meeting_id)
        if not begun.owner_matches:
            raise ErasureNotFound
        # ``begin`` installs the durable tombstone and cancellation first. Completion may be replayed
        # only after the holder has released, so a prior receipt never lets an external model outlive
        # this erasure response.
        self._state.drain_processing(user_id=user_id, meeting_id=meeting_id)

        if self._live is not None:
            erase_live = getattr(self._live, "erase", None)
            if erase_live is not None:
                erase_live(meeting_id)
        if begun.completed is not None:
            return begun.completed

        try:
            stopped = str(self._stop_workload(f"agent-meet-{meeting_id}") or "")
        except Exception:  # noqa: BLE001 - transport detail must not escape the internal edge
            raise ErasurePending("meeting workload stop is unconfirmed") from None
        if stopped not in _TERMINAL_WORKLOAD_STATES:
            raise ErasurePending("meeting workload stop is unconfirmed")
        self._state.purge_carriers(meeting_id=meeting_id)

        workspace_count = self._purge_workspace_documents(user_id=user_id, meeting_id=meeting_id)
        brain_count = self._brain.erase_meeting(
            user_id=user_id,
            meeting_id=f"meeting:{meeting_id}",
            write_origin="meeting_ingest",
            source_spoke="minutes",
            source_item_ids=(f"transcript:{meeting_id}", f"summary:{meeting_id}"),
            idempotency_key=f"minutes-erasure:v1:{user_id}:{meeting_id}",
        )
        if type(brain_count) is not int or brain_count < 0:
            raise ErasurePending("Brain purge did not return a valid count")
        brain_count = self._state.remember_count(
            meeting_id=meeting_id,
            user_id=user_id,
            field="brain_records",
            observed=brain_count,
        )

        self._state.purge_carriers(meeting_id=meeting_id)
        counts = {
            "unit_streams": begun.unit_streams,
            "workspace_documents": workspace_count,
            "brain_records": brain_count,
        }
        if any(type(counts[field]) is not int or counts[field] < 0 for field in _COUNT_FIELDS):
            raise ErasurePending("erasure count is invalid")
        return self._state.complete(meeting_id=meeting_id, user_id=user_id, counts=counts)

    def _purge_workspace_documents(self, *, user_id: int, meeting_id: str) -> int:
        root = self._root.resolve(strict=True)
        workspaces = _owner_workspace_dirs(root, user_id)
        identities = {workspace: _real_directory_identity(root, workspace) for workspace in workspaces}
        with ExitStack() as locks:
            for workspace in workspaces:
                locks.enter_context(workspace_write_lock(workspace))
            for workspace, identity in identities.items():
                if _real_directory_identity(root, workspace) != identity:
                    raise ErasurePending("workspace changed during erasure")
            by_workspace: dict[Path, list[Path]] = {}
            for workspace in workspaces:
                found = _workspace_provenance_targets(workspace, meeting_id)
                _prove_targets_absent_from_git(workspace, found)
                by_workspace[workspace] = found
            observed = sum(
                int(_workspace_target_exists(workspace, target))
                for workspace, found in by_workspace.items()
                for target in found
            )
            stable = self._state.remember_count(
                meeting_id=meeting_id,
                user_id=user_id,
                field="workspace_documents",
                observed=observed,
            )
            for workspace, found in by_workspace.items():
                for target in found:
                    _unlink_workspace_target(workspace, target)
            if any(
                _workspace_target_exists(workspace, target)
                for workspace, found in by_workspace.items()
                for target in found
            ):
                raise ErasurePending("workspace purge is incomplete")
            for workspace, identity in identities.items():
                if _real_directory_identity(root, workspace) != identity:
                    raise ErasurePending("workspace changed during erasure")
            return stable
