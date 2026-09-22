"""Bounded Agent-owned account erasure for Minutes derivatives.

The Redis account hash is the permanent Agent-ingest gate. Registration and account begin serialize
with WATCH/MULTI so a meeting is either captured in the immutable snapshot or refused after that gate
exists. It is not a cross-store transaction: the Brain writer and erasers must additionally serialize
against permanent account/meeting tombstones inside the Brain store itself. Destructive workspace and
Brain ports must durably de-duplicate their supplied keys and replay their first verified count.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import re
import time
from typing import Protocol

from control_plane.minutes_erasure import ErasureNotFound, ErasurePending
from shared.meeting_retention import PROCESSED_SCOPE, retention_fence_key


MAX_ACCOUNT_MEETINGS = 10_000
MAX_DELETED_COUNT = 2_147_483_647
_WATCH_RETRIES = 8
_VERIFY_BATCH = 100
_COUNT_FIELDS = ("unit_streams", "workspace_documents", "brain_records")
_ROW_ID = re.compile(r"^[1-9][0-9]{0,18}$")
_MAX_DB_ID = 2**63 - 1


class MinutesAccountErasurePending(RuntimeError):
    """The permanent account tombstone exists, but complete erasure is not yet proven."""


class MinutesAccountErasureFenced(MinutesAccountErasurePending):
    """A Minutes write was refused because account erasure has begun."""


@dataclass(frozen=True)
class AccountErasureBegin:
    meeting_ids: tuple[str, ...]
    completed: dict | None


class MeetingEraser(Protocol):
    def erase(self, *, user_id: int, meeting_id: str) -> dict: ...


class AccountWorkspaceEraser(Protocol):
    """Purge and prove absence of all owner workspace artifacts with Minutes provenance.

    Implementations must durably de-duplicate ``idempotency_key`` before destructive work and fail
    closed on Git history/remotes, symlinks, scan bounds, or storage uncertainty.
    """

    def erase_account(
        self,
        *,
        user_id: int,
        write_origin: str,
        source_spoke: str,
        idempotency_key: str,
    ) -> int: ...


class AccountBrainEraser(Protocol):
    """Permanently tombstone, purge, and verify an owner in one Brain transaction.

    The governed Minutes writer must check this account tombstone and the corresponding meeting
    tombstone in that same store and transaction before inserting any derivative. Redis registration
    is only an early gate and must never be treated as the final Brain write authorization.
    """

    def erase_account(
        self,
        *,
        user_id: int,
        write_origin: str,
        source_spoke: str,
        idempotency_key: str,
    ) -> int: ...


def _account_key(user_id: int) -> str:
    return f"zaki:agent:minutes-account-erasure:{user_id}"


def _processing_key(user_id: int) -> str:
    return f"zaki:agent:minutes-processing:{user_id}"


def _owner_index_key(user_id: int) -> str:
    return f"zaki:agent:minutes-owner-index:{user_id}"


def _meeting_state_key(meeting_id: str) -> str:
    return f"zaki:agent:minutes-erasure:{meeting_id}"


def _carrier_keys(meeting_id: str) -> tuple[str, str, str, str, str]:
    return (
        f"unit:agent-meet-{meeting_id}:out",
        f"unit:agent-meet-{meeting_id}:in",
        f"proc:meeting:{meeting_id}",
        f"proc:meeting:{meeting_id}:on",
        f"proc:meeting:{meeting_id}:cursor",
    )


def _canonical_user(user_id: object) -> int:
    if type(user_id) is not int or not 0 < user_id <= _MAX_DB_ID:
        raise MinutesAccountErasurePending("account erasure identity is invalid")
    return user_id


def _canonical_meeting(meeting_id: object) -> str:
    value = str(meeting_id) if type(meeting_id) is int else meeting_id
    if (
        not isinstance(value, str)
        or not _ROW_ID.fullmatch(value)
        or int(value) > _MAX_DB_ID
    ):
        raise MinutesAccountErasurePending("account meeting identity is invalid")
    return value


def _redis_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _decoded_hash(value: dict) -> dict[str, str]:
    return {
        _redis_text(key) or "": _redis_text(item) or ""
        for key, item in value.items()
    }


def _processing_state(value: dict, *, user_id: int) -> dict[str, str] | None:
    if not value:
        return None
    try:
        stored = _decoded_hash(value)
    except Exception:
        raise MinutesAccountErasurePending("Minutes processing claim is invalid") from None
    if (
        set(stored) != {"token", "user_id", "state", "meeting_id"}
        or not stored["token"]
        or stored["user_id"] != str(user_id)
        or stored["state"] not in {"active", "cancelled"}
        or (
            stored["meeting_id"] != ""
            and (
                not _ROW_ID.fullmatch(stored["meeting_id"])
                or int(stored["meeting_id"]) > _MAX_DB_ID
            )
        )
    ):
        raise MinutesAccountErasurePending("Minutes processing claim is invalid")
    return stored


def _bounded_owner_snapshot(
    pipe,
    *,
    index_key: str,
    cardinality: int,
    maximum: int,
) -> tuple[list[str], str | None]:
    """Read at most the watched SCARD using bounded SSCAN pages.

    SSCAN is deliberately checked against the earlier cardinality. WATCH protects the subsequent
    transaction from a real concurrent mutation; the explicit equality and cursor checks also fail
    closed for incomplete, looping, or otherwise inconsistent client responses.
    """

    if cardinality == 0:
        return [], None
    meetings: set[str] = set()
    cursor = 0
    observed_cursors = {0}
    completed = False
    scan_count = max(1, min(cardinality, 256))
    for _scan in range(cardinality + 1):
        next_cursor, batch = pipe.sscan(index_key, cursor=cursor, count=scan_count)
        try:
            cursor_text = _redis_text(next_cursor)
            if cursor_text is None or not cursor_text.isdigit():
                return [], "index_changed"
            normalized_cursor = int(cursor_text)
            if not isinstance(batch, (list, tuple, set)):
                return [], "index_changed"
            for item in batch:
                meetings.add(_canonical_meeting(item))
                if len(meetings) > maximum:
                    return [], "census_too_large"
        except MinutesAccountErasurePending:
            return [], "index_invalid"
        if normalized_cursor == 0:
            completed = True
            break
        if normalized_cursor in observed_cursors:
            return [], "index_changed"
        observed_cursors.add(normalized_cursor)
        cursor = normalized_cursor
    if not completed or len(meetings) != cardinality:
        return [], "index_changed"
    return sorted(meetings, key=int), None


def _census_error_message(index_error: str) -> str:
    if index_error == "census_too_large":
        return "Minutes account census exceeds the configured bound"
    if index_error == "index_changed":
        return "Minutes account census changed during snapshot"
    return "Minutes account census is invalid"


def _parse_snapshot(raw: str, *, maximum: int) -> tuple[str, ...]:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        raise MinutesAccountErasurePending("account erasure snapshot is invalid") from None
    if not isinstance(value, list) or len(value) > maximum:
        raise MinutesAccountErasurePending("account erasure snapshot is invalid")
    normalized = tuple(_canonical_meeting(item) for item in value)
    if normalized != tuple(sorted(set(normalized), key=int)):
        raise MinutesAccountErasurePending("account erasure snapshot is invalid")
    return normalized


def _validate_receipt(value: object, *, user_id: int) -> dict:
    if not isinstance(value, dict) or set(value) != {"user_id", "tombstoned", "deleted"}:
        raise MinutesAccountErasurePending("account erasure receipt is invalid")
    if value.get("user_id") != str(user_id) or value.get("tombstoned") is not True:
        raise MinutesAccountErasurePending("account erasure receipt is invalid")
    deleted = value.get("deleted")
    if not isinstance(deleted, dict) or set(deleted) != set(_COUNT_FIELDS):
        raise MinutesAccountErasurePending("account erasure receipt is invalid")
    if any(type(deleted[field]) is not int or not 0 <= deleted[field] <= MAX_DELETED_COUNT
           for field in _COUNT_FIELDS):
        raise MinutesAccountErasurePending("account erasure receipt is invalid")
    return {
        "user_id": str(user_id),
        "tombstoned": True,
        "deleted": {field: deleted[field] for field in _COUNT_FIELDS},
    }


def _validate_completed_meeting_state(stored: dict[str, str], *, user_id: int) -> None:
    expected = {"user_id", "state", *_COUNT_FIELDS}
    if set(stored) not in (expected, expected | {"signed_receipt"}):
        raise MinutesAccountErasurePending("meeting erasure receipt is incomplete")
    if stored.get("user_id") != str(user_id) or stored.get("state") != "complete":
        raise MinutesAccountErasurePending("meeting erasure is incomplete")
    try:
        counts = {field: int(stored[field]) for field in _COUNT_FIELDS}
    except (TypeError, ValueError):
        raise MinutesAccountErasurePending("meeting erasure receipt is invalid") from None
    if any(
        str(counts[field]) != stored[field] or not 0 <= counts[field] <= MAX_DELETED_COUNT
        for field in _COUNT_FIELDS
    ):
        raise MinutesAccountErasurePending("meeting erasure receipt is invalid")


class RedisMinutesAccountErasureState:
    """Permanent account tombstone, immutable owner-index snapshot, and stable receipt."""

    def __init__(
        self,
        redis_client,
        *,
        max_meetings: int = MAX_ACCOUNT_MEETINGS,
        processing_drain_timeout: float = 5.0,
        processing_poll_interval: float = 0.01,
    ) -> None:
        if type(max_meetings) is not int or max_meetings < 0 or max_meetings > MAX_ACCOUNT_MEETINGS:
            raise ValueError("account erasure census bound is invalid")
        if processing_drain_timeout <= 0 or processing_poll_interval <= 0:
            raise ValueError("Minutes processing drain policy is invalid")
        self._redis = redis_client
        self._maximum = max_meetings
        self._processing_drain_timeout = processing_drain_timeout
        self._processing_poll_interval = processing_poll_interval

    def register(self, *, user_id: int, meeting_id: str | int) -> None:
        """Register one owner row, or refuse it atomically once any account tombstone exists."""
        uid = _canonical_user(user_id)
        mid = _canonical_meeting(meeting_id)
        from control_plane.minutes_ownership import (
            MinutesOwnershipDenied,
            RedisMinutesOwnershipRegistry,
        )

        try:
            RedisMinutesOwnershipRegistry(self._redis).register_before_write(
                user_id=uid,
                meeting_id=f"meeting:{mid}",
            )
        except MinutesOwnershipDenied:
            try:
                tombstoned = bool(self._redis.exists(_account_key(uid)))
            except Exception:
                tombstoned = False
            if tombstoned:
                raise MinutesAccountErasureFenced(
                    "Minutes account erasure has begun"
                ) from None
            raise MinutesAccountErasurePending(
                "Minutes owner registration is unavailable"
            ) from None

    def begin(self, *, user_id: int) -> AccountErasureBegin:
        """Install the tombstone and snapshot+clear the bounded owner index in one transaction."""

        from redis.exceptions import WatchError

        uid = _canonical_user(user_id)
        account_key = _account_key(uid)
        index_key = _owner_index_key(uid)
        processing_key = _processing_key(uid)
        for _attempt in range(_WATCH_RETRIES):
            try:
                with self._redis.pipeline(transaction=True) as pipe:
                    pipe.watch(account_key, index_key, processing_key)
                    processing = _processing_state(
                        pipe.hgetall(processing_key),
                        user_id=uid,
                    )
                    if bool(pipe.exists(account_key)):
                        stored = _decoded_hash(pipe.hgetall(account_key))
                        begun = self._stored_begin(uid, stored)
                        if int(pipe.scard(index_key)) != 0:
                            pipe.unwatch()
                            raise MinutesAccountErasurePending(
                                "Minutes owner index changed after account erasure began"
                            )
                        pipe.multi()
                        pipe.persist(account_key)
                        pipe.delete(index_key)
                        if processing is not None:
                            pipe.hset(processing_key, "state", "cancelled")
                            pipe.persist(processing_key)
                        pipe.execute()
                        return begun

                    cardinality = int(pipe.scard(index_key))
                    if cardinality < 0 or cardinality > self._maximum:
                        index_error = "census_too_large"
                    else:
                        members, index_error = _bounded_owner_snapshot(
                            pipe,
                            index_key=index_key,
                            cardinality=cardinality,
                            maximum=self._maximum,
                        )
                    if index_error is not None:
                        pipe.multi()
                        pipe.hset(account_key, mapping={
                            "user_id": str(uid),
                            "state": "pending",
                            "error": index_error,
                        })
                        pipe.persist(account_key)
                        pipe.persist(index_key)
                        if processing is not None:
                            pipe.hset(processing_key, "state", "cancelled")
                            pipe.persist(processing_key)
                        pipe.execute()
                        raise MinutesAccountErasurePending(
                            _census_error_message(index_error)
                        )
                    snapshot = json.dumps(members, separators=(",", ":"))
                    pipe.multi()
                    pipe.hset(account_key, mapping={
                        "user_id": str(uid), "state": "pending", "snapshot": snapshot,
                    })
                    pipe.persist(account_key)
                    pipe.delete(index_key)
                    if processing is not None:
                        pipe.hset(processing_key, "state", "cancelled")
                        pipe.persist(processing_key)
                    pipe.execute()
                    return AccountErasureBegin(meeting_ids=tuple(members), completed=None)
            except WatchError:
                continue
            except MinutesAccountErasurePending:
                raise
            except Exception:
                raise MinutesAccountErasurePending(
                    "Minutes account erasure state is unavailable"
                ) from None
        raise MinutesAccountErasurePending("Minutes account erasure state is busy")

    def drain_processing(self, *, user_id: int) -> None:
        """Wait for the account claim holder to observe cancellation and release."""

        uid = _canonical_user(user_id)
        processing_key = _processing_key(uid)
        deadline = time.monotonic() + self._processing_drain_timeout
        while True:
            try:
                processing = _processing_state(
                    self._redis.hgetall(processing_key),
                    user_id=uid,
                )
            except MinutesAccountErasurePending:
                raise
            except Exception:
                raise MinutesAccountErasurePending(
                    "Minutes processing drain is unavailable"
                ) from None
            if processing is None:
                return
            if processing["state"] != "cancelled":
                raise MinutesAccountErasurePending(
                    "Minutes processing cancellation is unconfirmed"
                )
            if time.monotonic() >= deadline:
                raise MinutesAccountErasurePending("Minutes processing drain is pending")
            time.sleep(self._processing_poll_interval)

    def _stored_begin(self, user_id: int, stored: dict[str, str]) -> AccountErasureBegin:
        if stored.get("user_id") != str(user_id):
            raise MinutesAccountErasurePending("account erasure owner is invalid")
        state = stored.get("state")
        if state == "pending" and set(stored) == {"user_id", "state", "error"}:
            raise MinutesAccountErasurePending("account erasure census is invalid")
        if state == "pending" and set(stored) == {"user_id", "state", "snapshot"}:
            return AccountErasureBegin(
                meeting_ids=_parse_snapshot(stored["snapshot"], maximum=self._maximum),
                completed=None,
            )
        complete_fields = {"user_id", "state", "snapshot", "receipt"}
        if state == "complete" and set(stored) in (
            complete_fields,
            complete_fields | {"signed_receipt"},
        ):
            meetings = _parse_snapshot(stored["snapshot"], maximum=self._maximum)
            try:
                receipt = json.loads(stored["receipt"])
            except json.JSONDecodeError:
                raise MinutesAccountErasurePending("account erasure receipt is invalid") from None
            return AccountErasureBegin(
                meeting_ids=meetings,
                completed=_validate_receipt(receipt, user_id=user_id),
            )
        raise MinutesAccountErasurePending("account erasure state is invalid")

    def complete(
        self, *, user_id: int, meeting_ids: tuple[str, ...], counts: dict[str, int]
    ) -> dict:
        """Verify all snapshotted row fences/carriers, then persist the canonical receipt once."""

        from redis.exceptions import WatchError

        uid = _canonical_user(user_id)
        normalized_meetings = tuple(_canonical_meeting(item) for item in meeting_ids)
        if normalized_meetings != tuple(sorted(set(normalized_meetings), key=int)):
            raise MinutesAccountErasurePending("account erasure snapshot is invalid")
        receipt = _validate_receipt(
            {"user_id": str(uid), "tombstoned": True, "deleted": counts}, user_id=uid,
        )
        account_key = _account_key(uid)
        index_key = _owner_index_key(uid)
        processing_key = _processing_key(uid)

        # The Redis tombstone serializes owner registration. Brain writes are independently serialized
        # by the Brain eraser/writer same-store contract above. Every indexed meeting must also have its
        # own completed Redis owner fence before this bounded verification can complete.
        try:
            if int(self._redis.scard(index_key)) != 0:
                raise MinutesAccountErasurePending("Minutes owner index is not empty")
            if _processing_state(self._redis.hgetall(processing_key), user_id=uid) is not None:
                raise MinutesAccountErasurePending("Minutes processing drain is pending")
            for start in range(0, len(normalized_meetings), _VERIFY_BATCH):
                batch = normalized_meetings[start:start + _VERIFY_BATCH]
                with self._redis.pipeline(transaction=False) as pipe:
                    for meeting_id in batch:
                        pipe.hgetall(_meeting_state_key(meeting_id))
                        pipe.exists(*_carrier_keys(meeting_id))
                        pipe.sismember("active_meetings", meeting_id)
                        pipe.hget(retention_fence_key(meeting_id), PROCESSED_SCOPE)
                    results = pipe.execute()
                for offset, meeting_id in enumerate(batch):
                    stored = _decoded_hash(results[offset * 4])
                    carriers = int(results[offset * 4 + 1])
                    active = bool(results[offset * 4 + 2])
                    processed_fence = _redis_text(results[offset * 4 + 3])
                    if stored.get("user_id") != str(uid):
                        raise MinutesAccountErasurePending("meeting erasure owner is invalid")
                    _validate_completed_meeting_state(stored, user_id=uid)
                    if processed_fence != "1":
                        raise MinutesAccountErasurePending(
                            "meeting erasure tombstone is incomplete"
                        )
                    if carriers or active:
                        raise MinutesAccountErasurePending("meeting erasure is incomplete")
        except MinutesAccountErasurePending:
            raise
        except Exception:
            raise MinutesAccountErasurePending(
                "Minutes account verification is unavailable"
            ) from None

        snapshot = json.dumps(list(normalized_meetings), separators=(",", ":"))
        encoded_receipt = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
        for _attempt in range(_WATCH_RETRIES):
            try:
                with self._redis.pipeline(transaction=True) as pipe:
                    pipe.watch(account_key, index_key, processing_key)
                    begun = self._stored_begin(uid, _decoded_hash(pipe.hgetall(account_key)))
                    if begun.completed is not None:
                        pipe.unwatch()
                        return begun.completed
                    if begun.meeting_ids != normalized_meetings or int(pipe.scard(index_key)) != 0:
                        pipe.unwatch()
                        raise MinutesAccountErasurePending("account erasure snapshot changed")
                    if _processing_state(pipe.hgetall(processing_key), user_id=uid) is not None:
                        pipe.unwatch()
                        raise MinutesAccountErasurePending("Minutes processing drain is pending")
                    pipe.multi()
                    pipe.hset(account_key, mapping={
                        "state": "complete", "receipt": encoded_receipt,
                    })
                    pipe.persist(account_key)
                    pipe.delete(index_key)
                    pipe.execute()
                    return receipt
            except WatchError:
                continue
            except MinutesAccountErasurePending:
                raise
            except Exception:
                raise MinutesAccountErasurePending(
                    "Minutes account completion is unavailable"
                ) from None
        raise MinutesAccountErasurePending("Minutes account completion is busy")


def _meeting_counts(receipt: object, meeting_id: str) -> dict[str, int]:
    if not isinstance(receipt, dict) or set(receipt) != {"meeting_id", "tombstoned", "deleted"}:
        raise MinutesAccountErasurePending("meeting erasure receipt is invalid")
    if receipt.get("meeting_id") != meeting_id or receipt.get("tombstoned") is not True:
        raise MinutesAccountErasurePending("meeting erasure receipt is invalid")
    deleted = receipt.get("deleted")
    if not isinstance(deleted, dict) or set(deleted) != set(_COUNT_FIELDS):
        raise MinutesAccountErasurePending("meeting erasure receipt is invalid")
    if any(type(deleted[field]) is not int or not 0 <= deleted[field] <= MAX_DELETED_COUNT
           for field in _COUNT_FIELDS):
        raise MinutesAccountErasurePending("meeting erasure receipt is invalid")
    return {field: deleted[field] for field in _COUNT_FIELDS}


def _add_counts(total: dict[str, int], observed: dict[str, int]) -> None:
    for field in _COUNT_FIELDS:
        value = total[field] + observed[field]
        if value > MAX_DELETED_COUNT:
            raise MinutesAccountErasurePending("account erasure count exceeds the receipt bound")
        total[field] = value


class AgentMinutesAccountErasure:
    """Orchestrate row erasure plus account-wide orphan residue purge and stable completion."""

    def __init__(
        self,
        *,
        state: RedisMinutesAccountErasureState,
        meeting_eraser: MeetingEraser,
        workspace_eraser: AccountWorkspaceEraser,
        brain_eraser: AccountBrainEraser,
    ) -> None:
        self._state = state
        self._meetings = meeting_eraser
        self._workspace = workspace_eraser
        self._brain = brain_eraser

    def erase(self, *, user_id: int) -> dict:
        try:
            return self._erase(user_id=user_id)
        except MinutesAccountErasurePending:
            raise
        except Exception:
            # Adapter failures may retain owner ids, Redis keys, or Brain storage details.
            raise MinutesAccountErasurePending(
                "Minutes account erasure requires retry"
            ) from None

    def _erase(self, *, user_id: int) -> dict:
        uid = _canonical_user(user_id)
        begun = self._state.begin(user_id=uid)
        # The permanent account tombstone and cancellation are committed by ``begin``. Never replay
        # or mint a completed receipt until the in-flight read/model/quarantine holder has left.
        self._state.drain_processing(user_id=uid)
        if begun.completed is not None:
            return begun.completed
        counts = {field: 0 for field in _COUNT_FIELDS}
        try:
            for meeting_id in begun.meeting_ids:
                receipt = self._meetings.erase(user_id=uid, meeting_id=meeting_id)
                _add_counts(counts, _meeting_counts(receipt, meeting_id))
            workspace_count = self._workspace.erase_account(
                user_id=uid,
                write_origin="meeting_ingest",
                source_spoke="minutes",
                idempotency_key=f"minutes-erasure:v1:account:{uid}:workspace",
            )
            brain_count = self._brain.erase_account(
                user_id=uid,
                write_origin="meeting_ingest",
                source_spoke="minutes",
                idempotency_key=f"minutes-erasure:v1:account:{uid}:brain",
            )
        except (MinutesAccountErasurePending, ErasurePending, ErasureNotFound):
            raise MinutesAccountErasurePending("Minutes account erasure requires retry") from None
        except Exception:
            raise MinutesAccountErasurePending(
                "Minutes account erasure requires retry"
            ) from None
        residue = {
            "unit_streams": 0,
            "workspace_documents": workspace_count,
            "brain_records": brain_count,
        }
        if any(type(residue[field]) is not int or not 0 <= residue[field] <= MAX_DELETED_COUNT
               for field in _COUNT_FIELDS):
            raise MinutesAccountErasurePending("account residue purge returned an invalid count")
        _add_counts(counts, residue)
        return self._state.complete(
            user_id=uid, meeting_ids=begun.meeting_ids, counts=counts,
        )
