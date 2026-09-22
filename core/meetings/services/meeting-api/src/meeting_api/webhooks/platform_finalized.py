"""Secret-free durable outbox for the operator-owned ``transcript.finalized`` edge.

Only routing metadata and the content-free platform envelope enter Redis while work is pending.
Successful work is reduced to a content-free completion tombstone so terminal-row recovery is
idempotent. The operator URL and signing secret stay in the injected process-local sink and are
resolved at delivery time.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import time
from typing import Any, Awaitable, Callable
from urllib.parse import urlsplit
from uuid import uuid4

import jsonschema
from redis.exceptions import WatchError
from referencing import Registry, Resource

from ..meeting_writes import TranscriptFinalizationOutcome
from .delivery import sign_payload


_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_PREFIX = "minutes:transcript-finalized:outbox"
_MAX_ATTEMPTS = 2_147_483_647
_BASE_ENTRY_FIELDS = frozenset(
    {"version", "meeting_id", "state", "attempts", "created_at"}
)
_MAX_BIGINT = 9_223_372_036_854_775_807


def _load_contract_schema() -> dict:
    rel = (
        Path("meetings")
        / "contracts"
        / "minutes-finalized.v1"
        / "minutes-finalized.schema.json"
    )
    for parent in Path(__file__).resolve().parents:
        candidate = parent / rel
        if candidate.is_file():
            return json.loads(candidate.read_text())
    raise FileNotFoundError(f"contract not found by path: {rel}")


_SCHEMA = _load_contract_schema()
_REGISTRY = Registry().with_resource(_SCHEMA["$id"], Resource.from_contents(_SCHEMA))


def _conforms(value: dict, shape: str) -> None:
    jsonschema.Draft202012Validator(
        {"$ref": f"{_SCHEMA['$id']}#/$defs/{shape}"}, registry=_REGISTRY
    ).validate(value)


def build_minutes_finalized_envelope(
    meeting_id: int,
    *,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Build the exact content-free ``minutes-finalized.v1`` platform event."""

    if (
        isinstance(meeting_id, bool)
        or not isinstance(meeting_id, int)
        or not 1 <= meeting_id <= _MAX_BIGINT
    ):
        raise ValueError(
            "transcript.finalized requires a positive PostgreSQL bigint meeting row"
        )
    timestamp = created_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    row_id = str(meeting_id)
    envelope = {
        "version": "minutes-finalized.v1",
        "event_id": f"evt_transcript_finalized_{row_id}",
        "event_type": "transcript.finalized",
        "created_at": timestamp,
        "data": {
            "meeting_id": row_id,
            "artifact": "transcript",
            "state": "finalized",
            "idempotency_key": f"minutes:meeting:{row_id}:transcript",
        },
    }
    _conforms(envelope, "Envelope")
    return envelope


@dataclass(frozen=True)
class FinalizedAttempt:
    meeting_id: int
    envelope: dict | None
    newly_finalized: bool
    delivered: bool
    stage: str


class PlatformDeliveryFailed(RuntimeError):
    """A content-free retry signal from the operator delivery boundary."""


class MinutesPlatformWebhookSink:
    """Process-local operator config and HMAC delivery for one platform endpoint."""

    def __init__(
        self,
        *,
        url: str,
        key_id: str,
        secret: str,
        transport: Callable[[str, bytes, dict[str, str]], Awaitable[Any]],
        now: Callable[[], float] = time.time,
    ):
        parsed = urlsplit(url) if isinstance(url, str) else None
        if (
            parsed is None
            or parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or bool(parsed.fragment)
            or not isinstance(key_id, str)
            or not _KEY_ID.fullmatch(key_id)
            or not isinstance(secret, str)
            or not secret
            or not callable(transport)
            or not callable(now)
        ):
            raise ValueError("Minutes platform finalized sink configuration is invalid")
        self.url = url
        self.key_id = key_id
        self._secret = secret
        self._transport = transport
        self._now = now

    async def deliver(self, envelope: dict) -> None:
        _conforms(envelope, "Envelope")
        body = json.dumps(
            envelope, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        timestamp = str(int(self._now()))
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Key-Id": self.key_id,
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": sign_payload(body, self._secret, timestamp),
        }
        _conforms(headers, "SignatureHeaders")
        try:
            response = await self._transport(self.url, body, headers)
            status = getattr(response, "status_code", 0)
        except Exception:
            raise PlatformDeliveryFailed("Minutes platform delivery requires retry") from None
        if not isinstance(status, int) or not 200 <= status < 300:
            raise PlatformDeliveryFailed("Minutes platform delivery requires retry")


class RedisTranscriptFinalizedOutbox:
    """Redis-backed finalization/delivery state containing no URL or signing secret."""

    def __init__(
        self,
        redis: Any,
        *,
        prefix: str = _PREFIX,
        now: Callable[[], float] = time.time,
        lock_seconds: int = 60,
    ):
        if redis is None or not isinstance(prefix, str) or not prefix or not callable(now):
            raise ValueError("Minutes finalized outbox configuration is invalid")
        if isinstance(lock_seconds, bool) or not isinstance(lock_seconds, int) or lock_seconds < 1:
            raise ValueError("Minutes finalized outbox lock is invalid")
        self._redis = redis
        self._prefix = prefix.rstrip(":")
        self._pending = f"{self._prefix}:pending"
        self._now = now
        self._lock_seconds = lock_seconds

    @staticmethod
    def _meeting_id(meeting_id: object) -> int:
        if (
            isinstance(meeting_id, bool)
            or not isinstance(meeting_id, int)
            or not 1 <= meeting_id <= _MAX_BIGINT
        ):
            raise ValueError(
                "Minutes finalized outbox requires a positive PostgreSQL bigint meeting row"
            )
        return meeting_id

    def _key(self, meeting_id: int) -> str:
        return f"{self._prefix}:{meeting_id}"

    def _lock_key(self, meeting_id: int) -> str:
        return f"{self._prefix}:lock:{meeting_id}"

    def _cancel_key(self, meeting_id: int) -> str:
        return f"{self._prefix}:cancelled:{meeting_id}"

    @staticmethod
    def _encode(entry: dict) -> str:
        return json.dumps(
            entry, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
        )

    async def enqueue(self, meeting_id: int) -> bool:
        meeting_id = self._meeting_id(meeting_id)
        key = self._key(meeting_id)
        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    if await pipe.exists(key) or await pipe.exists(
                        self._cancel_key(meeting_id)
                    ):
                        await pipe.unwatch()
                        return False
                    created_at = self._now()
                    if (
                        isinstance(created_at, bool)
                        or not isinstance(created_at, (int, float))
                        or not math.isfinite(created_at)
                    ):
                        await pipe.unwatch()
                        raise ValueError("Minutes finalized outbox clock is invalid")
                    entry = {
                        "version": "minutes-finalized-outbox.v1",
                        "meeting_id": meeting_id,
                        "state": "pending_finalize",
                        "attempts": 0,
                        "created_at": float(created_at),
                    }
                    pipe.multi()
                    pipe.set(key, self._encode(entry))
                    pipe.sadd(self._pending, str(meeting_id))
                    await pipe.execute()
                    return True
            except WatchError:
                # Another callback/recovery worker won the idempotent insert. Re-read its
                # generation rather than ever resetting completed or in-flight work.
                continue

    async def depth(self) -> int:
        return int(await self._redis.scard(self._pending))

    async def cancel(self, meeting_id: int) -> None:
        """Fence future delivery and compact pending work to a content-free tombstone."""

        meeting_id = self._meeting_id(meeting_id)
        key = self._key(meeting_id)
        cancel_key = self._cancel_key(meeting_id)
        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    raw = self._text(await pipe.get(key))
                    entry = None
                    if isinstance(raw, str):
                        try:
                            parsed = json.loads(raw)
                        except json.JSONDecodeError:
                            parsed = None
                        if self._validate_entry(parsed, meeting_id) is not None:
                            entry = parsed
                    if isinstance(entry, dict) and entry.get("state") == "delivered":
                        replacement = entry
                    else:
                        created_at = (
                            entry.get("created_at") if isinstance(entry, dict) else self._now()
                        )
                        if (
                            isinstance(created_at, bool)
                            or not isinstance(created_at, (int, float))
                            or not math.isfinite(created_at)
                        ):
                            await pipe.unwatch()
                            raise ValueError("Minutes finalized outbox clock is invalid")
                        replacement = {
                            "version": "minutes-finalized-outbox.v1",
                            "meeting_id": meeting_id,
                            "state": "cancelled",
                            "attempts": (
                                entry.get("attempts", 0) if isinstance(entry, dict) else 0
                            ),
                            "created_at": float(created_at),
                        }
                    pipe.multi()
                    pipe.set(cancel_key, "1")
                    pipe.set(key, self._encode(replacement))
                    pipe.srem(self._pending, str(meeting_id))
                    await pipe.execute()
                    return
            except WatchError:
                continue

    @staticmethod
    def _text(value: object) -> object:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value

    async def _transition(
        self,
        *,
        key: str,
        lock_key: str,
        token: str,
        expected_raw: str,
        replacement: dict,
        complete: bool = False,
    ) -> bool:
        """Commit a state transition only for this exact lock and entry generation."""

        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                await pipe.watch(lock_key, key)
                current_token = self._text(await pipe.get(lock_key))
                current_raw = self._text(await pipe.get(key))
                if current_token != token or current_raw != expected_raw:
                    await pipe.unwatch()
                    return False
                pipe.multi()
                pipe.set(key, self._encode(replacement))
                if complete:
                    pipe.srem(self._pending, str(replacement["meeting_id"]))
                await pipe.execute()
                return True
        except WatchError:
            return False

    async def _save_failure(
        self,
        *,
        key: str,
        lock_key: str,
        token: str,
        expected_raw: str,
        entry: dict,
        stage: str,
    ) -> bool:
        replacement = {
            **entry,
            "attempts": min(entry["attempts"] + 1, _MAX_ATTEMPTS),
            "last_failed_stage": stage,
        }
        return await self._transition(
            key=key,
            lock_key=lock_key,
            token=token,
            expected_raw=expected_raw,
            replacement=replacement,
        )

    @staticmethod
    def _validate_entry(entry: object, meeting_id: int) -> str | None:
        """Return the canonical creation timestamp for an exact durable entry."""

        if not isinstance(entry, dict):
            return None
        state = entry.get("state")
        expected_fields = set(_BASE_ENTRY_FIELDS)
        if state == "pending_delivery":
            expected_fields.add("envelope")
        elif state not in {"pending_finalize", "delivered", "cancelled"}:
            return None
        if "last_failed_stage" in entry:
            if state in {"delivered", "cancelled"}:
                return None
            expected_fields.add("last_failed_stage")
        if set(entry) != expected_fields:
            return None
        attempts = entry.get("attempts")
        created_at = entry.get("created_at")
        if (
            entry.get("version") != "minutes-finalized-outbox.v1"
            or entry.get("meeting_id") != meeting_id
            or type(attempts) is not int
            or not 0 <= attempts <= _MAX_ATTEMPTS
            or isinstance(created_at, bool)
            or not isinstance(created_at, (int, float))
            or not math.isfinite(created_at)
        ):
            return None
        if "last_failed_stage" in entry:
            failed_stage = entry["last_failed_stage"]
            if (
                failed_stage not in {"finalize", "delivery"}
                or (state == "pending_finalize" and failed_stage != "finalize")
                or (state == "pending_delivery" and failed_stage != "delivery")
            ):
                return None
        try:
            return datetime.fromtimestamp(
                float(created_at), tz=timezone.utc
            ).isoformat().replace("+00:00", "Z")
        except (OverflowError, OSError, ValueError):
            return None

    async def _release_lock(self, lock_key: str, token: str) -> None:
        """Delete only the lock generation this worker acquired.

        WATCH closes the expiry/takeover race between observing the token and deleting
        it.  A successor's token is never removed by the expired predecessor.
        """

        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                await pipe.watch(lock_key)
                current = await pipe.get(lock_key)
                if isinstance(current, bytes):
                    current = current.decode("utf-8", errors="replace")
                if current != token:
                    await pipe.unwatch()
                    return
                pipe.multi()
                pipe.delete(lock_key)
                await pipe.execute()
        except WatchError:
            return

    async def process(
        self,
        meeting_id: int,
        finalizer: Callable[[int], Awaitable[object]],
        sink: MinutesPlatformWebhookSink,
    ) -> FinalizedAttempt:
        meeting_id = self._meeting_id(meeting_id)
        if not callable(finalizer) or sink is None or not callable(getattr(sink, "deliver", None)):
            raise ValueError("Minutes finalized processing boundaries are invalid")
        token = uuid4().hex
        lock_key = self._lock_key(meeting_id)
        acquired = await self._redis.set(
            lock_key, token, nx=True, ex=self._lock_seconds
        )
        if not acquired:
            return FinalizedAttempt(meeting_id, None, False, False, "busy")
        key = self._key(meeting_id)
        try:
            if await self._redis.exists(self._cancel_key(meeting_id)):
                await self.cancel(meeting_id)
                return FinalizedAttempt(
                    meeting_id, None, False, True, "cancelled"
                )
            raw = await self._redis.get(key)
            if raw is None:
                await self._redis.srem(self._pending, str(meeting_id))
                return FinalizedAttempt(meeting_id, None, False, True, "complete")
            raw = self._text(raw)
            if not isinstance(raw, str):
                return FinalizedAttempt(meeting_id, None, False, False, "invalid")
            try:
                entry = json.loads(raw)
            except (TypeError, UnicodeDecodeError, json.JSONDecodeError):
                return FinalizedAttempt(meeting_id, None, False, False, "invalid")
            expected_created_at = self._validate_entry(entry, meeting_id)
            if expected_created_at is None:
                return FinalizedAttempt(meeting_id, None, False, False, "invalid")

            newly_finalized = False
            envelope = entry.get("envelope")
            if entry["state"] in {"delivered", "cancelled"}:
                # A compact completion tombstone makes database backfill idempotent.  It carries
                # only the row id/counters/timestamp: never transcript or operator credentials.
                await self._redis.srem(self._pending, str(meeting_id))
                stage = "cancelled" if entry["state"] == "cancelled" else "complete"
                return FinalizedAttempt(meeting_id, None, False, True, stage)
            if entry["state"] == "pending_finalize":
                try:
                    outcome = await finalizer(meeting_id)
                except Exception:
                    await self._save_failure(
                        key=key,
                        lock_key=lock_key,
                        token=token,
                        expected_raw=raw,
                        entry=entry,
                        stage="finalize",
                    )
                    return FinalizedAttempt(meeting_id, None, False, False, "finalize")
                if (
                    isinstance(outcome, TranscriptFinalizationOutcome)
                    and outcome.state == "cancelled"
                ):
                    tombstone = {
                        "version": entry["version"],
                        "meeting_id": meeting_id,
                        "state": "cancelled",
                        "attempts": entry["attempts"],
                        "created_at": entry["created_at"],
                    }
                    committed = await self._transition(
                        key=key,
                        lock_key=lock_key,
                        token=token,
                        expected_raw=raw,
                        replacement=tombstone,
                        complete=True,
                    )
                    if not committed:
                        return FinalizedAttempt(
                            meeting_id, None, False, False, "busy"
                        )
                    return FinalizedAttempt(
                        meeting_id, None, False, True, "cancelled"
                    )
                envelope = build_minutes_finalized_envelope(
                    meeting_id, created_at=expected_created_at
                )
                entry = {
                    **entry,
                    "state": "pending_delivery",
                    "envelope": envelope,
                }
                entry.pop("last_failed_stage", None)
                committed = await self._transition(
                    key=key,
                    lock_key=lock_key,
                    token=token,
                    expected_raw=raw,
                    replacement=entry,
                )
                if not committed:
                    return FinalizedAttempt(
                        meeting_id, None, False, False, "busy"
                    )
                raw = self._encode(entry)
                newly_finalized = True
            if not isinstance(envelope, dict):
                return FinalizedAttempt(meeting_id, None, False, False, "invalid")
            expected_envelope = build_minutes_finalized_envelope(
                meeting_id, created_at=expected_created_at
            )
            if envelope != expected_envelope:
                return FinalizedAttempt(meeting_id, None, False, False, "invalid")
            if await self._redis.exists(self._cancel_key(meeting_id)):
                await self.cancel(meeting_id)
                return FinalizedAttempt(
                    meeting_id, None, False, True, "cancelled"
                )
            try:
                await sink.deliver(envelope)
            except Exception:
                await self._save_failure(
                    key=key,
                    lock_key=lock_key,
                    token=token,
                    expected_raw=raw,
                    entry=entry,
                    stage="delivery",
                )
                return FinalizedAttempt(
                    meeting_id, envelope, newly_finalized, False, "delivery"
                )
            tombstone = {
                "version": entry["version"],
                "meeting_id": meeting_id,
                "state": "delivered",
                "attempts": entry["attempts"],
                "created_at": entry["created_at"],
            }
            committed = await self._transition(
                key=key,
                lock_key=lock_key,
                token=token,
                expected_raw=raw,
                replacement=tombstone,
                complete=True,
            )
            if not committed:
                return FinalizedAttempt(
                    meeting_id, envelope, newly_finalized, False, "busy"
                )
            return FinalizedAttempt(
                meeting_id, envelope, newly_finalized, True, "complete"
            )
        finally:
            try:
                await self._release_lock(lock_key, token)
            except Exception:
                # The bounded lock expires by itself. Cleanup failure must not overwrite
                # the useful retry result or risk deleting a successor's generation.
                pass

    async def drain(
        self,
        finalizer: Callable[[int], Awaitable[object]],
        sink: MinutesPlatformWebhookSink,
        *,
        limit: int = 100,
    ) -> list[FinalizedAttempt]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("Minutes finalized drain limit is invalid")
        # Redis COUNT is the hard upper bound here; never materialize an unbounded
        # pending set just to process one worker tick.
        raw_ids = await self._redis.srandmember(self._pending, number=limit)
        if raw_ids is None:
            raw_ids = []
        elif isinstance(raw_ids, (bytes, str)):
            raw_ids = [raw_ids]
        meeting_ids = []
        invalid_ids = []
        for raw in raw_ids:
            original = raw
            if isinstance(raw, bytes):
                try:
                    raw = raw.decode("ascii")
                except UnicodeDecodeError:
                    invalid_ids.append(original)
                    continue
            if not isinstance(raw, str) or not re.fullmatch(r"[1-9][0-9]{0,18}", raw):
                invalid_ids.append(original)
                continue
            try:
                meeting_id = int(raw)
            except ValueError:
                invalid_ids.append(original)
                continue
            if meeting_id > _MAX_BIGINT:
                invalid_ids.append(original)
                continue
            meeting_ids.append(meeting_id)
        if invalid_ids:
            await self._redis.srem(self._pending, *invalid_ids)
        return [
            await self.process(meeting_id, finalizer, sink)
            for meeting_id in sorted(meeting_ids)
        ]
