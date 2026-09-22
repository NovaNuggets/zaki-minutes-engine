"""The fakeredis-backed retry queue + the worker sweep.

Derived from the parent's `webhook_retry_worker.py`, reimplemented clean. Failed
deliveries are persisted to a Redis list (`webhook:retry_queue`); each entry carries its
own `next_retry_at` + `attempt`, and the exponential `BACKOFF_SCHEDULE`. `drain_retry_queue`
is ONE worker tick (the parent's `_process_queue` loop body) — the eval calls it directly
instead of running the background poll loop, so the test is deterministic (no sleeps).

The redis client is async (`redis.asyncio` / `fakeredis.aioredis`). The transport is
injected, same as `WebhookSink`, so the worker drains against the fake receiver too.
"""
from __future__ import annotations

import asyncio
import json
import secrets
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

from redis.exceptions import WatchError

from .delivery import build_headers

RETRY_QUEUE_KEY = "webhook:retry_queue"

# A dead-letter list for envelopes that exhaust the schedule or age out — so a
# permanently-failed delivery (e.g. a meeting.completed) is observable, not silently dropped.
DEAD_LETTER_KEY = "webhook:dead_letter"
DEAD_LETTER_MAX = 1000  # cap the DLQ length (keep the most recent N entries)
MEETING_RETRY_INDEX_PREFIX = "webhook:meeting:retry"
MEETING_DLQ_INDEX_PREFIX = "webhook:meeting:dead-letter"
MEETING_CANCEL_PREFIX = "webhook:meeting:cancelled"
MEETING_INFLIGHT_PREFIX = "webhook:meeting:inflight"
DELIVERY_CLAIM_WAIT_SECONDS = 30.0
DELIVERY_CLAIM_POLL_SECONDS = 0.01

# attempt -> delay until next retry (seconds). The parent's exact schedule.
BACKOFF_SCHEDULE = [60, 300, 1800, 7200]  # 1m, 5m, 30m, 2h

MAX_AGE_SECONDS = 86400  # 24h — drop entries older than this

Transport = Callable[[str, bytes, Dict[str, str]], Awaitable[Any]]


def _meeting_id(value: object) -> Optional[int]:
    if isinstance(value, bool):
        return None
    try:
        meeting_id = int(value)
    except (TypeError, ValueError):
        return None
    return meeting_id if 0 < meeting_id <= 9_223_372_036_854_775_807 else None


def _envelope_meeting_id(envelope: object) -> Optional[int]:
    data = envelope.get("data") if isinstance(envelope, dict) else None
    meeting = data.get("meeting") if isinstance(data, dict) else None
    return _meeting_id(meeting.get("id") if isinstance(meeting, dict) else None)


def _retry_index(meeting_id: int) -> str:
    return f"{MEETING_RETRY_INDEX_PREFIX}:{meeting_id}"


def _dlq_index(meeting_id: int) -> str:
    return f"{MEETING_DLQ_INDEX_PREFIX}:{meeting_id}"


def _cancel_key(meeting_id: int) -> str:
    return f"{MEETING_CANCEL_PREFIX}:{meeting_id}"


def _inflight_key(meeting_id: int) -> str:
    return f"{MEETING_INFLIGHT_PREFIX}:{meeting_id}"


async def claim_meeting_webhook_delivery(
    redis: Any,
    envelope: object,
    *,
    meeting_id: object = None,
) -> tuple[Optional[int], Optional[str]]:
    """Atomically claim one outbound POST behind the permanent meeting cancel fence.

    ``(None, None)`` means the payload is not meeting-scoped. ``(meeting_id, None)`` means erasure
    already cancelled delivery. A random claim contains no payload or routing data; erasure waits
    for every live claim to release before it can report completion.
    """

    resolved = _meeting_id(meeting_id) or _envelope_meeting_id(envelope)
    if resolved is None:
        return None, None
    cancel_key = _cancel_key(resolved)
    claim_id = secrets.token_urlsafe(24)
    while True:
        try:
            async with redis.pipeline(transaction=True) as pipe:
                await pipe.watch(cancel_key)
                if await pipe.exists(cancel_key):
                    await pipe.unwatch()
                    return resolved, None
                pipe.multi()
                pipe.sadd(_inflight_key(resolved), claim_id)
                await pipe.execute()
                return resolved, claim_id
        except WatchError:
            continue


async def release_meeting_webhook_delivery(
    redis: Any,
    meeting_id: Optional[int],
    claim_id: Optional[str],
) -> None:
    """Release a completed/cancelled claim; Redis removes an empty set automatically."""

    if meeting_id is not None and claim_id is not None:
        await redis.srem(_inflight_key(meeting_id), claim_id)


async def _requeue(redis: Any, key: str, raw: str, meeting_id: Optional[int]) -> None:
    await _append_indexed_if_not_cancelled(
        redis,
        list_key=key,
        raw=raw,
        meeting_id=meeting_id,
        index_key=_retry_index(meeting_id) if meeting_id is not None else None,
    )


async def _append_indexed_if_not_cancelled(
    redis: Any,
    *,
    list_key: str,
    raw: str,
    meeting_id: Optional[int],
    index_key: Optional[str],
) -> bool:
    """Atomically append a PII carrier and its erasure index behind the cancel fence."""

    if meeting_id is None:
        async with redis.pipeline(transaction=True) as pipe:
            pipe.rpush(list_key, raw)
            await pipe.execute()
        return True
    cancel_key = _cancel_key(meeting_id)
    while True:
        try:
            async with redis.pipeline(transaction=True) as pipe:
                await pipe.watch(cancel_key)
                if await pipe.exists(cancel_key):
                    await pipe.unwatch()
                    return False
                pipe.multi()
                pipe.rpush(list_key, raw)
                if index_key is not None:
                    pipe.sadd(index_key, raw)
                await pipe.execute()
                return True
        except WatchError:
            # Erasure changed the tombstone generation after our check. Re-read it before any
            # retry so a stale producer can never recreate a purged carrier.
            continue


class RetryQueue:
    """A thin async wrapper over the Redis list that holds failed deliveries."""

    def __init__(self, redis: Any, key: str = RETRY_QUEUE_KEY):
        self.redis = redis
        self.key = key

    async def enqueue(
        self,
        url: str,
        envelope: Dict[str, Any],
        webhook_secret: Optional[str] = None,
        label: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        now: Optional[float] = None,
    ) -> bool:
        ts = time.time() if now is None else now
        entry = {
            "url": url,
            "payload": envelope,
            "webhook_secret": webhook_secret,
            "label": label,
            "attempt": 0,
            "next_retry_at": ts + BACKOFF_SCHEDULE[0],  # first retry after the 1st backoff
            "created_at": ts,
        }
        meeting_id = _envelope_meeting_id(envelope)
        if meeting_id is not None:
            entry["meeting_id"] = meeting_id
        if metadata:
            entry["metadata"] = metadata
        raw = json.dumps(entry)
        return await _append_indexed_if_not_cancelled(
            self.redis,
            list_key=self.key,
            raw=raw,
            meeting_id=meeting_id,
            index_key=_retry_index(meeting_id) if meeting_id is not None else None,
        )

    async def depth(self) -> int:
        return await self.redis.llen(self.key)

    async def claim_delivery(
        self, envelope: object, *, meeting_id: object = None
    ) -> tuple[Optional[int], Optional[str]]:
        return await claim_meeting_webhook_delivery(
            self.redis,
            envelope,
            meeting_id=meeting_id,
        )

    async def release_delivery(
        self, meeting_id: Optional[int], claim_id: Optional[str]
    ) -> None:
        await release_meeting_webhook_delivery(self.redis, meeting_id, claim_id)


async def _deliver_one(entry: dict, transport: Transport) -> tuple[bool, Optional[int], Optional[str]]:
    """Attempt one queued delivery.

    Returns ``(success, status_code, error)``. ``success`` is True on a 2xx (or a
    permanent 4xx → stop retrying); the status_code/error are surfaced so a permanently
    failed entry can be dead-lettered with its last outcome.
    """
    url = entry["url"]
    envelope = entry["payload"]
    secret = entry.get("webhook_secret")
    payload_bytes = json.dumps(envelope).encode()
    ts = str(int(time.time()))
    headers = build_headers(secret, payload_bytes, timestamp=ts)
    try:
        resp = await transport(url, payload_bytes, headers)
        code = getattr(resp, "status_code", 0)
        if code < 300:
            return True, code, None
        if code >= 500 or code == 429:
            return False, code, f"HTTP {code}"  # transient — re-enqueue
        return True, code, f"HTTP {code}"  # 4xx (non-429) — permanent, drop (don't re-enqueue)
    except Exception as e:  # noqa: BLE001 — transport error is transient
        return False, None, str(e)


async def _dead_letter(
    redis: Any,
    entry: dict,
    *,
    reason: str,
    status_code: Optional[int] = None,
    error: Optional[str] = None,
    now: float,
    key: str = DEAD_LETTER_KEY,
) -> None:
    """Persist a permanently-failed envelope to the dead-letter list + log it.

    Without this an exhausted / aged-out webhook (e.g. a meeting.completed) would vanish
    with no operator visibility. The DLQ record carries the routing + last-failure metadata;
    the list is capped (LTRIM) so it can't grow unbounded.
    """
    record = {
        "url": entry.get("url"),
        "payload": entry.get("payload"),
        "label": entry.get("label", ""),
        "attempts": entry.get("attempt", 0),
        "reason": reason,
        "last_status_code": status_code,
        "last_error": error,
        "created_at": entry.get("created_at"),
        "dead_lettered_at": now,
    }
    meeting_id = _meeting_id(entry.get("meeting_id"))
    if meeting_id is not None:
        record["meeting_id"] = meeting_id
    if entry.get("metadata"):
        record["metadata"] = entry["metadata"]
    raw = json.dumps(record)
    persisted = await _append_indexed_if_not_cancelled(
        redis,
        list_key=key,
        raw=raw,
        meeting_id=meeting_id,
        index_key=_dlq_index(meeting_id) if meeting_id is not None else None,
    )
    if persisted:
        # Keep only the most recent records and remove each evicted record from its meeting index;
        # otherwise the index itself would become an uncapped duplicate PII carrier.
        while await redis.llen(key) > DEAD_LETTER_MAX:
            evicted = await redis.lpop(key)
            if evicted is None:
                break
            try:
                evicted_entry = json.loads(evicted)
            except (json.JSONDecodeError, TypeError):
                continue
            evicted_mid = _meeting_id(evicted_entry.get("meeting_id"))
            if evicted_mid is not None:
                await redis.srem(_dlq_index(evicted_mid), evicted)

    try:
        from ..obs import log_event
    except Exception:  # noqa: BLE001 — never let logging wiring break the drain
        log_event = None
    if log_event is not None and persisted:
        log_event(
            "webhook_dead_lettered", audience="system", level="warning",
            span="webhook.retry_drain",
            fields={
                "url": record["url"], "label": record["label"],
                "attempts": record["attempts"], "reason": reason,
                "last_status_code": status_code, "last_error": error,
                "created_at": record["created_at"],
            },
        )


async def drain_retry_queue(
    redis: Any,
    transport: Transport,
    *,
    now: Optional[float] = None,
    key: str = RETRY_QUEUE_KEY,
) -> int:
    """One worker sweep: process every READY entry once. Returns #processed.

    Entries not yet due (`next_retry_at > now`) are re-queued untouched. Entries past
    MAX_AGE, or that exhaust the schedule, are dead-lettered (not silently dropped).
    Failed-but-retryable entries get a bumped `attempt` + the next backoff and are
    re-queued. Pass `now` to drive the clock forward deterministically in the eval.

    Backoff is indexed by `attempt + 1`: `enqueue` already set the first wait to
    BACKOFF_SCHEDULE[0] (60s), so the drain schedules the *next* wait. The effective wait
    sequence a target experiences is therefore exactly the schedule (60, 300, 1800, 7200),
    and the total bounded HTTP attempts are 1 sync + len(BACKOFF_SCHEDULE) drain = 5.
    """
    clock = time.time() if now is None else now
    queue_len = await redis.llen(key)
    if queue_len == 0:
        return 0

    processed = 0
    for _ in range(queue_len):
        raw = await redis.lpop(key)
        if raw is None:
            break
        try:
            entry = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            processed += 1  # corrupt — drop
            continue

        meeting_id = _meeting_id(entry.get("meeting_id")) or _envelope_meeting_id(
            entry.get("payload")
        )
        if meeting_id is not None:
            await redis.srem(_retry_index(meeting_id), raw)
            if await redis.exists(_cancel_key(meeting_id)):
                processed += 1
                continue

        created_at = entry.get("created_at", 0)
        next_retry_at = entry.get("next_retry_at", 0)
        attempt = entry.get("attempt", 0)

        if clock - created_at > MAX_AGE_SECONDS:
            processed += 1  # expired — dead-letter (don't deliver)
            await _dead_letter(redis, entry, reason="max_age_exceeded", now=clock)
            continue

        if next_retry_at > clock:
            await _requeue(redis, key, raw, meeting_id)  # not due yet
            continue

        claim_meeting_id, claim_id = await claim_meeting_webhook_delivery(
            redis,
            entry.get("payload"),
            meeting_id=meeting_id,
        )
        if claim_meeting_id is not None and claim_id is None:
            processed += 1
            continue
        try:
            success, status_code, error = await _deliver_one(entry, transport)
        finally:
            await release_meeting_webhook_delivery(
                redis,
                claim_meeting_id,
                claim_id,
            )
        processed += 1

        if success:
            continue
        # The first wait (BACKOFF[0]) was already applied at enqueue, so the next wait is
        # BACKOFF[attempt + 1]. When that index runs off the end the schedule is exhausted.
        next_idx = attempt + 1
        if next_idx >= len(BACKOFF_SCHEDULE):
            # exhausted — dead-letter (permanently failed)
            await _dead_letter(
                redis, entry, reason="schedule_exhausted",
                status_code=status_code, error=error, now=clock,
            )
            continue
        entry["attempt"] = next_idx
        entry["next_retry_at"] = clock + BACKOFF_SCHEDULE[next_idx]
        await _requeue(redis, key, json.dumps(entry), meeting_id)

    return processed


async def _purge_indexed_list(
    redis: Any, *, index_key: str, list_key: str
) -> None:
    """Remove one meeting's exact indexed list members in bounded Redis batches."""

    while True:
        _cursor, members = await redis.sscan(index_key, cursor=0, count=100)
        if not members:
            break
        async with redis.pipeline(transaction=True) as pipe:
            for raw in members:
                pipe.lrem(list_key, 0, raw)
                pipe.srem(index_key, raw)
            await pipe.execute()
    if await redis.scard(index_key):
        raise RuntimeError("meeting webhook purge requires retry")
    await redis.delete(index_key)


async def purge_meeting_webhook_state(redis: Any, meeting_id: int) -> None:
    """Fence and remove one meeting's retry/DLQ copies without scanning foreign payloads."""

    meeting_id = _meeting_id(meeting_id)
    if meeting_id is None:
        raise ValueError("meeting webhook purge id is invalid")
    await redis.set(_cancel_key(meeting_id), "1")
    await _purge_indexed_list(
        redis,
        index_key=_retry_index(meeting_id),
        list_key=RETRY_QUEUE_KEY,
    )
    await _purge_indexed_list(
        redis,
        index_key=_dlq_index(meeting_id),
        list_key=DEAD_LETTER_KEY,
    )
    deadline = time.monotonic() + DELIVERY_CLAIM_WAIT_SECONDS
    while await redis.scard(_inflight_key(meeting_id)):
        if time.monotonic() >= deadline:
            raise RuntimeError("meeting webhook delivery drain requires retry")
        await asyncio.sleep(DELIVERY_CLAIM_POLL_SECONDS)
