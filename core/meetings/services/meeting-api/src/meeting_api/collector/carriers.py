"""Meeting-row Redis carrier identities and lifecycle erasure.

The numeric meetings row id is the only key component: native meeting ids are neither tenant- nor
run-unique. P23's semantic writers remain unchanged (meeting-api XADDs ``tc``; agent-worker XADDs
``proc``). This module performs lifecycle deletion only; it never creates or transforms content.
"""
from __future__ import annotations

import json

ACTIVE_MEETINGS_KEY = "active_meetings"
PROC_PENDING_KEY = "processed_pending"
TRANSCRIPTION_SOURCE_STREAM = "transcription_segments"
SOURCE_SCAN_PAGE_SIZE = 500
SOURCE_SCAN_ENTRY_LIMIT = 125_000
SOURCE_PURGE_MAX_PASSES = 3
PRIVATE_PURGE_MAX_PASSES = 3
CARRIER_FENCE_PREFIX = "zaki:retention:meeting"
CARRIER_FENCE_SCOPES = frozenset({"raw", "processed"})


def _meeting_id(value) -> int:
    if isinstance(value, bool):
        raise ValueError("meeting carrier id must be a positive integer")
    if isinstance(value, int):
        meeting_id = value
    elif isinstance(value, float) and value.is_integer():
        meeting_id = int(value)
    elif isinstance(value, str) and value.isascii() and value.isdigit():
        meeting_id = int(value)
    else:
        raise ValueError("meeting carrier id must be a positive integer") from None
    if meeting_id <= 0:
        raise ValueError("meeting carrier id must be a positive integer")
    return meeting_id


def transcript_stream_key(meeting_id) -> str:
    return f"tc:meeting:{_meeting_id(meeting_id)}"


def segments_hash_key(meeting_id) -> str:
    return f"meeting:{_meeting_id(meeting_id)}:segments"


def proc_stream_key(meeting_id) -> str:
    return f"proc:meeting:{_meeting_id(meeting_id)}"


def proc_on_key(meeting_id) -> str:
    return f"{proc_stream_key(meeting_id)}:on"


def proc_cursor_key(meeting_id) -> str:
    return f"{proc_stream_key(meeting_id)}:cursor"


def carrier_fence_key(meeting_id) -> str:
    """Permanent, content-free Redis authority for one numeric meeting row's carriers."""

    return f"{CARRIER_FENCE_PREFIX}:{_meeting_id(meeting_id)}:fence"


async def fence_meeting_redis_carriers(
    redis_client,
    meeting_id,
    *,
    raw: bool,
    processed: bool,
) -> None:
    """Monotonically and permanently fence selected carrier classes before deletion.

    ``HSET`` only turns a scope on; no lifecycle path clears it. ``PERSIST`` removes an accidental
    legacy TTL so a deleted database row cannot later lose its anti-resurrection tombstone.
    """

    if redis_client is None or not isinstance(raw, bool) or not isinstance(processed, bool):
        raise RuntimeError("meeting carrier fence is unavailable")
    fields = {
        scope: "1"
        for scope, selected in (("raw", raw), ("processed", processed))
        if selected
    }
    if not fields:
        return
    async with redis_client.pipeline(transaction=True) as pipe:
        pipe.hset(carrier_fence_key(meeting_id), mapping=fields)
        pipe.persist(carrier_fence_key(meeting_id))
        await pipe.execute()


async def xadd_if_carrier_writable(
    redis_client,
    meeting_id,
    *,
    scope: str | tuple[str, ...],
    stream: str,
    fields: dict,
) -> bool:
    """Atomically refuse a stream append once the meeting's selected fence is durable.

    Production cross-spoke writers implement the same check-and-write contract with one Lua
    command. This WATCH transaction is the asyncio meeting-api equivalent and the executable
    reference contract used by retention tests.
    """

    scopes = (scope,) if isinstance(scope, str) else tuple(scope)
    if (
        redis_client is None
        or not scopes
        or any(item not in CARRIER_FENCE_SCOPES for item in scopes)
    ):
        raise RuntimeError("meeting carrier append authority is unavailable")
    from redis.exceptions import WatchError

    fence = carrier_fence_key(meeting_id)
    while True:
        async with redis_client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(fence)
                values = await pipe.hmget(fence, *scopes)
                if any(_text(value) == "1" for value in values):
                    await pipe.unwatch()
                    return False
                pipe.multi()
                pipe.xadd(stream, fields)
                await pipe.execute()
                return True
            except WatchError:
                continue


async def xadd_many_if_carrier_writable(
    redis_client,
    meeting_id,
    *,
    scope: str | tuple[str, ...],
    stream: str,
    entries: list[dict],
) -> bool:
    """Atomically append a bounded batch only while every selected scope remains writable."""

    if not entries:
        return True
    scopes = (scope,) if isinstance(scope, str) else tuple(scope)
    if (
        redis_client is None
        or not scopes
        or any(item not in CARRIER_FENCE_SCOPES for item in scopes)
    ):
        raise RuntimeError("meeting carrier append authority is unavailable")
    from redis.exceptions import WatchError

    fence = carrier_fence_key(meeting_id)
    while True:
        async with redis_client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(fence)
                values = await pipe.hmget(fence, *scopes)
                if any(_text(value) == "1" for value in values):
                    await pipe.unwatch()
                    return False
                pipe.multi()
                for fields in entries:
                    pipe.xadd(stream, fields)
                await pipe.execute()
                return True
            except WatchError:
                continue


async def hset_segments_if_carrier_writable(
    redis_client,
    meeting_id,
    *,
    entries: dict[str, str],
    ttl: int,
) -> bool:
    """Atomically append the live transcript hash behind the permanent raw fence.

    The hash and ``active_meetings`` set are raw transcript carriers just like the
    row-scoped ``tc`` stream. Watching the fence makes either ordering safe: an
    append that commits first is removed by the subsequent purge, while a fence
    that commits first invalidates or rejects the append transaction.
    """

    if redis_client is None or isinstance(ttl, bool) or not isinstance(ttl, int) or ttl <= 0:
        raise RuntimeError("meeting transcript hash authority is unavailable")
    if not entries:
        return True
    from redis.exceptions import WatchError

    mid = _meeting_id(meeting_id)
    fence = carrier_fence_key(mid)
    hash_key = segments_hash_key(mid)
    while True:
        async with redis_client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(fence)
                if _text(await pipe.hget(fence, "raw")) == "1":
                    await pipe.unwatch()
                    return False
                pipe.multi()
                pipe.sadd(ACTIVE_MEETINGS_KEY, str(mid))
                pipe.hset(hash_key, mapping=entries)
                pipe.expire(hash_key, ttl)
                await pipe.execute()
                return True
            except WatchError:
                continue


async def publish_if_carrier_writable(
    redis_client,
    meeting_id,
    *,
    scope: str,
    channel: str,
    message: str,
) -> bool:
    """Atomically refuse a transient live publication after a retention fence."""

    if redis_client is None or scope not in CARRIER_FENCE_SCOPES:
        raise RuntimeError("meeting carrier publication authority is unavailable")
    from redis.exceptions import WatchError

    fence = carrier_fence_key(meeting_id)
    while True:
        async with redis_client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(fence)
                if _text(await pipe.hget(fence, scope)) == "1":
                    await pipe.unwatch()
                    return False
                pipe.multi()
                pipe.publish(channel, message)
                await pipe.execute()
                return True
            except WatchError:
                continue


async def zadd_if_carrier_writable(
    redis_client,
    meeting_id,
    *,
    scope: str,
    key: str,
    mapping: dict,
) -> bool:
    """Atomically refuse delayed queue re-arming after a carrier scope was fenced."""

    if redis_client is None or scope not in CARRIER_FENCE_SCOPES:
        raise RuntimeError("meeting carrier queue authority is unavailable")
    from redis.exceptions import WatchError

    fence = carrier_fence_key(meeting_id)
    while True:
        async with redis_client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(fence)
                if _text(await pipe.hget(fence, scope)) == "1":
                    await pipe.unwatch()
                    return False
                pipe.multi()
                pipe.zadd(key, mapping)
                await pipe.execute()
                return True
            except WatchError:
                continue


def _text(value):
    return value.decode() if isinstance(value, (bytes, bytearray)) else value


async def _purge_source_snapshot(redis_client, meeting_id: int) -> tuple[int, int]:
    """XDEL one meeting's entries from one finite snapshot.

    Return ``(matched, deleted)`` so the caller can distinguish a quiescent source from a target
    producer that is recreating rows between bounded passes.
    """

    tail = await redis_client.xrevrange(
        TRANSCRIPTION_SOURCE_STREAM, max="+", min="-", count=1
    )
    if not tail:
        return 0, 0
    snapshot_tail = _text(tail[0][0])
    cursor = "-"
    matched = 0
    deleted = 0
    scanned = 0
    while True:
        rows = await redis_client.xrange(
            TRANSCRIPTION_SOURCE_STREAM,
            min=cursor,
            max=snapshot_tail,
            count=SOURCE_SCAN_PAGE_SIZE,
        )
        if not rows:
            return matched, deleted
        scanned += len(rows)
        if scanned > SOURCE_SCAN_ENTRY_LIMIT:
            raise RuntimeError("meeting source carrier scan exceeded its safety bound")
        target_ids: list[str] = []
        for entry_id, fields in rows:
            normalized = {_text(key): _text(value) for key, value in fields.items()}
            try:
                payload = json.loads(normalized["payload"])
            except (KeyError, json.JSONDecodeError, TypeError, ValueError):
                raise RuntimeError("meeting source carrier cannot be attributed safely") from None
            if not isinstance(payload, dict) or "meeting_id" not in payload:
                raise RuntimeError("meeting source carrier cannot be attributed safely")
            try:
                source_meeting_id = _meeting_id(payload["meeting_id"])
            except ValueError:
                raise RuntimeError(
                    "meeting source carrier cannot be attributed safely"
                ) from None
            if source_meeting_id == meeting_id:
                target_ids.append(_text(entry_id))
        if target_ids:
            matched += len(target_ids)
            deleted += int(await redis_client.xdel(TRANSCRIPTION_SOURCE_STREAM, *target_ids))
        last_id = _text(rows[-1][0])
        if last_id == snapshot_tail or len(rows) < SOURCE_SCAN_PAGE_SIZE:
            return matched, deleted
        cursor = f"({last_id}"


async def _purge_source_entries(redis_client, meeting_id: int) -> int:
    """Remove one meeting from the shared source, failing if its producer stays active.

    An ``erasing`` database fence stops collector/derived writers, but it cannot stop the bot's
    global XADD producer. Multiple finite passes distinguish ordinary residue from a producer that
    has not quiesced. We never issue a successful erasure receipt after exhausting the bound.
    """

    deleted = 0
    for _ in range(SOURCE_PURGE_MAX_PASSES):
        matched, removed = await _purge_source_snapshot(redis_client, meeting_id)
        deleted += removed
        if matched == 0:
            return deleted
    raise RuntimeError("meeting source carrier producer did not quiesce")


def _private_keys(meeting_id: int, *, raw: bool, processed: bool) -> tuple[str, ...]:
    keys: list[str] = []
    if raw:
        keys.extend((transcript_stream_key(meeting_id), segments_hash_key(meeting_id)))
    if processed:
        keys.extend(
            (
                proc_stream_key(meeting_id),
                proc_on_key(meeting_id),
                proc_cursor_key(meeting_id),
            )
        )
    return tuple(keys)


async def _delete_private_once(
    redis_client, meeting_id: int, *, raw: bool, processed: bool
) -> int:
    keys = _private_keys(meeting_id, raw=raw, processed=processed)
    async with redis_client.pipeline(transaction=True) as pipe:
        pipe.delete(*keys)
        if raw:
            pipe.srem(ACTIVE_MEETINGS_KEY, str(meeting_id))
        if processed:
            pipe.zrem(PROC_PENDING_KEY, str(meeting_id))
        results = await pipe.execute()
    return int(results[0] or 0)


async def _private_carriers_remain(
    redis_client, meeting_id: int, *, raw: bool, processed: bool
) -> bool:
    keys = _private_keys(meeting_id, raw=raw, processed=processed)
    async with redis_client.pipeline(transaction=True) as pipe:
        pipe.exists(*keys)
        if raw:
            pipe.sismember(ACTIVE_MEETINGS_KEY, str(meeting_id))
        if processed:
            pipe.zscore(PROC_PENDING_KEY, str(meeting_id))
        results = await pipe.execute()
    index = 1
    remains = bool(results[0])
    if raw:
        remains = remains or bool(results[index])
        index += 1
    if processed:
        remains = remains or results[index] is not None
    return remains


async def _purge_private_carriers(
    redis_client, meeting_id: int, *, raw: bool, processed: bool
) -> int:
    deleted = 0
    for _ in range(PRIVATE_PURGE_MAX_PASSES):
        deleted += await _delete_private_once(
            redis_client, meeting_id, raw=raw, processed=processed
        )
        if not await _private_carriers_remain(
            redis_client, meeting_id, raw=raw, processed=processed
        ):
            return deleted
    raise RuntimeError("meeting private carriers did not quiesce")


async def purge_meeting_redis_carriers(
    redis_client,
    meeting_id,
    *,
    raw: bool = True,
    processed: bool = True,
) -> int:
    """Delete and verify one row's selected PII carriers with bounded Redis operations.

    Shared discovery collections are mutated by member, never deleted. A dependency failure
    propagates so retention does not commit its durable expiry marker ahead of carrier deletion.
    Source and row-private producers must quiesce within their bounded passes. The return value
    counts deleted stream rows/private keys, not shared-set membership.
    """

    mid = _meeting_id(meeting_id)
    if redis_client is None or not isinstance(raw, bool) or not isinstance(processed, bool):
        raise RuntimeError("meeting carrier purge is unavailable")
    if not raw and not processed:
        return 0
    # This surviving, monotonic fence is what turns a bounded quiescence observation into a durable
    # guarantee: an Agent final beat or bot buffer that wakes after purge returns cannot recreate PII.
    await fence_meeting_redis_carriers(
        redis_client, mid, raw=raw, processed=processed
    )
    deleted = 0
    if raw:
        deleted += await _purge_source_entries(redis_client, mid)
    deleted += await _purge_private_carriers(
        redis_client, mid, raw=raw, processed=processed
    )
    return deleted
