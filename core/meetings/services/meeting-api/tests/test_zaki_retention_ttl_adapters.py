"""Production-boundary proof for bounded Minutes TTL composition."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json

import fakeredis.aioredis
import httpx
import pytest

from meeting_api import create_app
from meeting_api.bot_spawn.fakes import InMemoryMeetingRepo
from meeting_api.collector import purge_meeting_redis_carriers
from meeting_api.collector.adapters import RedisStreamBus, _RedisTranscriptBatchWriter
from meeting_api.collector.carriers import (
    carrier_fence_key,
    fence_meeting_redis_carriers,
    xadd_if_carrier_writable,
)
from meeting_api.collector.ports import TranscriptWriteRefused
from meeting_api.retention import run_production_ttl_once
from meeting_api.retention.ttl import DueScope
from meeting_api.retention.ttl_adapters import SqlAlchemyTtlStore
from meeting_api.retention.fakes import InMemoryRetentionStorage


NOW = datetime(2026, 7, 14, 12, 0, tzinfo=timezone.utc)


class FakeResult:
    def __init__(self, rows=()):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class SelectionSession:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement, params=None):
        self.calls.append((" ".join(statement.split()), params or {}))
        return FakeResult(self.rows)


async def test_postgres_ttl_store_lists_a_bounded_deterministic_due_scope_batch():
    rows = [
        {
            "user_id": 7,
            "meeting_id": 41,
            "scope": "audio",
            "expires_at": NOW - timedelta(seconds=2),
        },
        {
            "user_id": 7,
            "meeting_id": 41,
            "scope": "transcript",
            "expires_at": NOW - timedelta(seconds=1),
        },
    ]
    session = SelectionSession(rows)
    store = SqlAlchemyTtlStore(
        lambda: session,
        object_storage=None,
        statement_factory=lambda sql: sql,
    )

    due = await store.list_due_scopes(now=NOW, limit=2)

    assert [(item.meeting_id, item.scope) for item in due] == [
        ("41", "audio"),
        ("41", "transcript"),
    ]
    sql, params = session.calls[0]
    assert "status IN ('completed', 'failed')" in sql
    assert "zaki_capture" in sql
    assert "expired_scopes" in sql
    assert "ORDER BY" in sql
    assert "LIMIT :limit" in sql
    assert params == {"now": NOW, "limit": 2}


@pytest.mark.parametrize("retention_markers", [
    {"zaki_retention": "corrupt-authority"},
    {
        "zaki_retention": {
            "state": "open",
            "scope_expiries": {},
            "expired_scopes": [{"unhashable": "private"}],
        }
    },
    {
        "zaki_capture": {
            "state": "authorized",
            "bot_name": "ZAKI Notetaker",
        }
    },
], ids=["invalid-root", "invalid-shape", "capture-without-retention"])
async def test_malformed_retention_root_is_fail_safe_purged_and_normalized(
    retention_markers,
):
    prefix = "recordings/7/91/session-a/"
    meeting = {
        "id": 41,
        "user_id": 7,
        "status": "completed",
        "data": {
            **retention_markers,
            "summary": {"text": "private summary"},
            "summaries": [{"text": "also private"}],
            "notes": [{"text": "private legacy note"}],
            "docs": [{"path": "kg/entities/meeting/41.md"}],
            "processed": {"views": [{"id": "private-view"}]},
            "zaki_recording_prefixes": [prefix],
            "recordings": [{
                "id": 91,
                "session_uid": "session-a",
                "media_files": [{
                    "storage_path": f"{prefix}audio/master.wav",
                }],
            }],
        },
    }
    session = MutationSession(meeting, transcript_rows=2)
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await redis_c.hset("meeting:41:segments", "s1", "private transcript")
    await redis_c.xadd("proc:meeting:41", {"note": "private processed note"})
    storage = InMemoryRetentionStorage()
    storage.seed(f"{prefix}audio/master.wav", b"private audio")
    store = SqlAlchemyTtlStore(
        lambda: session,
        storage,
        redis_client=redis_c,
        statement_factory=lambda sql: sql,
    )

    deleted = await store.expire_scope(DueScope(
        user_id="7",
        meeting_id="41",
        scope="transcript",
        expires_at=NOW,
        expiry_invalid=True,
    ))

    assert deleted == 6
    assert session.transcript_rows == 0
    assert storage.snapshot(prefix) == {}
    assert "summary" not in meeting["data"]
    assert "summaries" not in meeting["data"]
    assert "processed" not in meeting["data"]
    assert "notes" not in meeting["data"]
    assert "docs" not in meeting["data"]
    assert meeting["data"]["recordings"] == []
    assert "zaki_recording_prefixes" not in meeting["data"]
    assert meeting["data"]["zaki_retention"]["expired_scopes"] == [
        "audio", "summary", "transcript",
    ]
    assert not await redis_c.exists("meeting:41:segments", "proc:meeting:41")
    await redis_c.aclose()


async def test_production_ttl_entry_point_is_no_io_when_operator_flag_is_off():
    def forbidden_session_factory():
        raise AssertionError("disabled TTL worker touched PostgreSQL")

    receipt = await run_production_ttl_once(
        enabled=False,
        now=NOW,
        limit=100,
        session_factory=forbidden_session_factory,
        object_storage=None,
    )

    assert receipt.attempted == 0
    assert receipt.expired == {"audio": 0, "transcript": 0, "summary": 0}
    assert receipt.failed == 0


@pytest.mark.parametrize(
    ("enabled", "limit", "message"),
    [
        ("true", 100, "operator flag"),
        (True, 0, "batch limit"),
        (True, 501, "batch limit"),
    ],
)
async def test_production_ttl_entry_point_rejects_malformed_activation_without_io(
    enabled, limit, message
):
    def forbidden_session_factory():
        raise AssertionError("invalid TTL configuration touched PostgreSQL")

    with pytest.raises(ValueError, match=message):
        await run_production_ttl_once(
            enabled=enabled,
            now=NOW,
            limit=limit,
            session_factory=forbidden_session_factory,
            object_storage=None,
        )


class MutationResult(FakeResult):
    def __init__(self, *, row=None, rowcount=0):
        super().__init__()
        self._row = row
        self.rowcount = rowcount

    def first(self):
        return self._row


class MutationSession:
    def __init__(self, meeting, *, transcript_rows=0):
        self.meeting = meeting
        self.transcript_rows = transcript_rows
        self.calls = []
        self.commits = 0
        self.active = False

    async def __aenter__(self):
        self.active = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.active = False
        return False

    async def commit(self):
        self.commits += 1

    async def execute(self, statement, params=None):
        import json

        params = params or {}
        sql = " ".join(statement.split())
        self.calls.append((sql, params))
        if "pg_advisory_xact_lock" in sql:
            return MutationResult()
        if sql.startswith("SELECT id, user_id, status, data FROM meetings"):
            return MutationResult(row=dict(self.meeting) if self.meeting else None)
        if sql.startswith("DELETE FROM transcriptions"):
            deleted = self.transcript_rows
            self.transcript_rows = 0
            return MutationResult(rowcount=deleted)
        if sql.startswith("UPDATE meetings SET data"):
            self.meeting["data"] = json.loads(params["data"])
            return MutationResult(rowcount=1)
        raise AssertionError(f"unexpected SQL: {sql}")


class FailingRedis:
    async def xrevrange(self, *args, **kwargs):
        raise RuntimeError("redis unavailable")

    def pipeline(self, **kwargs):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def delete(self, *keys):
        return self

    def hset(self, *args, **kwargs):
        return self

    def persist(self, *args):
        return self

    def srem(self, *args):
        return self

    def zrem(self, *args):
        return self

    async def execute(self):
        raise RuntimeError("redis unavailable")


class OutsideTransactionRedis:
    def __init__(self, session):
        self._session = session
        self._client = fakeredis.aioredis.FakeRedis(decode_responses=True)

    def _outside(self):
        assert not self._session.active, "Redis carrier purge ran inside a DB transaction"

    def pipeline(self, **kwargs):
        self._outside()
        return self._client.pipeline(**kwargs)

    async def xrevrange(self, *args, **kwargs):
        self._outside()
        return await self._client.xrevrange(*args, **kwargs)

    async def xrange(self, *args, **kwargs):
        self._outside()
        return await self._client.xrange(*args, **kwargs)

    async def xdel(self, *args, **kwargs):
        self._outside()
        return await self._client.xdel(*args, **kwargs)

    async def aclose(self):
        await self._client.aclose()


class ReappearingSourceRedis(OutsideTransactionRedis):
    """A still-running bot that recreates target source rows after every targeted delete."""

    def __init__(self, session, meeting_id):
        super().__init__(session)
        self._meeting_id = meeting_id

    async def xdel(self, *args, **kwargs):
        deleted = await super().xdel(*args, **kwargs)
        await self._client.xadd(
            "transcription_segments",
            {
                "payload": json.dumps(
                    {
                        "type": "transcription",
                        "meeting_id": self._meeting_id,
                        "segments": [],
                    }
                )
            },
        )
        return deleted


class _ReappearingPrivatePipeline:
    def __init__(self, owner, pipeline):
        self._owner = owner
        self._pipeline = pipeline
        self._deleted_processed = False

    async def __aenter__(self):
        await self._pipeline.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return await self._pipeline.__aexit__(exc_type, exc, tb)

    def delete(self, *keys):
        self._deleted_processed = any(
            str(key).startswith(f"proc:meeting:{self._owner.meeting_id}")
            for key in keys
        )
        self._pipeline.delete(*keys)
        return self

    def hset(self, *args, **kwargs):
        self._pipeline.hset(*args, **kwargs)
        return self

    def persist(self, *args):
        self._pipeline.persist(*args)
        return self

    def srem(self, *args):
        self._pipeline.srem(*args)
        return self

    def zrem(self, *args):
        self._pipeline.zrem(*args)
        return self

    def exists(self, *keys):
        self._pipeline.exists(*keys)
        return self

    def sismember(self, *args):
        self._pipeline.sismember(*args)
        return self

    def zscore(self, *args):
        self._pipeline.zscore(*args)
        return self

    async def execute(self):
        results = await self._pipeline.execute()
        if self._deleted_processed:
            await self._owner._client.xadd(
                f"proc:meeting:{self._owner.meeting_id}", {"note": "recreated"}
            )
        return results


class ReappearingPrivateRedis(OutsideTransactionRedis):
    def __init__(self, session, meeting_id):
        super().__init__(session)
        self.meeting_id = meeting_id

    def pipeline(self, **kwargs):
        self._outside()
        return _ReappearingPrivatePipeline(
            self, self._client.pipeline(**kwargs)
        )


async def test_source_stream_purge_fails_boundedly_when_target_producer_does_not_stop():
    session = MutationSession(None)
    redis_c = ReappearingSourceRedis(session, meeting_id=41)
    await redis_c._client.xadd(
        "transcription_segments",
        {
            "payload": json.dumps(
                {"type": "transcription", "meeting_id": 41, "segments": []}
            )
        },
    )

    with pytest.raises(RuntimeError, match="did not quiesce"):
        await purge_meeting_redis_carriers(
            redis_c, 41, raw=True, processed=False
        )

    await redis_c.aclose()


async def test_source_stream_purge_fails_closed_when_a_row_cannot_be_attributed():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await redis_c.xadd("transcription_segments", {"unexpected": "opaque"})

    with pytest.raises(RuntimeError, match="cannot be attributed safely"):
        await purge_meeting_redis_carriers(
            redis_c, 41, raw=True, processed=False
        )

    await redis_c.aclose()


async def test_source_stream_purge_rejects_a_non_integral_meeting_identifier():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await redis_c.xadd(
        "transcription_segments",
        {
            "payload": json.dumps(
                {"type": "transcription", "meeting_id": 41.5, "segments": []}
            )
        },
    )

    with pytest.raises(RuntimeError, match="cannot be attributed safely"):
        await purge_meeting_redis_carriers(
            redis_c, 41, raw=True, processed=False
        )

    await redis_c.aclose()


async def test_private_carrier_purge_fails_boundedly_when_agent_keeps_recreating_proc():
    redis_c = ReappearingPrivateRedis(MutationSession(None), meeting_id=41)
    await redis_c._client.xadd("proc:meeting:41", {"note": "private"})

    with pytest.raises(RuntimeError, match="private carriers did not quiesce"):
        await purge_meeting_redis_carriers(
            redis_c, 41, raw=False, processed=True
        )

    await redis_c.aclose()


async def test_ttl_store_expires_transcript_and_summary_idempotently_after_owner_recheck():
    meeting = {
        "id": 41,
        "user_id": 7,
        "status": "completed",
        "data": {
            "summary": {"text": "private"},
            "summaries": [{"text": "also private"}],
            "zaki_retention": {
                "scope_expiries": {
                    "transcript": NOW.isoformat(),
                    "summary": NOW.isoformat(),
                }
            },
        },
    }
    session = MutationSession(meeting, transcript_rows=3)
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = SqlAlchemyTtlStore(
        lambda: session,
        object_storage=None,
        redis_client=redis_c,
        statement_factory=lambda sql: sql,
    )

    transcript = DueScope("7", "41", "transcript", NOW)
    summary = DueScope("7", "41", "summary", NOW)
    assert await store.expire_scope(transcript) == 3
    assert await store.expire_scope(summary) == 2
    assert await store.expire_scope(summary) == 0

    assert "summary" not in meeting["data"]
    assert "summaries" not in meeting["data"]
    assert meeting["data"]["zaki_retention"]["expired_scopes"] == [
        "summary",
        "transcript",
    ]
    assert session.commits == 2
    lock_calls = [sql for sql, _ in session.calls if "pg_advisory_xact_lock" in sql]
    # Content-bearing Redis scopes validate before external I/O and revalidate before mutation.
    assert len(lock_calls) == 5
    await redis_c.aclose()


async def test_transcript_ttl_redis_failure_aborts_database_delete_and_expiry_marker():
    meeting = {
        "id": 41,
        "user_id": 7,
        "status": "completed",
        "data": {
            "zaki_retention": {
                "scope_expiries": {"transcript": NOW.isoformat()},
            },
        },
    }
    session = MutationSession(meeting, transcript_rows=2)
    store = SqlAlchemyTtlStore(
        lambda: session,
        object_storage=None,
        redis_client=FailingRedis(),
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(RuntimeError, match="redis unavailable"):
        await store.expire_scope(DueScope("7", "41", "transcript", NOW))

    assert session.transcript_rows == 2
    assert session.commits == 0
    assert "expired_scopes" not in meeting["data"]["zaki_retention"]


async def test_transcript_ttl_purges_redis_outside_the_database_transaction():
    meeting = {
        "id": 41,
        "user_id": 7,
        "status": "completed",
        "data": {
            "zaki_retention": {
                "scope_expiries": {"transcript": NOW.isoformat()},
            },
        },
    }
    session = MutationSession(meeting, transcript_rows=1)
    redis_c = OutsideTransactionRedis(session)
    store = SqlAlchemyTtlStore(
        lambda: session,
        object_storage=None,
        redis_client=redis_c,
        statement_factory=lambda sql: sql,
    )

    assert await store.expire_scope(DueScope("7", "41", "transcript", NOW)) == 1
    assert session.commits == 1
    await redis_c.aclose()


async def test_transcript_ttl_purges_only_the_owned_meeting_redis_carriers():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    for meeting_id in (41, 42):
        await redis_c.xadd(
            "transcription_segments",
            {
                "payload": json.dumps(
                    {
                        "type": "transcription",
                        "meeting_id": "041" if meeting_id == 41 else meeting_id,
                        "segments": [],
                    }
                )
            },
        )
        await redis_c.xadd(f"tc:meeting:{meeting_id}", {"payload": "private"})
        await redis_c.hset(f"meeting:{meeting_id}:segments", "s1", "private")
        await redis_c.xadd(f"proc:meeting:{meeting_id}", {"note": "private"})
        await redis_c.set(f"proc:meeting:{meeting_id}:on", "1")
        await redis_c.set(f"proc:meeting:{meeting_id}:cursor", "1-0")
        await redis_c.sadd("active_meetings", str(meeting_id))
        await redis_c.zadd("processed_pending", {str(meeting_id): 1})
    meeting = {
        "id": 41,
        "user_id": 7,
        "status": "completed",
        "data": {
            "processed": {"views": [{"id": "copilot-notes", "doc": {"notes": []}}]},
            "zaki_retention": {
                "scope_expiries": {"transcript": NOW.isoformat()},
            },
        },
    }
    session = MutationSession(meeting, transcript_rows=2)
    store = SqlAlchemyTtlStore(
        lambda: session,
        object_storage=None,
        redis_client=redis_c,
        statement_factory=lambda sql: sql,
    )

    assert await store.expire_scope(DueScope("7", "41", "transcript", NOW)) == 3

    assert not await redis_c.exists(
        "tc:meeting:41",
        "meeting:41:segments",
        "proc:meeting:41",
        "proc:meeting:41:on",
        "proc:meeting:41:cursor",
    )
    assert not await redis_c.sismember("active_meetings", "41")
    assert await redis_c.zscore("processed_pending", "41") is None
    assert await redis_c.exists(
        "tc:meeting:42",
        "meeting:42:segments",
        "proc:meeting:42",
        "proc:meeting:42:on",
        "proc:meeting:42:cursor",
    ) == 5
    assert await redis_c.sismember("active_meetings", "42")
    assert await redis_c.zscore("processed_pending", "42") == 1
    source_rows = await redis_c.xrange("transcription_segments")
    assert len(source_rows) == 1
    assert json.loads(source_rows[0][1]["payload"])["meeting_id"] == 42
    assert "processed" not in meeting["data"]
    await redis_c.aclose()


async def test_summary_ttl_purges_processed_carriers_but_preserves_raw_transcript():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await redis_c.xadd("tc:meeting:41", {"payload": "raw"})
    await redis_c.hset("meeting:41:segments", "s1", "raw")
    await redis_c.xadd("proc:meeting:41", {"note": "derived"})
    await redis_c.set("proc:meeting:41:on", "1")
    await redis_c.set("proc:meeting:41:cursor", "1-0")
    await redis_c.sadd("active_meetings", "41")
    await redis_c.zadd("processed_pending", {"41": 1})
    meeting = {
        "id": 41,
        "user_id": 7,
        "status": "completed",
        "data": {
            "summary": {"text": "private"},
            "summaries": [{"text": "also private"}],
            "notes": [{"text": "private legacy note"}],
            "docs": [{"path": "kg/entities/meeting/41.md"}],
            "processed": {"views": [{"id": "copilot-notes", "doc": {"notes": []}}]},
            "zaki_retention": {
                "scope_expiries": {"summary": NOW.isoformat()},
            },
        },
    }
    session = MutationSession(meeting)
    store = SqlAlchemyTtlStore(
        lambda: session,
        object_storage=None,
        redis_client=redis_c,
        statement_factory=lambda sql: sql,
    )

    assert await store.expire_scope(DueScope("7", "41", "summary", NOW)) == 3

    assert await redis_c.exists("tc:meeting:41", "meeting:41:segments") == 2
    assert await redis_c.sismember("active_meetings", "41")
    assert not await redis_c.exists(
        "proc:meeting:41",
        "proc:meeting:41:on",
        "proc:meeting:41:cursor",
    )
    assert await redis_c.zscore("processed_pending", "41") is None
    assert "summary" not in meeting["data"]
    assert "summaries" not in meeting["data"]
    assert "processed" not in meeting["data"]
    assert "notes" not in meeting["data"]
    assert "docs" not in meeting["data"]
    await redis_c.aclose()


async def test_processed_fence_rejects_a_delayed_agent_write_after_purge_returns():
    """A final Agent beat can wake seconds after bounded purge quiescence looked empty."""
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)

    await purge_meeting_redis_carriers(
        redis_c, 41, raw=False, processed=True
    )
    accepted = await xadd_if_carrier_writable(
        redis_c,
        41,
        scope="processed",
        stream="proc:meeting:41",
        fields={"note": "private late beat"},
    )

    assert accepted is False
    assert await redis_c.xlen("proc:meeting:41") == 0
    assert await redis_c.hget(carrier_fence_key(41), "processed") == "1"
    await redis_c.aclose()


async def test_processed_fence_rejects_a_delayed_terminal_reap_from_production_bus():
    """The lifecycle callback can reach its final XADD after the retention purge returned."""
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    bus = RedisStreamBus(redis_c)
    await fence_meeting_redis_carriers(redis_c, 41, raw=False, processed=True)

    with pytest.raises(TranscriptWriteRefused, match="no longer writable"):
        await bus.xadd(
            "tc:meeting:41",
            {"type": "session_end", "uid": "native-private"},
        )

    assert await redis_c.xlen("tc:meeting:41") == 0
    assert await redis_c.hget(carrier_fence_key(41), "processed") == "1"
    await redis_c.aclose()


async def test_terminal_callback_cannot_resurrect_raw_stream_after_erasure_fence():
    """The app delegates its terminal marker to RedisStreamBus, whose append is fence-atomic."""

    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    production_bus = RedisStreamBus(redis_c)
    append_started = asyncio.Event()
    release_append = asyncio.Event()

    class PausingProductionBus:
        def __getattr__(self, name):
            return getattr(production_bus, name)

        async def xadd(self, stream, payload):
            append_started.set()
            await release_append.wait()
            return await production_bus.xadd(stream, payload)

    repo = InMemoryMeetingRepo()
    meeting = await repo.create_meeting(
        user_id=7,
        platform="google_meet",
        native_meeting_id="private-native-id",
        data={},
    )
    await repo.create_session(meeting_id=meeting["id"], session_uid="session-1")
    app = create_app(meeting_repo=repo, redis=PausingProductionBus())

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://meeting-api.test",
    ) as client:
        for status in ("joining", "active"):
            response = await client.post(
                "/bots/internal/callback/lifecycle",
                json={"connection_id": "session-1", "status": status},
            )
            assert response.status_code == 200

        terminal = asyncio.create_task(client.post(
            "/bots/internal/callback/lifecycle",
            json={
                "connection_id": "session-1",
                "status": "completed",
                "completion_reason": "stopped",
            },
        ))
        await append_started.wait()
        await purge_meeting_redis_carriers(
            redis_c,
            meeting["id"],
            raw=True,
            processed=False,
        )
        release_append.set()
        assert (await terminal).status_code == 200

    stream = f"tc:meeting:{meeting['id']}"
    assert await redis_c.xlen(stream) == 0
    assert await redis_c.hget(carrier_fence_key(meeting["id"]), "raw") == "1"
    await redis_c.aclose()


async def test_production_transcript_batch_uses_raw_not_summary_fence():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    bus = RedisStreamBus(redis_c)
    await fence_meeting_redis_carriers(redis_c, 41, raw=False, processed=True)

    writer = _RedisTranscriptBatchWriter(redis_c, 41)
    await writer.append_segments([{
        "segment_id": "retained",
        "text": "still retained",
    }])
    assert await bus.xadd_many(
        "tc:meeting:41", [{"type": "transcription", "text": "still retained"}]
    ) is True
    await fence_meeting_redis_carriers(redis_c, 41, raw=True, processed=False)

    with pytest.raises(TranscriptWriteRefused, match="no longer writable"):
        await writer.append_segments([{
            "segment_id": "late",
            "text": "late private",
        }])
    with pytest.raises(TranscriptWriteRefused, match="no longer writable"):
        await bus.xadd_many(
            "tc:meeting:41", [{"type": "transcription", "text": "late private"}]
        )
    assert await redis_c.hlen("meeting:41:segments") == 1
    assert await redis_c.xlen("tc:meeting:41") == 1
    await redis_c.aclose()


async def test_raw_fence_wins_an_inflight_live_hash_append_without_resurrection():
    """A fence committed after WATCH but before HSET invalidates the whole hash transaction."""

    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    observed = asyncio.Event()
    resume = asyncio.Event()

    class PausingPipeline:
        def __init__(self, inner):
            self._inner = inner

        async def __aenter__(self):
            await self._inner.__aenter__()
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return await self._inner.__aexit__(exc_type, exc, traceback)

        async def hget(self, key, field):
            value = await self._inner.hget(key, field)
            observed.set()
            await resume.wait()
            return value

        def __getattr__(self, name):
            return getattr(self._inner, name)

    class RedisProxy:
        def pipeline(self, *args, **kwargs):
            return PausingPipeline(redis_c.pipeline(*args, **kwargs))

        def __getattr__(self, name):
            return getattr(redis_c, name)

    writer = _RedisTranscriptBatchWriter(RedisProxy(), 41)
    append = asyncio.create_task(writer.append_segments([{
        "segment_id": "late-private",
        "text": "must never return",
    }]))
    await observed.wait()
    await fence_meeting_redis_carriers(redis_c, 41, raw=True, processed=False)
    resume.set()

    with pytest.raises(TranscriptWriteRefused, match="no longer writable"):
        await append
    assert not await redis_c.exists("meeting:41:segments")
    assert not await redis_c.sismember("active_meetings", "41")
    assert await redis_c.hget(carrier_fence_key(41), "raw") == "1"
    await redis_c.aclose()


async def test_raw_fence_wins_an_inflight_mutable_publish_without_pii_delivery():
    """A mutable transcript publication cannot cross a fence committed after WATCH."""

    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    observed = asyncio.Event()
    resume = asyncio.Event()

    class PausingPipeline:
        def __init__(self, inner):
            self._inner = inner

        async def __aenter__(self):
            await self._inner.__aenter__()
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return await self._inner.__aexit__(exc_type, exc, traceback)

        async def hget(self, key, field):
            value = await self._inner.hget(key, field)
            observed.set()
            await resume.wait()
            return value

        def __getattr__(self, name):
            return getattr(self._inner, name)

    class RedisProxy:
        def pipeline(self, *args, **kwargs):
            return PausingPipeline(redis_c.pipeline(*args, **kwargs))

        def __getattr__(self, name):
            return getattr(redis_c, name)

    channel = "tc:meeting:41:mutable"
    subscriber = redis_c.pubsub()
    await subscriber.subscribe(channel)
    await subscriber.get_message(timeout=1)
    publish = asyncio.create_task(
        RedisStreamBus(RedisProxy()).publish(channel, "private transcript")
    )
    await observed.wait()
    await fence_meeting_redis_carriers(redis_c, 41, raw=True, processed=False)
    resume.set()

    with pytest.raises(TranscriptWriteRefused, match="no longer writable"):
        await publish
    assert await subscriber.get_message(
        ignore_subscribe_messages=True, timeout=0.05
    ) is None
    assert await redis_c.hget(carrier_fence_key(41), "raw") == "1"
    await subscriber.aclose()
    await redis_c.aclose()


async def test_carrier_fence_is_monotonic_permanent_idempotent_and_row_isolated():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    fence = carrier_fence_key(41)
    await redis_c.hset(fence, mapping={"raw": "1"})
    await redis_c.expire(fence, 60)  # repair an accidental legacy TTL while adding the next scope

    await fence_meeting_redis_carriers(redis_c, 41, raw=False, processed=True)
    await fence_meeting_redis_carriers(redis_c, 41, raw=False, processed=True)

    assert await redis_c.hgetall(fence) == {"raw": "1", "processed": "1"}
    assert await redis_c.ttl(fence) == -1
    assert await xadd_if_carrier_writable(
        redis_c,
        41,
        scope="raw",
        stream="tc:meeting:41",
        fields={"payload": "private"},
    ) is False
    assert await xadd_if_carrier_writable(
        redis_c,
        42,
        scope=("raw", "processed"),
        stream="tc:meeting:42",
        fields={"payload": "other row"},
    ) is True
    assert await redis_c.xlen("tc:meeting:41") == 0
    assert await redis_c.xlen("tc:meeting:42") == 1
    await redis_c.aclose()


async def test_ttl_store_expires_only_owned_audio_under_the_meeting_write_barrier():
    prefix = "recordings/7/91/session-a/"
    foreign_prefix = "recordings/8/92/session-b/"
    meeting = {
        "id": 41,
        "user_id": 7,
        "status": "failed",
        "data": {
            "zaki_recording_prefixes": [prefix],
            "recordings": [
                {
                    "id": 91,
                    "session_uid": "session-a",
                    "media_files": [
                        {"storage_path": f"{prefix}audio/master.wav"},
                    ],
                }
            ],
            "zaki_retention": {
                "scope_expiries": {"audio": NOW.isoformat()},
            },
        },
    }
    session = MutationSession(meeting)
    storage = InMemoryRetentionStorage()
    storage.seed(f"{prefix}audio/master.wav", b"private")
    storage.seed(f"{prefix}video/master.webm", b"private")
    storage.seed(f"{foreign_prefix}audio/master.wav", b"foreign")
    store = SqlAlchemyTtlStore(
        lambda: session,
        storage,
        statement_factory=lambda sql: sql,
    )
    audio = DueScope("7", "41", "audio", NOW)

    assert await store.expire_scope(audio) == 2
    assert await store.expire_scope(audio) == 0

    assert storage.snapshot(prefix) == {}
    assert storage.snapshot(foreign_prefix) == {
        f"{foreign_prefix}audio/master.wav": b"foreign"
    }
    assert meeting["data"]["recordings"] == []
    assert "zaki_recording_prefixes" not in meeting["data"]
    assert meeting["data"]["zaki_retention"]["expired_scopes"] == ["audio"]
    lock_calls = [sql for sql, _ in session.calls if "pg_advisory_xact_lock" in sql]
    assert len(lock_calls) == 2


async def test_ttl_store_immediately_purges_a_scope_with_malformed_expiry():
    prefix = "recordings/7/91/session-a/"
    meeting = {
        "id": 41,
        "user_id": 7,
        "status": "completed",
        "data": {
            "zaki_recording_prefixes": [prefix],
            "recordings": [],
            "zaki_retention": {
                "scope_expiries": {"audio": "not-a-timestamp"},
            },
        },
    }
    session = MutationSession(meeting)
    storage = InMemoryRetentionStorage()
    storage.seed(f"{prefix}audio/master.wav", b"private")
    store = SqlAlchemyTtlStore(
        lambda: session,
        storage,
        statement_factory=lambda sql: sql,
    )

    deleted = await store.expire_scope(
        DueScope("7", "41", "audio", NOW, expiry_invalid=True)
    )

    assert deleted == 1
    assert storage.snapshot(prefix) == {}
    assert meeting["data"]["zaki_retention"]["expired_scopes"] == ["audio"]


async def test_ttl_store_purges_parseable_but_noncanonical_expiry_as_invalid():
    prefix = "recordings/7/91/session-a/"
    meeting = {
        "id": 41,
        "user_id": 7,
        "status": "completed",
        "data": {
            "zaki_recording_prefixes": [prefix],
            "recordings": [],
            "zaki_retention": {
                "scope_expiries": {"audio": "2026-07-14 12:00:00+00:00"},
            },
        },
    }
    session = MutationSession(meeting)
    storage = InMemoryRetentionStorage()
    storage.seed(f"{prefix}audio/master.wav", b"private")
    store = SqlAlchemyTtlStore(
        lambda: session,
        storage,
        statement_factory=lambda sql: sql,
    )

    deleted = await store.expire_scope(
        DueScope("7", "41", "audio", NOW, expiry_invalid=True)
    )

    assert deleted == 1
    assert storage.snapshot(prefix) == {}
    assert meeting["data"]["zaki_retention"]["expired_scopes"] == ["audio"]


async def test_ttl_store_does_not_mutate_a_meeting_owned_by_another_user():
    prefix = "recordings/7/91/session-a/"
    meeting = {
        "id": 41,
        "user_id": 7,
        "status": "completed",
        "data": {
            "zaki_recording_prefixes": [prefix],
            "recordings": [],
            "zaki_retention": {
                "scope_expiries": {"audio": NOW.isoformat()},
            },
        },
    }
    session = MutationSession(meeting)
    storage = InMemoryRetentionStorage()
    storage.seed(f"{prefix}audio/master.wav", b"private")
    store = SqlAlchemyTtlStore(
        lambda: session,
        storage,
        statement_factory=lambda sql: sql,
    )

    assert await store.expire_scope(DueScope("8", "41", "audio", NOW)) == 0

    assert storage.snapshot(prefix) == {
        f"{prefix}audio/master.wav": b"private"
    }
    assert "expired_scopes" not in meeting["data"]["zaki_retention"]
    assert session.commits == 0


class FailingStorage(InMemoryRetentionStorage):
    async def delete_prefix(self, prefix: str) -> int:
        raise RuntimeError("storage unavailable")


async def test_ttl_store_durably_defers_a_failed_scope_without_changing_its_expiry():
    meeting = {
        "id": 41,
        "user_id": 7,
        "status": "completed",
        "data": {
            "zaki_retention": {
                "scope_expiries": {"audio": NOW.isoformat()},
            },
        },
    }
    session = MutationSession(meeting)
    store = SqlAlchemyTtlStore(
        lambda: session,
        object_storage=None,
        statement_factory=lambda sql: sql,
    )
    retry_at = NOW + timedelta(minutes=5)

    await store.defer_scope(
        DueScope("7", "41", "audio", NOW),
        retry_at=retry_at,
    )

    retention = meeting["data"]["zaki_retention"]
    assert retention["scope_expiries"] == {"audio": NOW.isoformat()}
    assert retention["ttl_retry_after"] == {"audio": retry_at.isoformat()}
    assert session.commits == 1


async def test_ttl_store_keeps_audio_due_when_object_deletion_fails():
    prefix = "recordings/7/91/session-a/"
    meeting = {
        "id": 41,
        "user_id": 7,
        "status": "completed",
        "data": {
            "zaki_recording_prefixes": [prefix],
            "recordings": [],
            "zaki_retention": {
                "scope_expiries": {"audio": NOW.isoformat()},
            },
        },
    }
    session = MutationSession(meeting)
    storage = FailingStorage()
    storage.seed(f"{prefix}audio/master.wav", b"private")
    store = SqlAlchemyTtlStore(
        lambda: session,
        storage,
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(RuntimeError, match="storage unavailable"):
        await store.expire_scope(DueScope("7", "41", "audio", NOW))

    assert storage.snapshot(prefix) == {
        f"{prefix}audio/master.wav": b"private"
    }
    assert "expired_scopes" not in meeting["data"]["zaki_retention"]
    assert session.commits == 0
