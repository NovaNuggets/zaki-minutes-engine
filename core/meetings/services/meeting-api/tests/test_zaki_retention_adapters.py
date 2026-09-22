"""Production-boundary tests for Minutes retention adapters."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import fakeredis.aioredis
import pytest

from meeting_api.erasure_receipts import sign_erasure_receipt
from meeting_api.recordings.adapters import SqlAlchemyRecordingRepo
from meeting_api.recordings.ports import RecordingWriteRefused
from meeting_api.retention.adapters import (
    S3RetentionStorage,
    SqlAlchemyRetentionRepo,
    recording_prefixes_for_meeting,
)
from meeting_api.retention.fakes import InMemoryRetentionStorage
from meeting_api.retention.service import ErasureFailed, erase_meeting
from meeting_api.webhooks import (
    BACKOFF_SCHEDULE,
    RetryQueue,
    RedisTranscriptFinalizedOutbox,
    WebhookSink,
    build_envelope,
    drain_retry_queue,
)
from meeting_api.webhooks.retry import (
    DEAD_LETTER_KEY,
    MAX_AGE_SECONDS,
    RETRY_QUEUE_KEY,
    purge_meeting_webhook_state,
)


class FakeS3Client:
    def __init__(self, keys: list[str]):
        self.keys = set(keys)
        self.list_calls = 0
        self.delete_batch_sizes: list[int] = []
        self.delete_quiet: list[bool | None] = []

    def get_bucket_versioning(self, *, Bucket):
        return {}

    def list_objects_v2(self, *, Bucket, Prefix, MaxKeys=1000, ContinuationToken=None):
        self.list_calls += 1
        matching = sorted(key for key in self.keys if key.startswith(Prefix))
        offset = int(ContinuationToken or 0)
        page = matching[offset : offset + MaxKeys]
        next_offset = offset + len(page)
        response = {
            "Contents": [{"Key": key} for key in page],
            "IsTruncated": next_offset < len(matching),
        }
        if response["IsTruncated"]:
            response["NextContinuationToken"] = str(next_offset)
        return response

    def delete_objects(self, *, Bucket, Delete):
        objects = Delete["Objects"]
        self.delete_batch_sizes.append(len(objects))
        self.delete_quiet.append(Delete.get("Quiet"))
        for obj in objects:
            self.keys.discard(obj["Key"])
        return {"Deleted": objects, "Errors": []}


async def test_s3_retention_storage_censuses_and_deletes_every_page():
    prefix = "recordings/user-a/recording-a/session-a/"
    client = FakeS3Client(
        [f"{prefix}audio/{index:06d}.wav" for index in range(2505)]
        + ["recordings/user-b/recording-b/session-b/audio/master.wav"]
    )
    storage = S3RetentionStorage(bucket="minutes-test", client=client)

    assert await storage.count_prefix(prefix) == 2505
    assert await storage.delete_prefix(prefix) == 2505
    assert await storage.count_prefix(prefix) == 0
    assert client.keys == {"recordings/user-b/recording-b/session-b/audio/master.wav"}
    assert client.delete_batch_sizes == [1000, 1000, 505]
    assert client.delete_quiet == [True, True, True]


class FakeVersionedS3Client:
    def __init__(self, objects: list[tuple[str, str, str]]):
        # (kind, key, version_id), where kind is "version" or "marker".
        self.objects = set(objects)
        self.delete_batch_sizes: list[int] = []
        self.delete_quiet: list[bool | None] = []

    def get_bucket_versioning(self, *, Bucket):
        return {"Status": "Enabled"}

    def list_object_versions(
        self, *, Bucket, Prefix, MaxKeys=1000, KeyMarker=None, VersionIdMarker=None
    ):
        matching = sorted(item for item in self.objects if item[1].startswith(Prefix))
        offset = int(KeyMarker or 0)
        page = matching[offset : offset + MaxKeys]
        next_offset = offset + len(page)
        response = {
            "Versions": [
                {"Key": key, "VersionId": version_id}
                for kind, key, version_id in page
                if kind == "version"
            ],
            "DeleteMarkers": [
                {"Key": key, "VersionId": version_id}
                for kind, key, version_id in page
                if kind == "marker"
            ],
            "IsTruncated": next_offset < len(matching),
        }
        if response["IsTruncated"]:
            response["NextKeyMarker"] = str(next_offset)
            response["NextVersionIdMarker"] = "cursor"
        return response

    def delete_objects(self, *, Bucket, Delete):
        objects = Delete["Objects"]
        self.delete_batch_sizes.append(len(objects))
        self.delete_quiet.append(Delete.get("Quiet"))
        for obj in objects:
            target = (obj["Key"], obj["VersionId"])
            self.objects = {
                item for item in self.objects if (item[1], item[2]) != target
            }
        return {"Deleted": objects, "Errors": []}


async def test_s3_retention_storage_deletes_versions_and_delete_markers():
    prefix = "recordings/user-a/recording-a/session-a/"
    client = FakeVersionedS3Client(
        [
            ("version", f"{prefix}audio/chunk.wav", "v1"),
            ("version", f"{prefix}audio/chunk.wav", "v2"),
            ("marker", f"{prefix}audio/chunk.wav", "d1"),
            ("version", f"{prefix}audio/master.wav", "v3"),
            ("version", "recordings/user-b/other/session/audio/master.wav", "v4"),
        ]
    )
    storage = S3RetentionStorage(bucket="minutes-test", client=client)

    assert await storage.count_prefix(prefix) == 4
    assert await storage.delete_prefix(prefix) == 4
    assert await storage.count_prefix(prefix) == 0
    assert client.objects == {
        ("version", "recordings/user-b/other/session/audio/master.wav", "v4")
    }
    assert client.delete_batch_sizes == [4]
    assert client.delete_quiet == [True]


async def test_s3_retention_storage_paginates_every_version_and_marker():
    prefix = "recordings/user-a/recording-a/session-a/"
    client = FakeVersionedS3Client(
        [
            (
                "marker" if index % 3 == 0 else "version",
                f"{prefix}audio/{index:06d}.wav",
                f"v{index}",
            )
            for index in range(2505)
        ]
    )
    storage = S3RetentionStorage(bucket="minutes-test", client=client)

    assert await storage.count_prefix(prefix) == 2505
    assert await storage.delete_prefix(prefix) == 2505
    assert await storage.count_prefix(prefix) == 0
    assert client.delete_batch_sizes == [1000, 1000, 505]


def test_recording_prefixes_are_derived_from_owned_recording_identity():
    data = {
        "recordings": [
            {
                "id": 41,
                "session_uid": "session-a",
                "media_files": [
                    {"storage_path": "recordings/7/41/session-a/audio/master.wav"},
                    {"storage_path": "recordings/7/41/session-a/video/000003.webm"},
                ],
            },
            {
                "id": 42,
                "session_uid": "session-b",
                "media_files": [
                    {"storage_path": "recordings/7/42/session-b/audio/000000.wav"},
                ],
            },
        ]
    }

    assert recording_prefixes_for_meeting(7, data) == (
        "recordings/7/41/session-a/",
        "recordings/7/42/session-b/",
    )


def test_recording_prefix_derivation_rejects_mismatched_storage_identity():
    data = {
        "recordings": [
            {
                "id": 41,
                "session_uid": "session-a",
                "media_files": [
                    {"storage_path": "recordings/other-user/41/session-a/audio/master.wav"},
                ],
            }
        ]
    }

    with pytest.raises(ValueError, match="recording storage identity mismatch"):
        recording_prefixes_for_meeting(7, data)


def test_recording_prefix_derivation_ignores_metadata_without_object_paths():
    assert recording_prefixes_for_meeting(
        7,
        {"recordings": [{"status": "pending", "media_files": []}]},
    ) == ()


def test_recording_prefix_derivation_includes_durable_preupload_intents():
    prefix = "recordings/7/41/session-a/"
    assert recording_prefixes_for_meeting(
        7,
        {"zaki_recording_prefixes": [prefix], "recordings": []},
    ) == (prefix,)


class FakeDbResult:
    def __init__(self, *, row=None, scalar=None, rowcount=0):
        self._row = row
        self._scalar = scalar
        self.rowcount = rowcount

    def mappings(self):
        return self

    def first(self):
        return self._row

    def scalar_one(self):
        return self._scalar

    def scalar_one_or_none(self):
        return self._scalar


class FakeDbSession:
    def __init__(self, state):
        self.state = state
        self.commits = 0

    async def __aenter__(self):
        self.active = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.active = False
        return False

    async def commit(self):
        self.commits += 1

    async def execute(self, statement, params=None):
        params = params or {}
        sql = " ".join(statement.split())
        if "pg_advisory_xact_lock" in sql:
            return FakeDbResult()
        if sql.startswith("SELECT id, user_id, status, data"):
            meeting = self.state.get("meeting")
            row = dict(meeting) if meeting else None
            if row is not None:
                row.setdefault("status", "completed")
            return FakeDbResult(row=row)
        if sql.startswith("SELECT count(*) FROM transcriptions"):
            return FakeDbResult(scalar=self.state["transcript_rows"])
        if sql.startswith("SELECT receipt FROM minutes_erasure_receipts"):
            return FakeDbResult(scalar=self.state.get("receipt"))
        if sql.startswith("UPDATE meetings SET data"):
            self.state["meeting"]["data"]["zaki_retention"] = json.loads(params["retention"])
            return FakeDbResult(rowcount=1)
        if sql.startswith("DELETE FROM transcriptions"):
            deleted = self.state["transcript_rows"]
            self.state["transcript_rows"] = 0
            return FakeDbResult(rowcount=deleted)
        if sql.startswith("DELETE FROM meeting_sessions"):
            deleted = self.state["session_rows"]
            self.state["session_rows"] = 0
            return FakeDbResult(rowcount=deleted)
        if sql.startswith("DELETE FROM meetings"):
            existed = int(self.state.get("meeting") is not None)
            self.state["meeting"] = None
            return FakeDbResult(rowcount=existed)
        if sql.startswith("INSERT INTO minutes_erasure_receipts"):
            self.state.setdefault("receipt", json.loads(params["receipt"]))
            return FakeDbResult(rowcount=1)
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

    async def set(self, *args, **kwargs):
        self._outside()
        return await self._client.set(*args, **kwargs)

    async def sscan(self, *args, **kwargs):
        self._outside()
        return await self._client.sscan(*args, **kwargs)

    async def scard(self, *args, **kwargs):
        self._outside()
        return await self._client.scard(*args, **kwargs)

    async def delete(self, *args, **kwargs):
        self._outside()
        return await self._client.delete(*args, **kwargs)

    async def aclose(self):
        await self._client.aclose()


async def test_postgres_adapter_persists_erasing_census_then_deletes_owned_rows():
    state = {
        "meeting": {
            "id": 1,
            "user_id": 7,
            "data": {
                "summary": {"text": "private"},
                "processed": {
                    "views": [{"id": "copilot-notes", "doc": {"notes": []}}]
                },
                "recordings": [
                    {
                        "id": 41,
                        "session_uid": "session-a",
                        "media_files": [
                            {"storage_path": "recordings/7/41/session-a/audio/master.wav"}
                        ],
                    }
                ],
            },
        },
        "transcript_rows": 2,
        "session_rows": 1,
    }
    session = FakeDbSession(state)
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    repo = SqlAlchemyRetentionRepo(
        lambda: session,
        redis_client=redis_c,
        statement_factory=lambda sql: sql,
    )

    plan = await repo.begin_erasure("7", "1")
    assert plan is not None
    assert plan.recording_prefixes == ("recordings/7/41/session-a/",)
    assert plan.recording_objects is None
    assert state["meeting"]["data"]["zaki_retention"]["state"] == "erasing"

    plan = await repo.record_object_census(plan, 3)
    assert plan.recording_objects == 3
    assert state["meeting"]["data"]["zaki_retention"]["recording_objects"] == 3

    deleted = await repo.commit_erasure(plan)
    assert deleted == {"meeting_rows": 1, "transcript_rows": 2, "summary_documents": 2}
    assert state["meeting"] is None
    await redis_c.aclose()


async def test_postgres_adapter_preserves_exact_cross_spoke_receipts_before_commit():
    now = datetime(2026, 7, 15, 12, 30, tzinfo=timezone.utc)
    state = {
        "meeting": {"id": 1, "user_id": 7, "status": "completed", "data": {}},
        "transcript_rows": 2,
        "session_rows": 0,
    }
    repo = SqlAlchemyRetentionRepo(
        lambda: FakeDbSession(state), statement_factory=lambda sql: sql
    )
    plan = await repo.begin_erasure("7", "1")
    assert plan is not None
    agent = sign_erasure_receipt(
        owner="agent",
        scope="meeting",
        user_id="7",
        meeting_id="1",
        counts={
            "agent_unit_streams": 1,
            "agent_workspace_documents": 2,
            "agent_brain_records": 3,
        },
        issued_at=now,
        key_id="agent-2026-07",
        nonce="01J2M3N4P5Q6R7S8T9V0WXYZA1",
        secret="agent-secret",
    )
    plan = await repo.record_agent_erasure(plan, agent)
    minutes = sign_erasure_receipt(
        owner="minutes",
        scope="meeting",
        user_id="7",
        meeting_id="1",
        counts={
            "meeting_rows": 1,
            "transcript_rows": 2,
            "summary_documents": 0,
            "recording_objects": 0,
            "agent_unit_streams": 1,
            "agent_workspace_documents": 2,
            "agent_brain_records": 3,
        },
        issued_at=now,
        key_id="minutes-2026-07",
        nonce="01J2M3N4P5Q6R7S8T9V0WXYZM1",
        secret="minutes-secret",
    )

    stable = await repo.record_erasure_receipt(plan, minutes)
    assert stable == minutes
    assert state["meeting"]["data"]["zaki_retention"]["agent_erasure"] == agent
    assert state["meeting"]["data"]["zaki_retention"]["minutes_erasure_receipt"] == minutes

    committed = await repo.commit_erasure(
        plan,
        erased_at=now,
        policy_version="minutes-erasure.v1",
        receipt=stable,
    )

    assert committed == minutes
    assert state["receipt"] == minutes
    assert state["meeting"] is None


async def test_full_erasure_requires_a_terminal_meeting_before_persisting_its_plan():
    state = {
        "meeting": {"id": 1, "user_id": 7, "status": "active", "data": {}},
        "transcript_rows": 1,
        "session_rows": 0,
    }
    repo = SqlAlchemyRetentionRepo(
        lambda: FakeDbSession(state),
        statement_factory=lambda sql: sql,
    )

    assert await repo.begin_erasure("7", "1") is None
    assert "zaki_retention" not in state["meeting"]["data"]


async def test_full_erasure_redis_failure_leaves_database_plan_retryable():
    state = {
        "meeting": {"id": 1, "user_id": 7, "data": {}},
        "transcript_rows": 2,
        "session_rows": 1,
    }
    repo = SqlAlchemyRetentionRepo(
        lambda: FakeDbSession(state),
        redis_client=FailingRedis(),
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(ErasureFailed, match="requires retry"):
        await erase_meeting(
            repo,
            InMemoryRetentionStorage(),
            user_id="7",
            meeting_id="1",
            erased_at=datetime(2026, 7, 14, 12, 30, tzinfo=timezone.utc),
            policy_version="minutes-retention-v1",
        )

    assert state["meeting"] is not None
    assert state["meeting"]["data"]["zaki_retention"]["state"] == "erasing"
    assert state["transcript_rows"] == 2
    assert state["session_rows"] == 1


async def test_full_erasure_purges_redis_outside_the_database_transaction():
    state = {
        "meeting": {"id": 1, "user_id": 7, "data": {}},
        "transcript_rows": 1,
        "session_rows": 0,
    }
    session = FakeDbSession(state)
    session.active = False
    redis_c = OutsideTransactionRedis(session)
    repo = SqlAlchemyRetentionRepo(
        lambda: session,
        redis_client=redis_c,
        statement_factory=lambda sql: sql,
    )

    receipt = await erase_meeting(
        repo,
        InMemoryRetentionStorage(),
        user_id="7",
        meeting_id="1",
        erased_at=datetime(2026, 7, 14, 12, 30, tzinfo=timezone.utc),
        policy_version="minutes-retention-v1",
    )

    assert receipt is not None
    assert state["meeting"] is None
    await redis_c.aclose()


async def test_full_erasure_purges_only_the_owned_meeting_redis_carriers():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    for meeting_id in (1, 2):
        await redis_c.xadd(
            "transcription_segments",
            {
                "payload": json.dumps(
                    {
                        "type": "transcription",
                        "meeting_id": meeting_id,
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
    state = {
        "meeting": {"id": 1, "user_id": 7, "data": {}},
        "transcript_rows": 1,
        "session_rows": 0,
    }
    session = FakeDbSession(state)
    repo = SqlAlchemyRetentionRepo(
        lambda: session,
        redis_client=redis_c,
        statement_factory=lambda sql: sql,
    )

    receipt = await erase_meeting(
        repo,
        InMemoryRetentionStorage(),
        user_id="7",
        meeting_id="1",
        erased_at=datetime(2026, 7, 14, 12, 30, tzinfo=timezone.utc),
        policy_version="minutes-retention-v1",
    )

    assert receipt is not None
    assert state["meeting"] is None
    assert not await redis_c.exists(
        "tc:meeting:1",
        "meeting:1:segments",
        "proc:meeting:1",
        "proc:meeting:1:on",
        "proc:meeting:1:cursor",
    )
    assert not await redis_c.sismember("active_meetings", "1")
    assert await redis_c.zscore("processed_pending", "1") is None
    assert await redis_c.exists(
        "tc:meeting:2",
        "meeting:2:segments",
        "proc:meeting:2",
        "proc:meeting:2:on",
        "proc:meeting:2:cursor",
    ) == 5
    assert await redis_c.sismember("active_meetings", "2")
    assert await redis_c.zscore("processed_pending", "2") == 1
    source_rows = await redis_c.xrange("transcription_segments")
    assert len(source_rows) == 1
    assert json.loads(source_rows[0][1]["payload"])["meeting_id"] == 2
    await redis_c.aclose()


async def test_full_erasure_purges_only_target_webhook_retries_dlq_and_platform_intent():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    queue = RetryQueue(redis_c)

    def envelope(meeting_id):
        return {
            "event_id": f"evt-{meeting_id}",
            "event_type": "meeting.completed",
            "data": {"meeting": {"id": meeting_id, "processed": "private"}},
        }

    # First pair ages into the DLQ; second pair remains queued for retry.
    for meeting_id in (1, 2):
        await queue.enqueue(
            "https://hooks.example.test/meeting",
            envelope(meeting_id),
            webhook_secret=f"secret-{meeting_id}",
            now=0.0,
        )

    async def should_not_deliver(*_args):
        raise AssertionError("aged webhook was delivered")

    await drain_retry_queue(
        redis_c, should_not_deliver, now=MAX_AGE_SECONDS + 1,
    )
    for meeting_id in (1, 2):
        await queue.enqueue(
            "https://hooks.example.test/meeting",
            envelope(meeting_id),
            webhook_secret=f"secret-{meeting_id}",
            now=MAX_AGE_SECONDS + 2,
        )
    outbox = RedisTranscriptFinalizedOutbox(
        redis_c, now=lambda: float(MAX_AGE_SECONDS + 2)
    )
    await outbox.enqueue(1)
    await outbox.enqueue(2)

    state = {
        "meeting": {"id": 1, "user_id": 7, "data": {}},
        "transcript_rows": 0,
        "session_rows": 0,
    }
    repo = SqlAlchemyRetentionRepo(
        lambda: FakeDbSession(state),
        redis_client=redis_c,
        statement_factory=lambda sql: sql,
    )

    receipt = await erase_meeting(
        repo,
        InMemoryRetentionStorage(),
        user_id="7",
        meeting_id="1",
        erased_at=datetime(2026, 7, 16, 12, 30, tzinfo=timezone.utc),
        policy_version="minutes-erasure.v1",
    )

    assert receipt is not None
    queued = [json.loads(raw) for raw in await redis_c.lrange(RETRY_QUEUE_KEY, 0, -1)]
    dead = [json.loads(raw) for raw in await redis_c.lrange(DEAD_LETTER_KEY, 0, -1)]
    assert [entry["meeting_id"] for entry in queued] == [2]
    assert [entry["meeting_id"] for entry in dead] == [2]
    assert "secret-1" not in json.dumps({"queued": queued, "dead": dead})
    assert await redis_c.exists(
        "webhook:meeting:retry:1", "webhook:meeting:dead-letter:1"
    ) == 0
    target_outbox = json.loads(await redis_c.get(outbox._key(1)))
    foreign_outbox = json.loads(await redis_c.get(outbox._key(2)))
    assert target_outbox["state"] == "cancelled"
    assert foreign_outbox["state"] == "pending_finalize"
    assert not await redis_c.sismember(outbox._pending, "1")
    assert await redis_c.sismember(outbox._pending, "2")
    await redis_c.aclose()


async def test_webhook_enqueue_cannot_race_past_the_erasure_cancellation_fence():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    execute_started = asyncio.Event()
    release_execute = asyncio.Event()

    class BlockingPipeline:
        def __init__(self, owner, pipeline):
            self._owner = owner
            self._pipeline = pipeline

        async def __aenter__(self):
            await self._pipeline.__aenter__()
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return await self._pipeline.__aexit__(exc_type, exc, tb)

        def __getattr__(self, name):
            return getattr(self._pipeline, name)

        async def execute(self):
            if self._owner.block_once:
                self._owner.block_once = False
                execute_started.set()
                await release_execute.wait()
            return await self._pipeline.execute()

    class RacingRedis:
        def __init__(self, inner):
            self.inner = inner
            self.block_once = True

        def __getattr__(self, name):
            return getattr(self.inner, name)

        def pipeline(self, *args, **kwargs):
            return BlockingPipeline(self, self.inner.pipeline(*args, **kwargs))

    queue = RetryQueue(RacingRedis(redis_c))
    enqueue = asyncio.create_task(queue.enqueue(
        "https://hooks.example.test/meeting",
        {
            "event_id": "evt-1",
            "event_type": "meeting.completed",
            "data": {"meeting": {"id": 1, "processed": "private"}},
        },
        webhook_secret="private-webhook-secret",
        now=1.0,
    ))
    await execute_started.wait()
    await purge_meeting_webhook_state(redis_c, 1)
    release_execute.set()
    await enqueue

    assert await redis_c.lrange(RETRY_QUEUE_KEY, 0, -1) == []
    assert await redis_c.exists("webhook:meeting:retry:1") == 0
    await redis_c.aclose()


async def test_webhook_erasure_waits_for_an_inflight_direct_delivery_claim():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    queue = RetryQueue(redis_c)
    transport_started = asyncio.Event()
    release_transport = asyncio.Event()
    calls = 0

    class Response:
        status_code = 200

    async def transport(*_args):
        nonlocal calls
        calls += 1
        transport_started.set()
        await release_transport.wait()
        return Response()

    sink = WebhookSink(
        transport,
        queue=queue,
        resolver=lambda _host: ["93.184.216.34"],
    )
    envelope = build_envelope(
        "meeting.completed",
        {"meeting": {"id": 1, "processed": "private"}},
    )
    delivery = asyncio.create_task(sink.deliver(
        "https://hooks.example.test/meeting",
        envelope,
        events_config={"meeting.completed": True},
    ))
    await transport_started.wait()

    erasure = asyncio.create_task(purge_meeting_webhook_state(redis_c, 1))
    await asyncio.sleep(0.02)
    assert erasure.done() is False

    release_transport.set()
    assert (await delivery).status == "delivered"
    await erasure

    # The completed erasure fence prevents every later direct attempt before transport starts.
    suppressed = await sink.deliver(
        "https://hooks.example.test/meeting",
        envelope,
        events_config={"meeting.completed": True},
    )
    assert suppressed.status == "suppressed"
    assert calls == 1
    assert await redis_c.exists("webhook:meeting:inflight:1") == 0
    await redis_c.aclose()


async def test_webhook_erasure_waits_for_an_inflight_retry_delivery_claim():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    queue = RetryQueue(redis_c)
    envelope = build_envelope(
        "meeting.completed",
        {"meeting": {"id": 1, "processed": "private"}},
    )
    await queue.enqueue(
        "https://hooks.example.test/meeting",
        envelope,
        webhook_secret="private-webhook-secret",
        now=0.0,
    )
    transport_started = asyncio.Event()
    release_transport = asyncio.Event()

    class Response:
        status_code = 200

    async def transport(*_args):
        transport_started.set()
        await release_transport.wait()
        return Response()

    drain = asyncio.create_task(
        drain_retry_queue(redis_c, transport, now=BACKOFF_SCHEDULE[0] + 1)
    )
    await transport_started.wait()
    erasure = asyncio.create_task(purge_meeting_webhook_state(redis_c, 1))
    await asyncio.sleep(0.02)
    assert erasure.done() is False

    release_transport.set()
    assert await drain == 1
    await erasure

    assert await redis_c.lrange(RETRY_QUEUE_KEY, 0, -1) == []
    assert await redis_c.exists(
        "webhook:meeting:retry:1",
        "webhook:meeting:dead-letter:1",
        "webhook:meeting:inflight:1",
    ) == 0
    await redis_c.aclose()


async def test_persisted_erasure_retry_prefix_cannot_cross_the_meeting_owner():
    foreign_prefix = "recordings/8/99/session-b/"
    state = {
        "meeting": {
            "id": 1,
            "user_id": 7,
            "data": {
                "zaki_retention": {
                    "state": "erasing",
                    "recording_prefixes": [foreign_prefix],
                    "recording_objects": 1,
                    "transcript_rows": 0,
                    "summary_documents": 0,
                }
            },
        },
        "transcript_rows": 0,
        "session_rows": 0,
    }
    repo = SqlAlchemyRetentionRepo(
        lambda: FakeDbSession(state), statement_factory=lambda sql: sql
    )
    storage = InMemoryRetentionStorage()
    foreign_key = f"{foreign_prefix}audio/master.wav"
    storage.seed(foreign_key, b"other tenant")

    with pytest.raises(ErasureFailed, match="planning requires retry"):
        await erase_meeting(
            repo,
            storage,
            user_id="7",
            meeting_id="1",
            erased_at=datetime(2026, 7, 14, 12, 30, tzinfo=timezone.utc),
            policy_version="minutes-retention-v1",
        )

    assert storage.snapshot(foreign_prefix) == {foreign_key: b"other tenant"}
    assert state["meeting"] is not None


class FakeGateSession:
    def __init__(self, data):
        self.data = data
        self.events: list[str] = []
        self.transaction_lock = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement, params=None):
        sql = " ".join(statement.split())
        if "pg_advisory_xact_lock_shared" in sql:
            self.events.append("lock")
            self.transaction_lock = True
            return FakeDbResult()
        if sql.startswith("SELECT data FROM meetings"):
            self.events.append("state")
            return FakeDbResult(row={"data": self.data} if self.data is not None else None)
        raise AssertionError(f"unexpected SQL: {sql}")

    async def commit(self):
        self.events.append("commit")
        self.transaction_lock = False

    async def rollback(self):
        self.events.append("rollback")
        self.transaction_lock = False

    async def invalidate(self):
        self.events.append("invalidate")
        self.transaction_lock = False


class SignedBigintGateSession(FakeGateSession):
    async def execute(self, statement, params=None):
        sql = " ".join(statement.split())
        if "pg_advisory_xact_lock_shared" in sql:
            if sql != "SELECT pg_advisory_xact_lock_shared(:meeting_lock_key)":
                raise OverflowError("two-key PostgreSQL advisory locks accept int4 ids")
            assert params == {"meeting_lock_key": -2_147_483_648}
        return await super().execute(statement, params)


async def test_recording_adapter_supports_meeting_ids_above_signed_int32():
    session = SignedBigintGateSession({})
    repo = SqlAlchemyRecordingRepo(
        lambda: session,
        statement_factory=lambda sql: sql,
    )

    async with repo.recording_write(2_147_483_648):
        session.events.append("body")

    assert session.events == ["lock", "state", "body", "commit"]


class CancellableRecordingGateSession:
    def __init__(self, *, rollback_fails=False):
        self.events = []
        self.transaction_lock = False
        self.rollback_fails = rollback_fails

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement, params=None):
        sql = " ".join(statement.split())
        if "pg_advisory_xact_lock" in sql:
            self.events.append("lock")
            self.transaction_lock = True
            return FakeDbResult()
        if sql.startswith("SELECT data FROM meetings"):
            self.events.append("state")
            return FakeDbResult(row={"data": {}})
        raise AssertionError(f"unexpected SQL: {sql}")

    async def commit(self):
        self.events.append("commit")
        self.transaction_lock = False

    async def rollback(self):
        self.events.append("rollback")
        if self.rollback_fails:
            raise RuntimeError("connection lost during rollback")
        self.transaction_lock = False

    async def invalidate(self):
        self.events.append("invalidate")
        self.transaction_lock = False


@pytest.mark.parametrize("lease_name", ["recording", "chunk", "manifest"])
async def test_recording_advisory_lease_cancellation_releases_transaction_lock(
    lease_name,
):
    session = CancellableRecordingGateSession()
    repo = SqlAlchemyRecordingRepo(
        lambda: session,
        statement_factory=lambda sql: sql,
    )
    entered = asyncio.Event()
    blocked = asyncio.Event()

    async def writer():
        if lease_name == "recording":
            lease = repo.recording_write(41)
        elif lease_name == "chunk":
            lease = repo.chunk_write("recordings/7/41/session/audio/chunk_000001.webm")
        else:
            lease = repo.manifest_write(41, "audio")
        async with lease:
            entered.set()
            await blocked.wait()

    task = asyncio.create_task(writer())
    await entered.wait()
    assert session.transaction_lock is True
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert session.transaction_lock is False
    expected_prefix = ["lock", "state"] if lease_name == "recording" else ["lock"]
    assert session.events == [*expected_prefix, "rollback"]


@pytest.mark.parametrize("lease_name", ["recording", "chunk", "manifest"])
async def test_recording_advisory_lease_invalidates_when_rollback_is_uncertain(
    lease_name,
):
    session = CancellableRecordingGateSession(rollback_fails=True)
    repo = SqlAlchemyRecordingRepo(
        lambda: session,
        statement_factory=lambda sql: sql,
    )
    if lease_name == "recording":
        lease = repo.recording_write(41)
    elif lease_name == "chunk":
        lease = repo.chunk_write("recordings/7/41/session/audio/chunk_000001.webm")
    else:
        lease = repo.manifest_write(41, "audio")

    with pytest.raises(ValueError, match="body failed"):
        async with lease:
            raise ValueError("body failed")

    assert session.transaction_lock is False
    expected_prefix = ["lock", "state"] if lease_name == "recording" else ["lock"]
    assert session.events == [*expected_prefix, "rollback", "invalidate"]


async def test_recording_adapter_uses_shared_lock_and_refuses_erasing_meeting():
    open_session = FakeGateSession({})
    repo = SqlAlchemyRecordingRepo(
        lambda: open_session,
        statement_factory=lambda sql: sql,
    )

    async with repo.recording_write(1):
        open_session.events.append("body")
    assert open_session.events == ["lock", "state", "body", "commit"]

    erasing_session = FakeGateSession({"zaki_retention": {"state": "erasing"}})
    repo = SqlAlchemyRecordingRepo(
        lambda: erasing_session,
        statement_factory=lambda sql: sql,
    )
    with pytest.raises(RecordingWriteRefused, match="not writable"):
        async with repo.recording_write(1):
            raise AssertionError("erasing meeting entered the write body")
    assert erasing_session.events == ["lock", "state", "rollback"]


async def test_recording_adapter_refuses_audio_expired_meeting():
    session = FakeGateSession(
        {"zaki_retention": {"expired_scopes": ["audio"]}}
    )
    repo = SqlAlchemyRecordingRepo(
        lambda: session,
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(RecordingWriteRefused, match="not writable"):
        async with repo.recording_write(1):
            raise AssertionError("audio-expired meeting entered the write body")

    assert session.events == ["lock", "state", "rollback"]


@pytest.mark.parametrize(
    "retention",
    [
        {
            "state": "corrupt",
            "expired_scopes": [],
            "scope_expiries": {"audio": "2099-01-01T00:00:00+00:00"},
        },
        {
            "state": "open",
            "expired_scopes": ["unknown"],
            "scope_expiries": {"audio": "2099-01-01T00:00:00+00:00"},
        },
        {
            "state": "open",
            "expired_scopes": [],
            "scope_expiries": {"audio": "2020-01-01T00:00:00+00:00"},
        },
    ],
)
async def test_recording_adapter_fails_closed_on_invalid_or_elapsed_retention_authority(
    retention,
):
    session = FakeGateSession({"zaki_retention": retention})
    repo = SqlAlchemyRecordingRepo(
        lambda: session,
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(RecordingWriteRefused, match="not writable"):
        async with repo.recording_write(1):
            raise AssertionError("invalid retention authority entered the write body")

    assert session.events == ["lock", "state", "rollback"]


async def test_recording_adapter_refuses_withdrawn_capture():
    session = FakeGateSession({"zaki_capture": {"state": "withdrawn"}})
    repo = SqlAlchemyRecordingRepo(
        lambda: session,
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(RecordingWriteRefused, match="not writable"):
        async with repo.recording_write(1):
            raise AssertionError("withdrawn capture entered the write body")

    assert session.events == ["lock", "state", "rollback"]


async def test_recording_adapter_fails_closed_when_capture_has_no_retention_authority():
    session = FakeGateSession({"zaki_capture": {"state": "authorized"}})
    repo = SqlAlchemyRecordingRepo(
        lambda: session,
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(RecordingWriteRefused, match="not writable"):
        async with repo.recording_write(1):
            raise AssertionError("capture without retention entered the write body")

    assert session.events == ["lock", "state", "rollback"]


async def test_recording_listing_filters_past_or_malformed_audio_authority_at_read_time():
    class Rows:
        def __init__(self, rows):
            self.rows = rows

        def mappings(self):
            return self

        def all(self):
            return self.rows

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def execute(self, statement, params):
            return Rows(
                [
                    {
                        "id": 41,
                        "data": {
                            "zaki_capture": {"state": "authorized"},
                            "zaki_retention": {
                                "state": "open",
                                "scope_expiries": {"audio": "2020-01-01T00:00:00Z"},
                                "expired_scopes": [],
                            },
                            "recordings": [{"id": 410}],
                        },
                    },
                    {
                        "id": 42,
                        "data": {
                            "zaki_capture": {"state": "authorized"},
                            "zaki_retention": {
                                "state": "open",
                                "scope_expiries": {"audio": "not-a-date"},
                                "expired_scopes": [],
                            },
                            "recordings": [{"id": 420}],
                        },
                    },
                    {
                        "id": 43,
                        "data": {
                            "zaki_capture": {"state": "authorized"},
                            "zaki_retention": {
                                "state": "open",
                                "scope_expiries": {"audio": "2099-01-01T00:00:00Z"},
                                "expired_scopes": [],
                            },
                            "recordings": [{"id": 430}],
                        },
                    },
                ]
            )

    repo = SqlAlchemyRecordingRepo(lambda: Session(), statement_factory=lambda sql: sql)

    assert await repo.list_meeting_recordings(7) == [{"id": 430, "meeting_id": 43}]


@pytest.mark.parametrize("capture", [None, {"state": "denied"}, {"state": "corrupt"}])
async def test_recording_adapter_fails_closed_on_malformed_capture_authority(capture):
    session = FakeGateSession(
        {
            "zaki_capture": capture,
            "zaki_retention": {
                "state": "open",
                "scope_expiries": {"audio": "2099-01-01T00:00:00+00:00"},
                "expired_scopes": [],
            },
        }
    )
    repo = SqlAlchemyRecordingRepo(
        lambda: session,
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(RecordingWriteRefused, match="not writable"):
        async with repo.recording_write(1):
            raise AssertionError("malformed capture authority entered the write body")

    assert session.events == ["lock", "state", "rollback"]
