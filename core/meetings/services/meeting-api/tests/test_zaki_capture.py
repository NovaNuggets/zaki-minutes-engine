"""ZAKI capture profile — authority intersection and visible consent evidence."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json

import pytest
from fastapi.testclient import TestClient

from meeting_api import create_app
from meeting_api.bot_spawn.fakes import FakeRuntimeClient, InMemoryMeetingRepo
from meeting_api.bot_spawn import QuotaExceeded
from meeting_api.lifecycle.stop_router import InMemoryCommandPublisher
from meeting_api.capture import (
    CaptureAuthority,
    CaptureDenial,
    CaptureDenied,
    CaptureTeardownUnconfirmed,
    ZAKI_NOTETAKER_NAME,
    request_capture,
    withdraw_capture as _withdraw_capture_impl,
)
from meeting_api.collector.carriers import (
    carrier_fence_key,
    fence_meeting_redis_carriers,
    zadd_if_carrier_writable,
)
from meeting_api.retention import ScopeExpiries


USER = 7
SECRET = "test-admin-token"
ATTESTED_AT = datetime(2026, 7, 15, 8, 30, tzinfo=timezone.utc)
AUTHORIZED_AT = ATTESTED_AT + timedelta(minutes=1)
VALID_UNTIL = AUTHORIZED_AT + timedelta(minutes=5)
RETENTION_EXPIRIES = ScopeExpiries(
    audio=AUTHORIZED_AT + timedelta(days=1),
    transcript=AUTHORIZED_AT + timedelta(days=7),
    summary=AUTHORIZED_AT + timedelta(days=30),
)
WITHDRAWN_AT = AUTHORIZED_AT + timedelta(minutes=10)


class _AdminStreamResponse:
    def __init__(self, status_code: int, body: dict):
        self.status_code = status_code
        self._body = json.dumps(body).encode()
        self.headers = {"content-length": str(len(self._body))}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def aiter_bytes(self, _chunk_size: int):
        yield self._body


async def _confirmed_carrier_fence(
    meeting_id: int, *, raw: bool, processed: bool
) -> None:
    """Default unit-test port: production composition injects the real Redis fence writer."""
    assert isinstance(meeting_id, int) and meeting_id > 0
    assert raw is True
    assert processed is True


async def withdraw_capture(*args, carrier_fencer=_confirmed_carrier_fence, **kwargs):
    return await _withdraw_capture_impl(
        *args, carrier_fencer=carrier_fencer, **kwargs
    )


def _allowed(native_meeting_id: str = "abc-defg-hij") -> CaptureAuthority:
    return CaptureAuthority(
        operator_enabled=True,
        tenant_enabled=True,
        tenant_attested=True,
        tenant_policy_version="capture-v1",
        tenant_attested_at=ATTESTED_AT,
        user_requested=True,
        quota_permitted=True,
        subject_user_id=USER,
        tenant_id="tenant-a",
        meeting_platform="google_meet",
        native_meeting_id=native_meeting_id,
        authorized_at=AUTHORIZED_AT,
        valid_until=VALID_UNTIL,
        scope_expiries=RETENTION_EXPIRIES,
        grant_id=f"grant-{native_meeting_id}",
    )


async def test_authorized_capture_forces_visible_bot_and_persists_content_free_evidence(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "tok-test")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed(),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        redis_url="redis://redis:6379/0",
        meeting_api_url="http://meeting-api:8080",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )

    invocation = json.loads(runtime.specs[0]["env"]["BOT_CONFIG"])
    assert invocation["botName"] == ZAKI_NOTETAKER_NAME
    assert invocation["recordingEnabled"] is True
    assert invocation["transcribeEnabled"] is True
    assert meeting["data"]["zaki_capture"] == {
        "bot_name": ZAKI_NOTETAKER_NAME,
        "tenant_id": "tenant-a",
        "state": "authorized",
        "tenant_attested": True,
        "tenant_policy_version": "capture-v1",
        "tenant_attested_at": "2026-07-15T08:30:00+00:00",
        "user_requested": True,
        "authorized_at": "2026-07-15T08:31:00+00:00",
        "authority_valid_until": "2026-07-15T08:36:00+00:00",
        "grant_id_sha256": hashlib.sha256(b"grant-abc-defg-hij").hexdigest(),
    }
    assert "transcript" not in json.dumps(meeting["data"]["zaki_capture"]).lower()


async def test_capture_uses_earliest_absolute_scope_expiry_as_hard_workload_deadline(
    monkeypatch,
):
    """Absolute scope deadlines cannot elapse under a still-capturing workload."""
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    expiries = ScopeExpiries(
        audio=AUTHORIZED_AT + timedelta(hours=2),
        transcript=AUTHORIZED_AT + timedelta(hours=3),
        summary=AUTHORIZED_AT + timedelta(minutes=30),
    )

    meeting = await request_capture(
        repo,
        runtime,
        authority=replace(_allowed("retention-deadline"), scope_expiries=expiries),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="retention-deadline",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )

    invocation = json.loads(runtime.specs[0]["env"]["VEXA_BOT_CONFIG"])
    assert invocation["captureExpiresAt"] == expiries.summary.isoformat()
    assert invocation["contractVersion"] == "invocation.v2"
    assert isinstance(invocation["meeting_id"], str)
    assert invocation["managedRetention"] == {
        "policyVersion": "capture-v1",
        "scopeExpiresAt": {
            "audio": expiries.audio.isoformat(),
            "transcript": expiries.transcript.isoformat(),
            "summary": expiries.summary.isoformat(),
        },
    }
    assert runtime.specs[0]["profile"] == "meeting-bot-v2"
    assert runtime.specs[0]["maxLifetimeSec"] == 30 * 60
    assert meeting["data"]["zaki_retention"]["scope_expiries"] == {
        "audio": expiries.audio.isoformat(),
        "transcript": expiries.transcript.isoformat(),
        "summary": expiries.summary.isoformat(),
    }


async def test_managed_capture_ignores_user_stt_and_preserves_operator_in_cluster_backend(monkeypatch):
    import httpx

    monkeypatch.setenv("ADMIN_API_URL", "http://admin-api:8080")
    monkeypatch.setenv("INTERNAL_API_SECRET", "internal-secret")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "http://transcription-service:8000")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "operator-token")
    requested = []

    class SettingsClient:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def stream(self, method, url, headers, follow_redirects):
            assert method == "GET"
            assert follow_redirects is False
            requested.append((url, headers))
            if url.endswith("/internal/users/7/bot-context"):
                return _AdminStreamResponse(200, {"transcription": {
                    "url": "https://user-controlled.example",
                    "token": "user-token",
                }})
            return _AdminStreamResponse(404, {})

    monkeypatch.setattr(httpx, "AsyncClient", SettingsClient)
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    await request_capture(
        repo,
        runtime,
        authority=_allowed("operator-stt"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="operator-stt",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )

    invocation = json.loads(runtime.specs[0]["env"]["BOT_CONFIG"])
    assert requested == [(
        "http://admin-api:8080/internal/settings/transcription",
        {"X-Internal-Secret": "internal-secret"},
    )]
    assert invocation["transcriptionServiceUrl"] == "http://transcription-service:8000"
    assert invocation["transcriptionServiceToken"] == "operator-token"


async def test_managed_capture_prefers_platform_stt_setting_over_env(monkeypatch):
    import httpx

    monkeypatch.setenv("ADMIN_API_URL", "http://admin-api:8080")
    monkeypatch.setenv("INTERNAL_API_SECRET", "internal-secret")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "http://env-transcription:8000")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "env-token")
    requested = []

    class PlatformSettingsClient:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def stream(self, method, url, headers, follow_redirects):
            assert method == "GET"
            assert follow_redirects is False
            requested.append((url, headers))
            return _AdminStreamResponse(200, {
                "key": "transcription",
                "value": {
                    "url": "http://platform-transcription:8000",
                    "token": "platform-token",
                },
            })

    monkeypatch.setattr(httpx, "AsyncClient", PlatformSettingsClient)
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    await request_capture(
        repo,
        runtime,
        authority=_allowed("platform-stt"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="platform-stt",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )

    invocation = json.loads(runtime.specs[0]["env"]["BOT_CONFIG"])
    assert requested == [(
        "http://admin-api:8080/internal/settings/transcription",
        {"X-Internal-Secret": "internal-secret"},
    )]
    assert invocation["transcriptionServiceUrl"] == "http://platform-transcription:8000"
    assert invocation["transcriptionServiceToken"] == "platform-token"


async def test_withdrawal_persists_before_teardown_and_returns_content_free_receipt(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    publisher = InMemoryCommandPublisher()
    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed(),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )

    receipt = await withdraw_capture(
        repo,
        publisher,
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at=WITHDRAWN_AT,
    )

    stored = await repo.find_latest(USER, "google_meet", "abc-defg-hij")
    assert receipt == {
        "meeting_id": meeting["id"],
        "state": "withdrawn",
        "changed": True,
        "withdrawn_at": "2026-07-15T08:41:00+00:00",
    }
    assert stored["status"] == "completed"
    assert stored["end_time"] is not None
    assert stored["data"]["zaki_capture"]["state"] == "withdrawn"
    assert stored["data"]["zaki_capture"]["withdrawal_reason"] == "consent_withdrawn"
    assert stored["data"]["zaki_capture"]["teardown_state"] == "confirmed"
    assert stored["data"]["completion_reason"] == "stopped"
    assert stored["data"]["stop_requested"] is True
    assert publisher.published == [
        (
            f"bot_commands:meeting:{meeting['id']}",
            json.dumps({"action": "leave", "meeting_id": meeting["id"]}),
        )
    ]
    assert runtime.deleted == [meeting["bot_container_id"]]
    assert "transcript" not in json.dumps(receipt).lower()


async def test_withdrawal_fences_delayed_raw_and_processed_writes_before_runtime_teardown(
    monkeypatch, fake_redis
):
    """Durable withdrawal closes both Redis egress classes before the bot can finish stopping."""
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    events = []
    write_results = {}
    repo = InMemoryMeetingRepo()

    async def install_fence(meeting_id: int, *, raw: bool, processed: bool) -> None:
        events.append("carrier_fence")
        await fence_meeting_redis_carriers(
            fake_redis, meeting_id, raw=raw, processed=processed
        )

    class DelayedWriterRuntime(FakeRuntimeClient):
        async def delete_workload(self, workload_id: str) -> None:
            events.append("kernel_delete")
            meeting_id = meeting["id"]
            # The bot's production Lua checks this same raw fence before atomically XADDing the
            # source stream and PUBLISHing the mutable transcript. A delayed completion must see
            # the tombstone and do neither operation.
            write_results["bot"] = (
                await fake_redis.hget(carrier_fence_key(meeting_id), "raw") != "1"
            )
            if write_results["bot"]:
                await fake_redis.xadd(
                    "transcription_segments", {"payload": "private late segment"}
                )
                await fake_redis.publish(
                    f"tc:meeting:{meeting_id}:mutable", "private late segment"
                )
            write_results["processing"] = await zadd_if_carrier_writable(
                fake_redis,
                meeting_id,
                scope="processed",
                key="processed_pending",
                mapping={str(meeting_id): 1.0},
            )
            await super().delete_workload(workload_id)

    runtime = DelayedWriterRuntime()
    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed("withdraw-fence"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="withdraw-fence",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    mutable = fake_redis.pubsub(ignore_subscribe_messages=True)
    await mutable.subscribe(f"tc:meeting:{meeting['id']}:mutable")

    await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        carrier_fencer=install_fence,
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="withdraw-fence",
        withdrawn_at=WITHDRAWN_AT,
    )

    assert events == ["carrier_fence", "kernel_delete"]
    assert write_results == {"bot": False, "processing": False}
    assert await fake_redis.xlen("transcription_segments") == 0
    assert await mutable.get_message(timeout=0.01) is None
    assert await fake_redis.zscore("processed_pending", str(meeting["id"])) is None
    assert await fake_redis.hgetall(carrier_fence_key(meeting["id"])) == {
        "raw": "1",
        "processed": "1",
    }
    await mutable.aclose()


async def test_redis_capture_carrier_fencer_installs_permanent_monotonic_tombstone(
    fake_redis,
):
    from meeting_api.capture import RedisCaptureCarrierFencer

    fence = carrier_fence_key(41)
    await fake_redis.hset(fence, mapping={"raw": "1"})
    await fake_redis.expire(fence, 60)

    await RedisCaptureCarrierFencer(fake_redis)(41, raw=True, processed=True)

    assert await fake_redis.hgetall(fence) == {"raw": "1", "processed": "1"}
    assert await fake_redis.ttl(fence) == -1


async def test_withdrawal_fencer_cancels_queued_webhook_copies_before_returning(fake_redis):
    from meeting_api.capture import RedisCaptureCarrierFencer
    from meeting_api.webhooks import RetryQueue, RedisTranscriptFinalizedOutbox

    queue = RetryQueue(fake_redis)
    await queue.enqueue(
        "https://hooks.example.test/minutes",
        {
            "event_id": "evt-private",
            "event_type": "meeting.completed",
            "data": {"meeting": {"id": 41, "processed": "private"}},
        },
        webhook_secret="private-secret",
        now=1.0,
    )
    outbox = RedisTranscriptFinalizedOutbox(fake_redis, now=lambda: 1.0)
    await outbox.enqueue(41)

    await RedisCaptureCarrierFencer(fake_redis)(41, raw=True, processed=True)

    assert await queue.depth() == 0
    assert json.loads(await fake_redis.get(outbox._key(41)))["state"] == "cancelled"
    assert not await fake_redis.sismember(outbox._pending, "41")


async def test_withdrawal_fence_failure_still_tears_down_and_reports_retryable_pending(
    monkeypatch,
):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    events = []
    repo = InMemoryMeetingRepo()

    async def failing_fence(meeting_id: int, *, raw: bool, processed: bool) -> None:
        events.append(("carrier_fence", meeting_id, raw, processed))
        raise RuntimeError("private redis diagnostic")

    class RecordingRuntime(FakeRuntimeClient):
        async def delete_workload(self, workload_id: str) -> None:
            events.append(("kernel_delete", workload_id))
            await super().delete_workload(workload_id)

    runtime = RecordingRuntime()
    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed("fence-failure"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="fence-failure",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )

    with pytest.raises(CaptureTeardownUnconfirmed) as exc:
        await withdraw_capture(
            repo,
            InMemoryCommandPublisher(),
            carrier_fencer=failing_fence,
            runtime=runtime,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="fence-failure",
            withdrawn_at=WITHDRAWN_AT,
        )

    stored = await repo.find_latest(USER, "google_meet", "fence-failure")
    assert str(exc.value) == "teardown_unconfirmed"
    assert "private" not in str(exc.value)
    assert events == [
        ("carrier_fence", meeting["id"], True, True),
        ("kernel_delete", meeting["bot_container_id"]),
    ]
    assert runtime.deleted == [meeting["bot_container_id"]]
    assert stored["data"]["zaki_capture"]["state"] == "withdrawn"
    assert stored["data"]["zaki_capture"]["teardown_state"] == "confirmed"


async def test_repeated_withdrawal_retries_fence_without_redeleting_confirmed_workload(
    monkeypatch,
):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    fence_attempts = 0

    async def flaky_fence(meeting_id: int, *, raw: bool, processed: bool) -> None:
        nonlocal fence_attempts
        fence_attempts += 1
        if fence_attempts == 1:
            raise RuntimeError("redis temporarily unavailable")

    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed("fence-retry"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="fence-retry",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )

    with pytest.raises(CaptureTeardownUnconfirmed):
        await withdraw_capture(
            repo,
            InMemoryCommandPublisher(),
            carrier_fencer=flaky_fence,
            runtime=runtime,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="fence-retry",
            withdrawn_at=WITHDRAWN_AT,
        )
    receipt = await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        carrier_fencer=flaky_fence,
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="fence-retry",
        withdrawn_at=WITHDRAWN_AT + timedelta(minutes=1),
    )

    assert receipt["changed"] is False
    assert fence_attempts == 2
    assert runtime.deleted == [meeting["bot_container_id"]]


async def test_kernel_delete_precedes_best_effort_leave_publication(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    events = []

    class OrderedRuntime(FakeRuntimeClient):
        async def delete_workload(self, workload_id: str) -> None:
            events.append("kernel_delete")
            await super().delete_workload(workload_id)

    class OrderedPublisher:
        async def publish(self, channel: str, message: str) -> int:
            events.append("redis_publish")
            return 1

    repo = InMemoryMeetingRepo()
    runtime = OrderedRuntime()
    await request_capture(
        repo,
        runtime,
        authority=_allowed("ordered-stop"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="ordered-stop",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )

    await withdraw_capture(
        repo,
        OrderedPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="ordered-stop",
        withdrawn_at=WITHDRAWN_AT,
    )

    assert events == ["kernel_delete", "redis_publish"]


async def test_withdrawal_still_tears_down_booting_workload_when_leave_publish_fails(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    class FailingPublisher:
        async def publish(self, channel: str, message: str) -> None:
            raise RuntimeError("redis unavailable")

    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed(),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )

    receipt = await withdraw_capture(
        repo,
        FailingPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at=WITHDRAWN_AT,
    )

    stored = await repo.find_latest(USER, "google_meet", "abc-defg-hij")
    assert receipt["state"] == "withdrawn"
    assert stored["data"]["zaki_capture"]["state"] == "withdrawn"
    assert runtime.deleted == [meeting["bot_container_id"]]


async def test_withdrawal_tears_down_active_workload_when_leave_publish_fails(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    class FailingPublisher:
        async def publish(self, channel: str, message: str) -> None:
            raise RuntimeError("redis unavailable")

    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed(),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    repo.set_status(meeting["id"], "active")

    receipt = await withdraw_capture(
        repo,
        FailingPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at=WITHDRAWN_AT,
    )

    stored = await repo.find_latest(USER, "google_meet", "abc-defg-hij")
    assert receipt["state"] == "withdrawn"
    assert stored["data"]["zaki_capture"]["state"] == "withdrawn"
    assert runtime.deleted == [meeting["bot_container_id"]]


@pytest.mark.parametrize(
    ("prior_status", "publish_result"),
    [
        ("active", 0),
        ("needs_help", 0),
        ("active", None),
        ("needs_help", None),
        ("active", 1),
        ("needs_help", 1),
    ],
)
async def test_withdrawal_hard_stops_listening_status_regardless_of_subscriber_count(
    monkeypatch, prior_status, publish_result
):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    class ResultPublisher:
        async def publish(self, channel: str, message: str):
            return publish_result

    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed(prior_status),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id=prior_status,
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    repo.set_status(meeting["id"], prior_status)

    receipt = await withdraw_capture(
        repo,
        ResultPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id=prior_status,
        withdrawn_at=WITHDRAWN_AT,
    )

    assert receipt["state"] == "withdrawn"
    assert runtime.deleted == [meeting["bot_container_id"]]


@pytest.mark.parametrize("prior_status", ["active", "needs_help"])
async def test_positive_subscriber_count_cannot_hide_missing_hard_teardown(
    monkeypatch, prior_status
):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    spawn_runtime = FakeRuntimeClient()

    class PositiveSubscriberPublisher:
        async def publish(self, channel: str, message: str) -> int:
            return 1

    await request_capture(
        repo,
        spawn_runtime,
        authority=_allowed(f"subscriber-{prior_status}"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id=f"subscriber-{prior_status}",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    meeting = await repo.find_latest(USER, "google_meet", f"subscriber-{prior_status}")
    repo.set_status(meeting["id"], prior_status)

    with pytest.raises(CaptureTeardownUnconfirmed, match="teardown_unconfirmed"):
        await withdraw_capture(
            repo,
            PositiveSubscriberPublisher(),
            runtime=None,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id=f"subscriber-{prior_status}",
            withdrawn_at=WITHDRAWN_AT,
        )

    stored = await repo.find_latest(USER, "google_meet", f"subscriber-{prior_status}")
    assert stored["status"] == "stopping"
    assert stored["data"]["zaki_capture"]["teardown_state"] == "pending"


@pytest.mark.parametrize("teardown_port", ["missing", "failing"])
async def test_withdrawal_reports_content_free_pending_when_hard_teardown_is_unconfirmed(
    monkeypatch, teardown_port
):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()

    class FailingDeleteRuntime(FakeRuntimeClient):
        async def delete_workload(self, workload_id: str) -> None:
            raise RuntimeError("sensitive runtime diagnostic")

    spawn_runtime = FailingDeleteRuntime() if teardown_port == "failing" else FakeRuntimeClient()
    await request_capture(
        repo,
        spawn_runtime,
        authority=_allowed(teardown_port),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id=teardown_port,
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    withdrawal_runtime = spawn_runtime if teardown_port == "failing" else None

    with pytest.raises(CaptureTeardownUnconfirmed) as exc:
        await withdraw_capture(
            repo,
            InMemoryCommandPublisher(),
            runtime=withdrawal_runtime,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id=teardown_port,
            withdrawn_at=WITHDRAWN_AT,
        )

    stored = await repo.find_latest(USER, "google_meet", teardown_port)
    assert str(exc.value) == "teardown_unconfirmed"
    assert "sensitive" not in str(exc.value)
    assert stored["status"] == "stopping"
    assert stored["data"]["zaki_capture"]["state"] == "withdrawn"
    assert stored["data"]["zaki_capture"]["teardown_state"] == "pending"


async def test_repeated_withdrawal_retries_pending_teardown_then_reuses_confirmation(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()

    class RetryRuntime(FakeRuntimeClient):
        def __init__(self):
            super().__init__()
            self.delete_attempts = 0

        async def delete_workload(self, workload_id: str) -> None:
            self.delete_attempts += 1
            if self.delete_attempts == 1:
                raise RuntimeError("runtime temporarily unavailable")
            if self.delete_attempts > 2:
                raise AssertionError("confirmed teardown must not be repeated")
            self.deleted.append(workload_id)

    runtime = RetryRuntime()
    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed("retry-teardown"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="retry-teardown",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )

    with pytest.raises(CaptureTeardownUnconfirmed):
        await withdraw_capture(
            repo,
            InMemoryCommandPublisher(),
            runtime=runtime,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="retry-teardown",
            withdrawn_at=WITHDRAWN_AT,
        )
    second = await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="retry-teardown",
        withdrawn_at=WITHDRAWN_AT + timedelta(minutes=1),
    )
    third = await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="retry-teardown",
        withdrawn_at=WITHDRAWN_AT + timedelta(minutes=2),
    )

    stored = await repo.find_latest(USER, "google_meet", "retry-teardown")
    assert second == {
        "meeting_id": meeting["id"],
        "state": "withdrawn",
        "changed": False,
        "withdrawn_at": WITHDRAWN_AT.isoformat(),
    }
    assert third == second
    assert runtime.delete_attempts == 2
    assert stored["status"] == "completed"
    assert stored["end_time"] is not None
    assert stored["data"]["zaki_capture"]["teardown_state"] == "confirmed"


async def test_withdrawal_retries_transient_confirmation_write_after_hard_stop(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")

    class FlakyConfirmationRepo(InMemoryMeetingRepo):
        def __init__(self):
            super().__init__()
            self.confirmation_attempts = 0

        async def confirm_capture_teardown(self, *, meeting_id: int) -> bool:
            self.confirmation_attempts += 1
            if self.confirmation_attempts == 1:
                raise RuntimeError("transient database diagnostic")
            return await super().confirm_capture_teardown(meeting_id=meeting_id)

    repo = FlakyConfirmationRepo()
    runtime = FakeRuntimeClient()
    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed("confirmation-retry"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="confirmation-retry",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )

    receipt = await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="confirmation-retry",
        withdrawn_at=WITHDRAWN_AT,
    )

    stored = await repo.find_latest(USER, "google_meet", "confirmation-retry")
    assert receipt["state"] == "withdrawn"
    assert runtime.deleted == [meeting["bot_container_id"]]
    assert repo.confirmation_attempts == 2
    assert stored["data"]["zaki_capture"]["teardown_state"] == "confirmed"


async def test_withdrawal_stays_unconfirmed_when_terminal_cas_does_not_persist(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")

    class RejectedConfirmationRepo(InMemoryMeetingRepo):
        async def confirm_capture_teardown(self, *, meeting_id: int) -> bool:
            return False

    repo = RejectedConfirmationRepo()
    runtime = FakeRuntimeClient()
    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed("confirmation-rejected"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="confirmation-rejected",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )

    with pytest.raises(CaptureTeardownUnconfirmed, match="teardown_unconfirmed"):
        await withdraw_capture(
            repo,
            InMemoryCommandPublisher(),
            runtime=runtime,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="confirmation-rejected",
            withdrawn_at=WITHDRAWN_AT,
        )

    stored = await repo.find_latest(USER, "google_meet", "confirmation-rejected")
    assert runtime.deleted == [meeting["bot_container_id"]]
    assert stored["status"] == "stopping"
    assert stored["data"]["zaki_capture"]["teardown_state"] == "pending"


async def test_late_nonterminal_callback_cannot_resurrect_withdrawn_capture(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed(),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at=WITHDRAWN_AT,
    )

    await repo.update_meeting_status(
        session_uid=repo.sessions[-1]["session_uid"],
        status="active",
    )

    stored = await repo.find_latest(USER, "google_meet", "abc-defg-hij")
    assert stored["id"] == meeting["id"]
    assert stored["status"] == "completed"
    assert stored["end_time"] is not None
    assert stored["data"]["zaki_capture"]["state"] == "withdrawn"


async def test_late_active_callback_is_not_emitted_after_withdrawal(monkeypatch, goldens):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    await request_capture(
        repo,
        runtime,
        authority=_allowed(),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    connection_id = repo.sessions[-1]["session_uid"]
    app = create_app(meeting_repo=repo, runtime=runtime)
    client = TestClient(app)
    joining = {**goldens["joining"], "connection_id": connection_id}
    active = {**goldens["active"], "connection_id": connection_id}
    assert client.post("/bots/internal/callback/lifecycle", json=joining).status_code == 200
    await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at=WITHDRAWN_AT,
    )

    response = client.post("/bots/internal/callback/lifecycle", json=active)

    assert response.status_code == 200
    assert response.json()["meeting_status"] == "completed"
    assert [
        envelope["data"]["status_change"]["new_status"]
        for envelope in app.state.status_change_webhooks
    ] == ["joining"]
    assert app.state.typed_webhooks == []

    completed = {**goldens["completed-stopped"], "connection_id": connection_id}
    completed_response = client.post("/bots/internal/callback/lifecycle", json=completed)

    assert completed_response.status_code == 200
    assert completed_response.json()["meeting_status"] == "completed"
    stored = await repo.find_latest(USER, "google_meet", "abc-defg-hij")
    assert stored["status"] == "completed"
    assert stored["data"]["zaki_capture"]["state"] == "withdrawn"
    assert stored["data"]["zaki_capture"]["teardown_state"] == "confirmed"
    assert stored["data"]["completion_reason"] == "stopped"
    assert [
        (entry["from"], entry["to"])
        for entry in stored["data"]["status_transition"]
    ] == [(None, "joining")]
    assert [
        (
            envelope["data"]["status_change"]["old_status"],
            envelope["data"]["status_change"]["new_status"],
        )
        for envelope in app.state.status_change_webhooks
    ] == [(None, "joining")]
    assert app.state.typed_webhooks == []


async def test_direct_terminal_callback_is_accepted_after_withdrawal_while_joining(monkeypatch, goldens):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    await request_capture(
        repo,
        runtime,
        authority=_allowed(),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    connection_id = repo.sessions[-1]["session_uid"]
    app = create_app(meeting_repo=repo, runtime=runtime)
    client = TestClient(app)
    joining = {**goldens["joining"], "connection_id": connection_id}
    completed = {**goldens["completed-stopped"], "connection_id": connection_id}
    assert client.post("/bots/internal/callback/lifecycle", json=joining).status_code == 200
    await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at=WITHDRAWN_AT,
    )

    response = client.post("/bots/internal/callback/lifecycle", json=completed)

    assert response.status_code == 200
    assert response.json()["meeting_status"] == "completed"
    stored = await repo.find_latest(USER, "google_meet", "abc-defg-hij")
    assert stored["status"] == "completed"
    assert stored["data"]["zaki_capture"]["state"] == "withdrawn"
    assert stored["data"]["zaki_capture"]["teardown_state"] == "confirmed"
    assert stored["data"]["completion_reason"] == "stopped"
    assert [
        (entry["from"], entry["to"])
        for entry in stored["data"]["status_transition"]
    ] == [(None, "joining")]
    assert [
        (
            envelope["data"]["status_change"]["old_status"],
            envelope["data"]["status_change"]["new_status"],
        )
        for envelope in app.state.status_change_webhooks
    ] == [(None, "joining")]
    assert app.state.typed_webhooks == []


async def test_late_conflicting_terminal_callback_cannot_replace_confirmed_consent_stop(
    monkeypatch, goldens
):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    await request_capture(
        repo,
        runtime,
        authority=_allowed(),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    connection_id = repo.sessions[-1]["session_uid"]
    app = create_app(meeting_repo=repo, runtime=runtime)
    client = TestClient(app)
    joining = {**goldens["joining"], "connection_id": connection_id}
    failed = {**goldens["failed-join"], "connection_id": connection_id}
    assert client.post("/bots/internal/callback/lifecycle", json=joining).status_code == 200
    await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at=WITHDRAWN_AT,
    )

    response = client.post("/bots/internal/callback/lifecycle", json=failed)

    stored = await repo.find_latest(USER, "google_meet", "abc-defg-hij")
    assert response.status_code == 409
    assert stored["status"] == "completed"
    assert stored["data"]["completion_reason"] == "stopped"
    assert stored["data"]["zaki_capture"]["teardown_state"] == "confirmed"
    assert [
        envelope["data"]["status_change"]["new_status"]
        for envelope in app.state.status_change_webhooks
    ] == ["joining"]
    assert app.state.typed_webhooks == []


@pytest.mark.parametrize("terminal_case", ["failed-join", "completed-stopped"])
async def test_terminal_cas_winning_after_callback_status_read_suppresses_stale_terminal(
    monkeypatch, goldens, terminal_case
):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")

    class StaleTerminalReadRepo(InMemoryMeetingRepo):
        stale_terminal_read = False

        async def get_status_by_session(self, *, session_uid):
            if self.stale_terminal_read:
                return "stopping"
            return await super().get_status_by_session(session_uid=session_uid)

    class RecordingRedis:
        def __init__(self):
            self.published = []
            self.stream_entries = []

        async def publish(self, channel, message):
            self.published.append((channel, message))
            return 1

        async def xadd(self, stream, fields):
            self.stream_entries.append((stream, fields))
            return "1-0"

    repo = StaleTerminalReadRepo()
    runtime = FakeRuntimeClient()
    redis = RecordingRedis()
    finalized = []

    async def finalize(meeting_id):
        finalized.append(meeting_id)

    await request_capture(
        repo,
        runtime,
        authority=_allowed("terminal-read-race"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="terminal-read-race",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    connection_id = repo.sessions[-1]["session_uid"]
    app = create_app(
        meeting_repo=repo,
        runtime=runtime,
        redis=redis,
        transcript_finalizer=finalize,
    )
    client = TestClient(app)
    joining = {**goldens["joining"], "connection_id": connection_id}
    terminal = {**goldens[terminal_case], "connection_id": connection_id}
    if terminal_case == "completed-stopped":
        terminal["completion_reason"] = "left_alone"
    assert client.post("/bots/internal/callback/lifecycle", json=joining).status_code == 200
    await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="terminal-read-race",
        withdrawn_at=WITHDRAWN_AT,
    )
    redis.published.clear()
    redis.stream_entries.clear()
    finalized.clear()
    # Model the lifecycle handler reading `stopping` just before the terminal CAS commits. Its
    # subsequent row-locked write must still observe and preserve the CAS's completed state.
    repo.stale_terminal_read = True

    response = client.post("/bots/internal/callback/lifecycle", json=terminal)

    stored = await repo.find_latest(USER, "google_meet", "terminal-read-race")
    assert response.status_code == 200
    assert response.json()["meeting_status"] == "completed"
    assert stored["status"] == "completed"
    assert stored["data"]["completion_reason"] == "stopped"
    assert "failure_stage" not in stored["data"]
    assert [
        envelope["data"]["status_change"]["new_status"]
        for envelope in app.state.status_change_webhooks
    ] == ["joining"]
    assert app.state.typed_webhooks == []
    assert redis.published == []
    assert redis.stream_entries == []
    assert finalized == []


async def test_withdrawal_is_idempotent_and_preserves_first_timestamp(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    publisher = InMemoryCommandPublisher()
    await request_capture(
        repo,
        runtime,
        authority=_allowed(),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    first = await withdraw_capture(
        repo,
        publisher,
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at=WITHDRAWN_AT,
    )
    second = await withdraw_capture(
        repo,
        publisher,
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at=WITHDRAWN_AT + timedelta(minutes=5),
    )

    assert first["changed"] is True
    assert second == {**first, "changed": False}
    stored = await repo.find_latest(USER, "google_meet", "abc-defg-hij")
    assert len(publisher.published) == 1
    assert runtime.deleted == [stored["bot_container_id"]]
    assert stored["status"] == "completed"
    assert stored["end_time"] is not None
    assert stored["data"]["zaki_capture"]["teardown_state"] == "confirmed"


@pytest.mark.parametrize("terminal_status", ["completed", "failed"])
async def test_terminal_capture_withdrawal_confirms_without_runtime_and_preserves_outcome(
    monkeypatch, terminal_status
):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    publisher = InMemoryCommandPublisher()
    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed(f"terminal-{terminal_status}"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id=f"terminal-{terminal_status}",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    preserved_end_time = "2026-07-15T08:40:00Z"
    repo._meetings[meeting["id"]]["status"] = terminal_status
    repo._meetings[meeting["id"]]["end_time"] = preserved_end_time
    repo._meetings[meeting["id"]]["data"]["terminal_evidence"] = "preserve-me"

    first = await withdraw_capture(
        repo,
        publisher,
        runtime=None,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id=f"terminal-{terminal_status}",
        withdrawn_at=WITHDRAWN_AT,
    )
    second = await withdraw_capture(
        repo,
        publisher,
        runtime=None,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id=f"terminal-{terminal_status}",
        withdrawn_at=WITHDRAWN_AT + timedelta(minutes=1),
    )

    stored = await repo.find_latest(USER, "google_meet", f"terminal-{terminal_status}")
    assert first["changed"] is True
    assert second == {**first, "changed": False}
    assert stored["status"] == terminal_status
    assert stored["end_time"] == preserved_end_time
    assert stored["data"]["terminal_evidence"] == "preserve-me"
    assert stored["data"]["zaki_capture"]["teardown_state"] == "confirmed"
    assert runtime.deleted == []
    assert publisher.published == []


async def test_legacy_confirmed_withdrawal_terminalizes_without_redeleting(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    publisher = InMemoryCommandPublisher()
    meeting = await request_capture(
        repo,
        runtime,
        authority=_allowed("legacy-confirmed"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="legacy-confirmed",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    repo._meetings[meeting["id"]]["status"] = "stopping"
    repo._meetings[meeting["id"]]["data"]["zaki_capture"].update(
        {
            "state": "withdrawn",
            "withdrawal_reason": "consent_withdrawn",
            "withdrawn_at": WITHDRAWN_AT.isoformat(),
            "teardown_state": "confirmed",
        }
    )

    receipt = await withdraw_capture(
        repo,
        publisher,
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="legacy-confirmed",
        withdrawn_at=WITHDRAWN_AT + timedelta(minutes=1),
    )

    stored = await repo.find_latest(USER, "google_meet", "legacy-confirmed")
    assert receipt["changed"] is False
    assert stored["status"] == "completed"
    assert stored["end_time"] is not None
    assert stored["data"]["zaki_capture"]["teardown_state"] == "confirmed"
    assert runtime.deleted == []
    assert publisher.published == []


async def test_withdrawn_capture_rejects_replay_of_the_same_consent_grant(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    authority = replace(
        _allowed(), grant_id="grant-20260715-0831-tenant-a-user-7"
    )
    first = await request_capture(
        repo,
        runtime,
        authority=authority,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at=AUTHORIZED_AT + timedelta(minutes=1),
    )

    with pytest.raises(CaptureDenied, match="authority_replayed"):
        await request_capture(
            repo,
            runtime,
            authority=authority,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            token_secret=SECRET,
            evaluated_at=AUTHORIZED_AT + timedelta(minutes=2),
        )

    latest = await repo.find_latest(USER, "google_meet", "abc-defg-hij")
    assert latest["id"] == first["id"]
    assert latest["data"]["zaki_capture"]["state"] == "withdrawn"
    assert len(runtime.specs) == 1


async def test_withdrawn_capture_rejects_a_different_grant_authorized_before_withdrawal(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    first_authority = replace(_allowed(), grant_id="grant-a")
    stale_authority = replace(_allowed(), grant_id="grant-b")
    await request_capture(
        repo,
        runtime,
        authority=first_authority,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at=AUTHORIZED_AT + timedelta(minutes=1),
    )

    with pytest.raises(CaptureDenied, match="authority_replayed"):
        await request_capture(
            repo,
            runtime,
            authority=stale_authority,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            token_secret=SECRET,
            evaluated_at=AUTHORIZED_AT + timedelta(minutes=2),
        )

    latest = await repo.find_latest(USER, "google_meet", "abc-defg-hij")
    assert latest["data"]["zaki_capture"]["state"] == "withdrawn"
    assert len(runtime.specs) == 1


async def test_withdrawal_tombstone_does_not_deny_another_tenant(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    await request_capture(
        repo,
        runtime,
        authority=replace(_allowed(), grant_id="grant-a"),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at=AUTHORIZED_AT + timedelta(minutes=1),
    )
    tenant_b_authority = replace(
        _allowed(), tenant_id="tenant-b", grant_id="grant-a"
    )

    meeting = await request_capture(
        repo,
        runtime,
        authority=tenant_b_authority,
        tenant_id="tenant-b",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT + timedelta(minutes=2),
    )

    assert meeting["data"]["zaki_capture"]["tenant_id"] == "tenant-b"
    assert meeting["data"]["zaki_capture"]["state"] == "authorized"
    assert len(runtime.specs) == 2


async def test_withdrawal_is_tenant_scoped_and_mutation_free_on_mismatch(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    publisher = InMemoryCommandPublisher()
    await request_capture(
        repo,
        runtime,
        authority=_allowed(),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )

    with pytest.raises(CaptureDenied) as exc:
        await withdraw_capture(
            repo,
            publisher,
            tenant_id="tenant-b",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            withdrawn_at=WITHDRAWN_AT,
        )

    stored = await repo.find_latest(USER, "google_meet", "abc-defg-hij")
    assert exc.value.code is CaptureDenial.AUTHORITY_SCOPE_MISMATCH
    assert stored["status"] == "requested"
    assert stored["data"]["zaki_capture"]["state"] == "authorized"
    assert publisher.published == []
    assert runtime.deleted == []


async def test_newer_other_tenant_row_does_not_shadow_withdrawal(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    tenant_a = await request_capture(
        repo,
        runtime,
        authority=_allowed(),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        token_secret=SECRET,
        evaluated_at=AUTHORIZED_AT,
    )
    other = dict(repo._meetings[tenant_a["id"]])
    other["id"] = 50
    other["status"] = "completed"
    other["data"] = {
        **other["data"],
        "zaki_capture": {
            **other["data"]["zaki_capture"],
            "tenant_id": "tenant-b",
        },
    }
    repo._meetings[50] = other

    receipt = await withdraw_capture(
        repo,
        InMemoryCommandPublisher(),
        runtime=runtime,
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at=WITHDRAWN_AT,
    )

    assert receipt["meeting_id"] == tenant_a["id"]
    assert repo._meetings[tenant_a["id"]]["data"]["zaki_capture"]["state"] == "withdrawn"
    assert repo._meetings[50]["data"]["zaki_capture"]["state"] == "authorized"


async def test_withdrawal_rejects_malformed_timestamp_without_io():
    repo = InMemoryMeetingRepo()
    publisher = InMemoryCommandPublisher()

    with pytest.raises(CaptureDenied) as exc:
        await withdraw_capture(
            repo,
            publisher,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            withdrawn_at="2026-07-15T08:41:00Z",
        )

    assert exc.value.code is CaptureDenial.USER_REQUEST_INVALID
    assert publisher.published == []


@pytest.mark.parametrize(
    ("authority", "denial"),
    [
        (replace(_allowed("blocked-meeting"), operator_enabled=False), CaptureDenial.OPERATOR_DISABLED),
        (replace(_allowed("blocked-meeting"), operator_enabled=None), CaptureDenial.OPERATOR_POLICY_INVALID),
        (replace(_allowed("blocked-meeting"), tenant_enabled=False), CaptureDenial.TENANT_DISABLED),
        (replace(_allowed("blocked-meeting"), tenant_attested=False), CaptureDenial.TENANT_ATTESTATION_REQUIRED),
        (replace(_allowed("blocked-meeting"), tenant_policy_version="  "), CaptureDenial.TENANT_POLICY_INVALID),
        (
            replace(_allowed("blocked-meeting"), tenant_attested_at=datetime(2026, 7, 15, 8, 30)),
            CaptureDenial.TENANT_POLICY_INVALID,
        ),
        (
            replace(
                _allowed("blocked-meeting"),
                tenant_attested_at=AUTHORIZED_AT + timedelta(seconds=1),
            ),
            CaptureDenial.TENANT_POLICY_INVALID,
        ),
        (replace(_allowed("blocked-meeting"), user_requested=False), CaptureDenial.USER_NOT_REQUESTED),
        (replace(_allowed("blocked-meeting"), quota_permitted=False), CaptureDenial.QUOTA_EXHAUSTED),
    ],
)
async def test_capture_denials_are_named_and_mutation_free(monkeypatch, authority, denial):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    with pytest.raises(CaptureDenied) as exc:
        await request_capture(
            repo,
            runtime,
            authority=authority,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="blocked-meeting",
            token_secret=SECRET,
            evaluated_at=AUTHORIZED_AT,
        )

    assert exc.value.code is denial
    assert await repo.find_latest(USER, "google_meet", "blocked-meeting") is None
    assert runtime.specs == []


async def test_atomic_concurrency_quota_denial_is_translated_and_mutation_free(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    with pytest.raises(CaptureDenied) as exc:
        await request_capture(
            repo,
            runtime,
            authority=_allowed("quota-blocked"),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="quota-blocked",
            max_concurrent=0,
            token_secret=SECRET,
            evaluated_at=AUTHORIZED_AT,
        )

    assert exc.value.code is CaptureDenial.QUOTA_EXHAUSTED
    assert await repo.find_latest(USER, "google_meet", "quota-blocked") is None
    assert runtime.specs == []


async def test_capture_authority_cannot_be_replayed_for_another_user(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    with pytest.raises(CaptureDenied) as exc:
        await request_capture(
            repo,
            runtime,
            authority=replace(_allowed(), subject_user_id=USER + 1),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            token_secret=SECRET,
            evaluated_at=AUTHORIZED_AT,
        )

    assert exc.value.code is CaptureDenial.AUTHORITY_SCOPE_MISMATCH
    assert await repo.find_latest(USER, "google_meet", "abc-defg-hij") is None
    assert runtime.specs == []


async def test_capture_authority_missing_subject_binding_is_rejected(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    with pytest.raises(CaptureDenied) as exc:
        await request_capture(
            repo,
            runtime,
            authority=replace(_allowed(), subject_user_id=None),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            token_secret=SECRET,
            evaluated_at=AUTHORIZED_AT,
        )

    assert exc.value.code is CaptureDenial.AUTHORITY_SCOPE_MISMATCH
    assert await repo.find_latest(USER, "google_meet", "abc-defg-hij") is None
    assert runtime.specs == []


async def test_capture_authority_is_bound_to_tenant_and_exact_meeting(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    authority = replace(
        _allowed(),
        subject_user_id=USER,
        tenant_id="tenant-a",
        meeting_platform="google_meet",
        native_meeting_id="allowed-meeting",
    )

    with pytest.raises(CaptureDenied) as exc:
        await request_capture(
            repo,
            runtime,
            authority=authority,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="different-meeting",
            token_secret=SECRET,
            evaluated_at=AUTHORIZED_AT,
        )

    assert exc.value.code is CaptureDenial.AUTHORITY_SCOPE_MISMATCH
    assert await repo.find_latest(USER, "google_meet", "different-meeting") is None
    assert runtime.specs == []


async def test_capture_authority_missing_tenant_or_meeting_binding_is_rejected(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    with pytest.raises(CaptureDenied) as exc:
        await request_capture(
            repo,
            runtime,
            authority=replace(
                _allowed(),
                tenant_id=None,
                meeting_platform=None,
                native_meeting_id=None,
            ),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            token_secret=SECRET,
            evaluated_at=AUTHORIZED_AT,
        )

    assert exc.value.code is CaptureDenial.AUTHORITY_SCOPE_MISMATCH
    assert await repo.find_latest(USER, "google_meet", "abc-defg-hij") is None
    assert runtime.specs == []


async def test_capture_authority_missing_validity_window_is_rejected(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    with pytest.raises(CaptureDenied) as exc:
        await request_capture(
            repo,
            runtime,
            authority=replace(_allowed(), authorized_at=None, valid_until=None),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            evaluated_at=AUTHORIZED_AT,
            token_secret=SECRET,
        )

    assert exc.value.code is CaptureDenial.AUTHORITY_EXPIRED
    assert await repo.find_latest(USER, "google_meet", "abc-defg-hij") is None
    assert runtime.specs == []


async def test_expired_capture_authority_is_rejected_before_io(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    authority = replace(
        _allowed(),
        authorized_at=AUTHORIZED_AT,
        valid_until=VALID_UNTIL,
    )

    with pytest.raises(CaptureDenied) as exc:
        await request_capture(
            repo,
            runtime,
            authority=authority,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            evaluated_at=VALID_UNTIL,
            token_secret=SECRET,
        )

    assert exc.value.code is CaptureDenial.AUTHORITY_EXPIRED
    assert await repo.find_latest(USER, "google_meet", "abc-defg-hij") is None
    assert runtime.specs == []


async def test_authorized_capture_materializes_per_scope_retention(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    expiries = ScopeExpiries(
        audio=AUTHORIZED_AT + timedelta(days=1),
        transcript=AUTHORIZED_AT + timedelta(days=7),
        summary=AUTHORIZED_AT + timedelta(days=30),
    )

    meeting = await request_capture(
        repo,
        runtime,
        authority=replace(_allowed(), scope_expiries=expiries),
        tenant_id="tenant-a",
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        evaluated_at=AUTHORIZED_AT,
        token_secret=SECRET,
    )

    assert meeting["data"]["zaki_retention"] == {
        "state": "open",
        "scope_expiries": {
            "audio": "2026-07-16T08:31:00+00:00",
            "transcript": "2026-07-22T08:31:00+00:00",
            "summary": "2026-08-14T08:31:00+00:00",
        },
        "expired_scopes": [],
    }


async def test_capture_without_retention_policy_is_rejected_before_io(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    with pytest.raises(CaptureDenied) as exc:
        await request_capture(
            repo,
            runtime,
            authority=replace(_allowed(), scope_expiries=None),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            evaluated_at=AUTHORIZED_AT,
            token_secret=SECRET,
        )

    assert exc.value.code is CaptureDenial.RETENTION_POLICY_INVALID
    assert await repo.find_latest(USER, "google_meet", "abc-defg-hij") is None
    assert runtime.specs == []


async def test_capture_rejects_unsafe_meeting_url_before_io(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    with pytest.raises(CaptureDenied) as exc:
        await request_capture(
            repo,
            runtime,
            authority=_allowed(),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            meeting_url="http://169.254.169.254/latest/meta-data",
            evaluated_at=AUTHORIZED_AT,
            token_secret=SECRET,
        )

    assert exc.value.code is CaptureDenial.MEETING_URL_INVALID
    assert await repo.find_latest(USER, "google_meet", "abc-defg-hij") is None
    assert runtime.specs == []


async def test_capture_authority_is_bound_to_exact_explicit_meeting_url(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    allowed_url = "https://meet.google.com/abc-defg-hij"
    authority = replace(
        _allowed(),
        meeting_url_sha256=hashlib.sha256(allowed_url.encode()).hexdigest(),
    )

    with pytest.raises(CaptureDenied) as exc:
        await request_capture(
            repo,
            runtime,
            authority=authority,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            meeting_url="https://meet.google.com/xyz-abcd-efg",
            evaluated_at=AUTHORIZED_AT,
            token_secret=SECRET,
        )

    assert exc.value.code is CaptureDenial.AUTHORITY_SCOPE_MISMATCH
    assert await repo.find_latest(USER, "google_meet", "abc-defg-hij") is None
    assert runtime.specs == []


async def test_runtime_quota_rejection_is_a_named_terminal_non_capture(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient(quota_exceeded=True)

    with pytest.raises(CaptureDenied) as exc:
        await request_capture(
            repo,
            runtime,
            authority=_allowed("runtime-quota"),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="runtime-quota",
            evaluated_at=AUTHORIZED_AT,
            token_secret=SECRET,
        )

    assert exc.value.code is CaptureDenial.QUOTA_EXHAUSTED
    meeting = await repo.find_latest(USER, "google_meet", "runtime-quota")
    assert meeting["status"] == "failed"
    assert meeting["data"]["zaki_capture"]["state"] == "denied"
    assert meeting["data"]["zaki_capture"]["denial"] == "quota_exhausted"
    assert repo.sessions == []


async def test_runtime_rejection_cannot_overwrite_concurrent_withdrawal(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()

    class DelayedQuotaRuntime(FakeRuntimeClient):
        def __init__(self):
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def create_workload(self, spec):
            self.specs.append(spec)
            self.started.set()
            await self.release.wait()
            raise QuotaExceeded("owner quota exceeded")

    runtime = DelayedQuotaRuntime()
    capture = asyncio.create_task(
        request_capture(
            repo,
            runtime,
            authority=_allowed("runtime-race"),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="runtime-race",
            evaluated_at=AUTHORIZED_AT,
            token_secret=SECRET,
        )
    )
    await runtime.started.wait()
    with pytest.raises(CaptureTeardownUnconfirmed):
        await withdraw_capture(
            repo,
            InMemoryCommandPublisher(),
            runtime=runtime,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="runtime-race",
            withdrawn_at=WITHDRAWN_AT,
        )
    runtime.release.set()

    with pytest.raises(CaptureDenied) as exc:
        await capture

    meeting = await repo.find_latest(USER, "google_meet", "runtime-race")
    assert exc.value.code is CaptureDenial.QUOTA_EXHAUSTED
    assert meeting["data"]["zaki_capture"]["state"] == "withdrawn"
    assert meeting["data"]["zaki_capture"]["withdrawn_at"] == WITHDRAWN_AT.isoformat()


async def test_spawn_withdraw_race_does_not_report_capture_when_teardown_is_unconfirmed(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()

    class DelayedRuntime(FakeRuntimeClient):
        def __init__(self):
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def create_workload(self, spec):
            self.specs.append(spec)
            self.started.set()
            await self.release.wait()
            return {"workloadId": spec["workloadId"], "state": "starting"}

        async def delete_workload(self, workload_id: str) -> None:
            raise RuntimeError("sensitive runtime diagnostic")

    runtime = DelayedRuntime()
    capture = asyncio.create_task(
        request_capture(
            repo,
            runtime,
            authority=_allowed("spawn-withdraw-race"),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="spawn-withdraw-race",
            evaluated_at=AUTHORIZED_AT,
            token_secret=SECRET,
        )
    )
    await runtime.started.wait()

    with pytest.raises(CaptureTeardownUnconfirmed):
        await withdraw_capture(
            repo,
            InMemoryCommandPublisher(),
            runtime=runtime,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="spawn-withdraw-race",
            withdrawn_at=WITHDRAWN_AT,
        )
    runtime.release.set()

    with pytest.raises(CaptureTeardownUnconfirmed) as exc:
        await capture

    meeting = await repo.find_latest(USER, "google_meet", "spawn-withdraw-race")
    assert str(exc.value) == "teardown_unconfirmed"
    assert "sensitive" not in str(exc.value)
    assert meeting["status"] == "stopping"
    assert meeting["data"]["zaki_capture"]["state"] == "withdrawn"
    assert meeting["data"]["zaki_capture"]["teardown_state"] == "pending"


async def test_spawn_withdraw_race_persists_confirmed_compensating_teardown(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()

    class DelayedRuntime(FakeRuntimeClient):
        def __init__(self):
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def create_workload(self, spec):
            self.specs.append(spec)
            self.started.set()
            await self.release.wait()
            return {"workloadId": spec["workloadId"], "state": "starting"}

    runtime = DelayedRuntime()
    capture = asyncio.create_task(
        request_capture(
            repo,
            runtime,
            authority=_allowed("spawn-withdraw-confirmed"),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="spawn-withdraw-confirmed",
            evaluated_at=AUTHORIZED_AT,
            token_secret=SECRET,
        )
    )
    await runtime.started.wait()

    with pytest.raises(CaptureTeardownUnconfirmed):
        await withdraw_capture(
            repo,
            InMemoryCommandPublisher(),
            runtime=runtime,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="spawn-withdraw-confirmed",
            withdrawn_at=WITHDRAWN_AT,
        )
    runtime.release.set()
    response = await capture

    stored = await repo.find_latest(USER, "google_meet", "spawn-withdraw-confirmed")
    assert response["status"] == "completed"
    assert runtime.deleted == [response["bot_container_id"]]
    assert stored["status"] == "completed"
    assert stored["end_time"] is not None
    assert stored["data"]["completion_reason"] == "stopped"
    assert stored["data"]["zaki_capture"]["state"] == "withdrawn"
    assert stored["data"]["zaki_capture"]["teardown_state"] == "confirmed"


async def test_spawn_withdraw_race_fails_closed_when_terminal_cas_does_not_persist(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")

    class RejectedConfirmationRepo(InMemoryMeetingRepo):
        async def confirm_capture_teardown(self, *, meeting_id: int) -> bool:
            return False

    class DelayedRuntime(FakeRuntimeClient):
        def __init__(self):
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def create_workload(self, spec):
            self.specs.append(spec)
            self.started.set()
            await self.release.wait()
            return {"workloadId": spec["workloadId"], "state": "starting"}

    repo = RejectedConfirmationRepo()
    runtime = DelayedRuntime()
    capture = asyncio.create_task(
        request_capture(
            repo,
            runtime,
            authority=_allowed("spawn-withdraw-cas-rejected"),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="spawn-withdraw-cas-rejected",
            evaluated_at=AUTHORIZED_AT,
            token_secret=SECRET,
        )
    )
    await runtime.started.wait()
    with pytest.raises(CaptureTeardownUnconfirmed):
        await withdraw_capture(
            repo,
            InMemoryCommandPublisher(),
            runtime=runtime,
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="spawn-withdraw-cas-rejected",
            withdrawn_at=WITHDRAWN_AT,
        )
    runtime.release.set()

    with pytest.raises(CaptureTeardownUnconfirmed, match="teardown_unconfirmed"):
        await capture

    stored = await repo.find_latest(USER, "google_meet", "spawn-withdraw-cas-rejected")
    assert runtime.deleted == [stored["bot_container_id"]]
    assert stored["status"] == "stopping"
    assert stored["data"]["zaki_capture"]["teardown_state"] == "pending"


async def test_spawn_refreshes_stale_row_when_withdrawal_commits_after_container_write(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")

    class PausedStatusRepo(InMemoryMeetingRepo):
        def __init__(self):
            super().__init__()
            self.status_check_started = asyncio.Event()
            self.release_status_check = asyncio.Event()

        async def set_bot_container(self, *, meeting_id: int, bot_container_id: str) -> dict:
            row = await super().set_bot_container(
                meeting_id=meeting_id,
                bot_container_id=bot_container_id,
            )
            return deepcopy(row)

        async def get_status_by_session(self, *, session_uid: str):
            self.status_check_started.set()
            await self.release_status_check.wait()
            return await super().get_status_by_session(session_uid=session_uid)

    repo = PausedStatusRepo()
    runtime = FakeRuntimeClient()
    capture = asyncio.create_task(
        request_capture(
            repo,
            runtime,
            authority=_allowed("stale-row-race"),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="stale-row-race",
            evaluated_at=AUTHORIZED_AT,
            token_secret=SECRET,
        )
    )
    await repo.status_check_started.wait()

    with pytest.raises(CaptureTeardownUnconfirmed):
        await withdraw_capture(
            repo,
            InMemoryCommandPublisher(),
            tenant_id="tenant-a",
            user_id=USER,
            platform="google_meet",
            native_meeting_id="stale-row-race",
            withdrawn_at=WITHDRAWN_AT,
        )
    repo.release_status_check.set()
    response = await capture

    stored = await repo.find_latest(USER, "google_meet", "stale-row-race")
    assert response["status"] == "completed"
    assert response["end_time"] is not None
    assert response["data"]["zaki_capture"]["state"] == "withdrawn"
    assert response["data"]["zaki_capture"]["teardown_state"] == "confirmed"
    assert runtime.deleted == [response["bot_container_id"]]
    assert stored["status"] == "completed"
    assert stored["end_time"] is not None
    assert stored["data"]["completion_reason"] == "stopped"
    assert stored["data"]["zaki_capture"]["teardown_state"] == "confirmed"
