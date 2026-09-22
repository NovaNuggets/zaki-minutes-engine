"""Operator-owned transcript.finalized delivery is durable and secret-separated."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import fakeredis.aioredis
import jsonschema
import pytest
from fastapi.testclient import TestClient
from referencing import Registry, Resource

from meeting_api import create_app
from meeting_api.bot_spawn.fakes import InMemoryMeetingRepo
from meeting_api.meeting_writes import TranscriptFinalizationOutcome
from meeting_api.webhooks import (
    DeliveryResult,
    MinutesPlatformWebhookSink,
    RedisTranscriptFinalizedOutbox,
    build_minutes_finalized_envelope,
    verify_signature,
)


PLATFORM_SECRET = "operator-minutes-platform-secret"
MAX_BIGINT = 9_223_372_036_854_775_807


def _contract_schema(name: str, schema_file: str) -> dict:
    rel = Path("meetings") / "contracts" / name / schema_file
    for parent in Path(__file__).resolve().parents:
        candidate = parent / rel
        if candidate.is_file():
            return json.loads(candidate.read_text())
    raise FileNotFoundError(f"{name} schema not found")


_MINUTES_SCHEMA = _contract_schema(
    "minutes-finalized.v1", "minutes-finalized.schema.json"
)
_MINUTES_REGISTRY = Registry().with_resource(
    _MINUTES_SCHEMA["$id"], Resource.from_contents(_MINUTES_SCHEMA)
)
_WEBHOOK_SCHEMA = _contract_schema("webhook.v1", "webhook.schema.json")
_WEBHOOK_REGISTRY = Registry().with_resource(
    _WEBHOOK_SCHEMA["$id"], Resource.from_contents(_WEBHOOK_SCHEMA)
)


def _conforms(schema: dict, registry: Registry, obj: object, shape: str) -> None:
    jsonschema.Draft202012Validator(
        {"$ref": f"{schema['$id']}#/$defs/{shape}"}, registry=registry
    ).validate(obj)


class _Response:
    status_code = 200


class _Transport:
    def __init__(self):
        self.deliveries = []

    async def __call__(self, url, body, headers):
        self.deliveries.append((url, body, headers))
        return _Response()


class _UserSink:
    def __init__(self):
        self.events = []

    async def deliver(self, _url, envelope, _secret=None, **kwargs):
        events = kwargs.get("events_config") or {}
        if not events.get(envelope["event_type"], False):
            return DeliveryResult(status="suppressed")
        self.events.append(envelope)
        return DeliveryResult(status="delivered", status_code=200)


class _Finalizer:
    def __init__(self, failures=0):
        self.failures = failures
        self.calls = []

    async def __call__(self, meeting_id):
        self.calls.append(meeting_id)
        if self.failures:
            self.failures -= 1
            raise RuntimeError("private transcript database detail")


class _CancelledFinalizer(_Finalizer):
    async def __call__(self, meeting_id):
        self.calls.append(meeting_id)
        return TranscriptFinalizationOutcome(state="cancelled")


def _minutes_data():
    return {
        "zaki_capture": {"state": "authorized"},
        "zaki_retention": {
            "state": "open",
            "expired_scopes": [],
            "scope_expiries": {
                "audio": "2099-01-01T00:00:00+00:00",
                "transcript": "2099-01-01T00:00:00+00:00",
                "summary": "2099-01-01T00:00:00+00:00",
            },
        },
    }


async def _repo(*, user_webhook=False, minutes=True):
    repo = InMemoryMeetingRepo()
    data = _minutes_data() if minutes else {}
    if user_webhook:
        data = {**data,
            "webhook_url": "https://user-hook.example.test/minutes",
            "webhook_secret": "user-owned-secret",
            "webhook_events": {"transcript.finalized": True},
        }
    meeting = await repo.create_meeting(
        user_id=7,
        platform="google_meet",
        native_meeting_id="platform-finalized",
        data=data,
    )
    await repo.create_session(meeting_id=meeting["id"], session_uid="platform-session")
    return repo, meeting


def _terminal(client):
    for status in ("joining", "active", "completed"):
        body = {"connection_id": "platform-session", "status": status}
        if status == "completed":
            body["completion_reason"] = "stopped"
        assert client.post("/bots/internal/callback/lifecycle", json=body).status_code == 200


def _stack(redis_c, repo, finalizer, transport, *, user_sink=None):
    outbox = RedisTranscriptFinalizedOutbox(redis_c, now=lambda: 1_783_000_000.0)
    sink = MinutesPlatformWebhookSink(
        url="http://hub-api:8080/internal/minutes/finalized",
        key_id="minutes-platform-2026-07",
        secret=PLATFORM_SECRET,
        transport=transport,
        now=lambda: 1_783_000_000.0,
    )
    return TestClient(create_app(
        meeting_repo=repo,
        transcript_finalizer=finalizer,
        minutes_finalized_enabled=True,
        minutes_finalized_outbox=outbox,
        minutes_finalized_sink=sink,
        webhook_sink=user_sink,
    )), outbox


def test_platform_finalized_composition_is_default_off_and_fail_closed():
    assert create_app().state.minutes_finalized_enabled is False
    with pytest.raises(ValueError, match="platform finalized"):
        create_app(minutes_finalized_enabled=True)
    with pytest.raises(ValueError, match="platform finalized"):
        create_app(
            minutes_finalized_outbox=object(),
            minutes_finalized_sink=object(),
        )
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    outbox = RedisTranscriptFinalizedOutbox(redis_c)
    sink = MinutesPlatformWebhookSink(
        url="http://hub-api:8080/internal/minutes/finalized",
        key_id="minutes-platform-2026-07",
        secret=PLATFORM_SECRET,
        transport=_Transport(),
    )
    with pytest.raises(ValueError, match="recovery"):
        create_app(
            meeting_repo=object(),
            transcript_finalizer=_Finalizer(),
            minutes_finalized_enabled=True,
            minutes_finalized_outbox=outbox,
            minutes_finalized_sink=sink,
        )
    for url in (
        "file:///tmp/finalized",
        "http://user:password@agent-api:8080/finalized",
        "http://agent-api:8080/finalized#credential",
    ):
        with pytest.raises(ValueError, match="configuration"):
            MinutesPlatformWebhookSink(
                url=url,
                key_id="minutes-platform-2026-07",
                secret=PLATFORM_SECRET,
                transport=_Transport(),
            )


def test_operator_sink_delivers_without_user_webhook_configuration():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    repo, meeting = asyncio.run(_repo())
    transport = _Transport()
    finalizer = _Finalizer()
    client, _outbox = _stack(redis_c, repo, finalizer, transport)

    _terminal(client)

    assert finalizer.calls == [meeting["id"]]
    assert len(transport.deliveries) == 1
    url, body, headers = transport.deliveries[0]
    event = json.loads(body)
    assert url == "http://hub-api:8080/internal/minutes/finalized"
    assert event["event_type"] == "transcript.finalized"
    assert event["data"]["meeting_id"] == str(meeting["id"])
    assert headers["X-Webhook-Key-Id"] == "minutes-platform-2026-07"
    assert "X-Webhook-Signature" in headers
    assert "Authorization" not in headers
    assert set(headers) == {
        "Content-Type",
        "X-Webhook-Key-Id",
        "X-Webhook-Signature",
        "X-Webhook-Timestamp",
    }
    _conforms(_MINUTES_SCHEMA, _MINUTES_REGISTRY, event, "Envelope")
    _conforms(_MINUTES_SCHEMA, _MINUTES_REGISTRY, headers, "SignatureHeaders")
    assert verify_signature(
        body,
        headers,
        PLATFORM_SECRET,
        now=lambda: 1_783_000_000.0,
    )


def test_ordinary_terminal_meeting_is_flushed_without_platform_event():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    repo, meeting = asyncio.run(_repo(minutes=False))
    transport = _Transport()
    finalizer = _Finalizer()
    client, outbox = _stack(redis_c, repo, finalizer, transport)

    _terminal(client)

    assert finalizer.calls == [meeting["id"]]
    assert transport.deliveries == []
    assert asyncio.run(outbox.depth()) == 0


def test_outbox_tombstones_cancelled_authority_without_platform_delivery():
    async def exercise():
        redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
        transport = _Transport()
        outbox = RedisTranscriptFinalizedOutbox(redis_c, now=lambda: 1_783_000_000.0)
        sink = MinutesPlatformWebhookSink(
            url="http://hub-api:8080/internal/minutes/finalized",
            key_id="minutes-platform-2026-07",
            secret=PLATFORM_SECRET,
            transport=transport,
            now=lambda: 1_783_000_000.0,
        )
        finalizer = _CancelledFinalizer()
        assert await outbox.enqueue(17) is True

        result = await outbox.process(17, finalizer, sink)

        assert result.stage == "cancelled"
        assert result.delivered is True
        assert result.envelope is None
        assert result.newly_finalized is False
        assert finalizer.calls == [17]
        assert transport.deliveries == []
        assert await outbox.depth() == 0
        assert await outbox.enqueue(17) is False
        await redis_c.aclose()

    asyncio.run(exercise())


def test_expired_outbox_worker_cannot_regress_successor_delivery_tombstone():
    async def exercise():
        redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
        outbox = RedisTranscriptFinalizedOutbox(redis_c, now=lambda: 1_783_000_000.0)
        await outbox.enqueue(19)

        async def takeover(_url, _body, _headers):
            await redis_c.set(outbox._lock_key(19), "successor-token")
            await redis_c.set(outbox._key(19), json.dumps({
                "version": "minutes-finalized-outbox.v1",
                "meeting_id": 19,
                "state": "delivered",
                "attempts": 0,
                "created_at": 1_783_000_000.0,
            }))
            await redis_c.srem(outbox._pending, "19")
            raise RuntimeError("stale transport completion")

        sink = MinutesPlatformWebhookSink(
            url="http://hub-api:8080/internal/minutes/finalized",
            key_id="minutes-platform-2026-07",
            secret=PLATFORM_SECRET,
            transport=takeover,
        )

        result = await outbox.process(19, _Finalizer(), sink)

        assert result.stage == "delivery"
        stored = json.loads(await redis_c.get(outbox._key(19)))
        assert stored["state"] == "delivered"
        assert "envelope" not in stored
        assert await outbox.depth() == 0
        await redis_c.aclose()

    asyncio.run(exercise())


def test_platform_event_never_crosses_the_legacy_user_webhook_boundary():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    repo, _meeting = asyncio.run(_repo(user_webhook=True))
    user_sink = _UserSink()
    client, _outbox = _stack(
        redis_c, repo, _Finalizer(), _Transport(), user_sink=user_sink
    )

    _terminal(client)

    assert user_sink.events == []


def test_minutes_finalized_builder_is_exact_content_free_and_bigint_safe():
    event = build_minutes_finalized_envelope(
        MAX_BIGINT, created_at="2026-07-15T12:30:00Z"
    )

    assert event == {
        "version": "minutes-finalized.v1",
        "event_id": f"evt_transcript_finalized_{MAX_BIGINT}",
        "event_type": "transcript.finalized",
        "created_at": "2026-07-15T12:30:00Z",
        "data": {
            "meeting_id": str(MAX_BIGINT),
            "artifact": "transcript",
            "state": "finalized",
            "idempotency_key": f"minutes:meeting:{MAX_BIGINT}:transcript",
        },
    }
    _conforms(_MINUTES_SCHEMA, _MINUTES_REGISTRY, event, "Envelope")
    with pytest.raises(jsonschema.ValidationError):
        _conforms(_WEBHOOK_SCHEMA, _WEBHOOK_REGISTRY, event, "Envelope")
    with pytest.raises(jsonschema.ValidationError):
        _conforms(
            _WEBHOOK_SCHEMA,
            _WEBHOOK_REGISTRY,
            "transcript.finalized",
            "EventType",
        )


@pytest.mark.parametrize("meeting_id", [True, 0, -1, MAX_BIGINT + 1])
def test_minutes_finalized_builder_rejects_non_canonical_meeting_rows(meeting_id):
    with pytest.raises(ValueError, match="positive PostgreSQL bigint"):
        build_minutes_finalized_envelope(meeting_id)


@pytest.mark.parametrize("meeting_id", [True, 0, -1, MAX_BIGINT + 1])
def test_minutes_finalized_outbox_rejects_non_canonical_meeting_rows(meeting_id):
    outbox = RedisTranscriptFinalizedOutbox(
        fakeredis.aioredis.FakeRedis(decode_responses=True)
    )
    with pytest.raises(ValueError, match="positive PostgreSQL bigint"):
        asyncio.run(outbox.enqueue(meeting_id))


def test_minutes_finalized_contract_forbids_bearer_and_extra_content():
    event = build_minutes_finalized_envelope(
        41, created_at="2026-07-15T12:30:00Z"
    )
    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Key-Id": "minutes-platform-2026-07",
        "X-Webhook-Timestamp": "1784118600",
        "X-Webhook-Signature": "sha256=" + "a" * 64,
    }

    _conforms(_MINUTES_SCHEMA, _MINUTES_REGISTRY, headers, "SignatureHeaders")
    for required in headers:
        missing = {key: value for key, value in headers.items() if key != required}
        with pytest.raises(jsonschema.ValidationError):
            _conforms(
                _MINUTES_SCHEMA,
                _MINUTES_REGISTRY,
                missing,
                "SignatureHeaders",
            )
    for extra in (
        {"Authorization": "Bearer forbidden"},
        {"X-Internal-Debug": "forbidden"},
    ):
        with pytest.raises(jsonschema.ValidationError):
            _conforms(
                _MINUTES_SCHEMA,
                _MINUTES_REGISTRY,
                {**headers, **extra},
                "SignatureHeaders",
            )

    with pytest.raises(jsonschema.ValidationError):
        _conforms(
            _MINUTES_SCHEMA,
            _MINUTES_REGISTRY,
            {
                **event,
                "data": {
                    **event["data"],
                    "transcript": "raw content must never cross this edge",
                },
            },
            "Envelope",
        )


def test_failed_finalization_is_retried_on_terminal_replay_and_not_lost(capsys):
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    repo, meeting = asyncio.run(_repo())
    transport = _Transport()
    finalizer = _Finalizer(failures=1)
    client, outbox = _stack(redis_c, repo, finalizer, transport)

    _terminal(client)
    assert transport.deliveries == []
    assert asyncio.run(outbox.depth()) == 1

    replay = client.post(
        "/bots/internal/callback/lifecycle",
        json={
            "connection_id": "platform-session",
            "status": "completed",
            "completion_reason": "stopped",
        },
    )

    assert replay.status_code == 200
    assert finalizer.calls == [meeting["id"], meeting["id"]]
    assert len(transport.deliveries) == 1
    assert asyncio.run(outbox.depth()) == 0
    captured = capsys.readouterr()
    assert PLATFORM_SECRET not in captured.out
    assert PLATFORM_SECRET not in captured.err


def test_terminal_commit_to_redis_crash_window_is_backfilled_on_replay():
    """A restart after the terminal DB commit but before Redis enqueue cannot lose intent."""

    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    repo, meeting = asyncio.run(_repo(user_webhook=True))
    # Model the exact crash window: the durable terminal write committed, while the
    # process died before ``minutes_finalized_outbox.enqueue`` touched Redis.
    repo.set_status(meeting["id"], "completed")
    transport = _Transport()
    finalizer = _Finalizer()
    user_sink = _UserSink()
    client, outbox = _stack(
        redis_c, repo, finalizer, transport, user_sink=user_sink
    )

    replay = client.post(
        "/bots/internal/callback/lifecycle",
        json={
            "connection_id": "platform-session",
            "status": "completed",
            "completion_reason": "stopped",
        },
    )

    assert replay.status_code == 200
    assert finalizer.calls == [meeting["id"]]
    assert len(transport.deliveries) == 1
    event = json.loads(transport.deliveries[0][1])
    assert event["data"] == {
        "meeting_id": str(meeting["id"]),
        "artifact": "transcript",
        "state": "finalized",
        "idempotency_key": f"minutes:meeting:{meeting['id']}:transcript",
    }
    assert user_sink.events == []
    assert asyncio.run(outbox.depth()) == 0


def test_recovery_backfill_is_idempotent_after_successful_delivery():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    repo, meeting = asyncio.run(_repo())
    repo.set_status(meeting["id"], "completed")
    transport = _Transport()
    finalizer = _Finalizer()
    client, outbox = _stack(redis_c, repo, finalizer, transport)
    terminal = {
        "connection_id": "platform-session",
        "status": "completed",
        "completion_reason": "stopped",
    }

    assert client.post(
        "/bots/internal/callback/lifecycle", json=terminal
    ).status_code == 200
    assert client.post(
        "/bots/internal/callback/lifecycle", json=terminal
    ).status_code == 200

    assert finalizer.calls == [meeting["id"]]
    assert len(transport.deliveries) == 1
    assert asyncio.run(outbox.depth()) == 0
    tombstone = json.loads(asyncio.run(redis_c.get(outbox._key(meeting["id"]))))
    assert tombstone == {
        "version": "minutes-finalized-outbox.v1",
        "meeting_id": meeting["id"],
        "state": "delivered",
        "attempts": 0,
        "created_at": 1_783_000_000.0,
    }
    assert "envelope" not in tombstone


def test_startup_backfill_advances_through_bounded_terminal_history():
    async def seed():
        repo, first = await _repo()
        repo.set_status(first["id"], "completed")
        for index in range(2, 106):
            meeting = await repo.create_meeting(
                user_id=7,
                platform="google_meet",
                native_meeting_id=f"backfill-{index}",
                data=_minutes_data(),
            )
            repo.set_status(meeting["id"], "completed")
        return repo

    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    repo = asyncio.run(seed())
    transport = _Transport()
    finalizer = _Finalizer()
    client, outbox = _stack(redis_c, repo, finalizer, transport)

    # Recovery is intentionally bounded to 100 rows per tick, but the cursor must
    # advance so older terminal commits are not stranded behind a busy batch.
    asyncio.run(client.app.state.minutes_finalized_drain())
    asyncio.run(client.app.state.minutes_finalized_drain())

    assert sorted(finalizer.calls) == list(range(1, 106))
    assert len(transport.deliveries) == 105
    assert asyncio.run(outbox.depth()) == 0


def test_recovery_scan_excludes_ordinary_and_withdrawn_terminal_rows():
    async def seed():
        repo, minutes = await _repo()
        repo.set_status(minutes["id"], "completed")
        ordinary = await repo.create_meeting(
            user_id=7, platform="google_meet", native_meeting_id="ordinary", data={},
        )
        repo.set_status(ordinary["id"], "completed")
        withdrawn = await repo.create_meeting(
            user_id=7,
            platform="google_meet",
            native_meeting_id="withdrawn",
            data={**_minutes_data(), "zaki_capture": {"state": "withdrawn"}},
        )
        repo.set_status(withdrawn["id"], "completed")
        return repo, minutes["id"]

    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    repo, minutes_id = asyncio.run(seed())
    finalizer = _Finalizer()
    transport = _Transport()
    client, _outbox = _stack(redis_c, repo, finalizer, transport)

    asyncio.run(client.app.state.minutes_finalized_drain())

    assert finalizer.calls == [minutes_id]
    assert len(transport.deliveries) == 1


def test_recovery_scan_failure_does_not_block_already_durable_intent():
    class ScanUnavailableRepo(InMemoryMeetingRepo):
        async def list_terminal_meeting_ids(self, **_kwargs):
            raise RuntimeError("private database connection detail")

    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    repo = ScanUnavailableRepo()
    transport = _Transport()
    finalizer = _Finalizer()
    client, outbox = _stack(redis_c, repo, finalizer, transport)
    asyncio.run(outbox.enqueue(71))

    results = asyncio.run(client.app.state.minutes_finalized_drain())

    assert len(results) == 1 and results[0].delivered is True
    assert finalizer.calls == [71]
    assert len(transport.deliveries) == 1
    assert asyncio.run(outbox.depth()) == 0


def test_outbox_survives_restart_and_never_persists_operator_secret():
    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    repo, meeting = asyncio.run(_repo())
    failed_transport = _Transport()
    first_finalizer = _Finalizer(failures=1)
    first, old_outbox = _stack(redis_c, repo, first_finalizer, failed_transport)
    _terminal(first)
    assert asyncio.run(old_outbox.depth()) == 1

    redis_dump = asyncio.run(_redis_dump(redis_c))
    meeting_dump = json.dumps(repo._meetings[meeting["id"]])
    assert PLATFORM_SECRET not in redis_dump
    assert PLATFORM_SECRET not in meeting_dump

    transport = _Transport()
    new_outbox = RedisTranscriptFinalizedOutbox(redis_c, now=lambda: 1_783_000_001.0)
    sink = MinutesPlatformWebhookSink(
        url="http://hub-api:8080/internal/minutes/finalized",
        key_id="minutes-platform-2026-07",
        secret=PLATFORM_SECRET,
        transport=transport,
        now=lambda: 1_783_000_001.0,
    )
    second_finalizer = _Finalizer()

    results = asyncio.run(new_outbox.drain(second_finalizer, sink))

    assert len(results) == 1
    assert results[0].delivered is True
    assert second_finalizer.calls == [meeting["id"]]
    assert len(transport.deliveries) == 1
    assert PLATFORM_SECRET.encode() not in transport.deliveries[0][1]
    assert asyncio.run(new_outbox.depth()) == 0


def test_tampered_pending_envelope_is_not_delivered():
    class _UnavailableTransport(_Transport):
        async def __call__(self, url, body, headers):
            self.deliveries.append((url, body, headers))
            response = _Response()
            response.status_code = 503
            return response

    redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
    repo, meeting = asyncio.run(_repo())
    first, outbox = _stack(redis_c, repo, _Finalizer(), _UnavailableTransport())
    _terminal(first)
    assert asyncio.run(outbox.depth()) == 1
    key = asyncio.run(redis_c.keys("minutes:transcript-finalized:outbox:1"))[0]
    entry = json.loads(asyncio.run(redis_c.get(key)))
    entry["envelope"]["data"]["meeting_id"] = 999
    asyncio.run(redis_c.set(key, json.dumps(entry)))

    transport = _Transport()
    sink = MinutesPlatformWebhookSink(
        url="http://hub-api:8080/internal/minutes/finalized",
        key_id="minutes-platform-2026-07",
        secret=PLATFORM_SECRET,
        transport=transport,
    )
    result = asyncio.run(outbox.process(meeting["id"], _Finalizer(), sink))

    assert result.stage == "invalid"
    assert transport.deliveries == []
    assert asyncio.run(outbox.depth()) == 1


def test_pending_delivery_uses_current_operator_key_after_restart_rotation():
    class _UnavailableTransport(_Transport):
        async def __call__(self, url, body, headers):
            self.deliveries.append((url, body, headers))
            response = _Response()
            response.status_code = 503
            return response

    async def scenario():
        redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
        outbox = RedisTranscriptFinalizedOutbox(
            redis_c, now=lambda: 1_783_000_000.0
        )
        await outbox.enqueue(17)
        old_sink = MinutesPlatformWebhookSink(
            url="http://hub-api:8080/internal/minutes/finalized",
            key_id="minutes-platform-2026-06",
            secret="old-platform-secret",
            transport=_UnavailableTransport(),
            now=lambda: 1_783_000_000.0,
        )
        first_finalizer = _Finalizer()
        first = await outbox.process(17, first_finalizer, old_sink)
        assert first.stage == "delivery"
        assert first_finalizer.calls == [17]

        redis_dump = await _redis_dump(redis_c)
        assert "minutes-platform-2026-06" not in redis_dump
        assert "old-platform-secret" not in redis_dump

        transport = _Transport()
        new_sink = MinutesPlatformWebhookSink(
            url="http://hub-api:8080/internal/minutes/finalized",
            key_id="minutes-platform-2026-07",
            secret=PLATFORM_SECRET,
            transport=transport,
            now=lambda: 1_783_000_001.0,
        )
        restarted = RedisTranscriptFinalizedOutbox(
            redis_c, now=lambda: 1_783_000_001.0
        )
        second_finalizer = _Finalizer()
        results = await restarted.drain(second_finalizer, new_sink)

        assert len(results) == 1 and results[0].delivered is True
        assert second_finalizer.calls == []
        assert len(transport.deliveries) == 1
        _url, body, headers = transport.deliveries[0]
        assert headers["X-Webhook-Key-Id"] == "minutes-platform-2026-07"
        assert verify_signature(
            body,
            headers,
            PLATFORM_SECRET,
            now=lambda: 1_783_000_001.0,
        )
        final_dump = await _redis_dump(redis_c)
        assert "minutes-platform-2026-07" not in final_dump
        assert PLATFORM_SECRET not in final_dump

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("attempts", "1"),
        ("attempts", -1),
        ("created_at", "not-a-timestamp"),
        ("created_at", True),
        ("created_at", float("inf")),
    ],
)
def test_corrupt_durable_counters_and_timestamps_fail_before_side_effects(
    field, value
):
    async def scenario():
        redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
        outbox = RedisTranscriptFinalizedOutbox(redis_c, now=lambda: 1_783_000_000.0)
        await outbox.enqueue(23)
        key = outbox._key(23)
        entry = json.loads(await redis_c.get(key))
        entry[field] = value
        await redis_c.set(key, json.dumps(entry))
        finalizer = _Finalizer()
        transport = _Transport()
        sink = MinutesPlatformWebhookSink(
            url="http://hub-api:8080/internal/minutes/finalized",
            key_id="minutes-platform-2026-07",
            secret=PLATFORM_SECRET,
            transport=transport,
        )

        result = await outbox.process(23, finalizer, sink)

        assert result.stage == "invalid"
        assert finalizer.calls == []
        assert transport.deliveries == []

    asyncio.run(scenario())


def test_corrupt_pending_members_do_not_starve_valid_finalization_work():
    async def scenario():
        redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)
        outbox = RedisTranscriptFinalizedOutbox(redis_c, now=lambda: 1_783_000_000.0)
        await outbox.enqueue(31)
        await redis_c.sadd(
            outbox._pending,
            "0",
            "-1",
            "01",
            str(MAX_BIGINT + 1),
            "not-a-row",
        )
        finalizer = _Finalizer()
        transport = _Transport()
        sink = MinutesPlatformWebhookSink(
            url="http://hub-api:8080/internal/minutes/finalized",
            key_id="minutes-platform-2026-07",
            secret=PLATFORM_SECRET,
            transport=transport,
        )

        results = await outbox.drain(finalizer, sink)

        assert [result.meeting_id for result in results] == [31]
        assert finalizer.calls == [31]
        assert len(transport.deliveries) == 1
        assert await outbox.depth() == 0

    asyncio.run(scenario())


def test_drain_samples_only_the_requested_bounded_pending_batch():
    async def scenario():
        redis_c = fakeredis.aioredis.FakeRedis(decode_responses=True)

        class BoundedRedis:
            def __init__(self):
                self.sample_sizes = []

            async def smembers(self, _key):
                raise AssertionError("drain must not materialize the full pending set")

            async def srandmember(self, key, number=None):
                self.sample_sizes.append(number)
                return await redis_c.srandmember(key, number=number)

            def __getattr__(self, name):
                return getattr(redis_c, name)

        bounded = BoundedRedis()
        outbox = RedisTranscriptFinalizedOutbox(
            bounded, now=lambda: 1_783_000_000.0
        )
        for meeting_id in (41, 42, 43):
            await outbox.enqueue(meeting_id)
        finalizer = _Finalizer()
        transport = _Transport()
        sink = MinutesPlatformWebhookSink(
            url="http://hub-api:8080/internal/minutes/finalized",
            key_id="minutes-platform-2026-07",
            secret=PLATFORM_SECRET,
            transport=transport,
        )

        results = await outbox.drain(finalizer, sink, limit=2)

        assert bounded.sample_sizes == [2]
        assert len(results) == 2
        assert len(finalizer.calls) == 2
        assert len(transport.deliveries) == 2
        assert await outbox.depth() == 1

    asyncio.run(scenario())


def test_expired_worker_cannot_delete_successor_lock_generation():
    async def scenario():
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

            async def watch(self, *keys):
                return await self._inner.watch(*keys)

            async def get(self, key):
                value = await self._inner.get(key)
                observed.set()
                await resume.wait()
                return value

            async def unwatch(self):
                return await self._inner.unwatch()

            def multi(self):
                return self._inner.multi()

            def delete(self, key):
                return self._inner.delete(key)

            async def execute(self):
                return await self._inner.execute()

        class RedisProxy:
            def pipeline(self, *args, **kwargs):
                return PausingPipeline(redis_c.pipeline(*args, **kwargs))

            def __getattr__(self, name):
                return getattr(redis_c, name)

        outbox = RedisTranscriptFinalizedOutbox(RedisProxy())
        lock_key = outbox._lock_key(29)
        await redis_c.set(lock_key, "expired-worker")
        release = asyncio.create_task(
            outbox._release_lock(lock_key, "expired-worker")
        )
        await observed.wait()
        await redis_c.set(lock_key, "successor-worker", ex=60)
        resume.set()
        await release

        assert await redis_c.get(lock_key) == "successor-worker"

    asyncio.run(scenario())


async def _redis_dump(redis_c) -> str:
    values = []
    async for key in redis_c.scan_iter(match="*"):
        values.append(str(key))
        kind = await redis_c.type(key)
        if kind == "string":
            values.append(str(await redis_c.get(key)))
        elif kind == "set":
            values.extend(map(str, await redis_c.smembers(key)))
    return "\n".join(values)
