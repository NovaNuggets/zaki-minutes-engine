"""Launch-facing managed Minutes capture API over the consent core."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json

from fastapi.testclient import TestClient as _TestClient
import pytest

from meeting_api.app import create_app as _create_app
from meeting_api.bot_spawn.fakes import FakeRuntimeClient, InMemoryMeetingRepo
from meeting_api.capture import CaptureDenial
from meeting_api.managed_minutes import _body, _public_capture_denial
from meeting_api.retention.fakes import InMemoryRetentionRepo, InMemoryRetentionStorage


NOW = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
HUB_TOKEN = "minutes-hub-service-token-0123456789"
MINUTES_ERASURE_SECRET = "minutes-erasure-secret-01234567890123"
USER_HEADERS = {"X-User-Id": "7", "X-User-Limits": "3"}


def create_app(**kwargs):
    kwargs.setdefault("minutes_hub_token", HUB_TOKEN)
    return _create_app(**kwargs)


def TestClient(app, **kwargs):
    headers = dict(kwargs.pop("headers", {}))
    headers.setdefault("X-Zaki-Minutes-Token", HUB_TOKEN)
    return _TestClient(app, headers=headers, **kwargs)


@pytest.fixture(autouse=True)
def _managed_stt(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.example/v1")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "test-stt-token")


class Publisher:
    def __init__(self):
        self.messages = []

    async def publish(self, channel, message):
        self.messages.append((channel, message))
        return 1


class Fencer:
    def __init__(self):
        self.calls = []

    async def __call__(self, meeting_id, *, raw, processed):
        self.calls.append((meeting_id, raw, processed))


class HistoricalAgentEraser:
    async def __call__(self, **_kwargs):  # pragma: no cover - withdrawal does not erase
        raise AssertionError("withdrawal reached account erasure")

    def verify_durable_receipt(self, *_args, **_kwargs):
        return False


def _settings(*, capture_enabled=True):
    async def lookup(user_id: int):
        if user_id != 7:
            return None
        return {
            "operator_enabled": True,
            "capture_enabled": capture_enabled,
            "agent_read_enabled": True,
            "policy_version": "minutes-capture.v1",
            "attested_at": "2026-07-15T11:00:00+00:00",
            "retention_days": {"audio": 7, "transcript": 30, "summary": 30},
        }
    return lookup


def _client(*, enabled=True, settings=None):
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    publisher = Publisher()
    fencer = Fencer()
    client = TestClient(create_app(
        meeting_repo=repo,
        runtime=runtime,
        redis=publisher,
        command_publisher=publisher,
        minutes_capture_enabled=enabled,
        minutes_invocation_v2_enabled=enabled,
        managed_minutes_only=True,
        minutes_settings=settings or _settings(),
        minutes_capture_fencer=fencer,
        minutes_now=lambda: NOW,
        token_secret="test-admin-token",
    ))
    return client, repo, runtime, publisher, fencer


def test_managed_routes_require_hub_auth_before_identity_quota_body_or_settings():
    settings_calls = []

    async def settings(user_id):
        settings_calls.append(user_id)
        return await _settings()(user_id)

    client, _repo, runtime, *_ = _client(settings=settings)
    uncredentialed = _TestClient(client.app)
    for headers in ({}, {"X-Zaki-Minutes-Token": "x" * 32}):
        response = uncredentialed.post(
            "/minutes/captures",
            headers=headers,
            content=b"not-json-and-must-not-be-read",
        )
        assert response.status_code == 401
        assert response.json() == {"detail": "Unauthorized"}
    assert settings_calls == []
    assert runtime.specs == []


def test_default_off_managed_only_vexa_mounts_no_minutes_routes_or_hub_requirement():
    app = _create_app(
        managed_minutes_only=True,
        minutes_settings=_settings(),
        minutes_capture_fencer=Fencer(),
    )
    client = _TestClient(app)

    assert client.post("/bots", json={}).status_code == 403
    assert client.post("/minutes/captures", json={}).status_code == 404
    assert client.get("/minutes/meetings/1/status").status_code == 404


def test_historical_erasure_boundary_requires_hub_auth_at_composition():
    with pytest.raises(ValueError, match="dedicated Hub token"):
        _create_app(
            managed_minutes_only=True,
            minutes_settings=_settings(),
            minutes_capture_fencer=Fencer(),
            token_secret="test-admin-token",
            minutes_retention_repo=InMemoryRetentionRepo(),
            minutes_retention_storage=InMemoryRetentionStorage(),
            minutes_agent_eraser=HistoricalAgentEraser(),
            minutes_erasure_signing_key_id="minutes-erasure-2026-07",
            minutes_erasure_signing_secret=MINUTES_ERASURE_SECRET,
            minutes_erasure_verification_keys={
                "minutes-erasure-2026-07": MINUTES_ERASURE_SECRET,
            },
            minutes_erasure_nonce_factory=lambda: "01J2M3N4P5Q6R7S8T9V0WXYZM1",
        )


@pytest.mark.parametrize(
    "hub_token",
    [None, "short", " " + HUB_TOKEN, HUB_TOKEN + " ", HUB_TOKEN + "\x1f", HUB_TOKEN + "é"],
)
def test_managed_route_composition_rejects_weak_padded_or_non_ascii_hub_token(hub_token):
    with pytest.raises(ValueError, match="dedicated Hub token"):
        _create_app(
            minutes_settings=_settings(),
            minutes_capture_fencer=Fencer(),
            minutes_capture_enabled=True,
            minutes_invocation_v2_enabled=True,
            token_secret="test-admin-token",
            redis=Publisher(),
            minutes_hub_token=hub_token,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        [(b"content-length", b"-1")],
        [(b"content-length", b"not-decimal")],
        [(b"content-length", b"2"), (b"content-length", b"2")],
        [(b"content-length", b"2"), (b"content-length", b"3")],
    ],
)
async def test_capture_body_rejects_negative_non_decimal_or_duplicate_lengths(headers):
    chunks = iter(({"type": "http.request", "body": b"{}", "more_body": False},))

    async def receive():
        return next(chunks)

    from starlette.requests import Request

    request = Request({"type": "http", "headers": headers}, receive)
    with pytest.raises(ValueError, match="request length is invalid"):
        await _body(request)


def test_operator_flag_off_unmounts_the_hub_trusting_management_edge():
    client, _repo, runtime, *_ = _client(enabled=False)
    response = client.post("/minutes/captures", headers=USER_HEADERS, json={})
    assert response.status_code == 404
    assert runtime.specs == []


def test_managed_only_mode_blocks_the_legacy_bot_create_bypass_even_when_capture_is_off():
    runtime = FakeRuntimeClient()
    client = TestClient(create_app(
        runtime=runtime,
        managed_minutes_only=True,
    ))

    response = client.post(
        "/bots",
        headers=USER_HEADERS,
        json={
            "platform": "google_meet",
            "native_meeting_id": "consent-bypass",
            "bot_name": "Hidden recorder",
        },
    )

    assert response.status_code == 403
    assert response.json() == {"error": {"code": "managed_capture_required"}}
    assert runtime.specs == []


def test_managed_only_mode_denies_legacy_create_before_parsing_its_body():
    runtime = FakeRuntimeClient()
    client = TestClient(create_app(
        runtime=runtime,
        managed_minutes_only=True,
    ))

    response = client.post(
        "/bots",
        headers={**USER_HEADERS, "Content-Type": "application/json"},
        content=b"not-json",
    )

    assert response.status_code == 403
    assert response.json() == {"error": {"code": "managed_capture_required"}}
    assert runtime.specs == []


def test_managed_only_mode_still_allows_the_authorized_minutes_route():
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    publisher = Publisher()
    client = TestClient(create_app(
        meeting_repo=repo,
        runtime=runtime,
        redis=publisher,
        command_publisher=publisher,
        managed_minutes_only=True,
        minutes_capture_enabled=True,
        minutes_invocation_v2_enabled=True,
        minutes_settings=_settings(),
        minutes_capture_fencer=Fencer(),
        minutes_now=lambda: NOW,
        token_secret="test-admin-token",
    ))

    response = client.post(
        "/minutes/captures",
        headers=USER_HEADERS,
        json={"platform": "google_meet", "native_meeting_id": "managed-only"},
    )

    assert response.status_code == 201
    assert len(runtime.specs) == 1
    assert runtime.specs[0]["env"]["BOT_CONFIG"]


def test_operator_rollback_keeps_withdrawal_available_for_an_existing_capture():
    client, repo, runtime, publisher, fencer = _client(enabled=True)
    created = client.post(
        "/minutes/captures", headers=USER_HEADERS,
        json={"platform": "google_meet", "native_meeting_id": "rollback-stop"},
    )
    assert created.status_code == 201

    rolled_back = TestClient(create_app(
        meeting_repo=repo,
        runtime=runtime,
        command_publisher=publisher,
        minutes_capture_enabled=False,
        managed_minutes_only=True,
        minutes_settings=_settings(),
        minutes_capture_fencer=fencer,
        minutes_now=lambda: NOW,
        token_secret="test-admin-token",
        minutes_retention_repo=InMemoryRetentionRepo(),
        minutes_retention_storage=InMemoryRetentionStorage(),
        minutes_agent_eraser=HistoricalAgentEraser(),
        minutes_erasure_signing_key_id="minutes-erasure-2026-07",
        minutes_erasure_signing_secret=MINUTES_ERASURE_SECRET,
        minutes_erasure_verification_keys={
            "minutes-erasure-2026-07": MINUTES_ERASURE_SECRET,
        },
        minutes_erasure_nonce_factory=lambda: "01J2M3N4P5Q6R7S8T9V0WXYZM1",
    ))
    stopped = rolled_back.delete(
        "/minutes/captures/google_meet/rollback-stop", headers=USER_HEADERS
    )

    assert stopped.status_code == 200
    assert stopped.json()["state"] == "withdrawn"
    assert runtime.deleted == [repo._meetings[int(created.json()["id"])]["bot_container_id"]]


def test_enabled_capture_requires_settings_and_fencer_at_composition():
    for kwargs in (
        {"minutes_settings": None, "minutes_capture_fencer": Fencer()},
        {"minutes_settings": _settings(), "minutes_capture_fencer": None},
    ):
        try:
            create_app(
                minutes_capture_enabled=True,
                minutes_invocation_v2_enabled=True,
                **kwargs,
            )
        except ValueError as error:
            assert "managed Minutes" in str(error)
        else:  # pragma: no cover
            raise AssertionError("managed capture accepted a missing authority")


def test_enabled_capture_refuses_boot_without_explicit_invocation_v2_route():
    with pytest.raises(ValueError, match="invocation.v2"):
        create_app(
            minutes_capture_enabled=True,
            minutes_settings=_settings(),
            minutes_capture_fencer=Fencer(),
        )


def test_enabled_capture_refuses_boot_without_token_key_or_transcript_bus():
    with pytest.raises(ValueError, match="MeetingToken signing key and transcript bus"):
        create_app(
            minutes_capture_enabled=True,
            minutes_invocation_v2_enabled=True,
            minutes_settings=_settings(),
            minutes_capture_fencer=Fencer(),
        )


def test_launch_derives_consent_visibility_retention_and_quota_server_side():
    client, repo, runtime, _publisher, _fencer = _client()
    response = client.post(
        "/minutes/captures",
        headers=USER_HEADERS,
        json={
            "platform": "google_meet",
            "native_meeting_id": "abc-defg-hij",
        },
    )
    assert response.status_code == 201, response.text
    assert response.json() == {"id": response.json()["id"], "status": "requested"}
    assert isinstance(response.json()["id"], str)
    row = repo._meetings[int(response.json()["id"])]
    assert row["data"]["zaki_capture"] == {
        **row["data"]["zaki_capture"],
        "bot_name": "ZAKI Notetaker",
        "tenant_attested": True,
        "tenant_policy_version": "minutes-capture.v1",
        "user_requested": True,
        "state": "authorized",
    }
    expiries = row["data"]["zaki_retention"]["scope_expiries"]
    assert expiries == {
        "audio": "2026-07-22T12:00:00+00:00",
        "transcript": "2026-08-14T12:00:00+00:00",
        "summary": "2026-08-14T12:00:00+00:00",
    }
    invocation = json.loads(runtime.specs[0]["env"]["VEXA_BOT_CONFIG"])
    assert invocation["botName"] == "ZAKI Notetaker"


def test_capture_rejects_caller_owned_policy_or_bot_overrides_before_spawn():
    client, _repo, runtime, *_ = _client()

    response = client.post(
        "/minutes/captures",
        headers=USER_HEADERS,
        json={
            "platform": "google_meet",
            "native_meeting_id": "abc-defg-hij",
            "bot_name": "Invisible",
            "recording_enabled": False,
            "retention_days": {"audio": 9999},
        },
    )

    assert response.status_code == 422
    assert response.json() == {"detail": "unsupported capture request field"}
    assert runtime.specs == []


def test_capture_fails_closed_when_identity_returns_retention_that_outlives_transcript():
    async def invalid_settings(user_id: int):
        assert user_id == 7
        value = await _settings()(user_id)
        value["retention_days"] = {"audio": 7, "transcript": 30, "summary": 31}
        return value

    client, _repo, runtime, *_ = _client(settings=invalid_settings)
    response = client.post(
        "/minutes/captures",
        headers=USER_HEADERS,
        json={"platform": "google_meet", "native_meeting_id": "abc-defg-hij"},
    )

    assert response.status_code == 403
    assert response.json() == {"error": {"code": "capture_policy_invalid"}}
    assert runtime.specs == []


def test_internal_capture_denials_collapse_to_the_sealed_public_error_taxonomy():
    allowed = {
        "quota_exhausted",
        "capture_disabled",
        "capture_policy_invalid",
        "meeting_url_invalid",
    }
    projected = {
        denial: _public_capture_denial(denial) for denial in CaptureDenial
    }

    assert {code for code, _status in projected.values()} <= allowed
    assert projected[CaptureDenial.MEETING_URL_INVALID] == ("meeting_url_invalid", 422)
    assert projected[CaptureDenial.QUOTA_EXHAUSTED] == ("quota_exhausted", 429)
    assert projected[CaptureDenial.AUTHORITY_SCOPE_MISMATCH] == (
        "capture_policy_invalid", 403,
    )


def test_disabled_user_invalid_native_and_depleted_quota_fail_before_spawn():
    disabled, _repo, disabled_runtime, *_ = _client(settings=_settings(capture_enabled=False))
    response = disabled.post(
        "/minutes/captures", headers=USER_HEADERS,
        json={"platform": "google_meet", "native_meeting_id": "abc-defg-hij"},
    )
    assert response.status_code == 403
    assert disabled_runtime.specs == []

    client, _repo, runtime, *_ = _client()
    oversized_user = client.post(
        "/minutes/captures",
        headers={"X-User-Id": str(2**63), "X-User-Limits": "3"},
        json={"platform": "google_meet", "native_meeting_id": "abc-defg-hij"},
    )
    assert oversized_user.status_code == 401
    assert runtime.specs == []

    traversal = client.post(
        "/minutes/captures", headers=USER_HEADERS,
        json={"platform": "google_meet", "native_meeting_id": "../../secret"},
    )
    assert traversal.status_code == 422
    quota = client.post(
        "/minutes/captures", headers={**USER_HEADERS, "X-User-Limits": "0"},
        json={"platform": "google_meet", "native_meeting_id": "abc-defg-hij"},
    )
    assert quota.status_code == 429
    assert runtime.specs == []


def test_capture_body_is_bounded_before_json_decode():
    client, _repo, runtime, *_ = _client()
    response = client.post(
        "/minutes/captures",
        headers={**USER_HEADERS, "Content-Type": "application/json"},
        content=b'{"padding":"' + b"x" * 20_000 + b'"}',
    )
    assert response.status_code == 413
    assert runtime.specs == []


def test_withdrawal_is_owner_scoped_fenced_and_idempotent():
    client, repo, runtime, publisher, fencer = _client()
    created = client.post(
        "/minutes/captures", headers=USER_HEADERS,
        json={"platform": "google_meet", "native_meeting_id": "abc-defg-hij"},
    )
    assert created.status_code == 201
    first = client.delete(
        "/minutes/captures/google_meet/abc-defg-hij", headers=USER_HEADERS
    )
    second = client.delete(
        "/minutes/captures/google_meet/abc-defg-hij", headers=USER_HEADERS
    )
    assert first.status_code == second.status_code == 200
    assert first.json()["meeting_id"] == created.json()["id"]
    assert first.json()["state"] == "withdrawn"
    assert set(first.json()) == {"meeting_id", "state", "changed", "withdrawn_at"}
    row_id = int(created.json()["id"])
    assert fencer.calls == [(row_id, True, True), (row_id, True, True)]
    assert runtime.deleted == [repo._meetings[row_id]["bot_container_id"]]
    assert all("abc-defg-hij" not in str(receipt) for receipt in (first.json(), second.json()))


def test_managed_openapi_advertises_exact_success_models_and_real_status_codes():
    client, *_ = _client()

    paths = client.app.openapi()["paths"]
    capture = paths["/minutes/captures"]["post"]
    withdrawal = paths[
        "/minutes/captures/{platform}/{native_meeting_id}"
    ]["delete"]
    status = paths["/minutes/meetings/{meeting_id}/status"]["get"]

    assert "201" in capture["responses"] and "200" not in capture["responses"]
    assert capture["responses"]["201"]["content"]["application/json"]["schema"]
    assert withdrawal["responses"]["200"]["content"]["application/json"]["schema"]
    status_ref = status["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
    schemas = client.app.openapi()["components"]["schemas"]
    status_schema = schemas[status_ref.rsplit("/", 1)[-1]]
    assert len(status_schema["oneOf"]) == 3
    branches = {
        ref["$ref"].rsplit("/", 1)[-1]: schemas[ref["$ref"].rsplit("/", 1)[-1]]
        for ref in status_schema["oneOf"]
    }
    assert set(branches["MinutesNonterminalStatusResponse"]["properties"]) == {
        "meeting_id", "status",
    }
    assert set(branches["MinutesCompletedStatusResponse"]["properties"]) == {
        "meeting_id", "status", "completion_reason",
    }
    assert set(branches["MinutesFailedStatusResponse"]["properties"]) == {
        "meeting_id", "status", "failure_stage",
    }
    assert all(branch["additionalProperties"] is False for branch in branches.values())


def test_status_is_exact_owner_scoped_and_uses_public_lifecycle_vocabulary():
    client, repo, *_ = _client()
    created = client.post(
        "/minutes/captures", headers=USER_HEADERS,
        json={"platform": "google_meet", "native_meeting_id": "status-meeting"},
    )
    meeting_id = created.json()["id"]
    row_id = int(meeting_id)

    active = client.get(f"/minutes/meetings/{meeting_id}/status", headers=USER_HEADERS)
    assert active.status_code == 200
    assert active.json() == {"meeting_id": meeting_id, "status": "requested"}

    repo._meetings[row_id]["status"] = "needs_help"
    needs_help = client.get(
        f"/minutes/meetings/{meeting_id}/status", headers=USER_HEADERS
    )
    assert needs_help.json() == {
        "meeting_id": meeting_id,
        "status": "needs_human_help",
    }

    repo._meetings[row_id]["status"] = "completed"
    repo._meetings[row_id]["data"]["completion_reason"] = "stopped"
    completed = client.get(
        f"/minutes/meetings/{meeting_id}/status", headers=USER_HEADERS
    )
    assert completed.json() == {
        "meeting_id": meeting_id,
        "status": "completed",
        "completion_reason": "stopped",
    }

    wrong_owner = client.get(
        f"/minutes/meetings/{meeting_id}/status",
        headers={"X-User-Id": "8", "X-User-Limits": "3"},
    )
    assert wrong_owner.status_code == 404
    assert wrong_owner.json() == {"error": {"code": "meeting_not_found"}}
    noncanonical = client.get(
        f"/minutes/meetings/0{meeting_id}/status", headers=USER_HEADERS
    )
    assert noncanonical.status_code == 404
    assert noncanonical.json() == {"error": {"code": "meeting_not_found"}}


def test_status_rejects_non_rows_and_non_minutes_rows_without_disclosing_them():
    client, repo, *_ = _client()
    # Seed an ordinary meeting without the managed Minutes capture authority.
    ordinary = asyncio.run(repo.create_meeting(
        user_id=7,
        platform="google_meet",
        native_meeting_id="ordinary-meeting",
        data={},
    ))

    for meeting_ref in (
        str(ordinary["id"]), "0", "01", str(2**63), "1" * 100, "not-a-row",
    ):
        response = client.get(
            f"/minutes/meetings/{meeting_ref}/status", headers=USER_HEADERS
        )
        assert response.status_code == 404
        assert response.json() == {"error": {"code": "meeting_not_found"}}


def test_status_fails_loudly_when_storage_or_terminal_attribution_is_invalid():
    client, repo, *_ = _client()
    created = client.post(
        "/minutes/captures", headers=USER_HEADERS,
        json={"platform": "google_meet", "native_meeting_id": "broken-status"},
    )
    meeting_id = created.json()["id"]
    repo._meetings[int(meeting_id)]["status"] = "completed"

    invalid = client.get(
        f"/minutes/meetings/{meeting_id}/status", headers=USER_HEADERS
    )
    assert invalid.status_code == 503
    assert invalid.json() == {"error": {"code": "capture_authority_unavailable"}}

    async def unavailable(*, user_id: int, meeting_id: int):
        raise RuntimeError("private database detail")

    repo.find_owned_minutes = unavailable
    unavailable_response = client.get(
        f"/minutes/meetings/{meeting_id}/status", headers=USER_HEADERS
    )
    assert unavailable_response.status_code == 503
    assert unavailable_response.json() == {
        "error": {"code": "capture_authority_unavailable"}
    }


def test_withdrawal_does_not_depend_on_settings_authority_after_capture_started():
    calls = 0

    async def settings(user_id: int):
        nonlocal calls
        calls += 1
        if calls > 1:
            raise RuntimeError("identity unavailable")
        return await _settings()(user_id)

    client, repo, runtime, _publisher, fencer = _client(settings=settings)
    created = client.post(
        "/minutes/captures", headers=USER_HEADERS,
        json={"platform": "google_meet", "native_meeting_id": "consent-stop"},
    )
    assert created.status_code == 201

    stopped = client.delete(
        "/minutes/captures/google_meet/consent-stop", headers=USER_HEADERS
    )

    assert stopped.status_code == 200
    assert stopped.json()["state"] == "withdrawn"
    row_id = int(created.json()["id"])
    assert runtime.deleted == [repo._meetings[row_id]["bot_container_id"]]
    assert fencer.calls == [(row_id, True, True)]
