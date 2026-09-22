"""bot_spawn — the POST /bots core flow (invocation.v1 + runtime.v1, eager MeetingSession).

Drives the SHIPPED ``request_bot`` / ``build_router`` over the in-memory fakes, OFFLINE (no DB, no
runtime kernel): the invocation + workload spec conform to the sealed contracts, the MeetingSession
is eager-created keyed by the bot's connectionId, and the quota / dedup seams surface 429 / 409.
"""
from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from fastapi.testclient import TestClient

from meeting_api.bot_spawn import (
    QuotaExceeded,
    SpawnFailed,
    TranscriptionNotConfigured,
    UnsafeMeetingUrl,
    build_invocation,
    build_router,
    build_workload_spec,
    mint_meeting_token,
    request_bot,
)
from meeting_api.bot_spawn.fakes import FakeRuntimeClient, InMemoryMeetingRepo
from meeting_api.bot_spawn.invocation import conforms_invocation, conforms_workload_spec

SECRET = "test-admin-token"
USER = 7
HEADERS = {"x-user-id": str(USER)}


class _HttpHandler(BaseHTTPRequestHandler):
    def _handle(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        self.server.requests.append({  # type: ignore[attr-defined]
            "method": self.command,
            "path": self.path,
            "headers": dict(self.headers),
            "body": body,
        })
        status, headers, body = self.server.responder(self)  # type: ignore[attr-defined]
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    do_GET = _handle
    do_POST = _handle

    def log_message(self, _format, *_args):
        pass


@contextmanager
def _serve_http(responder):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _HttpHandler)
    server.requests = []  # type: ignore[attr-defined]
    server.responder = responder  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


# ── unit: invocation + workload spec conform to the sealed contracts ─────────────────────────────

def test_invocation_conforms_to_invocation_v1():
    token = mint_meeting_token(
        1, USER, "google_meet", "abc-defg-hij", session_uid="conn-1", secret=SECRET
    )
    inv = build_invocation(
        meeting_id=1, platform="google_meet",
        meeting_url="https://meet.google.com/abc-defg-hij", bot_name="VexaBot",
        token=token, native_meeting_id="abc-defg-hij", connection_id="conn-1",
        redis_url="redis://redis:6379/0",
    )
    conforms_invocation(inv)  # raises on non-conformance
    assert inv["platform"] == "google_meet"
    assert inv["connectionId"] == "conn-1"
    assert isinstance(inv["meeting_id"], int)
    assert "contractVersion" not in inv


def test_managed_invocation_v2_uses_string_id_and_retention_authority():
    retention = {
        "policyVersion": "minutes-capture.v1",
        "scopeExpiresAt": {
            "audio": "2026-07-17T12:00:00+00:00",
            "transcript": "2026-08-15T12:00:00+00:00",
            "summary": "2026-08-15T12:00:00+00:00",
        },
    }
    inv = build_invocation(
        meeting_id=2**63 - 1, platform="google_meet",
        meeting_url="https://meet.google.com/abc-defg-hij", bot_name="ZAKI Notetaker",
        token="token", native_meeting_id="abc-defg-hij", connection_id="conn-v2",
        redis_url=None, contract_version="invocation.v2",
        transcript_ingest_url="http://meeting-api:8080/bots/internal/transcripts/ingest",
        retention_fence_url="http://meeting-api:8080/bots/internal/transcripts/fence",
        meeting_api_callback_url="http://meeting-api:8080/bots/internal/callback/lifecycle",
        transcription_service_url="https://stt.example/v1",
        transcription_service_token="stt-token",
        recording_enabled=True,
        recording_upload_url="http://meeting-api:8080/internal/recordings/upload",
        capture_modes=["audio", "video"],
        capture_expires_at="2026-07-17T12:00:00+00:00",
        managed_retention=retention,
    )
    conforms_invocation(inv, contract_version="invocation.v2")
    assert inv["contractVersion"] == "invocation.v2"
    assert inv["meeting_id"] == "9223372036854775807"
    assert inv["managedRetention"] == retention
    assert "redisUrl" not in inv
    assert inv["transcriptIngestUrl"].endswith("/bots/internal/transcripts/ingest")


def test_v1_builder_refuses_managed_fields_instead_of_sending_them_to_an_old_bot():
    with pytest.raises(ValueError, match="invocation.v2"):
        build_invocation(
            meeting_id=1, platform="google_meet",
            meeting_url="https://meet.google.com/abc-defg-hij", bot_name="VexaBot",
            token="token", native_meeting_id="abc-defg-hij", connection_id="conn-1",
            redis_url="redis://redis:6379/0",
            capture_expires_at="2026-07-17T12:00:00+00:00",
        )


def test_v2_builder_rejects_deadline_that_is_not_the_earliest_scope_expiry():
    with pytest.raises(ValueError, match="earliest"):
        build_invocation(
            meeting_id=1, platform="google_meet",
            meeting_url="https://meet.google.com/abc-defg-hij", bot_name="ZAKI Notetaker",
            token="token", native_meeting_id="abc-defg-hij", connection_id="conn-1",
            redis_url=None, contract_version="invocation.v2",
            transcript_ingest_url="http://meeting-api:8080/bots/internal/transcripts/ingest",
            retention_fence_url="http://meeting-api:8080/bots/internal/transcripts/fence",
            meeting_api_callback_url="http://meeting-api:8080/bots/internal/callback/lifecycle",
            transcription_service_url="https://stt.example/v1",
            transcription_service_token="stt-token",
            recording_enabled=True,
            recording_upload_url="http://meeting-api:8080/internal/recordings/upload",
            capture_modes=["audio", "video"],
            capture_expires_at="2026-07-18T12:00:00+00:00",
            managed_retention={
                "policyVersion": "minutes-capture.v1",
                "scopeExpiresAt": {
                    "audio": "2026-07-17T12:00:00+00:00",
                    "transcript": "2026-08-15T12:00:00+00:00",
                    "summary": "2026-08-15T12:00:00+00:00",
                },
            },
        )


def test_v2_builder_does_not_retain_malformed_retention_input_in_exception_cause():
    marker = "private-retention-marker"
    with pytest.raises(ValueError, match="retention timestamps") as caught:
        build_invocation(
            meeting_id=1,
            platform="google_meet",
            meeting_url="https://meet.google.com/abc-defg-hij",
            bot_name="ZAKI Notetaker",
            token="token",
            native_meeting_id="abc-defg-hij",
            connection_id="conn-1",
            redis_url=None,
            contract_version="invocation.v2",
            transcript_ingest_url="http://meeting-api:8080/bots/internal/transcripts/ingest",
            retention_fence_url="http://meeting-api:8080/bots/internal/transcripts/fence",
            meeting_api_callback_url="http://meeting-api:8080/bots/internal/callback/lifecycle",
            transcription_service_url="https://stt.example/v1",
            transcription_service_token="stt-token",
            recording_enabled=True,
            recording_upload_url="http://meeting-api:8080/internal/recordings/upload",
            capture_modes=["audio", "video"],
            capture_expires_at=marker,
            managed_retention={
                "policyVersion": "minutes-capture.v1",
                "scopeExpiresAt": {
                    "audio": marker,
                    "transcript": "2026-08-15T12:00:00+00:00",
                    "summary": "2026-08-15T12:00:00+00:00",
                },
            },
        )

    assert caught.value.__cause__ is None
    assert marker not in str(caught.value)


def test_invocation_carries_stt_creds_when_provided():
    """The bot can only transcribe if the invocation carries the STT URL+token (the mock-bot/dashboard
    validation found these were dropped). When provided they ride the invocation; when not, they are
    omitted (None-stripped) and the bot joins+captures without transcribing."""
    token = mint_meeting_token(
        1, USER, "google_meet", "abc-defg-hij", session_uid="conn-1", secret=SECRET
    )
    base = dict(meeting_id=1, platform="google_meet", meeting_url="https://meet.google.com/abc-defg-hij",
                bot_name="VexaBot", token=token, native_meeting_id="abc-defg-hij",
                connection_id="conn-1", redis_url="redis://redis:6379/0")
    inv = build_invocation(**base, transcription_service_url="https://transcription.vexa.ai",
                           transcription_service_token="tok-123")
    conforms_invocation(inv)
    assert inv["transcriptionServiceUrl"] == "https://transcription.vexa.ai"
    assert inv["transcriptionServiceToken"] == "tok-123"
    # absent → omitted, not null
    assert "transcriptionServiceUrl" not in build_invocation(**base)


def test_workload_spec_conforms_to_runtime_v1():
    inv = build_invocation(
        meeting_id=1, platform="google_meet", meeting_url="https://meet.google.com/x",
        bot_name="VexaBot", token="t", native_meeting_id="x", connection_id="conn-1",
        redis_url="redis://redis:6379/0",
    )
    spec = build_workload_spec(workload_id="mtg-1-conn", invocation=inv,
                               callback_url="http://meeting-api:8080/runtime/callback")
    conforms_workload_spec(spec)
    assert spec["profile"] == "meeting-bot"
    # The invocation rides as the ONE BOT_CONFIG env var (12-factor).
    assert json.loads(spec["env"]["BOT_CONFIG"])["connectionId"] == "conn-1"


def test_v2_workload_uses_a_separate_profile_and_explicit_parser_selector():
    inv = build_invocation(
        meeting_id=1, platform="google_meet", meeting_url="https://meet.google.com/x",
        bot_name="ZAKI Notetaker", token="t", native_meeting_id="x", connection_id="conn-v2",
        redis_url=None, contract_version="invocation.v2",
        transcript_ingest_url="http://meeting-api:8080/bots/internal/transcripts/ingest",
        retention_fence_url="http://meeting-api:8080/bots/internal/transcripts/fence",
        meeting_api_callback_url="http://meeting-api:8080/bots/internal/callback/lifecycle",
        transcription_service_url="https://stt.example/v1",
        transcription_service_token="stt-token",
        recording_enabled=True,
        recording_upload_url="http://meeting-api:8080/internal/recordings/upload",
        capture_modes=["audio", "video"],
        capture_expires_at="2026-07-17T12:00:00+00:00",
        managed_retention={
            "policyVersion": "minutes-capture.v1",
            "scopeExpiresAt": {
                "audio": "2026-07-17T12:00:00+00:00",
                "transcript": "2026-08-15T12:00:00+00:00",
                "summary": "2026-08-15T12:00:00+00:00",
            },
        },
    )
    spec = build_workload_spec(
        workload_id="mtg-1-v2", invocation=inv,
        invocation_contract="invocation.v2", profile="meeting-bot-v2",
    )
    conforms_workload_spec(spec)
    assert spec["profile"] == "meeting-bot-v2"
    assert spec["env"]["VEXA_INVOCATION_CONTRACT"] == "invocation.v2"
    assert "redisUrl" not in spec["env"]["VEXA_BOT_CONFIG"]


def test_meeting_token_roundtrips_under_secret():
    from meeting_api.recordings.service import _verify_meeting_token

    token = mint_meeting_token(
        42, USER, "google_meet", "abc", session_uid="conn-token", secret=SECRET
    )
    claims = _verify_meeting_token(token, secret=SECRET)
    assert claims["meeting_id"] == 42
    assert claims["user_id"] == USER
    assert claims["aud"] == ["transcription-collector", "meeting-lifecycle"]
    assert claims["scope"] == "transcribe:write lifecycle:write"
    assert claims["session_uid"] == "conn-token"


# ── flow: request_bot eager-creates the session + writes the container back ──────────────────────

async def test_request_bot_eager_creates_session_and_spawns(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "tok-test")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    meeting = await request_bot(
        repo, runtime, user_id=USER, platform="google_meet",
        native_meeting_id="abc-defg-hij", bot_name="VexaBot",
        redis_url="redis://redis:6379/0", meeting_api_url="http://meeting-api:8080",
        token_secret=SECRET,
    )
    assert meeting["status"] == "requested"
    assert meeting["bot_container_id"] == runtime.specs[0]["workloadId"]
    # The eager MeetingSession is keyed by the bot's connectionId.
    assert len(repo.sessions) == 1
    spawned = json.loads(runtime.specs[0]["env"]["BOT_CONFIG"])
    assert repo.sessions[0]["session_uid"] == spawned["connectionId"]
    assert "internalSecret" not in spawned
    assert "internal-secret" not in runtime.specs[0]["env"]["BOT_CONFIG"]
    assert runtime.specs[0]["env"]["VEXA_BOT_CONFIG"] == runtime.specs[0]["env"]["BOT_CONFIG"]


async def test_new_workload_config_never_contains_the_platform_internal_secret(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "tok-test")
    monkeypatch.setenv("INTERNAL_API_SECRET", "platform-wide-do-not-delegate")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    await request_bot(
        repo,
        runtime,
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        redis_url="redis://redis:6379/0",
        meeting_api_url="http://meeting-api:8080",
        token_secret=SECRET,
    )

    spec = runtime.specs[0]
    for name in ("VEXA_BOT_CONFIG", "BOT_CONFIG"):
        assert "platform-wide-do-not-delegate" not in spec["env"][name]
        assert "internalSecret" not in json.loads(spec["env"][name])


async def test_request_bot_dedup_raises(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "tok-test")
    from meeting_api.bot_spawn import DuplicateMeeting

    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    kw = dict(user_id=USER, platform="google_meet", native_meeting_id="dup",
              redis_url="r", token_secret=SECRET)
    await request_bot(repo, runtime, **kw)
    with pytest.raises(DuplicateMeeting):
        await request_bot(repo, runtime, **kw)


async def test_request_bot_quota_propagates(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "tok-test")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient(quota_exceeded=True)
    with pytest.raises(QuotaExceeded):
        await request_bot(repo, runtime, user_id=USER, platform="google_meet",
                          native_meeting_id="x", redis_url="r", token_secret=SECRET)
    rejected = await repo.find_latest(USER, "google_meet", "x")
    assert rejected["status"] == "failed"
    assert rejected["data"]["failure_stage"] == "requested"
    assert rejected["data"]["spawn_failure_reason"] == "quota_exhausted"


async def test_request_bot_rejects_unsafe_explicit_url_before_any_side_effect(monkeypatch):
    """Every caller, including auto-join, crosses the navigation guard at the spawn sink."""
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    with pytest.raises(UnsafeMeetingUrl):
        await request_bot(
            repo,
            runtime,
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            meeting_url="https://meet.google.com.attacker.example/abc-defg-hij",
            token_secret=SECRET,
        )

    assert runtime.specs == []
    assert await repo.find_latest(USER, "google_meet", "abc-defg-hij") is None


# ── route: POST /bots maps outcomes onto HTTP status ─────────────────────────────────────────────

def _client(repo=None, runtime=None):
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(build_router(repo or InMemoryMeetingRepo(), runtime or FakeRuntimeClient()))
    return TestClient(app)


def test_post_bots_201(monkeypatch):
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "tok-test")
    client = _client()
    r = client.post("/bots", headers=HEADERS,
                    json={"platform": "google_meet", "native_meeting_id": "abc-defg-hij"})
    assert r.status_code == 201, r.text
    assert r.json()["status"] == "requested"


@pytest.mark.parametrize(
    "native_id",
    ["../secret", "room/child", r"room\\child", "room\nchild", "x" * 257, {"room": "x"}],
)
def test_post_bots_rejects_unsafe_native_meeting_identifiers(monkeypatch, native_id):
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "tok-test")
    repo, runtime = InMemoryMeetingRepo(), FakeRuntimeClient()

    response = _client(repo, runtime).post(
        "/bots",
        headers=HEADERS,
        json={"platform": "google_meet", "native_meeting_id": native_id},
    )

    assert response.status_code == 422
    assert runtime.specs == []


def test_post_bots_409_on_duplicate(monkeypatch):
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "tok-test")
    repo, runtime = InMemoryMeetingRepo(), FakeRuntimeClient()
    client = _client(repo, runtime)
    body = {"platform": "google_meet", "native_meeting_id": "dup"}
    assert client.post("/bots", headers=HEADERS, json=body).status_code == 201
    assert client.post("/bots", headers=HEADERS, json=body).status_code == 409


def test_post_bots_429_on_quota(monkeypatch):
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "tok-test")
    client = _client(runtime=FakeRuntimeClient(quota_exceeded=True))
    r = client.post("/bots", headers=HEADERS,
                    json={"platform": "google_meet", "native_meeting_id": "x"})
    assert r.status_code == 429


def test_post_bots_401_without_identity(monkeypatch):
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt.vexa.ai")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "tok-test")
    client = _client()
    r = client.post("/bots", json={"platform": "google_meet", "native_meeting_id": "x"})
    assert r.status_code == 401


def test_post_bots_transcribe_without_stt_fails_loud(monkeypatch):
    """No env TRANSCRIPTION_SERVICE_URL and no Settings backend → 503 when transcribe_enabled."""
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)
    monkeypatch.delenv("TRANSCRIPTION_SERVICE_URL", raising=False)
    monkeypatch.delenv("TRANSCRIPTION_SERVICE_TOKEN", raising=False)
    client = _client()
    r = client.post("/bots", headers=HEADERS,
                    json={"platform": "google_meet", "native_meeting_id": "no-stt"})
    assert r.status_code == 503
    assert "no transcription backend configured" in r.text


def test_post_bots_transcribe_with_settings_stt_passes(monkeypatch):
    """Settings-configured backend (monkeypatched _resolve_transcription_backend) → spawn proceeds."""
    from meeting_api.bot_spawn import service as spawn_service

    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)

    async def fake_resolve(user_id):
        return {"url": "https://stt-settings.example.com"}

    monkeypatch.setattr(spawn_service, "_resolve_transcription_backend", fake_resolve)

    repo, runtime = InMemoryMeetingRepo(), FakeRuntimeClient()
    client = _client(repo, runtime)
    r = client.post("/bots", headers=HEADERS,
                    json={"platform": "google_meet", "native_meeting_id": "settings-stt"})
    assert r.status_code == 201, r.text
    inv = json.loads(runtime.specs[0]["env"]["BOT_CONFIG"])
    assert inv["transcriptionServiceUrl"] == "https://stt-settings.example.com"


# ── Settings → transcription backend: the configured STT (user pref > platform) beats the env ────

async def test_request_bot_configured_transcription_backend_overrides_env(monkeypatch):
    """A backend configured in Settings (resolved by admin-api's bot-context: user pref >
    platform setting) rides the invocation INSTEAD of the process env — including the token:
    the env token belongs to the ENV backend, never to a user-supplied endpoint."""
    from meeting_api.bot_spawn import service as spawn_service

    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt-env.vexa.ai")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "tok-env")

    async def fake_resolve(user_id):
        assert user_id == USER
        return {"url": "https://stt-mine.example.com"}

    monkeypatch.setattr(spawn_service, "_resolve_transcription_backend", fake_resolve)
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    await request_bot(repo, runtime, user_id=USER, platform="google_meet",
                      native_meeting_id="abc-defg-hij", redis_url="redis://redis:6379/0",
                      token_secret=SECRET)
    inv = json.loads(runtime.specs[0]["env"]["BOT_CONFIG"])
    assert inv["transcriptionServiceUrl"] == "https://stt-mine.example.com"
    assert "transcriptionServiceToken" not in inv  # env token does NOT leak to the custom backend


async def test_request_bot_blocked_personal_transcription_never_falls_back_to_env(monkeypatch):
    from meeting_api.bot_spawn import service as spawn_service

    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt-env.vexa.ai")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "tok-env")

    async def fake_resolve(_user_id):
        return {
            "blocked": True,
            "config_status": "blocked",
            "validation_error": "Personal transcription endpoint is no longer operator-approved.",
        }

    monkeypatch.setattr(spawn_service, "_resolve_transcription_backend", fake_resolve)
    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()

    with pytest.raises(TranscriptionNotConfigured, match="blocked"):
        await request_bot(
            repo,
            runtime,
            user_id=USER,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            redis_url="redis://redis:6379/0",
            token_secret=SECRET,
        )

    assert runtime.specs == []
    assert repo._meetings == {}


async def test_request_bot_env_transcription_stays_without_settings(monkeypatch):
    """No configured backend (unset ADMIN_API_URL / nothing stored) → the pre-Settings env path,
    unchanged."""
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://stt-env.vexa.ai")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "tok-env")
    monkeypatch.delenv("ADMIN_API_URL", raising=False)

    repo = InMemoryMeetingRepo()
    runtime = FakeRuntimeClient()
    await request_bot(repo, runtime, user_id=USER, platform="google_meet",
                      native_meeting_id="abc-defg-hij", redis_url="redis://redis:6379/0",
                      token_secret=SECRET)
    inv = json.loads(runtime.specs[0]["env"]["BOT_CONFIG"])
    assert inv["transcriptionServiceUrl"] == "https://stt-env.vexa.ai"
    assert inv["transcriptionServiceToken"] == "tok-env"


async def test_settings_transcription_lookup_rejects_oversized_response(monkeypatch):
    """A compromised/internal admin response must not be buffered without a ceiling."""
    from meeting_api.bot_spawn import service as spawn_service

    payload = json.dumps({
        "transcription": {"url": "https://stt.example.com", "token": "secret"},
        "padding": "x" * (1024 * 1024),
    }).encode()
    with _serve_http(lambda _request: (
        200,
        {"Content-Type": "application/json", "Content-Length": str(len(payload))},
        payload,
    )) as (_, base_url):
        monkeypatch.setenv("ADMIN_API_URL", base_url)
        monkeypatch.setenv("INTERNAL_API_SECRET", "internal-secret")

        resolved = await spawn_service._resolve_transcription_backend(USER)

    assert resolved == {}


async def test_settings_transcription_lookup_caps_chunked_response(monkeypatch):
    """The streaming ceiling still applies when an upstream omits Content-Length."""
    from meeting_api.bot_spawn import service as spawn_service

    payload = json.dumps({
        "transcription": {"url": "https://stt.example.com", "token": "secret"},
        "padding": "x" * (1024 * 1024),
    }).encode()
    with _serve_http(lambda _request: (200, {"Content-Type": "application/json"}, payload)) as (_, base_url):
        monkeypatch.setenv("ADMIN_API_URL", base_url)
        monkeypatch.setenv("INTERNAL_API_SECRET", "internal-secret")

        resolved = await spawn_service._resolve_transcription_backend(USER)

    assert resolved == {}


async def test_settings_transcription_lookup_refuses_redirect_with_internal_secret(monkeypatch):
    """The admin secret stays bound to ADMIN_API_URL and is never replayed to a redirect target."""
    from meeting_api.bot_spawn import service as spawn_service

    valid = json.dumps({"transcription": {"url": "https://stt.example.com"}}).encode()
    with _serve_http(lambda _request: (200, {"Content-Type": "application/json"}, valid)) as (sink, sink_url):
        with _serve_http(lambda request: (
            302,
            {"Location": f"{sink_url}{request.path}"},
            b"",
        )) as (_, redirect_url):
            monkeypatch.setenv("ADMIN_API_URL", redirect_url)
            monkeypatch.setenv("INTERNAL_API_SECRET", "internal-secret")

            assert await spawn_service._resolve_transcription_backend(USER) == {}
            assert await spawn_service._resolve_operator_transcription_backend() == {}

    assert sink.requests == []


async def test_settings_transcription_lookup_rejects_malformed_config(monkeypatch):
    """Internal JSON is still untrusted at the process boundary; only scalar stt.v1 fields pass."""
    from meeting_api.bot_spawn import service as spawn_service

    def malformed(request):
        config = {"url": {"nested": "https://stt.example.com"}, "token": ["secret"], "blocked": "false"}
        body = {"value": config} if request.path.endswith("/internal/settings/transcription") else {
            "transcription": config,
        }
        return 200, {"Content-Type": "application/json"}, json.dumps(body).encode()

    with _serve_http(malformed) as (_, base_url):
        monkeypatch.setenv("ADMIN_API_URL", base_url)
        monkeypatch.setenv("INTERNAL_API_SECRET", "internal-secret")

        assert await spawn_service._resolve_transcription_backend(USER) == {}
        assert await spawn_service._resolve_operator_transcription_backend() == {}


async def test_runtime_create_never_redirects_credential_bearing_bot_config():
    import httpx

    from meeting_api.bot_spawn.adapters import HttpRuntimeClient

    accepted = json.dumps({"workloadId": "stolen"}).encode()
    with _serve_http(lambda _request: (201, {"Content-Type": "application/json"}, accepted)) as (sink, sink_url):
        with _serve_http(lambda request: (
            307,
            {"Location": f"{sink_url}{request.path}"},
            b"redirecting",
        )) as (_, redirect_url):
            async with httpx.AsyncClient(follow_redirects=True) as client:
                runtime = HttpRuntimeClient(client, redirect_url)
                with pytest.raises(SpawnFailed, match="307"):
                    await runtime.create_workload({
                        "workloadId": "mtg-1-secret",
                        "env": {"BOT_CONFIG": "meeting-token-and-stt-token"},
                    })

    assert sink.requests == []


async def test_runtime_create_caps_request_and_response_before_use():
    import httpx

    from meeting_api.bot_spawn.adapters import HttpRuntimeClient

    accepted = json.dumps({"workloadId": "mtg-1"}).encode()
    with _serve_http(lambda _request: (201, {"Content-Type": "application/json"}, accepted)) as (direct, base_url):
        async with httpx.AsyncClient() as client:
            runtime = HttpRuntimeClient(client, base_url)
            with pytest.raises(SpawnFailed, match="request is too large"):
                await runtime.create_workload({
                    "workloadId": "mtg-1",
                    "env": {"BOT_CONFIG": "x" * (1024 * 1024)},
                })
    assert direct.requests == []

    oversized = json.dumps({"workloadId": "mtg-1", "padding": "x" * (1024 * 1024)}).encode()
    with _serve_http(lambda _request: (
        201,
        {"Content-Type": "application/json", "Content-Length": str(len(oversized))},
        oversized,
    )) as (_, base_url):
        async with httpx.AsyncClient() as client:
            with pytest.raises(SpawnFailed, match="response is too large"):
                await HttpRuntimeClient(client, base_url).create_workload({"workloadId": "mtg-1", "env": {}})


async def test_runtime_get_caps_response_and_refuses_redirect():
    import httpx

    from meeting_api.bot_spawn.adapters import HttpRuntimeClient

    oversized = json.dumps({"workloadId": "mtg-1", "padding": "x" * (1024 * 1024)}).encode()
    with _serve_http(lambda _request: (200, {"Content-Type": "application/json"}, oversized)) as (_, base_url):
        async with httpx.AsyncClient() as client:
            with pytest.raises(SpawnFailed, match="response is too large"):
                await HttpRuntimeClient(client, base_url).get_workload("mtg-1")

    valid = json.dumps({"workloadId": "mtg-1", "state": "running"}).encode()
    with _serve_http(lambda _request: (200, {"Content-Type": "application/json"}, valid)) as (sink, sink_url):
        with _serve_http(lambda request: (307, {"Location": f"{sink_url}{request.path}"}, b"")) as (_, redirect_url):
            async with httpx.AsyncClient(follow_redirects=True) as client:
                with pytest.raises(SpawnFailed, match="307"):
                    await HttpRuntimeClient(client, redirect_url).get_workload("mtg-1")
    assert sink.requests == []


async def test_runtime_create_and_get_accept_small_direct_responses():
    import httpx

    from meeting_api.bot_spawn.adapters import HttpRuntimeClient

    def runtime_response(request):
        status = 201 if request.command == "POST" else 200
        body = json.dumps({"workloadId": "mtg-1", "state": "running"}).encode()
        return status, {"Content-Type": "application/json"}, body

    with _serve_http(runtime_response) as (server, base_url):
        async with httpx.AsyncClient(follow_redirects=True) as client:
            runtime = HttpRuntimeClient(
                client,
                base_url,
                control_secret="meeting-runtime-control-secret",
            )
            created = await runtime.create_workload({"workloadId": "mtg-1", "env": {"BOT_CONFIG": "safe"}})
            current = await runtime.get_workload("mtg-1")

    assert created["workloadId"] == "mtg-1"
    assert current == {"workloadId": "mtg-1", "state": "running"}
    assert [request["method"] for request in server.requests] == ["POST", "GET"]
    assert {
        request["headers"]["X-Runtime-Control-Secret"]
        for request in server.requests
    } == {"meeting-runtime-control-secret"}


async def test_runtime_scrub_timeout_covers_kernel_grace_and_substrate_cleanup():
    from meeting_api.bot_spawn.adapters import HttpRuntimeClient

    class _Response:
        status_code = 200

    class _Stream:
        async def __aenter__(self):
            return _Response()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Client:
        def __init__(self):
            self.request = None

        def stream(self, method, url, **kwargs):
            self.request = (method, url, kwargs)
            return _Stream()

    client = _Client()
    await HttpRuntimeClient(
        client,
        "http://runtime:8090",
        control_secret="meeting-runtime-control-secret",
    ).scrub_workload("mtg-1-d93eee39")

    method, url, kwargs = client.request
    assert method == "POST"
    assert url == "http://runtime:8090/workloads/mtg-1-d93eee39/scrub"
    # Runtime's launch default can spend 35s on graceful stop and up to 60s proving
    # Kubernetes deletion. The caller must not time out before that proof can finish.
    assert kwargs["timeout"] == 120.0
    assert kwargs["follow_redirects"] is False
    assert kwargs["headers"] == {
        "X-Runtime-Control-Secret": "meeting-runtime-control-secret"
    }


async def test_runtime_status_errors_never_reflect_upstream_body():
    import httpx

    from meeting_api.bot_spawn.adapters import HttpRuntimeClient

    with _serve_http(lambda _request: (
        500,
        {"Content-Type": "text/plain"},
        b"database password and internal stack trace",
    )) as (_, base_url):
        async with httpx.AsyncClient() as client:
            runtime = HttpRuntimeClient(client, base_url)
            with pytest.raises(SpawnFailed) as create_error:
                await runtime.create_workload({"workloadId": "mtg-1", "env": {}})
            with pytest.raises(SpawnFailed) as get_error:
                await runtime.get_workload("mtg-1")

    assert str(create_error.value) == "runtime kernel returned 500"
    assert str(get_error.value) == "runtime kernel get_workload returned 500"


# ── route: meeting_url passthrough is SSRF-validated at entry (jitsi/zoom, TAKE on #543) ─────────
#
# platform=jitsi (and zoom) carries an arbitrary caller URL straight to the bot's browser.
# The route now 422s non-https, IP-literal, and localhost URLs; a real hostname deployment
# is the negative control that proves the guard discriminates.

def test_post_bots_jitsi_http_url_422(monkeypatch):
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)
    r = _client().post("/bots", headers=HEADERS,
                       json={"platform": "jitsi", "native_meeting_id": "Room",
                             "meeting_url": "http://meet.example.org/Room"})
    assert r.status_code == 422, r.text
    assert "https" in r.json()["detail"]


def test_post_bots_jitsi_private_ip_url_422(monkeypatch):
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)
    r = _client().post("/bots", headers=HEADERS,
                       json={"platform": "jitsi", "native_meeting_id": "Room",
                             "meeting_url": "https://10.0.0.5/Room"})
    assert r.status_code == 422, r.text
    assert "IP literal" in r.json()["detail"]


def test_post_bots_jitsi_localhost_and_ipv6_422(monkeypatch):
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)
    client = _client()
    for bad in ("https://localhost/Room", "https://foo.localhost/Room", "https://[::1]/Room",
                "https://169.254.169.254/Room"):
        r = client.post("/bots", headers=HEADERS,
                        json={"platform": "jitsi", "native_meeting_id": "Room",
                              "meeting_url": bad})
        assert r.status_code == 422, f"{bad}: {r.status_code} {r.text}"


@pytest.mark.parametrize(
    "bad",
    (
        "https://2130706433/Room",
        "https://127.1/Room",
        "https://0177.0.0.1/Room",
        "https://0x7f000001/Room",
        "https://%31%32%37.0.0.1/Room",
    ),
)
def test_post_bots_rejects_browser_normalized_loopback_hosts(monkeypatch, bad):
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)

    response = _client().post(
        "/bots",
        headers=HEADERS,
        json={"platform": "jitsi", "native_meeting_id": "Room", "meeting_url": bad},
    )

    assert response.status_code == 422, f"{bad}: {response.status_code} {response.text}"


def test_post_bots_rejects_browser_parser_backslash_confusion(monkeypatch):
    """Python and Chromium disagree about ``\\`` in a special-scheme URL authority."""
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)

    response = _client().post(
        "/bots",
        headers=HEADERS,
        json={
            "platform": "google_meet",
            "native_meeting_id": "Room",
            "meeting_url": "https://attacker-controlled.example\\@meet.google.com/Room",
        },
    )

    assert response.status_code == 422, response.text


def test_post_bots_jitsi_hostname_url_accepted(monkeypatch):
    """Negative control: a real https hostname deployment sails through the guard → 201."""
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)
    monkeypatch.setenv("VEXA_JITSI_HOSTS", "meet.example.org")
    r = _client().post("/bots", headers=HEADERS,
                       json={"platform": "jitsi", "native_meeting_id": "Room",
                             "meeting_url": "https://meet.example.org/room"})
    assert r.status_code == 201, r.text


def test_post_bots_rejects_unconfigured_hostname(monkeypatch):
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)
    monkeypatch.delenv("VEXA_JITSI_HOSTS", raising=False)

    response = _client().post(
        "/bots",
        headers=HEADERS,
        json={
            "platform": "jitsi",
            "native_meeting_id": "Room",
            "meeting_url": "https://attacker-controlled.example/Room",
        },
    )

    assert response.status_code == 422, response.text
    assert "approved for platform" in response.json()["detail"]


def test_post_bots_zoom_shares_meeting_url_guard(monkeypatch):
    """The zoom passthrough rides the SAME validator (one shared entry-point guard)."""
    monkeypatch.setenv("MEETING_TOKEN_SECRET", SECRET)
    r = _client().post("/bots", headers=HEADERS,
                       json={"platform": "zoom", "native_meeting_id": "123456",
                             "meeting_url": "https://192.168.1.10/j/123456"})
    assert r.status_code == 422, r.text
