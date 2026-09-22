"""config.v1 (ADR-0026) — meeting-api's declaration, boot preflight, capability tri-state,
/health rows, and the CANONICAL capability gate: the spawn-time STT 503 driven by the declared
`stt` capability instead of ad-hoc os.getenv checks.

All offline: the STT live probe is monkeypatched where a test exercises it (`_run_probe` is the
seam); env-level tri-state tests pass explicit env dicts (pure, no monkeypatching).
"""
from __future__ import annotations

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest
from fastapi.testclient import TestClient

from meeting_api import create_app
from meeting_api import config_preflight as cp
from meeting_api.bot_spawn.fakes import FakeRuntimeClient, InMemoryMeetingRepo

HEADERS = {"x-user-id": "7"}


@pytest.fixture(autouse=True)
def _admin_token(monkeypatch):
    monkeypatch.setenv("MEETING_TOKEN_SECRET", "test-meeting-token-secret")
    monkeypatch.setenv("INTERNAL_API_SECRET", "test-internal-secret")
    monkeypatch.setenv("RUNTIME_CONTROL_SECRET", "test-runtime-control-secret")
    monkeypatch.setenv("RUNTIME_CALLBACK_SECRET", "test-runtime-callback-secret")


@pytest.fixture(autouse=True)
def _fresh_probe_cache():
    cp._reset_probe_cache()
    yield
    cp._reset_probe_cache()


def _client(repo=None):
    return TestClient(create_app(meeting_repo=repo or InMemoryMeetingRepo(), runtime=FakeRuntimeClient()))


@contextmanager
def _serve(handler):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


# ── the declaration itself ───────────────────────────────────────────────────────────────────────


def test_declaration_loads_and_is_internally_consistent():
    decl = cp.load_declaration()
    assert decl["service"] == "meeting-api"
    caps = decl["capabilities"]
    assert set(caps) == {"stt", "object_storage"}
    # the canonical capability carries the live auth probe (the silent-401 incident's fix)
    assert caps["stt"]["probe"]["kind"] == "http"
    # every capability-classed key resolves (load_declaration raises otherwise) and stt's members
    # are exactly the two keys the original ad-hoc guard checked
    stt_keys = {k["key"] for k in decl["keys"] if k.get("capability") == "stt"}
    assert stt_keys == {"TRANSCRIPTION_SERVICE_URL", "TRANSCRIPTION_SERVICE_TOKEN"}
    # required-explicit is exactly the A4 boot bar
    required = {k["key"] for k in decl["keys"] if k["class"] == "required-explicit"}
    assert required == {
        "MEETING_TOKEN_SECRET",
        "INTERNAL_API_SECRET",
        "RUNTIME_CALLBACK_SECRET",
        "RUNTIME_CONTROL_SECRET",
    }
    database_ssl = next(key for key in decl["keys"] if key["key"] == "DB_SSL_MODE")
    assert database_ssl["default"] == "disable"


def test_minutes_operator_controls_are_declared_default_off():
    declaration = cp.load_declaration()
    keys = {entry["key"]: entry for entry in declaration["keys"]}
    assert keys["ZAKI_MINUTES_CAPTURE_ENABLED"]["default"] == "false"
    assert keys["ZAKI_MINUTES_INVOCATION_V2_ENABLED"]["default"] == "false"
    assert keys["ZAKI_MINUTES_AUTO_JOIN_ENABLED"]["default"] == "false"
    assert keys["ZAKI_MINUTES_MANAGED_ONLY"]["default"] == "false"
    assert keys["ZAKI_MINUTES_READ_ENABLED"]["default"] == "false"
    assert keys["MINUTES_TTL_ENABLED"]["default"] == "false"
    assert keys["MINUTES_TTL_INTERVAL_S"]["default"] == "60"
    assert keys["MINUTES_TTL_BATCH_SIZE"]["default"] == "100"
    assert keys["ZAKI_READ_TOKEN_MINUTES"]["secret"] is True
    assert keys["ZAKI_READ_TOKEN_MINUTES"]["default"].startswith("(unset")
    assert keys["ZAKI_MINUTES_HUB_TOKEN"]["secret"] is True
    assert keys["ZAKI_MINUTES_HUB_TOKEN"]["default"].startswith("(unset")
    assert keys["AGENT_API_URL"]["default"] == "http://agent-api:8080"
    for key in (
        "ZAKI_AGENT_ERASURE_VERIFICATION_SECRET",
        "ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET",
        "ZAKI_MINUTES_ERASURE_SIGNING_SECRET",
        "ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET",
    ):
        assert keys[key]["secret"] is True
        assert keys[key]["default"].startswith("(unset")
    for key in (
        "ZAKI_AGENT_ERASURE_VERIFICATION_KEY_ID",
        "ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID",
        "ZAKI_MINUTES_ERASURE_SIGNING_KEY_ID",
        "ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_KEY_ID",
    ):
        assert keys[key]["default"].startswith("(unset")
    assert keys["ZAKI_MINUTES_FINALIZED_ENABLED"]["default"] == "false"
    for key in (
        "ZAKI_MINUTES_FINALIZED_URL",
        "ZAKI_MINUTES_FINALIZED_KEY_ID",
        "ZAKI_MINUTES_FINALIZED_SECRET",
    ):
        assert keys[key]["default"].startswith("(unset")
    assert keys["ZAKI_MINUTES_FINALIZED_SECRET"]["secret"] is True


# ── boot preflight (A4, now declaration-driven) ──────────────────────────────────────────────────


def test_preflight_refuses_to_boot_without_admin_token(monkeypatch):
    monkeypatch.delenv("MEETING_TOKEN_SECRET", raising=False)
    with pytest.raises(cp.ConfigError) as ei:
        cp.preflight()
    assert "MEETING_TOKEN_SECRET" in str(ei.value), "the boot error must NAME the missing required key"


def test_preflight_reports_capability_rows(monkeypatch):
    # STT env-configured (conftest) + a passing probe → the boot report carries the rows.
    monkeypatch.setattr(cp, "_run_probe", lambda spec, env: {"ok": True, "status": 405})
    report = cp.preflight()
    assert report["service"] == "meeting-api"
    assert report["capabilities"]["stt"]["state"] == cp.CONFIGURED
    assert report["capabilities"]["stt"]["probe"]["ok"] is True
    assert "object_storage" in report["capabilities"]


def test_redis_client_options_are_bounded_and_fail_fast():
    from meeting_api.collector.adapters import redis_client_options

    assert redis_client_options({}) == {
        "socket_connect_timeout": 5.0,
        "socket_timeout": 5.0,
        "retry_on_timeout": False,
    }
    assert redis_client_options(
        {"REDIS_CONNECT_TIMEOUT_S": "1.25", "REDIS_IO_TIMEOUT_S": "2.5"}
    ) == {
        "socket_connect_timeout": 1.25,
        "socket_timeout": 2.5,
        "retry_on_timeout": False,
    }
    with pytest.raises(ValueError, match="REDIS_IO_TIMEOUT_S"):
        redis_client_options({"REDIS_IO_TIMEOUT_S": "0"})


# ── the capability tri-state (env-level, pure) ───────────────────────────────────────────────────


def test_stt_tri_state():
    both = {"TRANSCRIPTION_SERVICE_URL": "http://stt", "TRANSCRIPTION_SERVICE_TOKEN": "t"}
    assert cp.capability_states(both)["stt"] == cp.CONFIGURED
    assert cp.capability_states({})["stt"] == cp.NOT_CONFIGURED
    # SOME-but-not-all set is its own state — a half-configured deploy must not look unconfigured
    url_only = {"TRANSCRIPTION_SERVICE_URL": "http://stt"}
    assert cp.capability_states(url_only)["stt"] == cp.MISCONFIGURED
    # empty string counts as unset (compose `${VAR:-}` defaults absent vars to "")
    blank = {"TRANSCRIPTION_SERVICE_URL": "http://stt", "TRANSCRIPTION_SERVICE_TOKEN": "  "}
    assert cp.capability_states(blank)["stt"] == cp.MISCONFIGURED


def test_unknown_capability_fails_loud():
    with pytest.raises(cp.ConfigError):
        cp.capability_state("no_such_capability", {})


# ── the live probe (incident 2: SET-but-rejected credentials must show as misconfigured) ─────────


def test_probe_rejection_demotes_health_row_to_misconfigured(monkeypatch):
    env = {"TRANSCRIPTION_SERVICE_URL": "http://stt", "TRANSCRIPTION_SERVICE_TOKEN": "bad-token"}
    monkeypatch.setattr(
        cp, "_run_probe",
        lambda spec, env: {"ok": False, "status": 401,
                           "reason": "unauthorized — the configured token was REJECTED by the endpoint"},
    )
    rows = cp.capability_health(env)
    assert rows["stt"]["state"] == cp.MISCONFIGURED, (
        "a SET-but-rejected STT token must surface as misconfigured on /health, not as a silent "
        "transcription-less meeting"
    )
    assert rows["stt"]["probe"]["status"] == 401


def test_http_probe_refuses_redirect_without_replaying_operator_token():
    sink_requests = []

    class Sink(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 — stdlib handler contract
            sink_requests.append(self.headers.get("Authorization"))
            self.send_response(204)
            self.end_headers()

        do_POST = do_GET

        def log_message(self, *_args):
            pass

    class Direct(BaseHTTPRequestHandler):
        authorization = None

        def do_POST(self):  # noqa: N802 — stdlib handler contract
            type(self).authorization = self.headers.get("Authorization")
            self.send_response(405)
            self.end_headers()

        def log_message(self, *_args):
            pass

    spec = {
        "url_key": "STT_URL",
        "auth_key": "STT_TOKEN",
        "method": "POST",
        "unauthorized_statuses": [401, 403],
    }
    with _serve(Sink) as sink_url:
        class Redirect(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 — stdlib handler contract
                self.send_response(302)
                self.send_header("Location", f"{sink_url}/credential-sink")
                self.end_headers()

            def log_message(self, *_args):
                pass

        with _serve(Redirect) as redirect_url:
            result = cp._http_probe(
                spec,
                {"STT_URL": redirect_url, "STT_TOKEN": "operator-secret"},
                timeout=2,
            )

    assert result["ok"] is False
    assert result["status"] == 302
    assert "redirect" in result["reason"].lower()
    assert sink_requests == [], "the redirect target must receive neither probe nor bearer token"

    with _serve(Direct) as direct_url:
        direct_result = cp._http_probe(
            spec,
            {"STT_URL": direct_url, "STT_TOKEN": "operator-secret"},
            timeout=2,
        )
    assert direct_result == {"ok": True, "status": 405}
    assert Direct.authorization == "Bearer operator-secret"


def test_probe_result_is_cached_per_ttl(monkeypatch):
    env = {"TRANSCRIPTION_SERVICE_URL": "http://stt", "TRANSCRIPTION_SERVICE_TOKEN": "t"}
    calls = []
    monkeypatch.setattr(cp, "_run_probe", lambda spec, e: (calls.append(1), {"ok": True, "status": 405})[1])
    cp.capability_health(env)
    cp.capability_health(env)
    assert len(calls) == 1, "within ttl_s the cached probe verdict is reused (no probe per health poll)"


def test_env_only_state_never_probes():
    # the spawn guard's oracle is pure — no probe I/O may ride the request path
    env = {"TRANSCRIPTION_SERVICE_URL": "http://stt", "TRANSCRIPTION_SERVICE_TOKEN": "t"}
    assert cp.capability_state("stt", env) == cp.CONFIGURED
    assert cp._probe_cache == {}


# ── /health rows (ADDITIVE) ──────────────────────────────────────────────────────────────────────


def test_health_carries_capability_rows_additively(monkeypatch):
    monkeypatch.setattr(cp, "_run_probe", lambda spec, env: {"ok": True, "status": 405})
    r = _client().get("/health")
    assert r.status_code == 200
    body = r.json()
    # the pre-existing consumers' keys are untouched
    assert body["status"] == "ok"
    assert body["service"] == "meeting-api"
    # the additive config.v1 rows (conftest sets the STT pair → configured)
    assert body["capabilities"]["stt"]["state"] == cp.CONFIGURED
    assert body["capabilities"]["stt"]["probe"]["ok"] is True
    assert "state" in body["capabilities"]["object_storage"]


# ── the spawn gate: POST /bots trusts the transcription RESOLVER, not the env tri-state ─────────
# (#502 C1 / PR #504): the `stt` capability tri-state still drives boot preflight + /health, but
# the spawn path now gates on what request_bot actually resolves (Settings backend > env) — the
# env-only capability check could never be satisfied by wizard-written Settings config.


def test_spawn_accepts_url_without_token(monkeypatch):
    """Semantics shift from the old capability gate: a URL with no token is a SPAWNABLE backend
    (the token belongs to the backend and may legitimately be empty — e.g. an unauthenticated
    self-hosted STT). /health's `stt` row still reads `misconfigured` for the env pair; the spawn
    gate no longer refuses on it."""
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "http://stt.test/transcribe")
    monkeypatch.delenv("TRANSCRIPTION_SERVICE_TOKEN", raising=False)
    r = _client().post("/bots", headers=HEADERS,
                       json={"platform": "google_meet", "native_meeting_id": "half-stt"})
    assert r.status_code == 201, f"{r.status_code} {r.text}"


def test_spawn_503_when_stt_fully_unset(monkeypatch):
    monkeypatch.delenv("TRANSCRIPTION_SERVICE_URL", raising=False)
    monkeypatch.delenv("TRANSCRIPTION_SERVICE_TOKEN", raising=False)
    monkeypatch.delenv("ADMIN_API_URL", raising=False)  # no Settings backend either
    repo = InMemoryMeetingRepo()
    r = _client(repo).post("/bots", headers=HEADERS,
                           json={"platform": "google_meet", "native_meeting_id": "no-stt"})
    assert r.status_code == 503
    detail = r.json()["detail"]
    # the typed resolver reason — actionable for BOTH config paths (wizard Settings and env)
    assert "no transcription backend configured" in detail
    assert "Settings" in detail
    assert "TRANSCRIPTION_SERVICE_URL" in detail and "TRANSCRIPTION_SERVICE_TOKEN" in detail
    # #504 review finding 1: the refusal fires BEFORE the meeting-row write — a refused spawn
    # leaves no orphaned `requested` row, so the post-config retry cannot 409 on the dedup guard.
    assert repo._meetings == {}, f"refused spawn wrote a meeting row: {repo._meetings}"
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "http://stt.test/transcribe")
    r2 = _client(repo).post("/bots", headers=HEADERS,
                            json={"platform": "google_meet", "native_meeting_id": "no-stt"})
    assert r2.status_code == 201, f"retry after configuring must not 409/503: {r2.status_code} {r2.text}"
