"""Settings → Models "Test" buttons — the on-demand credential tests (control_plane.config_test).

Grades the exact failure modes observed live on 2026-07-09: stale Keychain export (expired
subscription file), zero-balance external transcription token (402 per segment), rejected
token, unreachable backend, and the happy paths.
"""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading

from fastapi.testclient import TestClient

from control_plane import config_test as ct
from control_plane.api import create_app
from control_plane.dispatch import Dispatcher
from shared.config import load_settings


# ── subscription file ─────────────────────────────────────────────────────────────────────────

def _write_creds(tmp_path, expires_ms):
    p = tmp_path / "creds.json"
    p.write_text(json.dumps({"claudeAiOauth": {"expiresAt": expires_ms}}))
    return str(p)


def test_subscription_missing_file(tmp_path):
    out = ct.test_subscription_credentials(str(tmp_path / "absent"))
    assert not out["ok"] and "HOST_CLAUDE_CREDENTIALS" in out["summary"]


def test_subscription_expired_carries_remedy(tmp_path):
    out = ct.test_subscription_credentials(_write_creds(tmp_path, 1_000_000), now=2_000.0)
    assert not out["ok"] and out.get("expired") is True
    assert ct.KEYCHAIN_REFRESH in out["summary"]  # the fix ships WITH the failure


def test_subscription_valid_reports_hours_left(tmp_path):
    out = ct.test_subscription_credentials(_write_creds(tmp_path, 10 * 3600 * 1000), now=0.0)
    assert out["ok"] and out["expires_in_hours"] == 10.0


def test_subscription_garbage_file(tmp_path):
    p = tmp_path / "creds.json"
    p.write_text("not json")
    out = ct.test_subscription_credentials(str(p))
    assert not out["ok"] and ct.KEYCHAIN_REFRESH in out["summary"]


# ── custom endpoint ───────────────────────────────────────────────────────────────────────────

def test_custom_endpoint_auth_failure():
    out = ct.test_custom_endpoint("https://gw.example", "bad-key",
                                  post=lambda u, p, h: (401, "{}"))
    assert not out["ok"] and "Authentication FAILED" in out["summary"]


def test_custom_endpoint_ok_anthropic_dialect():
    calls = []
    def post(url, payload, headers):
        calls.append(url)
        return 200, "{}"
    out = ct.test_custom_endpoint("https://gw.example/", "k", "m1", post=post)
    assert out["ok"] and calls == ["https://gw.example/v1/messages"]


def test_custom_endpoint_falls_back_to_openai_dialect():
    def post(url, payload, headers):
        return (404, "") if url.endswith("/v1/messages") else (200, "{}")
    out = ct.test_custom_endpoint("https://gw.example", "k", post=post)
    assert out["ok"]


def test_custom_endpoint_unreachable():
    def post(url, payload, headers):
        raise OSError("connection refused")
    out = ct.test_custom_endpoint("https://gw.example", "k", post=post)
    assert not out["ok"] and "unreachable" in out["summary"]


def test_custom_endpoint_never_reflects_an_upstream_body_that_echoes_the_api_key():
    out = ct.test_custom_endpoint(
        "https://gw.example",
        "model-super-secret",
        post=lambda _url, _payload, _headers: (
            500,
            "bad Authorization: Bearer model-super-secret",
        ),
    )
    assert not out["ok"]
    assert out["status"] == 500
    assert "model-super-secret" not in out["summary"]
    assert "bad Authorization" not in out["summary"]


def test_authenticated_model_and_stt_probes_bound_success_bodies_before_decode(monkeypatch):
    read_sizes = []

    class OversizedResponse:
        status = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, size=-1):
            read_sizes.append(size)
            return b"x" * (ct.MAX_HTTP_RESPONSE_BYTES + 1)

    monkeypatch.setattr(ct, "open_no_redirect", lambda *_args, **_kwargs: OversizedResponse())

    model = ct.test_custom_endpoint("https://model.example", "model-secret")
    transcription = ct.run_transcription_test(
        "https://stt.example", "stt-secret", "operator",
    )

    assert not model["ok"] and model["summary"] == "Endpoint unreachable."
    assert not transcription["ok"] and transcription["summary"] == "Backend unreachable."
    assert read_sizes == [ct.MAX_HTTP_RESPONSE_BYTES + 1] * 2
    assert "model-secret" not in json.dumps(model)
    assert "stt-secret" not in json.dumps(transcription)


def test_run_models_test_routes_custom_vs_subscription(tmp_path):
    out = ct.run_models_test({"mode": "custom", "base_url": "https://gw", "api_key": "k"},
                             env={}, post=lambda u, p, h: (200, "{}"))
    assert out["mode"] == "custom" and out["ok"]
    out = ct.run_models_test({}, env={}, creds_path=str(tmp_path / "absent"))
    assert out["mode"] == "subscription" and not out["ok"]
    # secrets never echo in provenance
    out = ct.run_models_test({"mode": "custom", "base_url": "https://gw", "api_key": "SECRET"},
                             env={}, post=lambda u, p, h: (200, "{}"))
    assert "api_key" not in out["config"] and "SECRET" not in json.dumps(out)


def test_run_models_test_keeps_settings_custom_bundle_atomic_from_env_credentials():
    calls = []

    def post(url, payload, headers):
        calls.append((url, headers))
        return 200, "{}"

    out = ct.run_models_test(
        {"mode": "custom", "base_url": "https://user-model.example"},
        env={
            "ANTHROPIC_BASE_URL": "https://operator-model.example",
            "ANTHROPIC_AUTH_TOKEN": "operator-model-secret",
        },
        post=post,
    )
    assert out["ok"]
    assert calls[0][0] == "https://user-model.example/v1/messages"
    assert "operator-model-secret" not in json.dumps(calls)

    calls.clear()
    incomplete = ct.run_models_test(
        {"mode": "custom"},
        env={
            "ANTHROPIC_BASE_URL": "https://operator-model.example",
            "ANTHROPIC_AUTH_TOKEN": "operator-model-secret",
        },
        post=post,
    )
    assert not incomplete["ok"]
    assert calls == []


def test_run_models_test_uses_the_complete_env_custom_bundle_when_settings_selects_none():
    calls = []

    def post(url, payload, headers):
        calls.append((url, headers))
        return 200, "{}"

    out = ct.run_models_test(
        {},
        env={
            "ANTHROPIC_BASE_URL": "https://operator-model.example",
            "ANTHROPIC_AUTH_TOKEN": "operator-model-secret",
        },
        post=post,
    )
    assert out["ok"] and out["mode"] == "custom"
    assert calls[0][0] == "https://operator-model.example/v1/messages"
    assert calls[0][1]["Authorization"] == "Bearer operator-model-secret"


def test_run_models_test_refuses_a_blocked_personal_tier_without_env_fallback():
    calls = []
    out = ct.run_models_test(
        {
            "mode": "custom",
            "blocked": True,
            "config_status": "blocked",
            "validation_error": "Personal model endpoint is no longer operator-approved.",
        },
        env={
            "ANTHROPIC_BASE_URL": "https://operator-model.example",
            "ANTHROPIC_AUTH_TOKEN": "operator-model-secret",
        },
        post=lambda *args: calls.append(args) or (200, "{}"),
    )
    assert not out["ok"]
    assert out["mode"] == "custom"
    assert "blocked" in out["summary"].lower()
    assert calls == []
    assert "operator-model-secret" not in json.dumps(out)


# ── transcription backend ─────────────────────────────────────────────────────────────────────

def _balance(email, minutes):
    return 200, json.dumps({"email": email, "balance_minutes": minutes})


@contextmanager
def _serve(handler):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_custom_model_probe_refuses_redirect_without_leaking_api_key_and_allows_direct_http():
    redirected = []

    class RedirectSink(BaseHTTPRequestHandler):
        def _capture(self):
            redirected.append((
                self.command,
                self.path,
                self.headers.get("X-API-Key"),
                self.headers.get("Authorization"),
            ))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b"{}")

        do_GET = _capture
        do_POST = _capture

        def log_message(self, *_args):
            pass

    with _serve(RedirectSink) as sink_url:
        class RedirectingModel(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                self.send_response(302)
                self.send_header("Location", f"{sink_url}/api-key-sink")
                self.end_headers()

            def log_message(self, *_args):
                pass

        with _serve(RedirectingModel) as model_url:
            out = ct.test_custom_endpoint(model_url, "model-secret", model="test-model")

    assert not out["ok"]
    assert out.get("status") == 302
    assert redirected == []

    direct = []

    class DirectModel(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            direct.append((
                self.path,
                self.headers.get("X-API-Key"),
                self.headers.get("Authorization"),
                len(body),
            ))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *_args):
            pass

    with _serve(DirectModel) as model_url:
        out = ct.test_custom_endpoint(model_url, "model-secret", model="test-model")

    assert out["ok"]
    assert len(direct) == 1
    assert direct[0][:3] == (
        "/v1/messages", "model-secret", "Bearer model-secret",
    )
    assert direct[0][3] > 0


def test_transcription_internal_token_billing_exempt():
    out = ct.run_transcription_test("https://transcription.vexa.ai", "tok", "env",
                                    get=lambda u, h: _balance("internal@vexa.ai", 0.0))
    assert out["ok"] and "billing-exempt" in out["summary"]


def test_transcription_zero_balance_external_fails_loud():
    out = ct.run_transcription_test("https://transcription.vexa.ai", "tok", "settings",
                                    get=lambda u, h: _balance("someone@gmail.com", 0.0))
    assert not out["ok"] and "402" in out["summary"] and out["source"] == "settings"


def test_transcription_funded_external_ok():
    out = ct.run_transcription_test("https://transcription.vexa.ai", "tok", "env",
                                    get=lambda u, h: _balance("someone@gmail.com", 42.5))
    assert out["ok"] and "42.5" in out["summary"]


def test_transcription_rejected_token():
    out = ct.run_transcription_test("https://x", "bad", "env", get=lambda u, h: (403, ""))
    assert not out["ok"] and "REJECTED" in out["summary"]


def test_transcription_strips_v1_path_for_balance_probe():
    seen = []
    def get(url, headers):
        seen.append(url)
        return _balance("internal@vexa.ai", 0.0)
    ct.run_transcription_test("https://t.vexa.ai/v1/audio/transcriptions", "tok", "env", get=get)
    assert seen == ["https://t.vexa.ai/balance"]


def test_transcription_no_backend_and_no_token():
    out = ct.run_transcription_test("", "", "env")
    assert not out["ok"] and "No transcription backend" in out["summary"]
    out = ct.run_transcription_test("https://t", "", "env")
    assert not out["ok"] and "NO token" in out["summary"]


def test_transcription_unreachable_and_non_gateway():
    def boom(url, headers):
        raise OSError("timeout")
    out = ct.run_transcription_test("https://t", "tok", "env", get=boom)
    assert not out["ok"] and "unreachable" in out["summary"]
    out = ct.run_transcription_test("https://t", "tok", "env", get=lambda u, h: (404, ""))
    assert out["ok"] and out.get("unverified") is True  # reachable, token unproven — says so


def test_transcription_probe_refuses_redirect_without_leaking_token_and_allows_direct_http():
    redirected = []

    class RedirectSink(BaseHTTPRequestHandler):
        def do_GET(self):
            redirected.append((self.path, self.headers.get("X-API-Key")))
            body = json.dumps({"email": "internal@vexa.ai", "balance_minutes": 0}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    with _serve(RedirectSink) as sink_url:
        class RedirectingBackend(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(302)
                self.send_header("Location", f"{sink_url}/private")
                self.end_headers()

            def log_message(self, *_args):
                pass

        with _serve(RedirectingBackend) as backend_url:
            out = ct.run_transcription_test(backend_url, "operator-secret", "operator")

    assert not out["ok"]
    assert out.get("status") == 302
    assert redirected == []

    direct = []

    class DirectBackend(BaseHTTPRequestHandler):
        def do_GET(self):
            direct.append((self.path, self.headers.get("X-API-Key")))
            body = json.dumps({"email": "internal@vexa.ai", "balance_minutes": 0}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    with _serve(DirectBackend) as backend_url:
        out = ct.run_transcription_test(backend_url, "operator-secret", "operator")

    assert out["ok"]
    assert direct == [("/balance", "operator-secret")]


def test_transcription_test_never_inherits_operator_token_for_settings_url(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "operator-env-token")
    requested = []

    class SettingsResponse:
        status = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, _size=-1):
            return json.dumps({
                "transcription_credential_owner": "user",
                "transcription": {"url": "https://user-stt.example"},
            }).encode()

    def open_no_redirect(request, **_kwargs):
        requested.append((request.full_url, dict(request.header_items())))
        return SettingsResponse()

    monkeypatch.setattr(ct, "open_no_redirect", open_no_redirect)
    dispatcher = Dispatcher(
        load_settings(admin_api_url="http://admin-api:8001",
                      internal_api_secret="internal-secret"),
        object(),
        object(),
    )

    response = TestClient(create_app(dispatcher)).get(
        "/api/transcription/test",
        headers={"X-User-Id": "7"},
    )

    assert response.status_code == 200
    assert response.json()["source"] == "settings"
    assert "NO token" in response.json()["summary"]
    assert requested == [(
        "http://admin-api:8001/internal/users/7/bot-context",
        {"X-internal-secret": "internal-secret"},
    )]


def test_transcription_test_refuses_blocked_personal_tier_without_env_fallback(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://operator-stt.example")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "operator-stt-secret")

    class SettingsResponse:
        status = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, _size=-1):
            return json.dumps({
                "transcription_credential_owner": "user",
                "transcription": {
                    "blocked": True,
                    "config_status": "blocked",
                    "validation_error": "Personal transcription endpoint is no longer operator-approved.",
                },
            }).encode()

    monkeypatch.setattr(ct, "open_no_redirect", lambda *_args, **_kwargs: SettingsResponse())
    dispatcher = Dispatcher(
        load_settings(
            admin_api_url="http://admin-api:8001",
            internal_api_secret="internal-secret",
        ),
        object(),
        object(),
    )

    response = TestClient(create_app(dispatcher)).get(
        "/api/transcription/test",
        headers={"X-User-Id": "7"},
    )

    assert response.status_code == 200
    assert response.json()["ok"] is False
    assert response.json()["blocked"] is True
    assert response.json()["source"] == "settings"
    assert "operator-stt-secret" not in json.dumps(response.json())


def test_transcription_test_admin_lookup_refuses_redirect_without_leaking_internal_secret():
    redirected = []

    class RedirectSink(BaseHTTPRequestHandler):
        def do_GET(self):
            redirected.append((self.path, self.headers.get("X-Internal-Secret")))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"transcription": {}}')

        def log_message(self, *_args):
            pass

    with _serve(RedirectSink) as sink_url:
        class RedirectingAdmin(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(302)
                self.send_header("Location", f"{sink_url}/internal-secret-sink")
                self.end_headers()

            def log_message(self, *_args):
                pass

        with _serve(RedirectingAdmin) as admin_url:
            dispatcher = Dispatcher(
                load_settings(
                    admin_api_url=admin_url,
                    internal_api_secret="internal-secret",
                ),
                object(),
                object(),
            )
            response = TestClient(create_app(dispatcher)).get(
                "/api/transcription/test",
                headers={"X-User-Id": "7"},
            )

    assert response.status_code == 200
    assert redirected == []


def test_models_test_never_spends_or_exposes_inherited_operator_credentials(monkeypatch):
    calls = []

    class OperatorModels:
        def resolve(self, _subject):
            return {
                "mode": "custom",
                "base_url": "https://private-operator-model.example",
                "api_key": "operator-model-secret",
                "credential_owner": "operator",
            }

    monkeypatch.setattr(
        ct,
        "run_models_test",
        lambda *args, **kwargs: calls.append((args, kwargs)) or {"ok": True, "summary": "spent"},
    )
    dispatcher = Dispatcher(
        load_settings(), object(), object(), model_config=OperatorModels(),
    )

    response = TestClient(create_app(dispatcher)).get(
        "/api/models/test", headers={"X-User-Id": "7"},
    )
    body = response.json()

    assert response.status_code == 200
    assert calls == []
    assert body["managed"] is True and body["source"] == "operator"
    assert "private-operator-model.example" not in json.dumps(body)
    assert "operator-model-secret" not in json.dumps(body)


def test_models_test_keeps_personal_custom_live_test_available(monkeypatch):
    calls = []

    class PersonalModels:
        def resolve(self, _subject):
            return {
                "mode": "custom",
                "base_url": "https://personal-model.example",
                "api_key": "personal-model-secret",
                "credential_owner": "user",
            }

    def run(config, **_kwargs):
        calls.append(config)
        return {"ok": True, "summary": "personal endpoint reached", "mode": "custom"}

    monkeypatch.setattr(ct, "run_models_test", run)
    dispatcher = Dispatcher(
        load_settings(), object(), object(), model_config=PersonalModels(),
    )

    response = TestClient(create_app(dispatcher)).get(
        "/api/models/test", headers={"X-User-Id": "7"},
    )

    assert response.status_code == 200
    assert calls and calls[0]["api_key"] == "personal-model-secret"
    assert response.json()["summary"] == "personal endpoint reached"


def test_transcription_test_never_probes_or_discloses_platform_backend(monkeypatch):
    requested = []
    probes = []

    class PlatformContext:
        status = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, _size=-1):
            return json.dumps({
                "transcription_credential_owner": "operator",
                "transcription": {
                    "url": "https://private-operator-stt.example",
                    "token": "operator-stt-secret",
                },
            }).encode()

    def open_no_redirect(request, **_kwargs):
        requested.append(request.full_url)
        return PlatformContext()

    monkeypatch.setattr(ct, "open_no_redirect", open_no_redirect)
    monkeypatch.setattr(
        ct,
        "run_transcription_test",
        lambda *args, **kwargs: probes.append((args, kwargs)) or {"ok": True, "summary": "spent"},
    )
    dispatcher = Dispatcher(
        load_settings(
            admin_api_url="http://admin-api:8001",
            internal_api_secret="internal-secret",
        ),
        object(),
        object(),
    )

    response = TestClient(create_app(dispatcher)).get(
        "/api/transcription/test", headers={"X-User-Id": "7"},
    )
    body = response.json()

    assert requested == ["http://admin-api:8001/internal/users/7/bot-context"]
    assert probes == []
    assert body["managed"] is True and body["source"] == "operator"
    assert "private-operator-stt.example" not in json.dumps(body)
    assert "operator-stt-secret" not in json.dumps(body)
    assert "account" not in body and "balance" not in body


def test_transcription_test_env_tier_is_generic_and_never_probed(monkeypatch):
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_URL", "https://private-env-stt.example")
    monkeypatch.setenv("TRANSCRIPTION_SERVICE_TOKEN", "operator-env-stt-secret")
    probes = []
    monkeypatch.setattr(
        ct,
        "run_transcription_test",
        lambda *args, **kwargs: probes.append((args, kwargs)) or {"ok": True, "summary": "spent"},
    )

    response = TestClient(create_app(Dispatcher(load_settings(), object(), object()))).get(
        "/api/transcription/test", headers={"X-User-Id": "7"},
    )
    body = response.json()

    assert probes == []
    assert body["managed"] is True and body["source"] == "operator"
    assert "private-env-stt.example" not in json.dumps(body)
    assert "operator-env-stt-secret" not in json.dumps(body)
