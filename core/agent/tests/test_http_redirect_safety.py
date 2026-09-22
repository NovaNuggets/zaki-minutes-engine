"""Authenticated agent HTTP adapters keep credentials bound to their configured origin."""
from __future__ import annotations

import json
import threading
import urllib.error
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from control_plane import admin_panel, schedule_digest, transcription_watcher
from control_plane.api import _http_meeting_owner_lookup
from shared.adapters import (
    AdminApiMembershipIndex,
    AdminApiModelConfig,
    RuntimeHttpClient,
    SchedulerHttpClient,
)


class _Handler(BaseHTTPRequestHandler):
    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        self.server.requests.append({  # type: ignore[attr-defined]
            "method": self.command,
            "path": self.path,
            "headers": dict(self.headers),
            "body": body,
        })
        status, headers, payload = self.server.responder(self)  # type: ignore[attr-defined]
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        if payload:
            self.wfile.write(payload)

    do_GET = _handle
    do_POST = _handle
    do_DELETE = _handle

    def log_message(self, _format: str, *_args) -> None:
        pass


@contextmanager
def _serve(responder):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
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


def _json_response(payload: dict) -> tuple[int, dict[str, str], bytes]:
    return 200, {"Content-Type": "application/json"}, json.dumps(payload).encode()


def _oversized_json(payload: dict) -> tuple[int, dict[str, str], bytes]:
    body = json.dumps({**payload, "padding": "x" * (1024 * 1024)}).encode()
    return 200, {
        "Content-Type": "application/json",
        "Content-Length": str(len(body)),
    }, body


def test_runtime_spawn_succeeds_directly_but_never_follows_a_redirect() -> None:
    secret_env = {"ANTHROPIC_API_KEY": "runtime-secret"}
    with _serve(lambda _request: _json_response({"workloadId": "unit-direct"})) as (direct, direct_url):
        assert RuntimeHttpClient(
            direct_url,
            control_secret="agent-runtime-control-secret",
        ).spawn("unit-requested", "agent", secret_env) == "unit-direct"
        assert json.loads(direct.requests[0]["body"])["env"] == secret_env
        assert direct.requests[0]["headers"]["X-Runtime-Control-Secret"] == (
            "agent-runtime-control-secret"
        )

    with _serve(lambda _request: _json_response({"workloadId": "sink"})) as (sink, sink_url):
        with _serve(lambda _request: (302, {"Location": f"{sink_url}/stolen"}, b"")) as (_, redirect_url):
            with pytest.raises(urllib.error.HTTPError) as exc:
                RuntimeHttpClient(redirect_url).spawn("unit-requested", "agent", secret_env)

    assert exc.value.code == 302
    assert sink.requests == []


def test_runtime_spawn_and_status_responses_are_origin_bound_and_bounded() -> None:
    oversized = _oversized_json({"workloadId": "unit-direct", "state": "running"})
    with _serve(lambda _request: oversized) as (server, base_url):
        runtime = RuntimeHttpClient(base_url)
        with pytest.raises(ValueError, match="too large"):
            runtime.spawn("unit-requested", "agent", {})
        with pytest.raises(ValueError, match="too large"):
            runtime.await_done("unit-requested")
        assert len(server.requests) == 2

    with _serve(lambda _request: _json_response({"state": "stolen"})) as (sink, sink_url):
        with _serve(lambda request: (302, {"Location": f"{sink_url}{request.path}"}, b"")) as (_, redirect_url):
            with pytest.raises(urllib.error.HTTPError) as exc:
                RuntimeHttpClient(redirect_url).await_done("unit-requested")
    assert exc.value.code == 302
    assert sink.requests == []

    with _serve(lambda _request: _json_response({"workloadId": "must-not-run"})) as (server, base_url):
        with pytest.raises(ValueError, match="request is too large"):
            RuntimeHttpClient(base_url).spawn(
                "unit-requested", "agent", {"PRIVATE_TOKEN": "x" * (1024 * 1024)}
            )
    assert server.requests == []


def test_schedule_digest_identity_headers_stay_on_origin_and_response_is_bounded() -> None:
    row = {"id": 42, "status": "scheduled", "data": {"title": "private"}}
    with _serve(lambda _request: _json_response({"meetings": [row]})) as (direct, direct_url):
        assert schedule_digest.fetch_user_meetings(direct_url, "user-1", ["private-workspace"]) == [row]
        assert len(direct.requests) == 4
        assert direct.requests[0]["headers"]["X-User-Id"] == "user-1"
        assert direct.requests[0]["headers"]["X-User-Workspaces"] == "private-workspace"

    with _serve(lambda _request: _json_response({"meetings": [row]})) as (sink, sink_url):
        with _serve(lambda request: (302, {"Location": f"{sink_url}{request.path}"}, b"")) as (_, redirect_url):
            with pytest.raises(urllib.error.HTTPError):
                schedule_digest.fetch_user_meetings(redirect_url, "user-1", ["private-workspace"])
    assert sink.requests == []

    with _serve(lambda _request: _oversized_json({"meetings": [row]})) as (_, base_url):
        with pytest.raises(ValueError, match="too large"):
            schedule_digest.fetch_user_meetings(base_url, "user-1")


def test_admin_diagnostics_reads_are_origin_bound_bounded_and_shape_checked() -> None:
    with _serve(lambda _request: _json_response([{"workloadId": "agent-1"}])) as (direct, base_url):
        assert admin_panel.fetch_workloads(
            base_url,
            control_secret="agent-runtime-control-secret",
        )[0]["kind"] == "agent-worker"
        assert direct.requests[0]["headers"]["X-Runtime-Control-Secret"] == (
            "agent-runtime-control-secret"
        )
    with _serve(lambda _request: _json_response({"status": "ok"})) as (_, base_url):
        _, body = admin_panel._http_health(f"{base_url}/health")
        assert body == {"status": "ok"}

    with _serve(lambda _request: _json_response([])) as (sink, sink_url):
        with _serve(lambda request: (302, {"Location": f"{sink_url}{request.path}"}, b"")) as (_, redirect_url):
            with pytest.raises(urllib.error.HTTPError):
                admin_panel.fetch_workloads(redirect_url)
    assert sink.requests == []

    with _serve(lambda _request: _oversized_json({"status": "ok"})) as (_, base_url):
        with pytest.raises(ValueError, match="too large"):
            admin_panel._http_health(f"{base_url}/health")


def test_admin_api_adapters_succeed_directly_but_never_forward_the_internal_secret() -> None:
    def admin_response(request):
        if request.path.endswith("/memberships"):
            return _json_response({"memberships": [{"workspace_id": "shared-a"}]})
        return _json_response({"models": {"model": "test-model"}})

    with _serve(admin_response) as (direct, direct_url):
        memberships = AdminApiMembershipIndex(direct_url, "internal-secret")
        models = AdminApiModelConfig(direct_url, "internal-secret")
        assert memberships.list("user-1") == [{"workspace_id": "shared-a"}]
        assert models.resolve("user-1") == {"model": "test-model"}
        assert [request["headers"]["X-Internal-Secret"] for request in direct.requests] == [
            "internal-secret",
            "internal-secret",
        ]

    with _serve(admin_response) as (sink, sink_url):
        with _serve(lambda request: (302, {"Location": f"{sink_url}{request.path}"}, b"")) as (_, redirect_url):
            memberships = AdminApiMembershipIndex(redirect_url, "internal-secret")
            models = AdminApiModelConfig(redirect_url, "internal-secret")
            with pytest.raises(urllib.error.HTTPError) as membership_exc:
                memberships.list("user-1")
            with pytest.raises(urllib.error.HTTPError) as model_exc:
                models.resolve("user-1")

    assert membership_exc.value.code == 302
    assert model_exc.value.code == 302
    assert sink.requests == []


def test_admin_api_adapter_responses_are_bounded() -> None:
    def oversized(request):
        if request.path.endswith("/memberships"):
            return _oversized_json({"memberships": [{"workspace_id": "shared-a"}]})
        return _oversized_json({"models": {"model": "test-model"}})

    with _serve(oversized) as (_, base_url):
        memberships = AdminApiMembershipIndex(base_url, "internal-secret")
        models = AdminApiModelConfig(base_url, "internal-secret")

        with pytest.raises(ValueError, match="too large"):
            memberships.list("user-1")
        with pytest.raises(ValueError, match="too large"):
            models.resolve("user-1")

    chunked_body = _oversized_json({"models": {"model": "test-model"}})[2]
    with _serve(lambda _request: (200, {"Content-Type": "application/json"}, chunked_body)) as (_, base_url):
        with pytest.raises(ValueError, match="too large"):
            AdminApiModelConfig(base_url, "internal-secret").resolve("user-1")


def test_admin_api_model_config_rejects_nonscalar_or_unknown_fields() -> None:
    malformed = {
        "models": {
            "mode": {"nested": "custom"},
            "base_url": "https://models.example.com",
            "api_key": ["not", "a", "scalar"],
            "unexpected": "field",
        },
    }
    with _serve(lambda _request: _json_response(malformed)) as (_, base_url):
        with pytest.raises(ValueError, match="invalid model config"):
            AdminApiModelConfig(base_url, "internal-secret").resolve("user-1")


def test_scheduler_keeps_job_data_on_origin_and_bounds_responses() -> None:
    with _serve(lambda _request: _json_response({"id": "job-1"})) as (direct, direct_url):
        assert SchedulerHttpClient(
            direct_url,
            control_secret="agent-runtime-control-secret",
        ).schedule({"prompt": "private meeting notes"}) == {"id": "job-1"}
        assert json.loads(direct.requests[0]["body"]) == {"prompt": "private meeting notes"}
        assert direct.requests[0]["headers"]["X-Runtime-Control-Secret"] == (
            "agent-runtime-control-secret"
        )


def test_runtime_stop_and_scheduler_reads_carry_the_control_secret() -> None:
    def responder(request):
        if request.path.startswith("/schedule") and request.command == "GET":
            return 200, {"Content-Type": "application/json"}, b"[]"
        return _json_response({"state": "stopped", "id": "job-1"})

    with _serve(responder) as (server, base_url):
        runtime = RuntimeHttpClient(
            base_url,
            control_secret="agent-runtime-control-secret",
        )
        scheduler = SchedulerHttpClient(
            base_url,
            control_secret="agent-runtime-control-secret",
        )

        assert runtime.stop("agent-meeting-1") == "stopped"
        assert scheduler.list_jobs() == []
        assert scheduler.cancel_job("job-1") == {"state": "stopped", "id": "job-1"}

    assert [request["method"] for request in server.requests] == ["POST", "GET", "DELETE"]
    assert {
        request["headers"]["X-Runtime-Control-Secret"]
        for request in server.requests
    } == {"agent-runtime-control-secret"}

    with _serve(lambda _request: _json_response({"id": "stolen"})) as (sink, sink_url):
        with _serve(lambda request: (302, {"Location": f"{sink_url}{request.path}"}, b"")) as (_, redirect_url):
            with pytest.raises(urllib.error.HTTPError) as exc:
                SchedulerHttpClient(redirect_url).schedule({"prompt": "private meeting notes"})

    assert exc.value.code == 302
    assert sink.requests == []

    with _serve(lambda _request: _oversized_json({"id": "job-1"})) as (_, oversized_url):
        with pytest.raises(ValueError, match="too large"):
            SchedulerHttpClient(oversized_url).schedule({"prompt": "bounded"})

    with _serve(lambda _request: _json_response({"id": "job-1"})) as (sink, direct_url):
        with pytest.raises(ValueError, match="request is too large"):
            SchedulerHttpClient(direct_url).schedule({"prompt": "x" * (1024 * 1024)})
    assert sink.requests == []


def test_meeting_owner_lookup_refuses_redirect_and_oversized_records() -> None:
    record = {"id": 42, "user_id": "user-1", "native_meeting_id": "abc-defg-hij"}
    with _serve(lambda _request: _json_response(record)) as (_, direct_url):
        assert _http_meeting_owner_lookup(direct_url)("user-1", "42") == record

    with _serve(lambda _request: _json_response(record)) as (sink, sink_url):
        with _serve(lambda request: (302, {"Location": f"{sink_url}{request.path}"}, b"")) as (_, redirect_url):
            assert _http_meeting_owner_lookup(redirect_url)("user-1", "42") is None

    assert sink.requests == []

    with _serve(lambda _request: _oversized_json(record)) as (_, oversized_url):
        assert _http_meeting_owner_lookup(oversized_url)("user-1", "42") is None


def test_transcription_owner_authority_is_origin_bound_bounded_and_shape_checked() -> None:
    record = {"meeting_id": "42", "user_id": "7"}
    with _serve(lambda _request: _json_response(record)) as (direct, direct_url):
        lookup = transcription_watcher._http_owner_lookup(direct_url, "internal-secret")
        assert lookup("42") == record
        assert direct.requests[0]["path"] == "/internal/meetings/42/owner"
        assert direct.requests[0]["headers"]["X-Internal-Secret"] == "internal-secret"

    with _serve(lambda _request: _json_response(record)) as (sink, sink_url):
        with _serve(lambda request: (302, {"Location": f"{sink_url}{request.path}"}, b"")) as (_, redirect_url):
            assert transcription_watcher._http_owner_lookup(
                redirect_url, "internal-secret"
            )("42") is None
    assert sink.requests == []

    with _serve(lambda _request: _oversized_json(record)) as (_, oversized_url):
        assert transcription_watcher._http_owner_lookup(
            oversized_url, "internal-secret"
        )("42") is None

    malformed = [
        {"meeting_id": "43", "user_id": "7"},
        {"meeting_id": "42", "user_id": "caller-supplied-owner"},
        {"meeting_id": "42", "user_id": str(2**63)},
        {"meeting_id": 42, "user_id": 7},
        {"meeting_id": "42", "user_id": "7", "private": "overshared"},
    ]
    for payload in malformed:
        with _serve(lambda _request, payload=payload: _json_response(payload)) as (_, base_url):
            assert transcription_watcher._http_owner_lookup(
                base_url, "internal-secret"
            )("42") is None


def test_transcription_owner_authority_requires_configuration_before_network() -> None:
    with _serve(lambda _request: _json_response({"meeting_id": "42", "user_id": "7"})) as (server, base_url):
        assert transcription_watcher._http_owner_lookup(base_url, "")("42") is None
        configured = transcription_watcher._http_owner_lookup(base_url, "internal-secret")
        assert configured("042") is None
        assert configured(str(2**63)) is None
    assert server.requests == []


def test_transcription_watcher_doc_link_is_exact_row_origin_bound_and_bounded(monkeypatch) -> None:
    golden_path = (
        Path(__file__).resolve().parents[2]
        / "meetings/contracts/agent-control.v1/golden/DocLinkResponse.connected.json"
    )
    record = json.loads(golden_path.read_text())
    assert transcription_watcher._validated_doc_link_response(record, 42) is True
    obsolete_native_path = json.loads(json.dumps(record))
    obsolete_native_path["doc"].update({
        "path": "kg/entities/meeting/abc-defg-hij.md",
        "title": "abc-defg-hij",
    })
    assert transcription_watcher._validated_doc_link_response(obsolete_native_path, 42) is False
    monkeypatch.setenv("VEXA_INTERNAL_API_SECRET", "internal-secret")
    monkeypatch.setenv("VEXA_BOT_API_KEY", "must-never-cross-this-edge")

    with _serve(lambda _request: _json_response(record)) as (direct, direct_url):
        monkeypatch.setenv("VEXA_MEETING_API_URL", direct_url)
        assert transcription_watcher._record_meeting_doc("42") is True
        assert direct.requests == [{
            "method": "POST",
            "path": "/internal/meetings/42/docs",
            "headers": direct.requests[0]["headers"],
            "body": b"",
        }]
        assert direct.requests[0]["headers"]["X-Internal-Secret"] == "internal-secret"
        assert "X-Api-Key" not in direct.requests[0]["headers"]

    with _serve(lambda _request: _json_response(record)) as (sink, sink_url):
        with _serve(lambda request: (302, {"Location": f"{sink_url}{request.path}"}, b"")) as (_, redirect_url):
            monkeypatch.setenv("VEXA_MEETING_API_URL", redirect_url)
            assert transcription_watcher._record_meeting_doc("42") is False
    assert sink.requests == []

    with _serve(lambda _request: _oversized_json(record)) as (_, oversized_url):
        monkeypatch.setenv("VEXA_MEETING_API_URL", oversized_url)
        assert transcription_watcher._record_meeting_doc("42") is False


def test_transcription_native_resolve_bounds_and_validates_gateway_rows(monkeypatch) -> None:
    monkeypatch.setenv("VEXA_BOT_API_KEY", "bot-secret")
    for responder in (
        lambda _request: _oversized_json({"meetings": []}),
        lambda _request: _json_response({
            "meetings": [
                {"id": index, "native_meeting_id": f"native-{index}"}
                for index in range(transcription_watcher.MEETINGS_LIST_LIMIT + 1)
            ],
        }),
        lambda _request: _json_response({"meetings": ["not-a-meeting"]}),
    ):
        transcription_watcher._native.clear()
        transcription_watcher._resolve_miss_at.clear()
        with _serve(responder) as (_, base_url):
            monkeypatch.setenv("VEXA_GATEWAY_URL", base_url)
            assert transcription_watcher._resolve_native("42") is None
