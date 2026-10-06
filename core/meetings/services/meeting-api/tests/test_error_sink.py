"""L-0990 error-sink pins — each one red without its mechanism (mutant map in the PR body).

  * exactly one envelope per unhandled exception and per 5xx the service answers,
    captured by a fake-DSN transport, tagged SENTRY_ENVIRONMENT + SENTRY_RELEASE;
  * planted PII (a transcript line, an email) absent from the raw envelope bytes;
  * SENTRY_DSN unset or blank → the SDK is never imported: no init, no requests;
  * the vendored module is byte-identical to deploy/contracts/error_sink.py;
  * the production entrypoint wires init + the ASGI middleware.
"""
from __future__ import annotations

import inspect
import io
import os
import subprocess
import sys
from pathlib import Path

import pytest

from meeting_api import error_sink

PKG_MODULE = "meeting_api"
SERVICE = "meeting-api"
FAKE_DSN = "https://0123456789abcdef0123456789abcdef@o0.ingest.example.test/1"
TRANSCRIPT_LINE = "Alice: the merger closes at two billion euros"
EMAIL = "alice@example.com"


def _capture_transport():
    """A sentry_sdk Transport that serializes each envelope to bytes instead of dialing."""
    from sentry_sdk.transport import Transport

    class _Capture(Transport):
        def __init__(self):
            super().__init__()
            self.payloads: list[bytes] = []

        def capture_envelope(self, envelope):
            buf = io.BytesIO()
            envelope.serialize_into(buf)
            self.payloads.append(buf.getvalue())

        def capture_event(self, event):  # pragma: no cover — envelopes carry every event
            raise AssertionError("events must reach the wire via capture_envelope")

    return _Capture()


@pytest.fixture()
def sink(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", FAKE_DSN)
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "test-env")
    monkeypatch.setenv("SENTRY_RELEASE", "l0990-test")
    transport = _capture_transport()
    assert error_sink.init(SERVICE, transport=transport) is True
    return transport


def _app():
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    app = FastAPI()

    @app.api_route("/boom", methods=["GET", "POST"])
    async def boom():
        transcript = TRANSCRIPT_LINE  # frame-local plant: leaks only if frame vars ship
        raise RuntimeError(f"capture died mid-ingest: {transcript} cc {EMAIL}")

    @app.get("/degraded")
    async def degraded():
        return JSONResponse(status_code=503, content={"detail": "downstream"})

    app.add_middleware(error_sink.ErrorSinkMiddleware)
    return app


def test_unhandled_exception_is_one_envelope(sink):
    from fastapi.testclient import TestClient

    resp = TestClient(_app(), raise_server_exceptions=False).get("/boom")
    assert resp.status_code == 500
    assert len(sink.payloads) == 1
    assert b"RuntimeError" in sink.payloads[0]
    assert b"test-env" in sink.payloads[0] and b"l0990-test" in sink.payloads[0]


def test_answered_5xx_is_one_envelope(sink):
    from fastapi.testclient import TestClient

    resp = TestClient(_app(), raise_server_exceptions=False).get("/degraded")
    assert resp.status_code == 503
    assert len(sink.payloads) == 1
    assert b"HTTP 503 GET /degraded" in sink.payloads[0]


def test_planted_pii_absent_from_envelope_bytes(sink):
    from fastapi.testclient import TestClient

    TestClient(_app(), raise_server_exceptions=False).post(
        f"/boom?return_to={EMAIL}",
        content=TRANSCRIPT_LINE.encode(),
        headers={"Authorization": f"Bearer {EMAIL}", "Cookie": f"session={EMAIL}"},
    )
    assert len(sink.payloads) == 1
    assert TRANSCRIPT_LINE.encode() not in sink.payloads[0]
    assert EMAIL.encode() not in sink.payloads[0]


@pytest.mark.parametrize("dsn", [None, ""])
def test_without_dsn_the_sdk_is_never_imported(dsn):
    script = (
        "import sys\n"
        f"import {PKG_MODULE}.error_sink as es\n"
        "assert es.init('svc') is False\n"
        "assert 'sentry_sdk' not in sys.modules, 'SDK imported without a DSN'\n"
    )
    env = {k: v for k, v in os.environ.items() if k != "SENTRY_DSN"}
    if dsn is not None:
        env["SENTRY_DSN"] = dsn
    env["PYTHONPATH"] = str(Path(error_sink.__file__).resolve().parent.parent)
    proc = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr


def test_vendored_module_is_the_canonical_copy():
    vendored = Path(error_sink.__file__).resolve()
    canonical = next(
        candidate
        for parent in vendored.parents
        if (candidate := parent / "deploy" / "contracts" / "error_sink.py").is_file()
    )
    assert vendored.read_bytes() == canonical.read_bytes()


def test_entrypoint_wires_init_and_middleware():
    import importlib

    src = inspect.getsource(importlib.import_module(f"{PKG_MODULE}.__main__"))
    assert f'error_sink.init("{SERVICE}")' in src
    assert "error_sink.ErrorSinkMiddleware" in src
