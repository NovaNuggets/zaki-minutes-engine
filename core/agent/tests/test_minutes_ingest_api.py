"""Server-owned HTTP invocation for the bounded Minutes read pipeline."""
from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time

import pytest
from fastapi.testclient import TestClient

from control_plane.api import create_app
from control_plane.dispatch import Dispatcher
from control_plane.minutes_ingest import (
    MinutesIngestDisabled,
    MinutesIngestError,
    MinutesIngestResult,
)
from shared.config import load_settings


_GATEWAY_PROOF = "minutes-gateway-proof-0123456789abcdef"


class _Runtime:
    def spawn(self, *_args, **_kwargs):
        raise AssertionError("Minutes invocation must not spawn a durable chat worker")


class _Identity:
    def mint(self, *_args, **_kwargs):
        raise AssertionError("Minutes invocation must not mint a worker identity")


class _Ingestor:
    def __init__(self, outcome):
        self.outcome = outcome
        self.calls = []

    def summarize_last_meeting(self, user_id):
        self.calls.append(user_id)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


def _client(ingestor=None, *, replay_store=None) -> TestClient:
    dispatcher = Dispatcher(
        load_settings(
            internal_api_secret="minutes-internal-secret-0123456789abcdef",
            gateway_identity_secret=_GATEWAY_PROOF,
        ),
        _Runtime(),
        _Identity(),
    )
    return TestClient(create_app(
        dispatcher,
        minutes_ingestor=ingestor,
        gateway_replay_store=replay_store,
    ))


def _minutes_headers(
    user_id: str = "7", *, body: bytes = b"", content_type: str | None = None,
) -> dict[str, str]:
    timestamp = str(int(time.time()))
    nonce = secrets.token_urlsafe(24)
    path = "/api/minutes/summarize-last"
    content_digest = hashlib.sha256(body).hexdigest()
    signed_headers = {
        "content-type": content_type,
        "last-event-id": None,
        "x-user-email": None,
        "x-user-id": user_id,
    }
    signed_headers_digest = hashlib.sha256(json.dumps(
        signed_headers,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()).hexdigest()
    canonical = (
        f"gateway-request.v1\nPOST\n{path}\n{user_id}\n\n{content_digest}\n"
        f"{signed_headers_digest}\n{timestamp}\n{nonce}"
    ).encode("utf-8")
    headers = {
        "X-User-Id": user_id,
        "X-Gateway-Key-Id": hashlib.sha256(_GATEWAY_PROOF.encode()).hexdigest()[:16],
        "X-Gateway-Timestamp": timestamp,
        "X-Gateway-Nonce": nonce,
        "X-Gateway-Content-Sha256": content_digest,
        "X-Gateway-Signature": hmac.new(
            _GATEWAY_PROOF.encode(), canonical, hashlib.sha256,
        ).hexdigest(),
    }
    if content_type is not None:
        headers["Content-Type"] = content_type
    return headers


def test_flag_off_does_not_register_a_minutes_invocation_route():
    response = _client().post(
        "/api/minutes/summarize-last",
        headers={"X-User-Id": "7"},
    )

    assert response.status_code == 404


def test_minutes_invocation_derives_subject_and_returns_only_sanitized_result():
    raw_marker = "RAW-MEETING-SECRET-MUST-NOT-LEAVE"
    ingestor = _Ingestor(MinutesIngestResult(
        answer="The team approved the bounded pilot.",
        candidates_quarantined=0,
        summary_fallback=False,
        source_item_id=f"transcript:{raw_marker}",
        meeting_id=f"meeting:{raw_marker}",
    ))
    client = _client(ingestor)
    body = json.dumps(
        {"user_id": "8", "prompt": raw_marker},
        separators=(",", ":"),
    ).encode()

    response = client.post(
        "/api/minutes/summarize-last",
        headers=_minutes_headers(body=body, content_type="application/json"),
        content=body,
    )

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    assert response.json() == {
        "answer": "The team approved the bounded pilot.",
        "candidates_quarantined": 0,
        "summary_fallback": False,
    }
    assert raw_marker not in response.text
    assert ingestor.calls == ["7"]
    assert client.get("/api/sessions", headers={"X-User-Id": "7"}).json()["sessions"] == []


def test_minutes_invocation_maps_live_opt_out_and_outage_without_sensitive_detail():
    disabled = _client(_Ingestor(MinutesIngestDisabled("Minutes read is disabled"))).post(
        "/api/minutes/summarize-last", headers=_minutes_headers(),
    )
    unavailable = _client(_Ingestor(MinutesIngestError("Minutes read failed"))).post(
        "/api/minutes/summarize-last", headers=_minutes_headers(),
    )

    assert disabled.status_code == 403
    assert disabled.json() == {"detail": "Minutes read is disabled"}
    assert unavailable.status_code == 503
    assert unavailable.json() == {"detail": "Minutes read is unavailable"}


@pytest.mark.parametrize("case", ["missing", "raw-bearer", "missing-subject"])
def test_minutes_invocation_requires_verified_gateway_identity_and_explicit_subject(case):
    ingestor = _Ingestor(MinutesIngestError("must not be called"))
    if case == "missing":
        headers = {"X-User-Id": "7"}
    elif case == "raw-bearer":
        headers = {"X-User-Id": "7", "X-Gateway-Verified": _GATEWAY_PROOF}
    else:
        headers = _minutes_headers()
        headers.pop("X-User-Id")

    response = _client(ingestor).post(
        "/api/minutes/summarize-last",
        headers=headers,
    )

    assert response.status_code == 401
    assert ingestor.calls == []


def test_minutes_invocation_rejects_a_replayed_gateway_request():
    ingestor = _Ingestor(MinutesIngestResult(
        answer="One bounded answer.",
        candidates_quarantined=0,
        summary_fallback=False,
        source_item_id="transcript:1",
        meeting_id="meeting:1",
    ))
    client = _client(ingestor)
    headers = _minutes_headers()

    assert client.post("/api/minutes/summarize-last", headers=headers).status_code == 200
    assert client.post("/api/minutes/summarize-last", headers=headers).status_code == 401
    assert ingestor.calls == ["7"]


def test_tampered_body_is_rejected_without_burning_the_legitimate_nonce():
    ingestor = _Ingestor(MinutesIngestResult(
        answer="Bounded answer.",
        candidates_quarantined=0,
        summary_fallback=False,
        source_item_id="transcript:1",
        meeting_id="meeting:1",
    ))
    client = _client(ingestor)
    body = b'{"prompt":"original"}'
    headers = _minutes_headers(body=body, content_type="application/json")

    tampered = client.post(
        "/api/minutes/summarize-last",
        headers=headers,
        content=b'{"prompt":"tampered"}',
    )
    legitimate = client.post(
        "/api/minutes/summarize-last",
        headers=headers,
        content=body,
    )

    assert tampered.status_code == 401
    assert legitimate.status_code == 200
    assert ingestor.calls == ["7"]


def test_replay_store_outage_is_retryable_without_calling_minutes():
    class Unavailable:
        def claim(self, *_args, **_kwargs):
            raise RuntimeError("redis unavailable")

    ingestor = _Ingestor(MinutesIngestError("must not be called"))
    response = _client(ingestor, replay_store=Unavailable()).post(
        "/api/minutes/summarize-last",
        headers=_minutes_headers(),
    )

    assert response.status_code == 503
    assert response.headers["Retry-After"] == "1"
    assert response.json() == {"detail": "gateway identity boundary is unavailable"}
    assert ingestor.calls == []


def test_agent_rechecks_a_hard_signed_body_cap_before_minutes(monkeypatch):
    from control_plane import api as api_mod

    monkeypatch.setattr(api_mod, "MAX_GATEWAY_SIGNED_BODY_BYTES", 4)
    ingestor = _Ingestor(MinutesIngestError("must not be called"))
    body = b"12345"
    response = _client(ingestor).post(
        "/api/minutes/summarize-last",
        headers=_minutes_headers(body=body),
        content=body,
    )

    assert response.status_code == 413
    assert response.json() == {"detail": "request body too large"}
    assert ingestor.calls == []


def test_minutes_route_refuses_composition_without_a_gateway_verification_secret():
    dispatcher = Dispatcher(
        load_settings(
            internal_api_secret="minutes-internal-secret-0123456789abcdef",
            gateway_identity_secret="",
        ),
        _Runtime(),
        _Identity(),
    )

    with pytest.raises(RuntimeError, match="GATEWAY_IDENTITY_SECRET"):
        create_app(dispatcher, minutes_ingestor=_Ingestor(object()))
