"""Bounded, redirect-free client for Agent-owned meeting derivative erasure."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from meeting_api.agent_erasure import AgentMinutesEraser
from meeting_api.erasure_receipts import sign_erasure_receipt


NOW = datetime(2026, 7, 15, 12, 30, tzinfo=timezone.utc)
AGENT_KEY_ID = "agent-erasure-2026-07"
AGENT_SIGNING_SECRET = "agent-signing-secret-01234567890123"


def _signed_agent_receipt(**overrides):
    values = {
        "owner": "agent",
        "scope": "meeting",
        "user_id": "7",
        "meeting_id": "41",
        "counts": {
            "agent_unit_streams": 1,
            "agent_workspace_documents": 2,
            "agent_brain_records": 3,
        },
        "issued_at": NOW,
        "key_id": AGENT_KEY_ID,
        "nonce": "01J2M3N4P5Q6R7S8T9V0WXYZAA",
        "secret": AGENT_SIGNING_SECRET,
    }
    values.update(overrides)
    return sign_erasure_receipt(**values)


def _eraser(handler, **overrides):
    values = {
        "verification_keys": {AGENT_KEY_ID: AGENT_SIGNING_SECRET},
        "now": lambda: NOW,
        "transport": httpx.MockTransport(handler),
    }
    values.update(overrides)
    return AgentMinutesEraser(
        "http://agent-api:8080",
        "internal-secret",
        **values,
    )


@pytest.mark.asyncio
async def test_agent_erasure_is_secret_authenticated_and_projects_counts_only():
    seen = {}

    def handler(request: httpx.Request):
        seen["request"] = request
        return httpx.Response(200, json=_signed_agent_receipt())

    eraser = _eraser(handler)
    receipt = await eraser(user_id=7, meeting_id=41)

    assert seen["request"].url.path == "/internal/minutes/meetings/41/erase"
    assert seen["request"].headers["x-internal-secret"] == "internal-secret"
    assert json.loads(seen["request"].content) == {"user_id": "7"}
    assert AGENT_SIGNING_SECRET not in str(seen["request"].headers)
    assert AGENT_SIGNING_SECRET.encode() not in seen["request"].content
    assert receipt.as_dict() == {
        **_signed_agent_receipt(),
    }


@pytest.mark.asyncio
async def test_agent_erasure_rejects_redirect_and_oversized_or_invalid_receipts():
    redirected = _eraser(
        lambda _request: httpx.Response(
            307, headers={"location": "http://attacker.invalid/steal"},
        ),
    )
    with pytest.raises(RuntimeError, match="redirect"):
        await redirected(user_id=7, meeting_id=41)

    oversized = _eraser(
        lambda _request: httpx.Response(
            200, content=json.dumps({"padding": "x" * 20_000}).encode(),
        ),
    )
    with pytest.raises(RuntimeError, match="too large"):
        await oversized(user_id=7, meeting_id=41)

    invalid = _eraser(
        lambda _request: httpx.Response(200, json={
            "meeting_id": "other",
            "tombstoned": True,
            "deleted": {"unit_streams": 1, "workspace_documents": 2, "brain_records": 3},
        }),
    )
    with pytest.raises(RuntimeError, match="invalid"):
        await invalid(user_id=7, meeting_id=41)


@pytest.mark.asyncio
async def test_agent_erasure_fails_closed_on_non_success_and_bad_identity():
    unavailable = _eraser(lambda _request: httpx.Response(503))
    with pytest.raises(RuntimeError, match="unavailable"):
        await unavailable(user_id=7, meeting_id=41)
    with pytest.raises(ValueError, match="identity"):
        await unavailable(user_id=0, meeting_id=41)


@pytest.mark.asyncio
async def test_agent_erasure_transport_failure_does_not_retain_the_internal_secret():
    secret = "internal-secret"

    def leaked_request(request: httpx.Request):
        raise httpx.ConnectError(
            f"failed request carried X-Internal-Secret: {secret}", request=request,
        )

    eraser = _eraser(leaked_request)

    with pytest.raises(RuntimeError, match="transport") as raised:
        await eraser(user_id=7, meeting_id=41)

    assert secret not in str(raised.value)
    assert raised.value.__cause__ is None


def test_agent_erasure_rejects_ambiguous_service_configuration():
    for url in (
        "file:///tmp/agent",
        "http://user:pass@agent-api:8080",
        "http://agent-api:8080/base?token=secret",
    ):
        with pytest.raises(ValueError, match="service URL"):
            AgentMinutesEraser(
                url,
                "secret",
                verification_keys={AGENT_KEY_ID: AGENT_SIGNING_SECRET},
                now=lambda: NOW,
            )
    with pytest.raises(ValueError, match="internal secret"):
        AgentMinutesEraser(
            "http://agent-api:8080",
            "",
            verification_keys={AGENT_KEY_ID: AGENT_SIGNING_SECRET},
            now=lambda: NOW,
        )
    for keys in (
        {},
        {AGENT_KEY_ID: ""},
        {"": AGENT_SIGNING_SECRET},
        {AGENT_KEY_ID: "secret"},
    ):
        with pytest.raises(ValueError, match="verification key"):
            AgentMinutesEraser(
                "http://agent-api:8080", "secret", verification_keys=keys, now=lambda: NOW
            )
    for timeout in (0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="timeout"):
            AgentMinutesEraser(
                "http://agent-api:8080",
                "secret",
                verification_keys={AGENT_KEY_ID: AGENT_SIGNING_SECRET},
                now=lambda: NOW,
                timeout_seconds=timeout,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipt",
    [
        # Correctly signed, but bound to the wrong owner/scope/subject.
        _signed_agent_receipt(owner="minutes", counts={
            "meeting_rows": 1,
            "transcript_rows": 1,
            "summary_documents": 1,
            "recording_objects": 1,
            "agent_unit_streams": 1,
            "agent_workspace_documents": 2,
            "agent_brain_records": 3,
        }),
        _signed_agent_receipt(scope="account", meeting_id=None),
        _signed_agent_receipt(user_id="8"),
        _signed_agent_receipt(meeting_id="42"),
    ],
    ids=["owner", "scope", "user", "meeting"],
)
async def test_agent_erasure_rejects_signed_context_mismatch(receipt):
    eraser = _eraser(lambda _request: httpx.Response(200, json=receipt))

    with pytest.raises(RuntimeError, match="invalid"):
        await eraser(user_id=7, meeting_id=41)


@pytest.mark.asyncio
async def test_agent_erasure_accepts_old_durable_idempotent_receipt():
    receipt = _signed_agent_receipt(issued_at=NOW - timedelta(days=30))
    eraser = _eraser(lambda _request: httpx.Response(200, json=receipt))

    accepted = await eraser(user_id=7, meeting_id=41)

    assert accepted.as_dict() == receipt


def test_agent_erasure_reverifies_exact_persisted_receipt_without_age_expiry():
    eraser = _eraser(lambda _request: httpx.Response(500))
    old = _signed_agent_receipt(issued_at=NOW - timedelta(days=30))
    tampered = _signed_agent_receipt()
    tampered["counts"]["agent_brain_records"] = 99

    assert eraser.verify_durable_receipt(old, user_id=7, meeting_id=41) is True
    assert eraser.verify_durable_receipt(tampered, user_id=7, meeting_id=41) is False


@pytest.mark.asyncio
async def test_agent_erasure_rejects_tamper_and_unknown_signing_key():
    tampered = _signed_agent_receipt()
    tampered["counts"]["agent_brain_records"] = 4
    unknown_key = _signed_agent_receipt(
        key_id="agent-erasure-unknown",
        secret="unknown-agent-secret-012345678901",
    )

    for receipt in (tampered, unknown_key):
        eraser = _eraser(lambda _request, body=receipt: httpx.Response(200, json=body))
        with pytest.raises(RuntimeError, match="invalid"):
            await eraser(user_id=7, meeting_id=41)


@pytest.mark.asyncio
async def test_agent_erasure_accepts_one_previous_verifier_during_rotation():
    previous_key_id = "agent-erasure-2026-06"
    previous_secret = "previous-agent-signing-secret-0123456"
    receipt = _signed_agent_receipt(
        key_id=previous_key_id,
        secret=previous_secret,
    )
    eraser = _eraser(
        lambda _request: httpx.Response(200, json=receipt),
        verification_keys={
            AGENT_KEY_ID: AGENT_SIGNING_SECRET,
            previous_key_id: previous_secret,
        },
    )

    accepted = await eraser(user_id=7, meeting_id=41)

    assert accepted.as_dict() == receipt
