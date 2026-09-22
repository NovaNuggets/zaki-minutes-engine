"""Agent-owned erasure.v1 signing compatibility with the Minutes consumer."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys
import traceback

import pytest
import fakeredis

from control_plane.agent_erasure_receipts import (
    AgentErasureSigningError,
    AgentErasureV1Signer,
    RedisAgentErasureReceiptStore,
    SignedAgentMinutesAccountErasure,
    SignedAgentMinutesErasure,
)
from control_plane.minutes_account_erasure import RedisMinutesAccountErasureState


NOW = datetime(2026, 7, 15, 12, 30, tzinfo=timezone.utc)
SECRET = "erasure-golden-secret-v1-0123456789"
PREVIOUS_SECRET = "agent-erasure-previous-secret-0123456789"
CURRENT_SECRET = "agent-erasure-current-secret-01234567890"
MEETING_NONCE = "01J2M3N4P5Q6R7S8T9V0WXYZAB"
ACCOUNT_NONCE = "01J2M3N4P5Q6R7S8T9V0WXYZAC"


def _signer(nonce: str) -> AgentErasureV1Signer:
    return AgentErasureV1Signer(
        key_id="agent-erasure-2026-07",
        secret=SECRET,
        clock=lambda: NOW,
        nonce=lambda: nonce,
    )


def _meeting_receipt() -> dict:
    return {
        "meeting_id": "41",
        "tombstoned": True,
        "deleted": {
            "unit_streams": 1,
            "workspace_documents": 2,
            "brain_records": 3,
        },
    }


def _account_receipt() -> dict:
    return {
        "user_id": "7",
        "tombstoned": True,
        "deleted": {
            "unit_streams": 4,
            "workspace_documents": 5,
            "brain_records": 6,
        },
    }


def test_agent_meeting_signature_matches_the_sealed_consumer_vector():
    signed = _signer(MEETING_NONCE).sign_meeting(
        user_id=7,
        meeting_id="41",
        receipt=_meeting_receipt(),
    )

    assert signed == {
        "version": "erasure.v1",
        "owner": "agent",
        "scope": "meeting",
        "subject": {"user_id": "7", "meeting_id": "41"},
        "counts": {
            "agent_unit_streams": 1,
            "agent_workspace_documents": 2,
            "agent_brain_records": 3,
        },
        "issued_at": "2026-07-15T12:30:00Z",
        "key_id": "agent-erasure-2026-07",
        "nonce": MEETING_NONCE,
        "digest": "sha256=7113abe6498881985d5213c17d5732677619a2d37929a9a37f8a92618aaf26b4",
        "signature": "sha256=2aaaf83cdeafb7a00efa965331bbd4ce9843b564fa8de82069f7540c48591608",
    }

    # Compatibility is checked against the independent Minutes consumer; Agent source never imports it.
    meeting_api_src = Path(__file__).resolve().parents[2] / "meetings/services/meeting-api/src"
    sys.path.insert(0, str(meeting_api_src))
    try:
        from meeting_api.erasure_receipts import verify_erasure_receipt

        assert verify_erasure_receipt(
            signed,
            SECRET,
            expected_owner="agent",
            expected_scope="meeting",
            expected_user_id="7",
            expected_meeting_id="41",
            now=lambda: NOW,
            max_age_seconds=300,
        )
    finally:
        sys.path.remove(str(meeting_api_src))


def test_agent_account_signature_maps_only_agent_counts_and_matches_vector():
    signed = _signer(ACCOUNT_NONCE).sign_account(user_id=7, receipt=_account_receipt())

    assert signed["scope"] == "account"
    assert signed["subject"] == {"user_id": "7"}
    assert signed["counts"] == {
        "agent_unit_streams": 4,
        "agent_workspace_documents": 5,
        "agent_brain_records": 6,
    }
    assert signed["digest"] == (
        "sha256=84d58efd44378f551d0c6d0d2bb0e127a14e42f9f40757097691812fc69d8189"
    )
    assert signed["signature"] == (
        "sha256=f746043bf4944145b6fd7ee75ffa950cc57ef56ee65bf5dd07da98b91e3de971"
    )


@pytest.mark.parametrize(
    "call",
    [
        lambda signer: signer.sign_meeting(
            user_id=7, meeting_id="42", receipt=_meeting_receipt(),
        ),
        lambda signer: signer.sign_meeting(
            user_id=7,
            meeting_id="41",
            receipt={**_meeting_receipt(), "content": "must never be signed"},
        ),
        lambda signer: signer.sign_account(
            user_id=8, receipt=_account_receipt(),
        ),
    ],
)
def test_signer_refuses_subject_mismatch_or_non_content_free_source(call):
    with pytest.raises(AgentErasureSigningError):
        call(_signer(MEETING_NONCE))


def test_meeting_wrapper_persists_one_signature_and_replays_it_byte_stably():
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.hset("zaki:agent:minutes-erasure:41", mapping={
        "user_id": "7",
        "state": "complete",
        "unit_streams": "1",
        "workspace_documents": "2",
        "brain_records": "3",
    })
    nonces: list[str] = []

    def nonce() -> str:
        value = MEETING_NONCE if not nonces else "01J2M3N4P5Q6R7S8T9V0WXYZAD"
        nonces.append(value)
        return value

    class StableRawEraser:
        def __init__(self):
            self.calls = 0

        def erase(self, *, user_id, meeting_id):
            self.calls += 1
            return _meeting_receipt()

    raw = StableRawEraser()
    wrapped = SignedAgentMinutesErasure(
        eraser=raw,
        receipts=RedisAgentErasureReceiptStore(
            redis,
            signer=AgentErasureV1Signer(
                key_id="agent-erasure-2026-07",
                secret=SECRET,
                clock=lambda: NOW,
                nonce=nonce,
            ),
        ),
    )

    first = wrapped.erase(user_id=7, meeting_id="41")
    encoded_first = redis.hget("zaki:agent:minutes-erasure:41", "signed_receipt")
    second = wrapped.erase(user_id=7, meeting_id="41")
    encoded_second = redis.hget("zaki:agent:minutes-erasure:41", "signed_receipt")

    assert first == second
    assert encoded_first == encoded_second
    assert first["nonce"] == MEETING_NONCE
    assert nonces == [MEETING_NONCE]
    assert raw.calls == 2


def test_account_wrapper_persists_one_signature_on_the_completed_account_hash():
    redis = fakeredis.FakeRedis(decode_responses=True)
    raw_receipt = _account_receipt()
    redis.hset("zaki:agent:minutes-account-erasure:7", mapping={
        "user_id": "7",
        "state": "complete",
        "snapshot": "[]",
        "receipt": (
            '{"deleted":{"brain_records":6,"unit_streams":4,'
            '"workspace_documents":5},"tombstoned":true,"user_id":"7"}'
        ),
    })

    class StableRawEraser:
        def erase(self, *, user_id):
            return raw_receipt

    wrapped = SignedAgentMinutesAccountErasure(
        eraser=StableRawEraser(),
        receipts=RedisAgentErasureReceiptStore(redis, signer=_signer(ACCOUNT_NONCE)),
    )

    first = wrapped.erase(user_id=7)
    second = wrapped.erase(user_id=7)

    assert first == second
    assert first["scope"] == "account"
    assert first["nonce"] == ACCOUNT_NONCE
    assert redis.hget("zaki:agent:minutes-account-erasure:7", "signed_receipt")
    assert RedisMinutesAccountErasureState(redis).begin(user_id=7).completed == raw_receipt


def test_persisted_receipt_replays_byte_stably_across_one_signing_key_rotation():
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.hset("zaki:agent:minutes-erasure:41", mapping={
        "user_id": "7",
        "state": "complete",
        "unit_streams": "1",
        "workspace_documents": "2",
        "brain_records": "3",
    })
    previous = AgentErasureV1Signer(
        key_id="agent-erasure-2026-06",
        secret=PREVIOUS_SECRET,
        clock=lambda: NOW,
        nonce=lambda: MEETING_NONCE,
    )
    first = RedisAgentErasureReceiptStore(redis, signer=previous).persist_meeting(
        user_id=7,
        meeting_id="41",
        receipt=_meeting_receipt(),
    )
    encoded = redis.hget("zaki:agent:minutes-erasure:41", "signed_receipt")
    current_nonce_calls: list[bool] = []
    current = AgentErasureV1Signer(
        key_id="agent-erasure-2026-07",
        secret=CURRENT_SECRET,
        previous_key_id="agent-erasure-2026-06",
        previous_secret=PREVIOUS_SECRET,
        clock=lambda: NOW,
        nonce=lambda: current_nonce_calls.append(True) or ACCOUNT_NONCE,
    )

    replayed = RedisAgentErasureReceiptStore(redis, signer=current).persist_meeting(
        user_id=7,
        meeting_id="41",
        receipt=_meeting_receipt(),
    )

    assert replayed == first
    assert redis.hget("zaki:agent:minutes-erasure:41", "signed_receipt") == encoded
    assert current_nonce_calls == []


def test_rotation_uses_the_previous_secret_for_verification_only():
    signer = AgentErasureV1Signer(
        key_id="agent-erasure-2026-07",
        secret=CURRENT_SECRET,
        previous_key_id="agent-erasure-2026-06",
        previous_secret=PREVIOUS_SECRET,
        clock=lambda: NOW,
        nonce=lambda: MEETING_NONCE,
    )

    signed = signer.sign_meeting(
        user_id=7,
        meeting_id="41",
        receipt=_meeting_receipt(),
    )

    assert signed["key_id"] == "agent-erasure-2026-07"
    meeting_api_src = Path(__file__).resolve().parents[2] / "meetings/services/meeting-api/src"
    sys.path.insert(0, str(meeting_api_src))
    try:
        from meeting_api.erasure_receipts import verify_erasure_receipt

        verification_context = {
            "expected_owner": "agent",
            "expected_scope": "meeting",
            "expected_user_id": "7",
            "expected_meeting_id": "41",
            "now": lambda: NOW,
            "max_age_seconds": 300,
        }
        assert verify_erasure_receipt(signed, CURRENT_SECRET, **verification_context)
        assert not verify_erasure_receipt(signed, PREVIOUS_SECRET, **verification_context)
    finally:
        sys.path.remove(str(meeting_api_src))


@pytest.mark.parametrize(
    "overrides",
    [
        {"secret": "too-short"},
        {"previous_key_id": "agent-erasure-2026-06"},
        {"previous_secret": PREVIOUS_SECRET},
        {
            "previous_key_id": "agent-erasure-2026-07",
            "previous_secret": PREVIOUS_SECRET,
        },
        {
            "previous_key_id": "agent-erasure-2026-06",
            "previous_secret": CURRENT_SECRET,
        },
        {
            "previous_key_id": "agent-erasure-2026-06",
            "previous_secret": "too-short",
        },
    ],
)
def test_signer_refuses_short_partial_or_alias_rotation_keys(overrides):
    kwargs = {
        "key_id": "agent-erasure-2026-07",
        "secret": CURRENT_SECRET,
        "previous_key_id": None,
        "previous_secret": None,
        "clock": lambda: NOW,
        "nonce": lambda: MEETING_NONCE,
        **overrides,
    }

    with pytest.raises(AgentErasureSigningError, match="signer is invalid"):
        AgentErasureV1Signer(**kwargs)


@pytest.mark.parametrize(
    "invalid_secret",
    [
        " " + "a" * 32,
        "a" * 32 + " ",
        "a" * 16 + "\n" + "b" * 16,
        "a" * 16 + "\x7f" + "b" * 16,
        "é" * 32,
        "a" * 513,
        b" " + b"a" * 32,
        b"a" * 16 + b"\x00" + b"b" * 16,
        b"\xff" * 32,
        b"a" * 513,
    ],
)
def test_signer_refuses_noncanonical_current_or_previous_secret_material(invalid_secret):
    common = {
        "key_id": "agent-erasure-2026-07",
        "clock": lambda: NOW,
        "nonce": lambda: MEETING_NONCE,
    }
    with pytest.raises(AgentErasureSigningError, match="signer is invalid"):
        AgentErasureV1Signer(secret=invalid_secret, **common)
    with pytest.raises(AgentErasureSigningError, match="signer is invalid"):
        AgentErasureV1Signer(
            secret=CURRENT_SECRET,
            previous_key_id="agent-erasure-2026-06",
            previous_secret=invalid_secret,
            **common,
        )


@pytest.mark.parametrize("boundary_secret", ["a" * 32, "z" * 512, b"a" * 32, b"z" * 512])
def test_signer_accepts_canonical_secret_length_boundaries(boundary_secret):
    AgentErasureV1Signer(
        key_id="agent-erasure-2026-07",
        secret=boundary_secret,
        clock=lambda: NOW,
        nonce=lambda: MEETING_NONCE,
    )


def test_signer_failure_does_not_retain_sensitive_native_context():
    marker = "private-native-id-and-signing-material"

    def unavailable_clock():
        raise RuntimeError(marker)

    signer = AgentErasureV1Signer(
        key_id="agent-erasure-2026-07",
        secret=CURRENT_SECRET,
        clock=unavailable_clock,
        nonce=lambda: MEETING_NONCE,
    )

    with pytest.raises(AgentErasureSigningError, match="signer is unavailable") as raised:
        signer.sign_meeting(user_id=7, meeting_id="41", receipt=_meeting_receipt())

    rendered = "".join(traceback.format_exception(raised.value))
    assert marker not in rendered
    assert raised.value.__cause__ is None


def test_receipt_store_pipeline_lifecycle_failure_is_sanitized():
    marker = "redis-key-private-native-id"

    class FailingPipeline:
        def __enter__(self):
            raise RuntimeError(marker)

        def __exit__(self, *_args):
            return None

    class FailingRedis:
        def pipeline(self, **_kwargs):
            return FailingPipeline()

    store = RedisAgentErasureReceiptStore(FailingRedis(), signer=_signer(MEETING_NONCE))

    with pytest.raises(AgentErasureSigningError, match="receipt is unavailable") as raised:
        store.persist_meeting(user_id=7, meeting_id="41", receipt=_meeting_receipt())

    assert marker not in "".join(traceback.format_exception(raised.value))
    assert raised.value.__cause__ is None
