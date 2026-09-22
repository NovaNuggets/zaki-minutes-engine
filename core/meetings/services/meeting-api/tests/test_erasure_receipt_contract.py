"""erasure.v1: canonical, context-bound, replay-aware receipt signatures."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from meeting_api.erasure_receipts import sign_erasure_receipt, verify_erasure_receipt


NOW = datetime(2026, 7, 15, 12, 30, tzinfo=timezone.utc)
SECRET = "erasure-test-secret"


def _receipt(**overrides):
    values = {
        "owner": "minutes",
        "scope": "meeting",
        "user_id": "7",
        "meeting_id": "41",
        "counts": {
            "meeting_rows": 1,
            "transcript_rows": 3,
            "summary_documents": 1,
            "recording_objects": 2,
            "agent_unit_streams": 1,
            "agent_workspace_documents": 1,
            "agent_brain_records": 4,
        },
        "issued_at": NOW,
        "key_id": "minutes-erasure-2026-07",
        "nonce": "01J2M3N4P5Q6R7S8T9V0WXYZAB",
        "secret": SECRET,
    }
    values.update(overrides)
    return sign_erasure_receipt(**values)


def test_receipt_signature_is_canonical_and_context_bound():
    first = _receipt()
    reordered = _receipt(counts=dict(reversed(list(first["counts"].items()))))

    assert first == reordered
    assert verify_erasure_receipt(
        first,
        SECRET,
        expected_owner="minutes",
        expected_scope="meeting",
        expected_user_id="7",
        expected_meeting_id="41",
        now=lambda: NOW,
        max_age_seconds=300,
    )


def test_receipt_rejects_tamper_wrong_scope_and_wrong_subject():
    receipt = _receipt()
    tampered = deepcopy(receipt)
    tampered["counts"]["transcript_rows"] = 4

    assert not verify_erasure_receipt(tampered, SECRET, now=lambda: NOW)
    assert not verify_erasure_receipt(
        receipt, SECRET, expected_scope="account", now=lambda: NOW
    )
    assert not verify_erasure_receipt(
        receipt, SECRET, expected_user_id="8", now=lambda: NOW
    )
    assert not verify_erasure_receipt(
        receipt, SECRET, expected_meeting_id="42", now=lambda: NOW
    )


def test_receipt_freshness_is_opt_in_and_clock_injected():
    old = _receipt(issued_at=NOW - timedelta(minutes=6))

    # Durable receipts remain cryptographically verifiable for audit unless a caller
    # explicitly requires a fresh command/response exchange.
    assert verify_erasure_receipt(old, SECRET, now=lambda: NOW)
    assert not verify_erasure_receipt(
        old, SECRET, now=lambda: NOW, max_age_seconds=300
    )


def test_receipt_rejects_future_issue_time_and_seen_nonce_when_requested():
    future = _receipt(issued_at=NOW + timedelta(seconds=31))
    receipt = _receipt()

    assert not verify_erasure_receipt(
        future, SECRET, now=lambda: NOW, max_future_seconds=30
    )
    assert not verify_erasure_receipt(
        receipt,
        SECRET,
        now=lambda: NOW,
        seen_nonces={receipt["nonce"]},
    )


def test_meeting_scope_requires_meeting_and_account_scope_forbids_it():
    for values in (
        {"scope": "meeting", "meeting_id": None},
        {"scope": "account", "meeting_id": "41"},
    ):
        try:
            _receipt(**values)
        except ValueError:
            pass
        else:  # pragma: no cover - the assertion above must fail closed
            raise AssertionError("invalid erasure scope identity was signed")


def test_receipt_signer_rejects_owner_specific_count_subsets_and_extras():
    for owner, scope, counts in (
        ("minutes", "meeting", {"meeting_rows": 1}),
        ("minutes", "account", {
            **_receipt()["counts"],
            "private_blobs": 1,
        }),
        ("agent", "meeting", {
            "agent_unit_streams": 1,
            "agent_workspace_documents": 2,
        }),
        ("agent", "account", {
            "agent_unit_streams": 1,
            "agent_workspace_documents": 2,
            "agent_brain_records": 3,
            "meeting_rows": 1,
        }),
    ):
        with pytest.raises(ValueError, match="counts"):
            _receipt(
                owner=owner,
                scope=scope,
                meeting_id="41" if scope == "meeting" else None,
                counts=counts,
            )


def test_receipt_wire_rejects_numeric_subject_ids_even_when_integrity_matches():
    receipt = _receipt()
    receipt["subject"]["meeting_id"] = 41

    assert verify_erasure_receipt(receipt, SECRET, now=lambda: NOW) is False


def test_signing_edge_roundtrips_adjacent_large_internal_ids_as_decimal_text():
    receipt = _receipt(
        user_id=9_007_199_254_740_992,
        meeting_id=9_007_199_254_740_993,
    )

    assert receipt["subject"] == {
        "user_id": "9007199254740992",
        "meeting_id": "9007199254740993",
    }
    assert verify_erasure_receipt(
        receipt,
        SECRET,
        expected_user_id=9_007_199_254_740_992,
        expected_meeting_id=9_007_199_254_740_993,
        now=lambda: NOW,
    )
