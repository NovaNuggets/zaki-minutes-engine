"""Canonical signing primitives for the cross-spoke ``erasure.v1`` receipt.

The primitive is intentionally detached from the HTTP erasure orchestrator.  Minutes and
Agent can therefore adopt the sealed receipt independently without weakening today's
fail-closed deletion path or sharing write ownership.
"""
from __future__ import annotations

from collections.abc import Collection, Mapping
from datetime import datetime, timezone
import hashlib
import hmac
import json
import math
import re
from typing import Any, Callable, Optional


ERASURE_RECEIPT_VERSION = "erasure.v1"
_OWNERS = frozenset({"minutes", "agent"})
_SCOPES = frozenset({"meeting", "account"})
_MINUTES_COUNT_FIELDS = frozenset({
    "meeting_rows",
    "transcript_rows",
    "summary_documents",
    "recording_objects",
    "agent_unit_streams",
    "agent_workspace_documents",
    "agent_brain_records",
})
_AGENT_COUNT_FIELDS = frozenset({
    "agent_unit_streams",
    "agent_workspace_documents",
    "agent_brain_records",
})
_COUNT_MANIFESTS = {
    ("minutes", "meeting"): _MINUTES_COUNT_FIELDS,
    ("minutes", "account"): _MINUTES_COUNT_FIELDS,
    ("agent", "meeting"): _AGENT_COUNT_FIELDS,
    ("agent", "account"): _AGENT_COUNT_FIELDS,
}
_EXACT_FIELDS = frozenset({
    "version",
    "owner",
    "scope",
    "subject",
    "counts",
    "issued_at",
    "key_id",
    "nonce",
    "digest",
    "signature",
})
_ROW_ID = re.compile(r"^[1-9][0-9]{0,18}$")
_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_NONCE = re.compile(r"^[A-Za-z0-9_-]{16,128}$")
_SHA256 = re.compile(r"^sha256=[0-9a-f]{64}$")
_MAX_DB_ID = 2**63 - 1
_MAX_COUNT = 2_147_483_647


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _secret_bytes(secret: str | bytes) -> bytes:
    if isinstance(secret, bytes):
        value = secret
    elif isinstance(secret, str):
        value = secret.encode("utf-8")
    else:
        raise ValueError("erasure receipt secret must be bytes or text")
    if not value:
        raise ValueError("erasure receipt secret must not be empty")
    return value


def _row_id(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not _ROW_ID.fullmatch(value)
        or int(value) > _MAX_DB_ID
    ):
        raise ValueError(f"{field} must be a positive database identifier")
    return value


def _signing_row_id(value: object, field: str) -> str:
    """Normalize trusted in-process integer identities before crossing the JSON boundary."""
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a positive database identifier")
    return _row_id(str(value) if isinstance(value, int) else value, field)


def _counts(value: object, *, owner: str, scope: str) -> dict[str, int]:
    manifest = _COUNT_MANIFESTS.get((owner, scope))
    if not isinstance(value, Mapping) or manifest is None or set(value) != manifest:
        raise ValueError("erasure receipt counts are invalid")
    normalized: dict[str, int] = {}
    for key, count in value.items():
        if type(count) is not int or not 0 <= count <= _MAX_COUNT:
            raise ValueError("erasure receipt counts are invalid")
        normalized[str(key)] = count
    return normalized


def _issued_at(value: object) -> tuple[str, datetime]:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("erasure receipt issued_at is invalid") from None
    else:
        raise ValueError("erasure receipt issued_at is invalid")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("erasure receipt issued_at must include an offset")
    parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat().replace("+00:00", "Z"), parsed


def _base(
    *,
    owner: object,
    scope: object,
    user_id: object,
    meeting_id: object,
    counts: object,
    issued_at: object,
    key_id: object,
    nonce: object,
) -> dict[str, Any]:
    if owner not in _OWNERS:
        raise ValueError("erasure receipt owner is invalid")
    if scope not in _SCOPES:
        raise ValueError("erasure receipt scope is invalid")
    if not isinstance(key_id, str) or not _KEY_ID.fullmatch(key_id):
        raise ValueError("erasure receipt key_id is invalid")
    if not isinstance(nonce, str) or not _NONCE.fullmatch(nonce):
        raise ValueError("erasure receipt nonce is invalid")

    subject = {"user_id": _row_id(user_id, "user_id")}
    if scope == "meeting":
        if meeting_id is None:
            raise ValueError("meeting erasure receipt requires meeting_id")
        subject["meeting_id"] = _row_id(meeting_id, "meeting_id")
    elif meeting_id is not None:
        raise ValueError("account erasure receipt forbids meeting_id")

    issued_text, _ = _issued_at(issued_at)
    return {
        "version": ERASURE_RECEIPT_VERSION,
        "owner": owner,
        "scope": scope,
        "subject": subject,
        "counts": _counts(counts, owner=owner, scope=scope),
        "issued_at": issued_text,
        "key_id": key_id,
        "nonce": nonce,
    }


def sign_erasure_receipt(
    *,
    owner: str,
    scope: str,
    user_id: str | int,
    counts: Mapping[str, int],
    issued_at: datetime,
    key_id: str,
    nonce: str,
    secret: str | bytes,
    meeting_id: str | int | None = None,
) -> dict[str, Any]:
    """Return an exact ``erasure.v1`` receipt signed over canonical JSON.

    ``digest`` protects the canonical unsigned payload. ``signature`` authenticates that
    payload plus the digest, binding owner, scope, subject, counts, key id, and nonce.
    """
    base = _base(
        owner=owner,
        scope=scope,
        user_id=_signing_row_id(user_id, "user_id"),
        meeting_id=(
            _signing_row_id(meeting_id, "meeting_id")
            if meeting_id is not None else None
        ),
        counts=counts,
        issued_at=issued_at,
        key_id=key_id,
        nonce=nonce,
    )
    digest = f"sha256={hashlib.sha256(_canonical(base)).hexdigest()}"
    signed = {**base, "digest": digest}
    signature = hmac.new(_secret_bytes(secret), _canonical(signed), hashlib.sha256).hexdigest()
    return {**signed, "signature": f"sha256={signature}"}


def verify_erasure_receipt(
    receipt: object,
    secret: str | bytes,
    *,
    expected_owner: Optional[str] = None,
    expected_scope: Optional[str] = None,
    expected_user_id: str | int | None = None,
    expected_meeting_id: str | int | None = None,
    now: Optional[Callable[[], datetime]] = None,
    max_age_seconds: Optional[float] = None,
    max_future_seconds: Optional[float] = 30,
    seen_nonces: Optional[Collection[str]] = None,
) -> bool:
    """Verify integrity plus optional request context and replay bounds.

    Historical audit verification leaves ``max_age_seconds`` unset.  A live exchange can
    require freshness and pass a nonce store; the clock is injected for deterministic callers.
    Invalid input always returns ``False`` and never leaks which check failed.
    """
    try:
        if not isinstance(receipt, Mapping) or set(receipt) != _EXACT_FIELDS:
            return False
        if receipt.get("version") != ERASURE_RECEIPT_VERSION:
            return False
        subject = receipt.get("subject")
        if not isinstance(subject, Mapping):
            return False
        expected_subject_keys = (
            {"user_id", "meeting_id"} if receipt.get("scope") == "meeting" else {"user_id"}
        )
        if set(subject) != expected_subject_keys:
            return False
        base = _base(
            owner=receipt.get("owner"),
            scope=receipt.get("scope"),
            user_id=subject.get("user_id"),
            meeting_id=subject.get("meeting_id"),
            counts=receipt.get("counts"),
            issued_at=receipt.get("issued_at"),
            key_id=receipt.get("key_id"),
            nonce=receipt.get("nonce"),
        )
        stored_digest = receipt.get("digest")
        stored_signature = receipt.get("signature")
        if (
            not isinstance(stored_digest, str)
            or not _SHA256.fullmatch(stored_digest)
            or not isinstance(stored_signature, str)
            or not _SHA256.fullmatch(stored_signature)
        ):
            return False
        digest = f"sha256={hashlib.sha256(_canonical(base)).hexdigest()}"
        if not hmac.compare_digest(stored_digest, digest):
            return False
        signed = {**base, "digest": digest}
        signature = "sha256=" + hmac.new(
            _secret_bytes(secret), _canonical(signed), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(stored_signature, signature):
            return False

        if expected_owner is not None and base["owner"] != expected_owner:
            return False
        if expected_scope is not None and base["scope"] != expected_scope:
            return False
        if expected_user_id is not None and base["subject"]["user_id"] != _row_id(
            str(expected_user_id) if isinstance(expected_user_id, int) else expected_user_id,
            "expected_user_id",
        ):
            return False
        if expected_meeting_id is not None and base["subject"].get("meeting_id") != _row_id(
            str(expected_meeting_id) if isinstance(expected_meeting_id, int) else expected_meeting_id,
            "expected_meeting_id",
        ):
            return False
        if seen_nonces is not None and base["nonce"] in seen_nonces:
            return False

        _, issued = _issued_at(base["issued_at"])
        current = (now or (lambda: datetime.now(timezone.utc)))()
        if not isinstance(current, datetime) or current.tzinfo is None or current.utcoffset() is None:
            return False
        age = (current.astimezone(timezone.utc) - issued).total_seconds()
        for bound in (max_age_seconds, max_future_seconds):
            if bound is not None and (
                isinstance(bound, bool)
                or not isinstance(bound, (int, float))
                or not math.isfinite(bound)
                or bound < 0
            ):
                return False
        if max_age_seconds is not None and age > max_age_seconds:
            return False
        if max_future_seconds is not None and age < -max_future_seconds:
            return False
        return True
    except (TypeError, ValueError, OverflowError):
        return False
