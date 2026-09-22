"""Dedicated external-Hub authentication for managed user-facing Minutes routes."""
from __future__ import annotations

import hmac


def validate_hub_token(value: object) -> str:
    """Return one unpadded printable-ASCII service token or fail content-free."""

    if (
        not isinstance(value, str)
        or not 32 <= len(value) <= 512
        or value != value.strip()
        or any(not 0x20 <= ord(character) <= 0x7E for character in value)
    ):
        raise ValueError("managed Minutes requires a dedicated Hub token")
    return value


def validated_hmac_secret_bytes(value: object) -> bytes:
    """Return one bounded, unpadded printable-ASCII HMAC key as bytes."""

    if isinstance(value, str):
        try:
            encoded = value.encode("ascii")
        except UnicodeEncodeError:
            raise ValueError("service HMAC secret is invalid") from None
    elif isinstance(value, bytes):
        encoded = value
    else:
        raise ValueError("service HMAC secret is invalid")
    if (
        not 32 <= len(encoded) <= 512
        or encoded != encoded.strip()
        or any(not 0x20 <= byte <= 0x7E for byte in encoded)
    ):
        raise ValueError("service HMAC secret is invalid")
    return encoded


def hub_token_matches(supplied: object, expected: str) -> bool:
    """Timing-safe equality; malformed/missing values collapse to the same denial."""

    candidate = supplied if isinstance(supplied, str) else ""
    try:
        candidate_bytes = candidate.encode("ascii")
    except UnicodeEncodeError:
        candidate_bytes = b""
    return hmac.compare_digest(candidate_bytes, expected.encode("ascii"))
