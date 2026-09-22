"""Request-bound Gateway-to-Agent identity proof.

The deployment secret is an HMAC key, never a bearer value. Each downstream Agent request carries
only a key identifier and a short-lived proof over its exact target, body digest, and Agent-consumed
headers. Query normalization is shared by proof generation and downstream forwarding.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
import time
import urllib.parse
from collections.abc import Callable, Mapping


_METHOD = re.compile(r"^[A-Z]{1,16}$")
_USER = re.compile(r"^[1-9][0-9]{0,18}$")
_NONCE = re.compile(r"^[A-Za-z0-9_-]{16,128}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_MAX_DB_ID = 2**63 - 1
_MAX_QUERY_BYTES = 8192
_MAX_QUERY_FIELDS = 256
_SIGNED_HEADERS = (
    "content-type",
    "last-event-id",
    "x-user-email",
    "x-user-id",
)
_SIGNED_HEADER_MAX_BYTES = {
    "content-type": 512,
    "last-event-id": 1024,
    "x-user-email": 320,
    "x-user-id": 19,
}


def _secret(value: object) -> bytes:
    if not isinstance(value, str):
        raise ValueError("Gateway identity signing secret is invalid")
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        raise ValueError("Gateway identity signing secret is invalid") from None
    if (
        not 32 <= len(encoded) <= 512
        or encoded != encoded.strip()
        or any(not 0x20 <= byte <= 0x7E for byte in encoded)
    ):
        raise ValueError("Gateway identity signing secret is invalid")
    return encoded


def canonical_query(value: str) -> str:
    """Normalize one query representation and reject ambiguous/control-heavy input."""
    if not isinstance(value, str):
        raise ValueError("Gateway identity query is invalid")
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        raise ValueError("Gateway identity query is invalid") from None
    if len(encoded) > _MAX_QUERY_BYTES or any(byte < 0x20 or byte == 0x7F for byte in encoded):
        raise ValueError("Gateway identity query is invalid")
    try:
        pairs = urllib.parse.parse_qsl(
            value,
            keep_blank_values=True,
            strict_parsing=False,
            max_num_fields=_MAX_QUERY_FIELDS,
        )
    except ValueError:
        raise ValueError("Gateway identity query is invalid") from None
    if any(
        any(ord(character) < 0x20 or ord(character) == 0x7F for character in part)
        for pair in pairs
        for part in pair
    ):
        raise ValueError("Gateway identity query is invalid")
    return urllib.parse.urlencode(pairs)


def _header(headers: Mapping[str, str], name: str) -> str | None:
    getlist = getattr(headers, "getlist", None)
    if callable(getlist):
        values = getlist(name)
        if len(values) > 1:
            raise ValueError("Gateway signed header is ambiguous")
    value = headers.get(name)
    if value is None:
        value = headers.get(name.title())
    return value if isinstance(value, str) else None


def _signed_headers_digest(headers: Mapping[str, str], *, user_id: str) -> str:
    values: dict[str, str | None] = {}
    for name in _SIGNED_HEADERS:
        value = _header(headers, name)
        if name == "x-user-id":
            if value not in (None, user_id):
                raise ValueError("Gateway identity user header is invalid")
            value = user_id
        if value is not None:
            try:
                encoded = value.encode("ascii")
            except UnicodeEncodeError:
                raise ValueError("Gateway signed header is invalid") from None
            if (
                len(encoded) > _SIGNED_HEADER_MAX_BYTES[name]
                or any(not 0x20 <= byte <= 0x7E for byte in encoded)
            ):
                raise ValueError("Gateway signed header is invalid")
        values[name] = value
    payload = json.dumps(
        values,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _canonical(
    *,
    method: str,
    path: str,
    user_id: str,
    query: str,
    content_sha256: str,
    signed_headers_sha256: str,
    timestamp: str,
    nonce: str,
) -> bytes:
    method = method.upper() if isinstance(method, str) else ""
    if not _METHOD.fullmatch(method):
        raise ValueError("Gateway identity method is invalid")
    if (
        not isinstance(path, str)
        or not path.startswith("/")
        or len(path) > 2048
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in path)
    ):
        raise ValueError("Gateway identity path is invalid")
    if not isinstance(user_id, str) or not _USER.fullmatch(user_id) or int(user_id) > _MAX_DB_ID:
        raise ValueError("Gateway identity user is invalid")
    query = canonical_query(query)
    if not isinstance(content_sha256, str) or not _DIGEST.fullmatch(content_sha256):
        raise ValueError("Gateway identity content digest is invalid")
    if not isinstance(signed_headers_sha256, str) or not _DIGEST.fullmatch(signed_headers_sha256):
        raise ValueError("Gateway identity header digest is invalid")
    if not isinstance(timestamp, str) or not timestamp.isascii() or not timestamp.isdecimal():
        raise ValueError("Gateway identity timestamp is invalid")
    if not isinstance(nonce, str) or not _NONCE.fullmatch(nonce):
        raise ValueError("Gateway identity nonce is invalid")
    return (
        f"gateway-request.v1\n{method}\n{path}\n{user_id}\n{query}\n{content_sha256}\n"
        f"{signed_headers_sha256}\n{timestamp}\n{nonce}"
    ).encode("utf-8")


class GatewayIdentitySigner:
    """Mint one non-reusable proof for an exact Agent request."""

    def __init__(
        self,
        secret: str,
        *,
        now: Callable[[], float] = time.time,
        nonce: Callable[[], str] = lambda: secrets.token_urlsafe(24),
    ) -> None:
        self._secret = _secret(secret)
        self._key_id = hashlib.sha256(self._secret).hexdigest()[:16]
        self._now = now
        self._nonce = nonce

    def headers(
        self,
        *,
        method: str,
        path: str,
        user_id: str,
        query: str = "",
        body: bytes = b"",
        identity_headers: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        if not isinstance(body, bytes):
            raise ValueError("Gateway identity body is invalid")
        timestamp = str(int(self._now()))
        nonce = self._nonce()
        content_sha256 = hashlib.sha256(body).hexdigest()
        signed_headers_sha256 = _signed_headers_digest(
            identity_headers or {},
            user_id=user_id,
        )
        canonical = _canonical(
            method=method,
            path=path,
            user_id=user_id,
            query=query,
            content_sha256=content_sha256,
            signed_headers_sha256=signed_headers_sha256,
            timestamp=timestamp,
            nonce=nonce,
        )
        return {
            "x-gateway-key-id": self._key_id,
            "x-gateway-timestamp": timestamp,
            "x-gateway-nonce": nonce,
            "x-gateway-content-sha256": content_sha256,
            "x-gateway-signature": hmac.new(
                self._secret, canonical, hashlib.sha256,
            ).hexdigest(),
        }
