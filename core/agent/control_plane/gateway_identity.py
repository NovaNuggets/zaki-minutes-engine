"""Fail-closed verification for request-bound Gateway identity proofs."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import threading
import time
import urllib.parse
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol


_METHOD = re.compile(r"^[A-Z]{1,16}$")
_USER = re.compile(r"^[1-9][0-9]{0,18}$")
_NONCE = re.compile(r"^[A-Za-z0-9_-]{16,128}$")
_SIGNATURE = re.compile(r"^[0-9a-f]{64}$")
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
MAX_PROOF_AGE_SECONDS = 30
REPLAY_TTL_SECONDS = 2 * MAX_PROOF_AGE_SECONDS + 1


class GatewayReplayStore(Protocol):
    def claim(self, key: str, *, ttl_seconds: int) -> bool: ...


class GatewayReplayUnavailable(RuntimeError):
    """The durable cross-replica replay fence could not make a decision."""


@dataclass(frozen=True)
class AuthenticatedGatewayMetadata:
    content_sha256: str
    replay_key: str
    issued_at: int


def _secret(value: object, *, required: bool) -> bytes | None:
    if value in (None, "") and not required:
        return None
    if not isinstance(value, str):
        raise ValueError("Gateway identity verification secret is invalid")
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        raise ValueError("Gateway identity verification secret is invalid") from None
    if (
        not 32 <= len(encoded) <= 512
        or encoded != encoded.strip()
        or any(not 0x20 <= byte <= 0x7E for byte in encoded)
    ):
        raise ValueError("Gateway identity verification secret is invalid")
    return encoded


def _canonical_query(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError
    encoded = value.encode("ascii")
    if len(encoded) > _MAX_QUERY_BYTES or any(byte < 0x20 or byte == 0x7F for byte in encoded):
        raise ValueError
    pairs = urllib.parse.parse_qsl(
        value,
        keep_blank_values=True,
        strict_parsing=False,
        max_num_fields=_MAX_QUERY_FIELDS,
    )
    if any(
        any(ord(character) < 0x20 or ord(character) == 0x7F for character in part)
        for pair in pairs
        for part in pair
    ):
        raise ValueError
    return urllib.parse.urlencode(pairs)


def _header(headers: Mapping[str, str], name: str) -> str | None:
    getlist = getattr(headers, "getlist", None)
    if callable(getlist):
        values = getlist(name)
        if len(values) > 1:
            raise ValueError
    value = headers.get(name)
    if value is None:
        value = headers.get(name.title())
    return value if isinstance(value, str) else None


def _signed_headers_digest(headers: Mapping[str, str], *, user_id: str) -> str:
    values: dict[str, str | None] = {}
    for name in _SIGNED_HEADERS:
        value = _header(headers, name)
        if name == "x-user-id":
            if value != user_id:
                raise ValueError
        if value is not None:
            encoded = value.encode("ascii")
            if (
                len(encoded) > _SIGNED_HEADER_MAX_BYTES[name]
                or any(not 0x20 <= byte <= 0x7E for byte in encoded)
            ):
                raise ValueError
        values[name] = value
    payload = json.dumps(
        values,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _canonical(
    *, method: str, path: str, user_id: str, query: str, content_sha256: str,
    signed_headers_sha256: str, timestamp: str, nonce: str,
) -> bytes:
    method = method.upper() if isinstance(method, str) else ""
    if not _METHOD.fullmatch(method):
        raise ValueError
    if (
        not isinstance(path, str)
        or not path.startswith("/")
        or len(path) > 2048
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in path)
    ):
        raise ValueError
    if not isinstance(user_id, str) or not _USER.fullmatch(user_id) or int(user_id) > _MAX_DB_ID:
        raise ValueError
    query = _canonical_query(query)
    if not isinstance(content_sha256, str) or not _DIGEST.fullmatch(content_sha256):
        raise ValueError
    if not isinstance(signed_headers_sha256, str) or not _DIGEST.fullmatch(signed_headers_sha256):
        raise ValueError
    if not isinstance(timestamp, str) or not timestamp.isascii() or not timestamp.isdecimal():
        raise ValueError
    if not isinstance(nonce, str) or not _NONCE.fullmatch(nonce):
        raise ValueError
    return (
        f"gateway-request.v1\n{method}\n{path}\n{user_id}\n{query}\n{content_sha256}\n"
        f"{signed_headers_sha256}\n{timestamp}\n{nonce}"
    ).encode("utf-8")


class InMemoryGatewayReplayStore:
    """Process-local test/direct-mode replay fence. Production uses the Redis implementation."""

    def __init__(self, *, now: Callable[[], float] = time.time) -> None:
        self._now = now
        self._claims: dict[str, float] = {}
        self._lock = threading.Lock()

    def claim(self, key: str, *, ttl_seconds: int) -> bool:
        current = self._now()
        with self._lock:
            self._claims = {
                candidate: expiry
                for candidate, expiry in self._claims.items()
                if expiry > current
            }
            if key in self._claims:
                return False
            self._claims[key] = current + ttl_seconds
            return True


class RedisGatewayReplayStore:
    """Cross-replica replay fence over one Redis ``SET NX EX`` claim."""

    def __init__(self, redis_client) -> None:
        self._redis = redis_client

    def claim(self, key: str, *, ttl_seconds: int) -> bool:
        return bool(self._redis.set(key, "1", nx=True, ex=ttl_seconds))


class GatewayIdentityVerifier:
    def __init__(
        self,
        current_secret: str,
        *,
        previous_secret: str = "",
        replay_store: GatewayReplayStore,
        now: Callable[[], float] = time.time,
    ) -> None:
        current = _secret(current_secret, required=True)
        previous = _secret(previous_secret, required=False)
        if previous is not None and hmac.compare_digest(current, previous):
            raise ValueError("Gateway identity rotation secrets must be distinct")
        secrets_by_id = {hashlib.sha256(current).hexdigest()[:16]: current}
        if previous is not None:
            secrets_by_id[hashlib.sha256(previous).hexdigest()[:16]] = previous
        self._secrets = secrets_by_id
        self._replay = replay_store
        self._now = now

    def authenticate_metadata(
        self,
        *, method: str, path: str, user_id: str, query: str,
        headers: Mapping[str, str],
    ) -> AuthenticatedGatewayMetadata | None:
        """Authenticate bounded request metadata without reading or claiming the body."""
        try:
            key_id = _header(headers, "x-gateway-key-id") or ""
            timestamp = _header(headers, "x-gateway-timestamp") or ""
            nonce = _header(headers, "x-gateway-nonce") or ""
            content_sha256 = _header(headers, "x-gateway-content-sha256") or ""
            supplied = _header(headers, "x-gateway-signature") or ""
            secret = self._secrets.get(key_id)
            if secret is None or not _SIGNATURE.fullmatch(supplied):
                return None
            signed_headers_sha256 = _signed_headers_digest(headers, user_id=user_id)
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
            issued_at = int(timestamp)
            if abs(self._now() - issued_at) > MAX_PROOF_AGE_SECONDS:
                return None
            expected = hmac.new(secret, canonical, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(supplied, expected):
                return None
            replay_key = "zaki:gateway-request:v1:" + hashlib.sha256(
                f"{key_id}:{nonce}".encode("ascii")
            ).hexdigest()
            return AuthenticatedGatewayMetadata(
                content_sha256=content_sha256,
                replay_key=replay_key,
                issued_at=issued_at,
            )
        except Exception:
            return None

    def verify_body_and_claim(
        self,
        proof: AuthenticatedGatewayMetadata,
        body: bytes,
    ) -> bool:
        """Re-check freshness, bind exact body bytes, then atomically claim the nonce."""
        try:
            if not isinstance(body, bytes):
                return False
            if abs(self._now() - proof.issued_at) > MAX_PROOF_AGE_SECONDS:
                return False
            actual = hashlib.sha256(body).hexdigest()
            if not hmac.compare_digest(actual, proof.content_sha256):
                return False
            try:
                return self._replay.claim(proof.replay_key, ttl_seconds=REPLAY_TTL_SECONDS)
            except Exception:
                raise GatewayReplayUnavailable("Gateway replay fence is unavailable") from None
        except GatewayReplayUnavailable:
            raise
        except Exception:
            return False

    def verify(
        self,
        *, method: str, path: str, user_id: str, headers: Mapping[str, str],
        query: str = "", body: bytes = b"",
    ) -> bool:
        """Synchronous convenience wrapper used by focused unit tests."""
        proof = self.authenticate_metadata(
            method=method,
            path=path,
            user_id=user_id,
            query=query,
            headers=headers,
        )
        try:
            return proof is not None and self.verify_body_and_claim(proof, body)
        except GatewayReplayUnavailable:
            return False
