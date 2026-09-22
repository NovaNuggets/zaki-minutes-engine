"""Internal client for the Agent-owned half of a meeting erasure receipt."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
import json
import math
import hmac
import re
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlparse

from .erasure_receipts import verify_erasure_receipt


MAX_AGENT_ERASURE_BYTES = 16 * 1024
_COUNT_KEYS = (
    "agent_unit_streams",
    "agent_workspace_documents",
    "agent_brain_records",
)
_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


@dataclass(frozen=True)
class AgentErasureReceipt:
    receipt: dict

    @property
    def unit_streams(self) -> int:
        return self.receipt["counts"]["agent_unit_streams"]

    @property
    def workspace_documents(self) -> int:
        return self.receipt["counts"]["agent_workspace_documents"]

    @property
    def brain_records(self) -> int:
        return self.receipt["counts"]["agent_brain_records"]

    def as_dict(self) -> dict:
        return deepcopy(self.receipt)


class AgentMinutesEraser:
    """Request a durable Agent tombstone and derivative purge without replaying secrets."""

    def __init__(
        self,
        base_url: str,
        internal_secret: str,
        *,
        verification_keys: Mapping[str, str | bytes],
        now: Callable[[], datetime],
        transport: Any = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        try:
            parsed = urlparse(base_url)
            parsed.port
        except (TypeError, ValueError):
            raise ValueError("Agent erasure requires a valid internal service URL") from None
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Agent erasure requires an unambiguous HTTP(S) service URL")
        if not isinstance(internal_secret, str) or not internal_secret:
            raise ValueError("Agent erasure requires an internal secret")
        if (
            not isinstance(verification_keys, Mapping)
            or not verification_keys
            or len(verification_keys) > 2
        ):
            raise ValueError("Agent erasure requires verification keys")
        normalized_keys: dict[str, str | bytes] = {}
        normalized_secrets: list[bytes] = []
        for key_id, secret in verification_keys.items():
            if not isinstance(secret, (str, bytes)):
                raise ValueError("Agent erasure verification key policy is invalid")
            encoded = secret.encode("utf-8") if isinstance(secret, str) else secret
            if (
                not isinstance(key_id, str)
                or _KEY_ID.fullmatch(key_id) is None
                or len(encoded) < 32
                or hmac.compare_digest(encoded, internal_secret.encode("utf-8"))
                or any(hmac.compare_digest(encoded, prior) for prior in normalized_secrets)
            ):
                raise ValueError("Agent erasure verification key policy is invalid")
            normalized_keys[key_id] = secret
            normalized_secrets.append(encoded)
        if not callable(now):
            raise ValueError("Agent erasure requires a verification clock")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("Agent erasure timeout must be positive")
        self._base_url = base_url.rstrip("/")
        self._secret = internal_secret
        self._verification_keys = normalized_keys
        self._now = now
        self._transport = transport
        self._timeout = float(timeout_seconds)

    def verify_durable_receipt(
        self,
        receipt: object,
        *,
        user_id: int,
        meeting_id: int,
    ) -> bool:
        """Verify an exact persisted Agent proof without imposing a retry-age limit.

        Agent's signed response is durable idempotent completion evidence, not a
        short-lived bearer token.  Old receipts therefore remain usable while their
        verification key is retained, but future-dated, mis-bound, or malformed proofs
        still fail closed.
        """

        if not isinstance(receipt, dict):
            return False
        verification_secret = self._verification_keys.get(receipt.get("key_id"))
        counts = receipt.get("counts")
        if (
            verification_secret is None
            or not isinstance(counts, dict)
            or set(counts) != set(_COUNT_KEYS)
        ):
            return False
        for key in _COUNT_KEYS:
            value = counts.get(key)
            if type(value) is not int or value < 0 or value > 2_147_483_647:
                return False
        return verify_erasure_receipt(
            receipt,
            verification_secret,
            expected_owner="agent",
            expected_scope="meeting",
            expected_user_id=user_id,
            expected_meeting_id=meeting_id,
            now=self._now,
            max_age_seconds=None,
            max_future_seconds=30,
        )

    async def __call__(self, *, user_id: int, meeting_id: int) -> AgentErasureReceipt:
        if (
            isinstance(user_id, bool)
            or not isinstance(user_id, int)
            or user_id <= 0
            or isinstance(meeting_id, bool)
            or not isinstance(meeting_id, int)
            or meeting_id <= 0
        ):
            raise ValueError("Agent erasure identity is invalid")

        import httpx

        try:
            async with httpx.AsyncClient(
                timeout=self._timeout,
                follow_redirects=False,
                transport=self._transport,
            ) as client:
                async with client.stream(
                    "POST",
                    f"{self._base_url}/internal/minutes/meetings/{meeting_id}/erase",
                    headers={
                        "X-Internal-Secret": self._secret,
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                    },
                    json={"user_id": str(user_id)},
                ) as response:
                    if 300 <= response.status_code < 400:
                        raise RuntimeError("Agent erasure redirect was rejected")
                    if response.status_code != 200:
                        raise RuntimeError("Agent erasure is unavailable")
                    raw_length = response.headers.get("content-length")
                    if raw_length:
                        try:
                            declared = int(raw_length)
                        except ValueError:
                            raise RuntimeError("Agent erasure response length is invalid") from None
                        if declared < 0 or declared > MAX_AGENT_ERASURE_BYTES:
                            raise RuntimeError("Agent erasure response is too large")
                    chunks: list[bytes] = []
                    total = 0
                    async for chunk in response.aiter_bytes():
                        total += len(chunk)
                        if total > MAX_AGENT_ERASURE_BYTES:
                            raise RuntimeError("Agent erasure response is too large")
                        chunks.append(chunk)
        except httpx.HTTPError:
            # HTTPX failures retain the credential-bearing Request object.  Never expose that
            # exception chain to orchestration logs or API error handlers.
            raise RuntimeError("Agent erasure transport failed") from None
        try:
            body = json.loads(b"".join(chunks))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise RuntimeError("Agent erasure response is invalid") from None
        if not self.verify_durable_receipt(
            body, user_id=user_id, meeting_id=meeting_id
        ):
            raise RuntimeError("Agent erasure response is invalid")
        return AgentErasureReceipt(receipt=deepcopy(body))
