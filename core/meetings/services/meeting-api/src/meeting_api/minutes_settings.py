"""Bounded internal client for Identity-owned Minutes product settings."""
from __future__ import annotations

import json
import math
from typing import Any, Optional
from urllib.parse import urlparse


MAX_SETTINGS_BYTES = 16 * 1024


class IdentityMinutesSettings:
    """Resolve one user's safe Minutes flags over the existing internal Identity edge.

    Redirects are never followed because the internal secret must not cross origins.  Both the
    declared and streamed response sizes are capped before JSON decoding.
    """

    def __init__(
        self,
        base_url: str,
        internal_secret: str,
        *,
        transport: Any = None,
        timeout_seconds: float = 5.0,
    ) -> None:
        try:
            parsed = urlparse(base_url)
            parsed.port
        except (TypeError, ValueError):
            raise ValueError("Minutes settings requires a valid internal service URL") from None
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Minutes settings requires an unambiguous HTTP(S) service URL")
        if not isinstance(internal_secret, str) or not internal_secret:
            raise ValueError("Minutes settings requires an internal secret")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("Minutes settings timeout must be positive")
        self._base_url = base_url.rstrip("/")
        self._secret = internal_secret
        self._transport = transport
        self._timeout = float(timeout_seconds)

    async def __call__(self, user_id: int) -> Optional[dict]:
        if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
            return None
        import httpx

        try:
            async with httpx.AsyncClient(
                timeout=self._timeout,
                follow_redirects=False,
                transport=self._transport,
            ) as client:
                async with client.stream(
                    "GET",
                    f"{self._base_url}/internal/users/{user_id}/minutes",
                    headers={"X-Internal-Secret": self._secret, "Accept": "application/json"},
                ) as response:
                    if 300 <= response.status_code < 400:
                        raise RuntimeError("Minutes settings redirect was rejected")
                    if response.status_code == 404:
                        return None
                    if response.status_code != 200:
                        raise RuntimeError("Minutes settings authority is unavailable")
                    raw_length = response.headers.get("content-length")
                    if raw_length:
                        try:
                            declared = int(raw_length)
                        except ValueError:
                            raise RuntimeError("Minutes settings response length is invalid") from None
                        if declared < 0 or declared > MAX_SETTINGS_BYTES:
                            raise RuntimeError("Minutes settings response is too large")
                    chunks: list[bytes] = []
                    total = 0
                    async for chunk in response.aiter_bytes():
                        total += len(chunk)
                        if total > MAX_SETTINGS_BYTES:
                            raise RuntimeError("Minutes settings response is too large")
                        chunks.append(chunk)
        except httpx.HTTPError:
            # HTTPX retains the credential-bearing Request object on transport failures.
            raise RuntimeError("Minutes settings transport failed") from None
        try:
            body = json.loads(b"".join(chunks))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise RuntimeError("Minutes settings response is invalid") from None
        if not isinstance(body, dict):
            raise RuntimeError("Minutes settings response is invalid")
        return body
