"""Durable RuntimeEvent callback delivery. 0.11's `lifecycle._deliver_callback` left a pending record
in Redis on burst-exhaustion so `idle_loop` could retry every tick — that is the single mechanism
making exit-callback delivery eventually-complete across consumer outages. We reimplement that here as
a small queue + sweep so the kernel's API doesn't fire-once-and-forget.

  • enqueue(url, event)  — record a pending delivery.
  • sweep()              — try every pending delivery once; drop the ones the receiver acked (2xx),
                          KEEP the ones that failed so the next sweep retries them.

The transport is injectable: production posts with httpx; the eval supplies a fake receiver. The
backing store is a PendingStore Protocol (in-memory by default; a Redis adapter mirrors 0.11's
`runtime:callback:*` keys)."""
from __future__ import annotations

import json
import logging
from typing import Callable, Optional, Protocol
from urllib.parse import urlsplit
from uuid import uuid4

logger = logging.getLogger("runtime_kernel.callbacks")


# A poster returns the HTTP status code (or raises on transport failure).
Poster = Callable[[str, dict, dict], int]


class PendingStore(Protocol):
    def put(self, key: str, value: dict) -> None: ...
    def get_all(self) -> dict[str, dict]: ...
    def delete(self, key: str) -> None: ...


class InMemoryPendingStore:
    def __init__(self) -> None:
        self._d: dict[str, dict] = {}

    def put(self, key: str, value: dict) -> None:
        self._d[key] = value

    def get_all(self) -> dict[str, dict]:
        return dict(self._d)

    def delete(self, key: str) -> None:
        self._d.pop(key, None)


class RedisPendingStore:
    """Mirrors 0.11's `runtime:callback:*` pending-callback keys."""

    PREFIX = "runtime:callback:"

    def __init__(self, redis, ttl: Optional[int] = None) -> None:
        if ttl is not None and (
            isinstance(ttl, bool) or not isinstance(ttl, int) or ttl < 1
        ):
            raise ValueError("pending callback TTL must be a positive integer")
        self._r = redis
        self._ttl = ttl

    @staticmethod
    def _s(v) -> str:
        return v.decode() if isinstance(v, (bytes, bytearray)) else v

    def put(self, key: str, value: dict) -> None:
        options = {"ex": self._ttl} if self._ttl is not None else {}
        self._r.set(f"{self.PREFIX}{key}", json.dumps(value), **options)

    def get_all(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for raw_key in self._r.scan_iter(match=f"{self.PREFIX}*"):
            k = self._s(raw_key)
            raw = self._r.get(k)
            if raw is None:
                continue
            try:
                record = json.loads(self._s(raw))
            except (TypeError, UnicodeDecodeError, json.JSONDecodeError):
                record = None
            if (
                not isinstance(record, dict)
                or set(record) != {"url", "headers", "event", "attempts"}
                or not isinstance(record.get("url"), str)
                or not isinstance(record.get("headers"), dict)
                or not all(
                    isinstance(name, str) and isinstance(value, str)
                    for name, value in record.get("headers", {}).items()
                )
                or not isinstance(record.get("event"), dict)
                or type(record.get("attempts")) is not int
                or record["attempts"] < 0
            ):
                # An impossible durable record can never be delivered. Remove only
                # that record so one corrupt Redis value cannot poison every sweep.
                logger.error("dropping corrupt pending callback %s", k[len(self.PREFIX):])
                self._r.delete(k)
                continue
            out[k[len(self.PREFIX):]] = record
        return out

    def delete(self, key: str) -> None:
        self._r.delete(f"{self.PREFIX}{key}")


def _http_poster(url: str, payload: dict, headers: dict) -> int:
    import httpx

    return httpx.post(
        url,
        json=payload,
        headers=headers,
        timeout=10.0,
        follow_redirects=False,
    ).status_code


class CallbackQueue:
    def __init__(
        self,
        poster: Optional[Poster] = None,
        store: Optional[PendingStore] = None,
        max_attempts: int = 0,
        default_headers: Optional[dict[str, str]] = None,
        trusted_origins: Optional[set[str]] = None,
    ) -> None:
        self.poster = poster or _http_poster
        self.store = store or InMemoryPendingStore()
        self.default_headers = dict(default_headers or {})
        self.trusted_origins = {
            origin
            for value in (trusted_origins or set())
            if (origin := self._origin(value)) is not None
        }
        # 0 ⇒ retry until acknowledged (the durable store has no implicit expiry).
        self.max_attempts = max_attempts

    @staticmethod
    def _origin(url: str) -> Optional[str]:
        try:
            parsed = urlsplit(url)
            port = parsed.port
        except (TypeError, ValueError):
            return None
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
        ):
            return None
        default_port = 80 if parsed.scheme == "http" else 443
        suffix = "" if port in (None, default_port) else f":{port}"
        return f"{parsed.scheme}://{parsed.hostname.lower().rstrip('.')}{suffix}"

    def enqueue(self, url: str, event: dict, headers: Optional[dict] = None) -> str:
        # A process-local counter restarts at one and can overwrite an older Redis
        # callback after a runtime restart. A random key preserves both generations.
        key = f"cb-{uuid4().hex}"
        self.store.put(key, {"url": url, "headers": headers or {}, "event": event, "attempts": 0})
        # Best-effort immediate attempt; whatever doesn't ack stays queued for the sweep.
        self._attempt(key)
        return key

    def _attempt(self, key: str) -> bool:
        rec = self.store.get_all().get(key)
        if rec is None:
            return True
        rec["attempts"] = rec.get("attempts", 0) + 1
        try:
            # Operator headers are injected at delivery time and never serialized into the pending
            # store. They override record headers so a caller cannot shadow the runtime credential.
            operator_headers = (
                self.default_headers
                if self._origin(rec["url"]) in self.trusted_origins
                else {}
            )
            headers = {**(rec.get("headers") or {}), **operator_headers}
            code = self.poster(rec["url"], rec["event"], headers)
            if 200 <= code < 300:
                self.store.delete(key)
                logger.info("callback %s delivered (attempt %d) -> %s", key, rec["attempts"], code)
                return True
            logger.warning("callback %s got %d (attempt %d)", key, code, rec["attempts"])
        except Exception as error:  # noqa: BLE001 — transport failures are retryable
            # Poster exceptions may retain the callback Request and its process-local credential.
            logger.warning(
                "callback %s delivery failed (attempt %d; %s)",
                key,
                rec["attempts"],
                type(error).__name__,
            )

        # Not acked. Give up only if a finite cap is set and reached; else keep for retry.
        if self.max_attempts and rec["attempts"] >= self.max_attempts:
            logger.error("callback %s exhausted %d attempts; dropping", key, self.max_attempts)
            self.store.delete(key)
            return False
        self.store.put(key, rec)
        return False

    def sweep(self) -> int:
        """Retry every still-pending delivery once. Returns how many remain pending afterward."""
        for key in list(self.store.get_all()):
            self._attempt(key)
        return self.pending_count()

    def pending_count(self) -> int:
        return len(self.store.get_all())
