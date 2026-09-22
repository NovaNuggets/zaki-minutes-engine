"""Bounded Agent-side consumer for the sealed Minutes ``zaki-read.v1`` profile.

The dedicated read token remains inside this client.  Callers receive validated JSON values only;
URLs are composed from one fixed service origin and redirects are never followed.
"""
from __future__ import annotations

import json
import math
import re
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Callable, Optional

import jsonschema
from referencing import Registry, Resource

from shared.http import open_no_redirect


MAX_CALLS_PER_TURN = 8
MAX_TURN_RESPONSE_BYTES = 1024 * 1024
MAX_RESPONSE_BYTES = 270_336
MAX_ITEM_CONTENT_BYTES = 256 * 1024
INDEX_LIMIT = 200
_USER_ID = re.compile(r"^[1-9][0-9]{0,18}$")
_MAX_DB_ID = 2**63 - 1
_SCHEMA = Path("meetings/contracts/zaki-read.v1/zaki-read.schema.json")


class MinutesReadError(RuntimeError):
    """A safe, content-free Minutes read failure."""


class MinutesReadDenied(MinutesReadError):
    """The spoke refused authentication, scope, or ownership."""


class MinutesReadItemTooLarge(MinutesReadError):
    """The full item exceeded the sealed response/content cap."""


_INVALID_RESPONSE = object()
_RESPONSE_BUDGET_EXHAUSTED = object()


@dataclass(frozen=True)
class MinutesTranscript:
    item: dict
    summary_fallback: bool = False


def validate_service_origin(value: str) -> str:
    """Return one normalized origin, rejecting paths and credential-bearing/ambiguous URLs."""
    if not isinstance(value, str) or not value or any(ch.isspace() for ch in value):
        raise ValueError("Minutes read requires a fixed HTTP(S) service origin")
    try:
        parsed = urllib.parse.urlsplit(value)
        parsed.port
    except (TypeError, ValueError):
        raise ValueError("Minutes read requires a fixed HTTP(S) service origin") from None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Minutes read requires a fixed HTTP(S) service origin")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def validate_read_token(value: str) -> str:
    """Mirror the sealed producer's token-material composition invariant."""
    if not isinstance(value, str) or not value:
        raise ValueError("Minutes read requires a dedicated token")
    if (
        not 32 <= len(value) <= 512
        or value != value.strip()
        or any(not 0x20 <= ord(character) <= 0x7E for character in value)
    ):
        raise ValueError(
            "Minutes read token must be unpadded printable ASCII between 32 and 512 characters"
        )
    return value


def _schema_path() -> Path:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / _SCHEMA
        if candidate.exists():
            return candidate
    raise FileNotFoundError("zaki-read.v1 schema is not packaged")


@lru_cache(maxsize=None)
def _validator(shape: str) -> jsonschema.Draft202012Validator:
    schema = json.loads(_schema_path().read_text())
    registry = Registry().with_resource(schema["$id"], Resource.from_contents(schema))
    return jsonschema.Draft202012Validator(
        {"$ref": f"{schema['$id']}#/$defs/{shape}"},
        registry=registry,
        format_checker=jsonschema.FormatChecker(),
    )


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp is not timezone-aware")
    return parsed.astimezone(timezone.utc)


def _validate_retention(payload: dict, now: datetime) -> None:
    records = payload.get("items")
    if records is None and "item" in payload:
        records = [payload["item"]]
    if not isinstance(records, list):
        raise ValueError("missing Minutes records")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("current time is not timezone-aware")
    current = now.astimezone(timezone.utc)
    for record in records:
        if _utc(record["retention"]["expires_at"]) <= current:
            raise ValueError("expired Minutes record")


def _validate_semantics(payload: dict) -> None:
    item = payload.get("item")
    if not isinstance(item, dict) or item.get("kind") != "transcript":
        return
    content = item.get("content")
    turns = content.get("turns") if isinstance(content, dict) else None
    if not isinstance(turns, list):  # summary variant has no speaker turns
        return
    prior: Optional[datetime] = None
    for turn in turns:
        start = _utc(turn["started_at"])
        end = _utc(turn["ended_at"]) if "ended_at" in turn else None
        if prior is not None and start < prior:
            raise ValueError("transcript turns are out of order")
        if end is not None and end < start:
            raise ValueError("transcript turn ends before it starts")
        prior = start


class MinutesReadClient:
    """Factory for isolated per-turn read budgets."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 5.0,
        open_fn: Callable = open_no_redirect,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        request_id: Callable[[], str] = lambda: str(uuid.uuid4()),
    ) -> None:
        self._origin = validate_service_origin(base_url)
        token = validate_read_token(token)
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("Minutes read timeout must be positive")
        self._token = token
        self._timeout = float(timeout)
        self._open = open_fn
        self._now = now
        self._request_id = request_id

    def begin_turn(self, user_id: str | int) -> "MinutesReadTurn":
        subject = str(user_id)
        if not _USER_ID.fullmatch(subject) or int(subject) > _MAX_DB_ID:
            raise ValueError("Minutes read requires a canonical numeric user")
        return MinutesReadTurn(self, subject)


class MinutesReadTurn:
    """One user-bound read budget; never reuse it across Agent turns."""

    def __init__(self, client: MinutesReadClient, user_id: str) -> None:
        self._client = client
        self._user_id = user_id
        self._calls = 0
        self._response_bytes = 0

    @property
    def calls(self) -> int:
        return self._calls

    @property
    def response_bytes(self) -> int:
        return self._response_bytes

    def _get(self, path: str, query: dict[str, object], *, shape: str) -> dict:
        if self._calls >= MAX_CALLS_PER_TURN:
            raise MinutesReadError("Minutes read call budget exhausted")
        self._calls += 1
        refusal = None
        transport_failed = False
        allowed = 0
        body = b""
        status = 0
        try:
            url = f"{self._client._origin}{path}"
            encoded = urllib.parse.urlencode(query)
            if encoded:
                url += f"?{encoded}"
            request = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    "X-Zaki-Read-Token": self._client._token,
                    "X-Zaki-User-Id": self._user_id,
                    "X-Request-Id": self._client._request_id(),
                },
                method="GET",
            )
            try:
                response = self._client._open(request, timeout=self._client._timeout)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                allowed = min(MAX_RESPONSE_BYTES, MAX_TURN_RESPONSE_BYTES - self._response_bytes)
                declared = response.headers.get("Content-Length")
                if declared is not None:
                    try:
                        declared_bytes = int(declared)
                    except (TypeError, ValueError):
                        refusal = _INVALID_RESPONSE
                    else:
                        if declared_bytes < 0 or declared_bytes > allowed:
                            refusal = _RESPONSE_BUDGET_EXHAUSTED
                if refusal is None:
                    body = response.read(allowed + 1)
                    self._response_bytes += len(body)
                    status_value = getattr(response, "status", None)
                    status = int(status_value if status_value is not None else response.getcode())
        except Exception:
            transport_failed = True
        if transport_failed:
            # Treat even a callback's pre-wrapped MinutesReadError as untrusted. Request setup,
            # transport, headers, streaming, status, and close can all retain the credential-bearing
            # Request; replace them only after leaving the exception context.
            raise MinutesReadError("Minutes read transport failed") from None
        if refusal is _INVALID_RESPONSE:
            raise MinutesReadError("Minutes read response is invalid") from None
        if refusal is _RESPONSE_BUDGET_EXHAUSTED:
            raise MinutesReadError("Minutes read response budget exhausted") from None
        if len(body) > allowed or self._response_bytes > MAX_TURN_RESPONSE_BYTES:
            raise MinutesReadError("Minutes read response budget exhausted")
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise MinutesReadError("Minutes read response is invalid") from None
        if status != 200:
            code = payload.get("error", {}).get("code") if isinstance(payload, dict) else None
            if status == 413 and code == "item_too_large":
                raise MinutesReadItemTooLarge("Minutes item exceeds the read cap")
            raise MinutesReadDenied("Minutes read was refused")
        try:
            _validator(shape).validate(payload)
            _validate_retention(payload, self._client._now())
            _validate_semantics(payload)
        except (jsonschema.ValidationError, KeyError, TypeError, ValueError):
            raise MinutesReadError("Minutes read response is invalid") from None
        item = payload.get("item")
        if isinstance(item, dict):
            content_bytes = json.dumps(
                item["content"], ensure_ascii=False, separators=(",", ":"),
            ).encode("utf-8")
            if len(content_bytes) > MAX_ITEM_CONTENT_BYTES:
                raise MinutesReadError("Minutes item exceeds the content cap")
        return payload

    def index(self, *, limit: int = INDEX_LIMIT, cursor: Optional[str] = None) -> dict:
        query: dict[str, object] = {"limit": min(max(int(limit), 1), INDEX_LIMIT)}
        if cursor is not None:
            query["cursor"] = cursor
        return self._get(
            f"/api/zaki/read/v1/{self._user_id}/index",
            query,
            shape="IndexResponse",
        )

    def item(self, item_id: str, *, variant: str = "full") -> dict:
        if variant not in {"full", "summary"}:
            raise ValueError("Minutes item variant must be full or summary")
        escaped = urllib.parse.quote(item_id, safe="")
        response = self._get(
            f"/api/zaki/read/v1/{self._user_id}/item/{escaped}",
            {"variant": variant},
            shape="ItemResponse",
        )
        item = response["item"]
        if item.get("kind") == "transcript":
            expected = "summary" if variant == "summary" else "speaker_turns"
            if item.get("content", {}).get("format") != expected:
                raise MinutesReadError(f"Minutes transcript did not return the {variant} variant")
        return response

    def last_transcript(self) -> MinutesTranscript:
        selected: Optional[dict] = None
        cursor: Optional[str] = None
        seen_cursors: set[str] = set()
        previous_occurrence: Optional[datetime] = None
        while True:
            # Reserve two calls for full item + its one permitted 413 summary fallback.
            if self._calls >= MAX_CALLS_PER_TURN - 2:
                raise MinutesReadError("Minutes index remained truncated within the call budget")
            page = self.index(limit=INDEX_LIMIT, cursor=cursor)
            try:
                for metadata in page["items"]:
                    occurrence = _utc(metadata["occurred_at"])
                    if previous_occurrence is not None and occurrence > previous_occurrence:
                        raise MinutesReadError("Minutes index was not newest-first")
                    previous_occurrence = occurrence
            except MinutesReadError:
                raise
            except (KeyError, TypeError, ValueError):
                raise MinutesReadError("Minutes read response is invalid") from None
            selected = next(
                (item for item in page["items"] if item.get("kind") == "transcript"),
                None,
            )
            if selected is not None:
                # zaki-read.v1 seals newest-first metadata. Once the first transcript has been
                # observed, older pages cannot change the answer and must not consume the turn's
                # read/PII budget.
                break
            if not page["truncated"]:
                raise MinutesReadError("No readable Minutes transcript was found")
            cursor = page["next_cursor"]
            if cursor in seen_cursors:
                raise MinutesReadError("Minutes index cursor did not advance")
            seen_cursors.add(cursor)
        try:
            response = self.item(selected["id"], variant="full")
            fallback = False
        except MinutesReadItemTooLarge:
            response = self.item(selected["id"], variant="summary")
            fallback = True
        item = response["item"]
        bound_fields = (
            "id",
            "kind",
            "meeting_id",
            "occurred_at",
            "updated_at",
            "sensitivity",
            "retention",
        )
        if (
            any(item.get(field) != selected.get(field) for field in bound_fields)
            or (
                "capture_notice" in selected
                and item.get("capture_notice") != selected["capture_notice"]
            )
        ):
            raise MinutesReadError("Minutes item identity did not match the index")
        return MinutesTranscript(item=item, summary_fallback=fallback)
