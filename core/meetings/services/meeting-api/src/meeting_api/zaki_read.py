"""Bounded, owner-scoped ``zaki-read.v1`` producer for Minutes.

The router is mounted only when the operator flag is on.  It has a dedicated service token and a
second, authoritative per-user opt-in lookup; neither ordinary Vexa API keys nor client-provided
settings can enable this internal read plane.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import inspect
import json
import re
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse


INDEX_LIMIT = 200
SEARCH_LIMIT = 50
ITEM_CONTENT_CAP_BYTES = 256 * 1024
RESPONSE_CAP_BYTES = 270_336
SCAN_BATCH = 100
MAX_SCAN_MEETINGS = 1_000
_USER_ID = re.compile(r"^[1-9][0-9]{0,18}$")
_ITEM_ID = re.compile(r"^(meeting|transcript|summary):([1-9][0-9]{0,18})$")
_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SUPPORTED_PLATFORMS = {"google_meet", "teams", "zoom", "jitsi"}
_MAX_DB_ID = 2**63 - 1


ReadScope = Callable[[int], Awaitable[Optional[dict]] | Optional[dict]]


def _compact_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _headers(request_id: Optional[str]) -> dict[str, str]:
    return {
        "Cache-Control": "no-store",
        "Pragma": "no-cache",
        **({"X-Request-Id": request_id} if request_id and _REQUEST_ID.fullmatch(request_id) else {}),
    }


def _response(body: dict, *, status_code: int = 200, request_id: Optional[str] = None) -> JSONResponse:
    if len(_compact_bytes(body)) > RESPONSE_CAP_BYTES:
        return _error(413, "response_too_large", "Response exceeds the read cap", request_id)
    return JSONResponse(content=body, status_code=status_code, headers=_headers(request_id))


def _error(status_code: int, code: str, message: str, request_id: Optional[str]) -> JSONResponse:
    return JSONResponse(
        content={"error": {"code": code, "message": message}, "truncated": False},
        status_code=status_code,
        headers=_headers(request_id),
    )


def _parse_time(value: object) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _safe_title(data: dict, meeting_id: int) -> str:
    title = data.get("title")
    if isinstance(title, str):
        title = " ".join(title.split())
        if title:
            return title[:500]
    return f"Meeting {meeting_id}"


def _capture_notice(data: dict, now: datetime) -> Optional[dict]:
    capture = data.get("zaki_capture")
    if not isinstance(capture, dict):
        return None
    attested_at = _parse_time(capture.get("tenant_attested_at"))
    policy = capture.get("tenant_policy_version")
    if (
        capture.get("state") != "authorized"
        or capture.get("bot_name") != "ZAKI Notetaker"
        or capture.get("tenant_attested") is not True
        or attested_at is None
        or attested_at > now
        or not isinstance(policy, str)
        or not policy.strip()
        or len(policy) > 80
    ):
        return None
    return {
        "bot_visible": True,
        "tenant_attested_at": _iso(attested_at),
        "policy_version": policy.strip(),
    }


def _retention(data: dict, scope: str, now: datetime) -> Optional[dict]:
    retention = data.get("zaki_retention")
    if not isinstance(retention, dict) or retention.get("state") != "open":
        return None
    expired = retention.get("expired_scopes")
    if not isinstance(expired, list) or scope in expired:
        return None
    expiries = retention.get("scope_expiries")
    expiry = _parse_time(expiries.get(scope) if isinstance(expiries, dict) else None)
    if expiry is None or expiry <= now:
        return None
    return {
        "scope": "minutes.summary" if scope == "summary" else "minutes.transcript",
        "expires_at": _iso(expiry),
    }


def _summary_text(data: dict) -> Optional[str]:
    value = data.get("summary")
    if isinstance(value, str) and value.strip():
        return value.strip()[:ITEM_CONTENT_CAP_BYTES]
    if isinstance(value, dict):
        text = value.get("text")
        if isinstance(text, str) and text.strip():
            return text.strip()[:ITEM_CONTENT_CAP_BYTES]
    summaries = data.get("summaries")
    if isinstance(summaries, list):
        for candidate in summaries:
            text = candidate.get("text") if isinstance(candidate, dict) else candidate
            if isinstance(text, str) and text.strip():
                return text.strip()[:ITEM_CONTENT_CAP_BYTES]
    return None


def _transcript_finalization(data: dict, now: datetime) -> Optional[dict]:
    """Return a validated immutable transcript marker, or fail closed.

    Terminal lifecycle state alone is not proof that Redis was flushed durably.  The marker is
    written only by the completion finalizer after the locked flush and is the source revision the
    consumer keys idempotency to (represented on v1 by its immutable ``finalized_at`` timestamp).
    """
    from .meeting_writes import validated_transcript_finalization_marker

    marker = validated_transcript_finalization_marker(data)
    if marker is None:
        return None
    revision = marker.get("revision")
    finalized_at = _parse_time(marker.get("finalized_at"))
    segment_count = marker.get("segment_count")
    if (
        finalized_at is None
        or finalized_at > now
    ):
        return None
    return {
        "revision": revision,
        "finalized_at": finalized_at,
        "segment_count": segment_count,
    }


def _speaker_turns(document: dict) -> Optional[dict]:
    started = _parse_time(document.get("start_time"))
    if started is None:
        return None
    turns: list[dict] = []
    language: Optional[str] = None
    segments = document.get("segments")
    if not isinstance(segments, list):
        return None
    # The producer must never pretend a partial transcript is complete.  The summary variant is
    # the bounded fallback for pathological/oversized source documents.
    if len(segments) > 4096:
        return {"invalid": "too_many_turns"}
    content_bytes = len(_compact_bytes({"format": "speaker_turns", "turns": []}))
    for segment in segments:
        if not isinstance(segment, dict):
            return {"invalid": "malformed_turn"}
        text = segment.get("text")
        if not isinstance(text, str):
            return {"invalid": "malformed_turn"}
        if not text.strip():
            continue
        if len(text) > 65_536:
            return {"invalid": "turn_too_large"}
        absolute_start = segment.get("absolute_start_time")
        absolute_end = segment.get("absolute_end_time")
        turn_start = _parse_time(absolute_start)
        turn_end = _parse_time(absolute_end)
        if (absolute_start is not None and turn_start is None) or (
            absolute_end is not None and turn_end is None
        ):
            return {"invalid": "malformed_turn"}
        try:
            turn_start = turn_start or started + timedelta(seconds=float(segment.get("start", 0)))
            turn_end = turn_end or started + timedelta(seconds=float(segment.get("end", segment.get("start", 0))))
        except (TypeError, ValueError, OverflowError):
            return {"invalid": "malformed_turn"}
        if turn_end < turn_start:
            return {"invalid": "malformed_turn"}
        speaker = segment.get("speaker")
        speaker = " ".join(speaker.split())[:200] if isinstance(speaker, str) else "Speaker"
        if not speaker:
            speaker = "Speaker"
        if any(ord(character) < 32 for character in speaker):
            return {"invalid": "malformed_turn"}
        turn = {"speaker": speaker, "started_at": _iso(turn_start), "text": text.strip()}
        if turn_end != turn_start:
            turn["ended_at"] = _iso(turn_end)
        candidate_language = segment.get("language")
        if language is None and isinstance(candidate_language, str):
            candidate_language = candidate_language.strip()
            if (
                2 <= len(candidate_language) <= 35
                and not any(ord(character) < 32 for character in candidate_language)
            ):
                language = candidate_language
                content_bytes += (
                    len(_compact_bytes({
                        "format": "speaker_turns", "language": language, "turns": [],
                    }))
                    - len(_compact_bytes({"format": "speaker_turns", "turns": []}))
                )
        candidate_bytes = content_bytes + len(_compact_bytes(turn)) + (1 if turns else 0)
        if candidate_bytes > ITEM_CONTENT_CAP_BYTES:
            return {"invalid": "content_too_large"}
        content_bytes = candidate_bytes
        turns.append(turn)
    turns.sort(key=lambda turn: turn["started_at"])
    if not turns:
        return None
    return {"format": "speaker_turns", **({"language": language} if language else {}), "turns": turns}


@dataclass(frozen=True)
class _Record:
    metadata: dict
    content: dict
    capture_notice: Optional[dict] = None
    summary_variant: Optional[dict] = None
    summary_variant_retention: Optional[dict] = None
    content_valid: bool = True
    content_error: Optional[str] = None

    def item(self, *, variant: str = "full") -> dict:
        uses_summary_variant = variant == "summary" and self.summary_variant is not None
        content = self.summary_variant if uses_summary_variant else self.content
        metadata = (
            {**self.metadata, "retention": self.summary_variant_retention}
            if uses_summary_variant and self.summary_variant_retention is not None
            else self.metadata
        )
        return {
            **metadata,
            **({"capture_notice": self.capture_notice} if self.capture_notice else {}),
            "content": content,
        }


def _records_for(
    row: dict, document: Optional[dict], now: datetime, *, metadata_only: bool = False,
) -> list[_Record]:
    try:
        meeting_id = int(row.get("id"))
    except (TypeError, ValueError):
        return []
    data = row.get("data") if isinstance(row.get("data"), dict) else {}
    notice = _capture_notice(data, now)
    finalization = _transcript_finalization(data, now)
    if (
        notice is None
        or finalization is None
        or row.get("status") not in {"completed", "failed"}
    ):
        return []
    occurred = _parse_time(row.get("start_time") or row.get("created_at"))
    updated = finalization["finalized_at"]
    if occurred is None or updated is None or occurred > now:
        return []
    title = _safe_title(data, meeting_id)
    meeting_ref = f"meeting:{meeting_id}"
    common = {
        "occurred_at": _iso(occurred),
        "updated_at": _iso(updated),
        "sensitivity": "sensitive_pii",
    }
    records: list[_Record] = []
    summary_retention = _retention(data, "summary", now)
    summary = _summary_text(data) if summary_retention is not None else None
    transcript_retention = _retention(data, "transcript", now)
    if transcript_retention is not None:
        platform = row.get("platform")
        ended = _parse_time(row.get("end_time"))
        attendees = data.get("attendees")
        attendees = attendees if isinstance(attendees, list) else []
        safe_attendees = [
            attendee.strip()[:500] for attendee in attendees[:1000]
            if isinstance(attendee, str) and attendee.strip()
        ]
        if platform in _SUPPORTED_PLATFORMS and ended is not None and ended >= occurred:
            records.append(_Record(
                metadata={
                    "id": meeting_ref,
                    "kind": "meeting",
                    "title": title,
                    **common,
                    "retention": transcript_retention,
                },
                capture_notice=notice,
                content={
                    "platform": platform,
                    "started_at": _iso(occurred),
                    "ended_at": _iso(ended),
                    "attendees": safe_attendees,
                },
            ))
        turns = None if metadata_only else (
            {"invalid": str(document["_zaki_read_invalid"])}
            if isinstance(document, dict) and document.get("_zaki_read_invalid")
            else _speaker_turns(document or {})
        )
        if finalization["segment_count"] > 0 and (metadata_only or turns is not None):
            summary_variant_retention = None
            if summary is not None and summary_retention is not None:
                summary_variant_retention = {
                    **transcript_retention,
                    "expires_at": min(
                        transcript_retention["expires_at"],
                        summary_retention["expires_at"],
                    ),
                }
            records.append(_Record(
                metadata={
                    "id": f"transcript:{meeting_id}",
                    "kind": "transcript",
                    "title": f"{title} transcript"[:500],
                    "meeting_id": meeting_ref,
                    **common,
                    "retention": transcript_retention,
                },
                capture_notice=notice,
                content=(
                    {"format": "speaker_turns", "turns": []}
                    if metadata_only or "invalid" in turns
                    else turns
                ),
                content_valid=bool(not metadata_only and "invalid" not in turns),
                content_error=(
                    turns.get("invalid")
                    if not metadata_only and isinstance(turns, dict)
                    else None
                ),
                summary_variant={"format": "summary", "text": summary} if summary else None,
                summary_variant_retention=summary_variant_retention,
            ))
    if summary_retention is not None and summary:
        records.append(_Record(
            metadata={
                "id": f"summary:{meeting_id}",
                "kind": "summary",
                "title": f"{title} summary"[:500],
                "meeting_id": meeting_ref,
                **common,
                "retention": summary_retention,
            },
            content={"format": "summary", "text": summary},
        ))
    return records


def _cursor_encode(token: str, state: dict) -> str:
    payload = base64.urlsafe_b64encode(_compact_bytes(state)).decode().rstrip("=")
    signature = hmac.new(token.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{signature}"


def _cursor_decode(token: str, value: Optional[str], *, route: str, query_hash: str = "") -> Optional[dict]:
    if not value:
        return None
    if len(value) > 2048 or value.count(".") != 1:
        raise ValueError("bad cursor")
    payload, supplied = value.split(".", 1)
    expected = hmac.new(token.encode(), payload.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(supplied, expected):
        raise ValueError("bad cursor")
    try:
        decoded = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        state = json.loads(decoded)
    except Exception as error:
        raise ValueError("bad cursor") from error
    if (
        not isinstance(state, dict)
        or state.get("route") != route
        or state.get("query_hash", "") != query_hash
        or _parse_time(state.get("snapshot")) is None
        or not isinstance(state.get("meeting_id"), int)
        or state["meeting_id"] <= 0
        or _parse_time(state.get("occurred_at")) is None
        or not isinstance(state.get("item_offset"), int)
        or state["item_offset"] < 0
        or state["item_offset"] > 3
    ):
        raise ValueError("bad cursor")
    return state


async def _await(value):
    return await value if inspect.isawaitable(value) else value


async def _authorized(
    *, token: str, supplied_token: Optional[str], path_user: str, header_user: Optional[str],
    scope: ReadScope, request_id: Optional[str],
) -> tuple[Optional[int], Optional[JSONResponse]]:
    if not supplied_token or not hmac.compare_digest(
        supplied_token.encode("utf-8"), token.encode("ascii")
    ):
        return None, _error(401, "bad_token", "Read token was rejected", request_id)
    if (
        not _USER_ID.fullmatch(path_user)
        or header_user != path_user
        or int(path_user) > _MAX_DB_ID
    ):
        return None, _error(404, "unknown_user", "User was not found", request_id)
    user_id = int(path_user)
    try:
        settings = await _await(scope(user_id))
    except Exception:
        return None, _error(403, "scope_disabled", "Minutes read scope is disabled", request_id)
    if settings is None:
        return None, _error(404, "unknown_user", "User was not found", request_id)
    if not isinstance(settings, dict) or settings.get("agent_read_enabled") is not True:
        return None, _error(403, "scope_disabled", "Minutes read scope is disabled", request_id)
    return user_id, None


async def _page_rows(
    store, user_id: int, *, snapshot: datetime, visible_at: datetime, state: Optional[dict]
) -> list[dict]:
    if hasattr(store, "list_zaki_read_meetings"):
        return await store.list_zaki_read_meetings(
            user_id,
            snapshot=snapshot,
            visible_at=visible_at,
            before_occurred=_parse_time(state.get("occurred_at")) if state else None,
            before_id=state.get("meeting_id") if state else None,
            inclusive=bool(state and state.get("item_offset", 0) > 0),
            limit=SCAN_BATCH,
        )
    rows = await store.list_meetings(user_id, limit=SCAN_BATCH)
    return [row for row in rows if row.get("user_id") == user_id]


async def _collect(
    store, *, user_id: int, now: datetime, limit: int, state: Optional[dict],
    predicate: Callable[[_Record], bool],
) -> tuple[list[dict], Optional[dict]]:
    snapshot = (_parse_time(state.get("snapshot")) if state else None) or now
    items: list[dict] = []
    item_states: list[dict] = []
    scanned = 0
    cursor_state = state
    while scanned < MAX_SCAN_MEETINGS:
        rows = await _page_rows(
            store, user_id, snapshot=snapshot, visible_at=now, state=cursor_state
        )
        if not rows:
            return items, None
        for index, row in enumerate(rows):
            scanned += 1
            if row.get("user_id") != user_id:
                continue
            # Index is metadata-only by contract. Transcript bodies are fetched only by the item
            # route through one atomic owner/retention/content snapshot.
            records = _records_for(row, None, now, metadata_only=True)
            occurred_at = row.get("start_time") or row.get("created_at")
            item_offset = cursor_state.get("item_offset", 0) if cursor_state and index == 0 else 0
            for record_index, record in enumerate(records[item_offset:], start=item_offset):
                next_offset = record_index + 1
                current = {
                    "route": state.get("route") if state else "index",
                    "query_hash": state.get("query_hash", "") if state else "",
                    "snapshot": _iso(snapshot),
                    "occurred_at": occurred_at,
                    "meeting_id": int(row["id"]),
                    "item_offset": next_offset if next_offset < len(records) else 0,
                }
                if predicate(record):
                    items.append(record.metadata)
                    item_states.append(current)
                    # Look one matching record ahead so an exactly-full final page does not claim
                    # to be truncated.  Resume immediately after the last item actually returned.
                    if len(items) > limit:
                        return items[:limit], item_states[limit - 1]
            cursor_state = {
                "route": state.get("route") if state else "index",
                "query_hash": state.get("query_hash", "") if state else "",
                "snapshot": _iso(snapshot),
                "occurred_at": occurred_at,
                "meeting_id": int(row["id"]),
                "item_offset": 0,
            }
            if scanned >= MAX_SCAN_MEETINGS:
                if len(items) >= limit:
                    return items[:limit], item_states[limit - 1]
                return items, cursor_state
        if len(rows) < SCAN_BATCH:
            return items, None
    return items, cursor_state


def build_router(*, store, token: str, scope: ReadScope, now: Callable[[], datetime]) -> APIRouter:
    """Build the read-only router.  Composition validates the operator-owned dependencies."""

    if not isinstance(token, str) or not token:
        raise ValueError("zaki-read.v1 requires a dedicated token")
    if (
        not 32 <= len(token) <= 512
        or token != token.strip()
        or any(not 0x20 <= ord(character) <= 0x7E for character in token)
    ):
        raise ValueError(
            "zaki-read.v1 token must be unpadded printable ASCII between 32 and 512 characters"
        )
    if scope is None:
        raise ValueError("zaki-read.v1 requires a per-user scope authority")
    if not callable(getattr(store, "get_zaki_read_snapshot", None)):
        raise ValueError("zaki-read.v1 requires an atomic item snapshot store")
    router = APIRouter(prefix="/api/zaki/read/v1")

    def query_value(request: Request, name: str, *, max_length: int) -> tuple[Optional[str], bool]:
        values = request.query_params.getlist(name)
        if len(values) > 1:
            return None, False
        if not values:
            return None, True
        value = values[0]
        return (value, len(value) <= max_length)

    def positive_limit(
        request: Request, *, default: int, maximum: int,
    ) -> tuple[Optional[int], bool]:
        raw, valid = query_value(request, "limit", max_length=10)
        if not valid:
            return None, False
        if raw is None:
            return default, True
        if not raw.isascii() or not raw.isdecimal():
            return None, False
        value = int(raw)
        if value < 1:
            return None, False
        return min(value, maximum), True

    async def auth(path_user: str, supplied_token: Optional[str], header_user: Optional[str], request_id: Optional[str]):
        return await _authorized(
            token=token, supplied_token=supplied_token, path_user=path_user,
            header_user=header_user, scope=scope, request_id=request_id,
        )

    @router.get("/{path_user}/index")
    async def index(
        path_user: str,
        request: Request,
        x_zaki_read_token: Optional[str] = Header(default=None),
        x_zaki_user_id: Optional[str] = Header(default=None),
        x_request_id: Optional[str] = Header(default=None),
    ):
        user_id, denied = await auth(path_user, x_zaki_read_token, x_zaki_user_id, x_request_id)
        if denied:
            return denied
        since, since_valid = query_value(request, "since", max_length=128)
        cursor, cursor_valid = query_value(request, "cursor", max_length=2048)
        limit, limit_valid = positive_limit(request, default=50, maximum=INDEX_LIMIT)
        if not since_valid:
            return _error(400, "bad_since", "since must be ISO-8601", x_request_id)
        if not cursor_valid:
            return _error(400, "bad_cursor", "cursor is invalid", x_request_id)
        if not limit_valid:
            return _error(400, "bad_limit", "limit must be a positive integer", x_request_id)
        read_now = now()
        since_at = _parse_time(since) if since else None
        if since and since_at is None:
            return _error(400, "bad_since", "since must be ISO-8601", x_request_id)
        since_key = _iso(since_at) if since_at is not None else ""
        query_hash = hashlib.sha256(since_key.encode()).hexdigest()
        try:
            state = _cursor_decode(
                token, cursor, route="index", query_hash=query_hash,
            )
        except ValueError:
            return _error(400, "bad_cursor", "cursor is invalid", x_request_id)
        if state is None:
            state = {"route": "index", "query_hash": query_hash}
        items, next_state = await _collect(
            store, user_id=user_id, now=read_now, limit=limit, state=state,
            predicate=lambda record: since_at is None or _parse_time(record.metadata["updated_at"]) >= since_at,
        )
        return _response({
            "items": items,
            "truncated": next_state is not None,
            **({"next_cursor": _cursor_encode(token, next_state)} if next_state else {}),
        }, request_id=x_request_id)

    @router.get("/{path_user}/search")
    async def search(
        path_user: str,
        request: Request,
        x_zaki_read_token: Optional[str] = Header(default=None),
        x_zaki_user_id: Optional[str] = Header(default=None),
        x_request_id: Optional[str] = Header(default=None),
    ):
        user_id, denied = await auth(path_user, x_zaki_read_token, x_zaki_user_id, x_request_id)
        if denied:
            return denied
        q, query_valid = query_value(request, "q", max_length=500)
        cursor, cursor_valid = query_value(request, "cursor", max_length=2048)
        _limit, limit_valid = positive_limit(request, default=20, maximum=SEARCH_LIMIT)
        if not query_valid or q is None:
            return _error(400, "bad_query", "q is required", x_request_id)
        if not cursor_valid:
            return _error(400, "bad_cursor", "cursor is invalid", x_request_id)
        if not limit_valid:
            return _error(400, "bad_limit", "limit must be a positive integer", x_request_id)
        query = " ".join(q.split()).casefold()
        if not query:
            return _error(400, "bad_query", "q is required", x_request_id)
        # Search is optional in the fleet contract and remains unavailable until its store query
        # can enforce owner/retention filtering and content bounds without N+1 transcript loads.
        return _error(404, "search_disabled", "Search is not available", x_request_id)

    @router.get("/{path_user}/item/{item_id}")
    async def item(
        path_user: str,
        item_id: str,
        request: Request,
        x_zaki_read_token: Optional[str] = Header(default=None),
        x_zaki_user_id: Optional[str] = Header(default=None),
        x_request_id: Optional[str] = Header(default=None),
    ):
        user_id, denied = await auth(path_user, x_zaki_read_token, x_zaki_user_id, x_request_id)
        if denied:
            return denied
        variant, variant_valid = query_value(request, "variant", max_length=16)
        variant = variant or "full"
        if not variant_valid or variant not in {"full", "summary"}:
            return _error(400, "bad_variant", "variant is invalid", x_request_id)
        parsed = _ITEM_ID.fullmatch(item_id)
        if not parsed:
            return _error(404, "unknown_item", "Item was not found", x_request_id)
        kind, meeting_id_text = parsed.groups()
        meeting_id = int(meeting_id_text)
        if meeting_id > _MAX_DB_ID:
            return _error(404, "unknown_item", "Item was not found", x_request_id)
        read_now = now()
        snapshot = await store.get_zaki_read_snapshot(
            user_id,
            meeting_id,
            include_transcript=kind == "transcript",
            readable_at=read_now,
        )
        row = snapshot.get("row") if isinstance(snapshot, dict) else None
        document = snapshot.get("document") if isinstance(snapshot, dict) else None
        record = next(
            (candidate for candidate in _records_for(row or {}, document or {}, read_now)
             if candidate.metadata["kind"] == kind),
            None,
        )
        if record is None:
            return _error(404, "unknown_item", "Item was not found", x_request_id)
        if variant == "summary" and record.summary_variant is None and kind != "summary":
            return _error(404, "unknown_item", "Item was not found", x_request_id)
        content = record.summary_variant if variant == "summary" and record.summary_variant else record.content
        if (
            variant == "full"
            and not record.content_valid
            and record.content_error == "revision_mismatch"
        ):
            return _error(
                503,
                "item_unavailable",
                "Item is temporarily unavailable",
                x_request_id,
            )
        if (variant == "full" and not record.content_valid) or len(_compact_bytes(content)) > ITEM_CONTENT_CAP_BYTES:
            return _error(413, "item_too_large", "Item exceeds the read cap", x_request_id)
        return _response({"item": record.item(variant=variant), "truncated": False}, request_id=x_request_id)

    return router
