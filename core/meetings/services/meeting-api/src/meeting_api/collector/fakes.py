"""In-process fakes satisfying the collector's ports — for the ingestion eval AND the gateway
conformance harness (both drive the SAME shipped ``create_app`` / ``ingest`` with these).

  * ``InMemoryTranscriptStore`` — a dict-backed ``TranscriptStore``. ``seed_meeting`` plants a
    meeting (mirrors a ``meetings`` row + its ``data`` JSONB); ``append_segment`` accumulates
    segments by ``segment_id`` (last-write-wins, the parent's Redis-hash identity). ``get_transcript``
    emits an api.v1 ``TranscriptionResponse``-shaped dict; ``list_meetings`` emits
    ``MeetingResponse``-shaped dicts.
  * ``FakeRedisBus`` — a fakeredis-backed ``RedisBus`` wrapper: ``xadd`` to enqueue a stream
    message, ``read_segments`` drains via XREADGROUP, ``publish`` records (and forwards to
    fakeredis pubsub) the ``:mutable`` updates so a test can assert the gateway-facing payload.

These carry NO production logic — they only stand in for Postgres + Redis so the eval/conformance
run OFFLINE (no docker), exactly like the gateway lane's port-fakes.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json
import re
from typing import Optional

from ..meeting_writes import (
    MAX_FINAL_TRANSCRIPT_CONTENT_BYTES,
    MAX_FINAL_TRANSCRIPT_SEGMENTS,
    content_scopes_are_writable,
)


_MAX_MANAGED_REDIS_SCAN_STEPS = 128


def _segment_to_api(seg: dict) -> dict:
    """A stored segment → api.v1 ``TranscriptionSegment`` (start/end/text/language required)."""
    out = {
        "start": float(seg.get("start", 0.0)),
        "end": float(seg.get("end", 0.0)),
        "text": seg.get("text", ""),
        "language": seg.get("language"),
    }
    for k in ("speaker", "completed", "segment_id", "source", "absolute_start_time", "absolute_end_time"):
        if seg.get(k) is not None:
            out[k] = seg[k]
    return out


class _InMemoryTranscriptBatchWriter:
    def __init__(self, store, meeting_id: int):
        self._store = store
        self._meeting_id = meeting_id

    async def append_segments(self, segments: list[dict]) -> None:
        await self._store._append_segments_unchecked(self._meeting_id, segments)


class InMemoryTranscriptStore:
    """A dict-backed ``TranscriptStore``. Owner-scoped by ``user_id`` (the authorization
    boundary). Keyed internally by the synthetic ``meeting_id``.

    Pass ``redis_client`` (fakeredis) to mirror the PRODUCTION topology exactly: ``append_segment``
    then lands live segments in the redis hash ``meeting:{id}:segments`` (+ ``active_meetings``),
    ``get_transcript`` merges the durable dict with that hash, and the db-writer tick
    (``db_writer.db_writer_tick``) moves segments from the hash into the durable dict via
    ``upsert_segments`` — so the flush/trim/read-merge seam is testable offline, no docker."""

    def __init__(self, redis_client=None):
        # meeting_id -> {user_id, platform, native_meeting_id, status, start_time, end_time,
        #                data, segments: {segment_id: seg}}
        self._meetings: dict[int, dict] = {}
        self._next_id = 1
        # Optional live-segment redis (fakeredis in tests) — mirrors the prod adapter's split
        # between the in-flight hash (redis) and the durable rows (the dict standing in for PG).
        self._redis = redis_client
        self._meeting_write_locks: dict[int, asyncio.Lock] = {}

    def _meeting_write_lock(self, meeting_id: int) -> asyncio.Lock:
        return self._meeting_write_locks.setdefault(meeting_id, asyncio.Lock())

    def seed_meeting(
        self,
        *,
        user_id: int,
        platform: str,
        native_meeting_id: str,
        status: str = "active",
        meeting_id: Optional[int] = None,
        start_time: Optional[str] = "2026-06-20T09:00:00Z",
        end_time: Optional[str] = None,
        bot_container_id: Optional[str] = None,
        data: Optional[dict] = None,
        created_at: str = "2026-06-20T08:59:00Z",
        updated_at: str = "2026-06-20T09:00:05Z",
        constructed_meeting_url: Optional[str] = None,
        segments: Optional[list[dict]] = None,
    ) -> int:
        mid = meeting_id if meeting_id is not None else self._next_id
        self._next_id = max(self._next_id, mid + 1)
        self._meetings[mid] = {
            "user_id": user_id,
            "platform": platform,
            "native_meeting_id": native_meeting_id,
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "bot_container_id": bot_container_id,
            "constructed_meeting_url": constructed_meeting_url,
            "data": dict(data or {}),
            "created_at": created_at,
            "updated_at": updated_at,
            "segments": {s["segment_id"]: s for s in (segments or [])},
        }
        return mid

    async def native_for(self, meeting_id):
        """Numeric meeting_id → (native_meeting_id, platform), cross-user (the internal segment
        consumer owns the mapping). Mirrors the SqlAlchemy store so ingest can stamp the live payload."""
        try:
            mid = int(meeting_id)
        except (TypeError, ValueError):
            return None
        m = self._meetings.get(mid)
        if not m or not m.get("native_meeting_id"):
            return None
        return (m["native_meeting_id"], m.get("platform") or "google_meet")

    async def owner_for(self, meeting_id):
        """Exact-row owner lookup used only by the secret-protected internal attribution edge."""
        try:
            mid = int(meeting_id)
        except (TypeError, ValueError):
            return None
        meeting = self._meetings.get(mid)
        if meeting is None:
            return None
        owner = meeting.get("user_id")
        return owner if isinstance(owner, int) and not isinstance(owner, bool) and owner > 0 else None

    async def connect_meeting_doc_by_id(self, meeting_id):
        """Exact-row internal doc link; derive every identity-bearing field from the row."""
        from .adapters import _upsert_doc

        try:
            mid = int(meeting_id)
        except (TypeError, ValueError):
            return None
        meeting = self._meetings.get(mid)
        if meeting is None:
            return None
        owner = meeting.get("user_id")
        if (
            not isinstance(owner, int)
            or isinstance(owner, bool)
            or owner <= 0
            or not content_scopes_are_writable(meeting.get("data"), "summary")
        ):
            return None
        doc = {
            "workspace": str(owner),
            "path": f"kg/entities/meeting/{mid}.md",
            "title": f"Meeting {mid}",
            "kind": "meeting",
        }
        data = meeting["data"]
        data["docs"] = _upsert_doc(list(data.get("docs", [])), doc)
        return doc

    def _find(self, user_id, platform, native_meeting_id) -> Optional[int]:
        # NEWEST-first, exactly like the SqlAlchemy store (``order_by(Meeting.created_at.desc())``): a user
        # with several rows on the SAME native link resolves to the LATEST run. This faithfully mirrors the
        # symptom-2 ambiguity — the native path can only ever address the newest row, which is precisely
        # why the by-ROW-id read path exists (P0). Tiebreak on the id so the pick is deterministic.
        matches = [
            (mid, m) for mid, m in self._meetings.items()
            if m["user_id"] == user_id
            and m["platform"] == platform
            and m["native_meeting_id"] == native_meeting_id
        ]
        if not matches:
            return None
        matches.sort(key=lambda kv: (kv[1].get("created_at") or "", kv[0]), reverse=True)
        return matches[0][0]

    async def _transcript_doc(self, mid, *, data=None) -> Optional[dict]:
        """Build the api.v1 ``TranscriptionResponse`` for row ``mid`` — shared by ``get_transcript``
        (native → newest) and ``get_transcript_by_id`` (exact row). Keyed by the row id ``mid``, so a
        by-id read returns exactly that row's segments/notes."""
        m = self._meetings[mid]
        projected_data = data if isinstance(data, dict) else m["data"]
        managed = (
            "zaki_capture" in projected_data or "zaki_retention" in projected_data
        )
        if managed and len(m["segments"]) > MAX_FINAL_TRANSCRIPT_SEGMENTS:
            return None
        by_id = dict(m["segments"])
        # Redis-wired (prod-topology) mode: merge the LIVE in-flight hash over the durable rows,
        # exactly like the SqlAlchemy store's read merge.
        if self._redis is not None:
            if managed:
                hscan = getattr(self._redis, "hscan", None)
                hlen = getattr(self._redis, "hlen", None)
                if not callable(hscan) or not callable(hlen):
                    return None
                try:
                    carrier_count = int(
                        await hlen(f"meeting:{mid}:segments")
                    )
                except Exception:
                    carrier_count = 0
                if carrier_count > MAX_FINAL_TRANSCRIPT_SEGMENTS:
                    return None
                if carrier_count < 0:
                    return None
                cursor = 0
                seen_cursors: set[int] = set()
                carrier_entries = 0
                carrier_bytes = 0
                scan_steps = 0
                while carrier_count:
                    scan_steps += 1
                    if scan_steps > _MAX_MANAGED_REDIS_SCAN_STEPS:
                        return None
                    next_cursor, raw = await hscan(
                        f"meeting:{mid}:segments", cursor=cursor, count=100
                    )
                    if not isinstance(raw, dict):
                        return None
                    carrier_entries += len(raw)
                    if carrier_entries > MAX_FINAL_TRANSCRIPT_SEGMENTS:
                        return None
                    for value in raw.values():
                        try:
                            value_bytes = (
                                bytes(value)
                                if isinstance(value, (bytes, bytearray))
                                else str(value).encode("utf-8")
                            )
                        except (TypeError, UnicodeEncodeError):
                            return None
                        carrier_bytes += len(value_bytes)
                        if carrier_bytes > MAX_FINAL_TRANSCRIPT_CONTENT_BYTES:
                            return None
                        try:
                            segment = json.loads(value_bytes)
                        except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
                            continue
                        sid = segment.get("segment_id")
                        if sid:
                            by_id[sid] = segment
                    try:
                        cursor = int(next_cursor)
                    except (TypeError, ValueError, OverflowError):
                        return None
                    if cursor == 0:
                        break
                    if cursor in seen_cursors:
                        return None
                    seen_cursors.add(cursor)
            else:
                raw = await self._redis.hgetall(f"meeting:{mid}:segments")
                for v in (raw.values() if isinstance(raw, dict) else []):
                    try:
                        seg = json.loads(v.decode() if isinstance(v, (bytes, bytearray)) else v)
                    except Exception:
                        continue
                    sid = seg.get("segment_id")
                    if sid:
                        by_id[sid] = seg
        segments = sorted(by_id.values(), key=lambda s: float(s.get("start", 0.0)))
        if managed:
            if len(segments) > MAX_FINAL_TRANSCRIPT_SEGMENTS:
                return None
            try:
                content_bytes = sum(
                    len(json.dumps(
                        _segment_to_api(segment),
                        ensure_ascii=False,
                        allow_nan=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode("utf-8"))
                    for segment in segments
                )
            except (TypeError, ValueError, OverflowError):
                return None
            if content_bytes > MAX_FINAL_TRANSCRIPT_CONTENT_BYTES:
                return None
            from ..meeting_writes import legacy_collector_read_projection

            projected_data = legacy_collector_read_projection(m["data"])
            if projected_data is None:
                return None
        return {
            "id": mid,
            "platform": m["platform"],
            "native_meeting_id": m["native_meeting_id"],
            "constructed_meeting_url": m.get("constructed_meeting_url"),
            "status": m["status"],
            "start_time": m["start_time"],
            "end_time": m["end_time"],
            "recordings": projected_data.get("recordings", []),
            "notes": projected_data.get("notes"),
            "data": projected_data,
            "segments": [_segment_to_api(s) for s in segments],
        }

    async def get_transcript(self, user_id, platform, native_meeting_id) -> Optional[dict]:
        mid = self._find(user_id, platform, native_meeting_id)
        if mid is None:
            return None
        from ..meeting_writes import legacy_collector_read_projection

        async with self._meeting_write_lock(mid):
            meeting = self._meetings.get(mid)
            projected = (
                legacy_collector_read_projection(meeting.get("data"))
                if meeting is not None
                else None
            )
            if meeting is None or projected is None:
                return None
            return await self._transcript_doc(mid, data=projected)

    async def get_transcript_by_id(self, user_id, meeting_id, member_workspaces=None) -> Optional[dict]:
        """Exact-row transcript authorized by owner OR transcript-viewer OR bound-workspace member (mirrors
        authorize_subscribe) — any other caller → ``None`` (a different tenant's row never leaks)."""
        try:
            mid = int(meeting_id)
        except (TypeError, ValueError):
            return None
        from ..meeting_writes import legacy_collector_read_projection

        async with self._meeting_write_lock(mid):
            m = self._meetings.get(mid)
            if m is None:
                return None
            data = m.get("data") if isinstance(m.get("data"), dict) else {}
            authorized = (
                m.get("user_id") == user_id
                or user_id in (data.get("transcript_viewers") or [])
                or (bool(member_workspaces) and data.get("workspace_id") in member_workspaces)
            )
            projected = legacy_collector_read_projection(data)
            if not authorized or projected is None:
                return None
            return await self._transcript_doc(mid, data=projected)

    async def list_meetings(self, user_id, *, status=None, platform=None, limit=None, offset=None, member_workspaces=None):
        from ..meeting_writes import legacy_collector_read_projection

        mws = member_workspaces or set()

        def accessible(m):
            data = m.get("data") if isinstance(m.get("data"), dict) else {}
            return (m["user_id"] == user_id
                    or user_id in (data.get("transcript_viewers") or [])
                    or data.get("workspace_id") in mws)
        rows = list(self._meetings.items())
        # newest first (by created_at desc, then id desc as a stable tiebreak)
        rows.sort(key=lambda kv: (kv[1]["created_at"], kv[0]), reverse=True)
        visible = []
        for mid, _meeting in rows:
            async with self._meeting_write_lock(mid):
                meeting = self._meetings.get(mid)
                projected = (
                    legacy_collector_read_projection(meeting.get("data"))
                    if meeting is not None
                    else None
                )
                if (
                    meeting is not None
                    and accessible(meeting)
                    and (status is None or meeting["status"] == status)
                    and (platform is None or meeting["platform"] == platform)
                    and projected is not None
                ):
                    visible.append((mid, meeting, projected))
        rows = visible
        if offset:
            rows = rows[offset:]
        if limit:
            rows = rows[:limit]
        return [
            {
                "id": mid,
                "user_id": m["user_id"],
                "platform": m["platform"],
                "native_meeting_id": m["native_meeting_id"],
                "constructed_meeting_url": m.get("constructed_meeting_url"),
                "status": m["status"],
                "bot_container_id": m.get("bot_container_id"),
                "start_time": m["start_time"],
                "end_time": m["end_time"],
                "data": data,
                "shared": m["user_id"] != user_id,
                "created_at": m["created_at"],
                "updated_at": m["updated_at"],
            }
            for mid, m, data in rows
        ]

    def _zaki_read_row(self, mid: int, meeting: dict) -> dict:
        return {
            "id": mid,
            "user_id": meeting["user_id"],
            "platform": meeting["platform"],
            "status": meeting["status"],
            "start_time": meeting.get("start_time"),
            "end_time": meeting.get("end_time"),
            "data": meeting.get("data") if isinstance(meeting.get("data"), dict) else {},
            "created_at": meeting.get("created_at"),
            "updated_at": meeting.get("updated_at"),
        }

    async def list_zaki_read_meetings(
        self, user_id, *, snapshot, visible_at, before_occurred=None, before_id=None,
        inclusive=False, limit=100,
    ):
        """Fake of the production owner-only keyset query (no share/membership widening)."""
        from datetime import datetime, timezone

        def parsed(value):
            if isinstance(value, datetime):
                return value.astimezone(timezone.utc)
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)

        snapshot_key = parsed(snapshot)
        visible_key = parsed(visible_at)
        before_key = (
            (parsed(before_occurred), int(before_id))
            if before_occurred is not None else None
        )
        rows = []
        for mid, meeting in self._meetings.items():
            if meeting.get("user_id") != user_id:
                continue
            data = meeting.get("data") if isinstance(meeting.get("data"), dict) else {}
            capture = data.get("zaki_capture") if isinstance(data.get("zaki_capture"), dict) else {}
            finalization = (
                data.get("zaki_transcript_finalization")
                if isinstance(data.get("zaki_transcript_finalization"), dict)
                else {}
            )
            retention = data.get("zaki_retention") if isinstance(data.get("zaki_retention"), dict) else {}
            expiries = retention.get("scope_expiries") if isinstance(retention.get("scope_expiries"), dict) else {}
            expired = retention.get("expired_scopes") if isinstance(retention.get("expired_scopes"), list) else []
            visible_scope = False
            for scope in ("transcript", "summary"):
                try:
                    visible_scope = (
                        scope not in expired
                        and parsed(expiries.get(scope)) > visible_key
                    )
                except (TypeError, ValueError):
                    visible_scope = False
                if visible_scope:
                    break
            if (
                meeting.get("status") not in {"completed", "failed"}
                or capture.get("state") != "authorized"
                or capture.get("bot_name") != "ZAKI Notetaker"
                or capture.get("tenant_attested") is not True
                or not isinstance(capture.get("tenant_policy_version"), str)
                or not 1 <= len(capture.get("tenant_policy_version", "").strip()) <= 80
                or finalization.get("state") != "finalized"
                or not isinstance(finalization.get("revision"), str)
                or re.fullmatch(r"sha256:[0-9a-f]{64}", finalization.get("revision", "")) is None
                or not isinstance(finalization.get("segment_count"), int)
                or isinstance(finalization.get("segment_count"), bool)
                or finalization.get("segment_count", -1) < 0
                or retention.get("state") != "open"
                or not visible_scope
            ):
                continue
            try:
                if (
                    parsed(capture.get("tenant_attested_at")) > visible_key
                    or parsed(finalization.get("finalized_at")) > visible_key
                ):
                    continue
            except (TypeError, ValueError):
                continue
            try:
                updated = parsed(meeting.get("updated_at"))
                occurred = parsed(meeting.get("start_time") or meeting.get("created_at"))
            except (TypeError, ValueError):
                continue
            if updated > snapshot_key:
                continue
            key = (occurred, mid)
            if before_key is not None and (key > before_key or (key == before_key and not inclusive)):
                continue
            rows.append((key, mid, meeting))
        rows.sort(key=lambda value: value[0], reverse=True)
        return [self._zaki_read_row(mid, meeting) for _key, mid, meeting in rows[:limit]]

    async def get_zaki_read_meeting(self, user_id, meeting_id):
        try:
            meeting_id = int(meeting_id)
        except (TypeError, ValueError):
            return None
        meeting = self._meetings.get(meeting_id)
        if meeting is None or meeting.get("user_id") != user_id:
            return None
        return self._zaki_read_row(meeting_id, meeting)

    async def get_zaki_read_snapshot(
        self, user_id, meeting_id, *, include_transcript=False, readable_at=None,
    ):
        """Owner-only item snapshot used by zaki-read.

        The fake is single-process, so reading both values without yielding is the equivalent of
        the production adapter's one row-locked transaction. Redis-backed tests may yield only
        while constructing a transcript after the owner row has been selected.
        """
        try:
            meeting_id = int(meeting_id)
        except (TypeError, ValueError):
            return None
        async with self._meeting_write_lock(meeting_id):
            meeting = self._meetings.get(meeting_id)
            if meeting is None or meeting.get("user_id") != user_id:
                return None
            row = self._zaki_read_row(meeting_id, meeting)
            content_authorized = False
            if include_transcript and readable_at is not None:
                from ..zaki_read import (
                    _capture_notice,
                    _parse_time,
                    _retention,
                    _transcript_finalization,
                )

                data = row.get("data") if isinstance(row.get("data"), dict) else {}
                occurred = _parse_time(row.get("start_time") or row.get("created_at"))
                content_authorized = (
                    row.get("status") in {"completed", "failed"}
                    and occurred is not None
                    and occurred <= readable_at
                    and _capture_notice(data, readable_at) is not None
                    and _retention(data, "transcript", readable_at) is not None
                    and _transcript_finalization(data, readable_at) is not None
                )
            document = (
                self._zaki_read_transcript_doc(meeting_id) if content_authorized else None
            )
            return {"row": row, "document": document}

    def _zaki_read_transcript_doc(self, meeting_id: int) -> dict:
        """Build a finalized, durable-only transcript without materializing an unbounded carrier.

        A zaki-read snapshot never merges the live Redis hash: the finalization marker proves the
        durable rows are the immutable source revision, and anything still live is late/unfenced.
        """
        from ..meeting_writes import (
            MAX_FINAL_TRANSCRIPT_CONTENT_BYTES,
            MAX_FINAL_TRANSCRIPT_SEGMENTS,
            MAX_FINAL_TRANSCRIPT_TURN_CHARS,
            TranscriptRevisionBuilder,
            canonical_transcript_segment,
            validated_transcript_finalization_marker,
        )

        m = self._meetings[meeting_id]
        data = m.get("data") if isinstance(m.get("data"), dict) else {}
        expected = validated_transcript_finalization_marker(data)
        if expected is None:
            return {"_zaki_read_invalid": "revision_mismatch", "segments": []}
        canonical_rows = []
        content_bytes = 0
        for index, (segment_id, segment) in enumerate(m["segments"].items()):
            if index >= MAX_FINAL_TRANSCRIPT_SEGMENTS:
                return {"_zaki_read_invalid": "content_too_large", "segments": []}
            text = segment.get("text") if isinstance(segment, dict) else None
            if isinstance(text, str) and len(text) > MAX_FINAL_TRANSCRIPT_TURN_CHARS:
                return {"_zaki_read_invalid": "content_too_large", "segments": []}
            try:
                canonical = canonical_transcript_segment({
                    **segment,
                    "segment_id": str(segment_id),
                })
            except (TypeError, ValueError):
                return {"_zaki_read_invalid": "revision_mismatch", "segments": []}
            content_bytes += len(json.dumps(
                canonical,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8"))
            if content_bytes > MAX_FINAL_TRANSCRIPT_CONTENT_BYTES:
                return {"_zaki_read_invalid": "content_too_large", "segments": []}
            canonical_rows.append(canonical)
        canonical_rows.sort(key=lambda row: (row["start"], row["segment_id"]))
        revision = TranscriptRevisionBuilder()
        segments = []
        for canonical in canonical_rows:
            revision.add(canonical)
            segments.append(_segment_to_api(canonical))
        actual = revision.marker()
        if (
            actual["segment_count"] != expected["segment_count"]
            or actual["revision"] != expected["revision"]
        ):
            return {"_zaki_read_invalid": "revision_mismatch", "segments": []}
        return {
            "id": meeting_id,
            "platform": m["platform"],
            "status": m["status"],
            "start_time": m["start_time"],
            "end_time": m["end_time"],
            "data": data,
            "segments": segments,
        }

    async def authorize_subscribe(self, user_id, platform, native_meeting_id, member_workspaces=None) -> Optional[int]:
        from ..meeting_writes import legacy_collector_read_projection

        mid = self._find(user_id, platform, native_meeting_id)
        if mid is not None:
            async with self._meeting_write_lock(mid):
                meeting = self._meetings.get(mid)
                if meeting is None or legacy_collector_read_projection(
                    meeting.get("data")
                ) is None:
                    return None
                return mid  # (a) owner
        for m_id, m in self._meetings.items():
            if not (m.get("platform") == platform and m.get("native_meeting_id") == native_meeting_id
                    and isinstance(m.get("data"), dict)):
                continue
            async with self._meeting_write_lock(m_id):
                current = self._meetings.get(m_id)
                if current is None:
                    continue
                data = current.get("data") if isinstance(current.get("data"), dict) else {}
                if legacy_collector_read_projection(data) is None:
                    continue
                if member_workspaces and data.get("workspace_id") in member_workspaces:
                    return m_id  # (b) member of the bound workspace
                if user_id in (data.get("transcript_viewers") or []):
                    return m_id  # (c) redeemed an independent transcript-share link
        return None

    async def bind_workspace(self, user_id, platform, native_meeting_id, workspace_id):
        mid = self._find(user_id, platform, native_meeting_id)
        if mid is None:
            return None
        self._meetings[mid]["data"]["workspace_id"] = workspace_id
        return workspace_id

    async def mint_transcript_share(self, user_id, platform, native_meeting_id, *,
                                    mode="open", allowed_emails=None, expires_in_sec=86400):
        from datetime import timedelta

        from .adapters import _now, _sha
        mid = self._find(user_id, platform, native_meeting_id)
        if mid is None:
            return None
        import secrets
        secret = secrets.token_urlsafe(24)
        gid = secrets.token_hex(8)
        expires_at = (_now() + timedelta(seconds=int(expires_in_sec))).isoformat()
        grant = {"id": gid, "secret_hash": _sha(secret), "mode": mode,
                 "allowed_emails": list(allowed_emails or []), "expires_at": expires_at, "revoked": False}
        self._meetings[mid]["data"].setdefault("share_grants", []).append(grant)
        return {"id": gid, "token": f"{mid}.{secret}", "mode": mode, "expires_at": expires_at}

    async def redeem_transcript_share(self, user_id, user_email, token):
        from .adapters import _sha, validate_transcript_grant
        if not token or "." not in token:
            return None
        mid_s, secret = token.split(".", 1)
        try:
            mid = int(mid_s)
        except ValueError:
            return None
        m = self._meetings.get(mid)
        if not m:
            return None
        grant = next((g for g in m["data"].get("share_grants", []) if g.get("secret_hash") == _sha(secret)), None)
        if not grant:
            return {"error": "invalid"}
        err = validate_transcript_grant(grant, user_email)
        if err:
            return {"error": err}
        viewers = m["data"].setdefault("transcript_viewers", [])
        if user_id not in viewers:
            viewers.append(user_id)
        return {"meeting_id": mid, "ok": True}

    async def connect_doc(self, user_id, platform, native_meeting_id, doc):
        from .adapters import _upsert_doc

        mid = self._find(user_id, platform, native_meeting_id)
        if mid is None:
            return None
        data = self._meetings[mid]["data"]
        docs = _upsert_doc(list(data.get("docs", [])), doc)
        data["docs"] = docs
        return docs

    async def disconnect_doc(self, user_id, platform, native_meeting_id, path):
        from .adapters import _remove_doc

        mid = self._find(user_id, platform, native_meeting_id)
        if mid is None:
            return None
        data = self._meetings[mid]["data"]
        docs = _remove_doc(list(data.get("docs", [])), path)
        data["docs"] = docs
        return docs

    async def set_intent(self, user_id, platform, native_meeting_id, status, scheduled_at=None):
        mid = self._find(user_id, platform, native_meeting_id)
        if mid is None:
            return None
        m = self._meetings[mid]
        data = m["data"]
        prev_status = m.get("status")
        prev_at = data.get("scheduled_at")
        new_at = scheduled_at if status == "scheduled" else None
        m["status"] = status
        if status == "scheduled":
            data["scheduled_at"] = new_at
        else:
            data.pop("scheduled_at", None)
        changed = (prev_status != status) or (prev_at != new_at)
        return {
            "id": mid,
            "user_id": user_id,
            "platform": platform,
            "native_id": native_meeting_id,
            "status": status,
            "scheduled_at": new_at,
            "changed": changed,
        }

    def _planned_row(self, mid) -> dict:
        m = self._meetings[mid]
        return {
            "id": mid,
            "user_id": m["user_id"],
            "platform": m["platform"],
            "native_meeting_id": m["native_meeting_id"],
            "constructed_meeting_url": m.get("constructed_meeting_url")
            or m["data"].get("constructed_meeting_url"),
            "status": m["status"],
            "bot_container_id": m.get("bot_container_id"),
            "start_time": m.get("start_time"),
            "end_time": m.get("end_time"),
            "data": m["data"],
            "shared": False,
            "created_at": m.get("created_at"),
            "updated_at": m.get("updated_at"),
        }

    def _dup_non_terminal(self, user_id, platform, native_meeting_id, exclude_id=None):
        """True when a NON-TERMINAL row already exists for (user, platform, native) — the fake's
        stand-in for the partial unique index + the adapter's dup check."""
        if native_meeting_id is None:
            return False
        return any(
            m["user_id"] == user_id and m["platform"] == platform
            and m["native_meeting_id"] == native_meeting_id
            and m["status"] not in ("completed", "failed")
            and mid != exclude_id
            for mid, m in self._meetings.items()
        )

    async def create_planned_meeting(self, user_id, *, platform, native_meeting_id,
                                     title=None, scheduled_at=None, meeting_url=None,
                                     workspace_id=None, auto_join=True, calendar_uid=None,
                                     workspace_source=None, attendees=None):
        if self._dup_non_terminal(user_id, platform, native_meeting_id):
            return {"error": "duplicate"}
        data: dict = {"auto_join": bool(auto_join)}
        if title:
            data["title"] = title
        if scheduled_at:
            data["scheduled_at"] = scheduled_at
        if meeting_url:
            data["constructed_meeting_url"] = meeting_url
        if workspace_id:
            data["workspace_id"] = workspace_id
            if workspace_source:
                data["workspace_source"] = workspace_source
        if calendar_uid:
            data["calendar_uid"] = calendar_uid
        if attendees:
            data["attendees"] = attendees
        mid = self.seed_meeting(
            user_id=user_id, platform=platform, native_meeting_id=native_meeting_id,
            status="scheduled" if scheduled_at else "idle",
            start_time=None, data=data, constructed_meeting_url=meeting_url,
        )
        return self._planned_row(mid)

    async def update_planned_meeting(self, user_id, meeting_id, updates):
        m = self._meetings.get(meeting_id)
        if m is None or m["user_id"] != user_id:
            return None
        if m["status"] not in ("idle", "scheduled"):
            return {"error": "conflict"}
        data = m["data"]
        if "native_meeting_id" in updates:
            new_platform = updates.get("platform") or m["platform"]
            new_native = updates["native_meeting_id"]
            if new_native is not None and self._dup_non_terminal(
                user_id, new_platform, new_native, exclude_id=meeting_id
            ):
                return {"error": "duplicate"}
            m["platform"] = new_platform
            m["native_meeting_id"] = new_native
        if "constructed_meeting_url" in updates:
            if updates["constructed_meeting_url"]:
                data["constructed_meeting_url"] = updates["constructed_meeting_url"]
                m["constructed_meeting_url"] = updates["constructed_meeting_url"]
            else:
                data.pop("constructed_meeting_url", None)
                m["constructed_meeting_url"] = None
        if "title" in updates:
            if updates["title"]:
                data["title"] = updates["title"]
            else:
                data.pop("title", None)
        if "scheduled_at" in updates:
            if updates["scheduled_at"]:
                data["scheduled_at"] = updates["scheduled_at"]
                m["status"] = "scheduled"
            else:
                data.pop("scheduled_at", None)
                m["status"] = "idle"
        if "workspace_id" in updates:
            if updates["workspace_id"]:
                data["workspace_id"] = updates["workspace_id"]
                data["workspace_source"] = "user"
                data.pop("workspace_unbound", None)
            else:
                data.pop("workspace_id", None)
                data.pop("workspace_source", None)
                if data.get("calendar_uid"):
                    data["workspace_unbound"] = True
        if "attendees" in updates:
            if updates["attendees"]:
                data["attendees"] = updates["attendees"]
            else:
                data.pop("attendees", None)
        if "auto_join" in updates:
            data["auto_join"] = bool(updates["auto_join"])
        if "calendar_uid" in updates:
            if updates["calendar_uid"]:
                data["calendar_uid"] = updates["calendar_uid"]
            else:
                data.pop("calendar_uid", None)
        return self._planned_row(meeting_id)

    async def delete_planned_meeting(self, user_id, meeting_id):
        m = self._meetings.get(meeting_id)
        if m is None or m["user_id"] != user_id:
            return None
        if m["status"] not in ("idle", "scheduled"):
            return False
        del self._meetings[meeting_id]
        return True

    def _row_or_placeholder(self, meeting_id) -> dict:
        m = self._meetings.get(meeting_id)
        if m is None:
            # An ingested segment for an unknown meeting — seed a placeholder so the segment is
            # not lost (the parent persists by meeting_id regardless; the meeting row exists by
            # the time segments flow). Keep it owner-less until seeded.
            m = self._meetings.setdefault(meeting_id, {
                "user_id": None, "platform": None, "native_meeting_id": None,
                "status": "active", "start_time": None, "end_time": None,
                "bot_container_id": None, "constructed_meeting_url": None,
                "data": {}, "created_at": "", "updated_at": "", "segments": {},
            })
        return m

    @asynccontextmanager
    async def transcript_write_lease(self, meeting_id, *, scopes=("transcript",)):
        from ..meeting_writes import content_scopes_are_writable, transcript_is_finalized
        from .ports import TranscriptWriteRefused

        async with self._meeting_write_lock(meeting_id):
            meeting = self._meetings.get(meeting_id)
            data = meeting.get("data") if meeting else None
            if meeting and (
                not content_scopes_are_writable(data, *scopes)
                or ("transcript" in scopes and transcript_is_finalized(data))
            ):
                raise TranscriptWriteRefused("meeting is not writable")
            yield _InMemoryTranscriptBatchWriter(self, meeting_id)

    async def _append_segments_unchecked(self, meeting_id, segments) -> None:
        if self._redis is not None:
            # Prod-topology mode: live segments land in the redis HASH (+ the db-writer's
            # active_meetings sweep set), exactly like SqlAlchemyTranscriptStore.append_segment;
            # only the db-writer tick moves them into the durable dict.
            from .carriers import hset_segments_if_carrier_writable
            from .ports import TranscriptWriteRefused

            accepted = await hset_segments_if_carrier_writable(
                self._redis,
                meeting_id,
                entries={
                    segment["segment_id"]: json.dumps(segment)
                    for segment in segments
                },
                ttl=3600,
            )
            if not accepted:
                raise TranscriptWriteRefused("meeting carrier is no longer writable")
            return
        stored = self._row_or_placeholder(meeting_id)["segments"]
        for segment in segments:
            stored[segment["segment_id"]] = segment

    async def append_segment(self, meeting_id, segment) -> None:
        async with self.transcript_write_lease(meeting_id) as writer:
            await writer.append_segments([segment])

    async def upsert_segments(self, meeting_id, segments) -> None:
        """The db-writer's durable sink (the dict stands in for the ``transcriptions`` table):
        upsert by ``segment_id`` — idempotent, a re-flush updates in place."""
        from ..meeting_writes import content_scopes_are_writable, transcript_is_finalized
        from .ports import TranscriptWriteRefused

        async with self._meeting_write_lock(meeting_id):
            m = self._row_or_placeholder(meeting_id)
            if (
                not content_scopes_are_writable(m.get("data"), "transcript")
                or transcript_is_finalized(m.get("data"))
            ):
                raise TranscriptWriteRefused("meeting is not writable")
            self._upsert_segments_unchecked(m, segments)

    @staticmethod
    def _upsert_segments_unchecked(meeting: dict, segments: list[dict]) -> None:
        for segment in segments:
            segment_id = segment.get("segment_id")
            if segment_id:
                meeting["segments"][segment_id] = dict(segment)

    async def finalize_transcript(self, redis_client, meeting_id: int):
        """Atomically drain the terminal hash and seal eligible Minutes transcript writes."""

        from ..meeting_writes import (
            MAX_FINAL_TRANSCRIPT_SEGMENTS,
            TranscriptFinalizationOutcome,
            build_transcript_finalization_marker,
            content_scopes_are_writable,
            minutes_transcript_is_finalizable,
            validated_transcript_finalization_marker,
        )
        from .carriers import ACTIVE_MEETINGS_KEY, segments_hash_key
        from .db_writer import iter_terminal_segment_batches

        async with self._meeting_write_lock(meeting_id):
            meeting = self._meetings.get(meeting_id)
            if meeting is None:
                await redis_client.delete(segments_hash_key(meeting_id))
                await redis_client.srem(ACTIVE_MEETINGS_KEY, str(meeting_id))
                return TranscriptFinalizationOutcome(state="cancelled")
            data = meeting.get("data") if isinstance(meeting.get("data"), dict) else {}
            existing = validated_transcript_finalization_marker(data)
            if existing is not None:
                await redis_client.delete(segments_hash_key(meeting_id))
                await redis_client.srem(ACTIVE_MEETINGS_KEY, str(meeting_id))
                if minutes_transcript_is_finalizable(data):
                    return TranscriptFinalizationOutcome(
                        state="finalized", marker=dict(existing)
                    )
                return TranscriptFinalizationOutcome(state="cancelled")
            if "zaki_transcript_finalization" in data:
                raise ValueError("transcript finalization marker is invalid")

            capture_tagged = "zaki_capture" in data
            authorized = minutes_transcript_is_finalizable(data)
            if (not capture_tagged and content_scopes_are_writable(data, "transcript")) or authorized:
                terminal: list[dict] = []
                async for batch in iter_terminal_segment_batches(redis_client, meeting_id):
                    terminal.extend(batch)
                self._upsert_segments_unchecked(meeting, terminal)
            if authorized:
                if len(meeting["segments"]) > MAX_FINAL_TRANSCRIPT_SEGMENTS:
                    raise ValueError("durable transcript exceeds safe bounds")
                marker = build_transcript_finalization_marker(
                    list(meeting["segments"].values())
                )
                meeting["data"] = {**data, "zaki_transcript_finalization": marker}
                outcome = TranscriptFinalizationOutcome(
                    state="finalized", marker=marker
                )
            else:
                outcome = TranscriptFinalizationOutcome(state="cancelled")
            await redis_client.delete(segments_hash_key(meeting_id))
            await redis_client.srem(ACTIVE_MEETINGS_KEY, str(meeting_id))
            return outcome

    async def processed_view_cursor(self, meeting_id, view_id) -> Optional[str]:
        from .adapters import _find_processed_view

        m = self._meetings.get(meeting_id)
        if not m:
            return None
        view = _find_processed_view(m["data"], view_id)
        return view.get("source_cursor") if view else None

    async def merge_processed_view(
        self, meeting_id, *, view_id, kind, notes, source_cursor, params=None,
    ) -> None:
        """Persist drained copilot notes into ``data['processed']['views']`` — the SAME pure
        upsert the SqlAlchemy store commits (the versioned multi-view shape, merged by note id)."""
        from ..meeting_writes import content_scopes_are_writable
        from .adapters import _upsert_processed_view
        from .ports import TranscriptWriteRefused

        m = self._row_or_placeholder(meeting_id)
        if not content_scopes_are_writable(m.get("data"), "transcript", "summary"):
            raise TranscriptWriteRefused("meeting is not writable")
        m["data"] = _upsert_processed_view(
            m["data"], view_id=view_id, kind=kind, notes=notes,
            source_cursor=source_cursor, params=params,
        )


class FakeRedisBus:
    """A ``RedisBus`` over fakeredis. Wraps a fakeredis async client for stream read/ack/publish,
    plus ``xadd`` (test-only) to enqueue stream messages and a ``published`` log of ``:mutable``
    payloads for assertions."""

    def __init__(self, client):
        self._client = client
        self.published: list[tuple[str, str]] = []  # (channel, raw_json)

    async def xadd(self, stream: str, payload: dict) -> str:
        """Enqueue one stream message (the bot's XADD). ``payload`` is the inner JSON; the stream
        field is ``payload`` (the parent's stream field name)."""
        return await self._client.xadd(stream, {"payload": json.dumps(payload)})

    async def xadd_many(self, stream: str, payloads: list[dict]) -> list:
        if not payloads:
            return []
        async with self._client.pipeline(transaction=True) as pipe:
            for payload in payloads:
                pipe.xadd(stream, {"payload": json.dumps(payload)})
            return await pipe.execute()

    async def read_segments(self, *, group, consumer, stream, count=10):
        try:
            await self._client.xgroup_create(name=stream, groupname=group, id="0", mkstream=True)
        except Exception:
            pass
        resp = await self._client.xreadgroup(
            groupname=group, consumername=consumer, streams={stream: ">"}, count=count
        )
        out: list[tuple[str, dict]] = []
        for _stream_name, messages in resp or []:
            for message_id, fields in messages:
                mid = message_id.decode() if isinstance(message_id, bytes) else message_id
                decoded = {
                    (k.decode() if isinstance(k, bytes) else k):
                    (v.decode() if isinstance(v, bytes) else v)
                    for k, v in fields.items()
                }
                out.append((mid, decoded))
        return out

    async def ack(self, *, group, stream, message_ids):
        if message_ids:
            await self._client.xack(stream, group, *message_ids)

    async def publish(self, channel, data):
        if channel.startswith("tc:meeting:") and channel.endswith(":mutable"):
            from .carriers import publish_if_carrier_writable, transcript_stream_key
            from .ports import TranscriptWriteRefused

            raw_id = channel.removeprefix("tc:meeting:").removesuffix(":mutable")
            try:
                meeting_id = int(raw_id)
                if channel != f"{transcript_stream_key(meeting_id)}:mutable":
                    raise ValueError
            except (TypeError, ValueError):
                raise TranscriptWriteRefused(
                    "meeting carrier identity is invalid"
                ) from None
            accepted = await publish_if_carrier_writable(
                self._client,
                meeting_id,
                scope="raw",
                channel=channel,
                message=data,
            )
            if not accepted:
                raise TranscriptWriteRefused(
                    "meeting carrier is no longer writable"
                )
            self.published.append((channel, data))
            return accepted
        self.published.append((channel, data))
        return await self._client.publish(channel, data)
