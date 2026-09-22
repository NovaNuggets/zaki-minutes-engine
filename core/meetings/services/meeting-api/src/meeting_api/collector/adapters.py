"""Production adapters — the real implementations of the ``ports.py`` Protocols.

These are the wiring used when the collector runs for real: a SQLAlchemy-async session bound to
the ``meetings`` / ``transcriptions`` tables for the ``TranscriptStore``, and a ``redis.asyncio``
client for the segment-ingestion ``RedisBus`` (XREADGROUP the ``transcription_segments`` stream,
PUBLISH ``tc:meeting:{id}:mutable``).

They are deliberately thin — the carved behavior lives in ``app.py`` / ``ingest.py``; these only
translate the port calls to the concrete clients, exactly as the deployed
``services/meeting-api/meeting_api/collector/`` does (``endpoints.py`` SELECTs; ``consumer.py``
XREADGROUP/XACK; ``processors.py`` HSET/PUBLISH). They carry NO test logic.

Importing the heavy symbols is LAZY (inside ``build_production_app`` / the methods) so the
package can be imported (and unit-tested with the in-memory fakes) without SQLAlchemy-async or
redis installed in the test venv — which is why ``pyproject.toml`` needs NO ``greenlet`` pin
(SQLAlchemy-async is never imported during the gates).
"""
from __future__ import annotations

from contextlib import asynccontextmanager
import hashlib
import json
import math
import os
import secrets
from datetime import datetime, timezone
from typing import Optional

from ..meeting_writes import (
    MAX_FINAL_TRANSCRIPT_CONTENT_BYTES,
    MAX_FINAL_TRANSCRIPT_SEGMENTS,
    content_scopes_are_writable,
    legacy_collector_read_projection,
    meeting_write_lock_key,
    transcript_is_finalized,
)
from .carriers import (
    hset_segments_if_carrier_writable,
    publish_if_carrier_writable,
    transcript_stream_key,
    xadd_if_carrier_writable,
    xadd_many_if_carrier_writable,
)
from .ports import RedisBus, TranscriptStore, TranscriptWriteRefused


_MAX_MANAGED_REDIS_SCAN_STEPS = 128


def redis_client_options(env=None) -> dict:
    """Finite Redis connect/I/O deadlines shared by every meeting-api composition root."""

    source = os.environ if env is None else env

    def positive(name: str, default: str) -> float:
        try:
            value = float(source.get(name, default))
        except (TypeError, ValueError):
            raise ValueError(f"{name} must be a positive finite number") from None
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be a positive finite number")
        return value

    return {
        "socket_connect_timeout": positive("REDIS_CONNECT_TIMEOUT_S", "5"),
        "socket_timeout": positive("REDIS_IO_TIMEOUT_S", "5"),
        "retry_on_timeout": False,
    }


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: Optional[datetime]) -> Optional[str]:
    """Serialize database timestamps as explicit UTC instants.

    PostgreSQL columns in the Vexa schema are ``timestamp without time zone`` and are written in
    UTC.  Treating those values as local/unspecified time would make the cross-spoke producer emit
    timestamps that the strict read contract correctly rejects.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat()


def _naive_utc(value: Optional[datetime]) -> Optional[datetime]:
    """Convert an API UTC instant to the database's naive-UTC representation."""
    if value is None or value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _expired(iso: Optional[str]) -> bool:
    """True if the ISO-8601 timestamp is in the past (None = never expires)."""
    if not iso:
        return False
    try:
        exp = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        return exp < _now()
    except ValueError:
        return False


def validate_transcript_grant(grant: dict, user_email: Optional[str]) -> Optional[str]:
    """Shared (fake + real) validation of a transcript share grant → an error code, or None if OK.
    open = anyone authenticated; restricted = the caller's verified email ∈ allowed_emails."""
    if grant.get("revoked"):
        return "revoked"
    if _expired(grant.get("expires_at")):
        return "expired"
    if grant.get("mode") == "restricted":
        allowed = {e.lower() for e in grant.get("allowed_emails", [])}
        if not user_email or user_email.lower() not in allowed:
            return "not_allowed"
    return None


class _RedisTranscriptBatchWriter:
    """Persist one decoded transcript message with one Redis transaction."""

    def __init__(self, redis_client, meeting_id: int):
        self._redis = redis_client
        self._meeting_id = meeting_id

    async def append_segments(self, segments: list[dict]) -> None:
        if self._redis is None or not segments:
            return
        ttl = int(os.environ.get("REDIS_SEGMENT_TTL", "3600"))
        accepted = await hset_segments_if_carrier_writable(
            self._redis,
            self._meeting_id,
            entries={
                segment["segment_id"]: json.dumps(segment)
                for segment in segments
            },
            ttl=ttl,
        )
        if not accepted:
            raise TranscriptWriteRefused("meeting carrier is no longer writable")


async def _rollback_or_invalidate_advisory_transaction(db) -> None:
    """Release transaction locks, invalidating a connection whose rollback is uncertain."""

    try:
        await db.rollback()
    except BaseException:
        invalidate = getattr(db, "invalidate", None)
        if callable(invalidate):
            try:
                await invalidate()
            except BaseException:
                pass


def _doc_ref(doc: dict) -> dict:
    """Normalize a connect-doc body to a stored ``data.docs[]`` ref: ``workspace`` + ``path`` are
    required; ``title`` / ``kind`` ride along when present. Doc bodies live in the agent workspace —
    only this ref is persisted."""
    ref = {"workspace": doc.get("workspace"), "path": doc["path"]}
    for k in ("title", "kind"):
        if doc.get(k) is not None:
            ref[k] = doc[k]
    return ref


def _upsert_doc(docs: list[dict], doc: dict) -> list[dict]:
    """Append the doc ref deduped by ``path`` — re-connecting the same path updates in place
    (idempotent, order-preserving)."""
    ref = _doc_ref(doc)
    out = [d for d in docs if d.get("path") != ref["path"]]
    out.append(ref)
    return out


def _remove_doc(docs: list[dict], path: str) -> list[dict]:
    """Drop the doc ref with ``path`` (idempotent when absent)."""
    return [d for d in docs if d.get("path") != path]


def _merge_notes_by_id(existing: list[dict], incoming: list[dict]) -> list[dict]:
    """Merge drained copilot notes into a processed view's ``doc['notes']`` list, keyed by note
    ``id`` (== segment_id): a refining re-emit UPDATES its note in place (order preserved);
    a new id appends. Notes without an id append as-is (nothing to key an upsert on)."""
    out = [dict(n) for n in existing]
    index = {str(n.get("id")): i for i, n in enumerate(out) if n.get("id") is not None}
    for note in incoming:
        nid = note.get("id")
        if nid is not None and str(nid) in index:
            out[index[str(nid)]].update(note)
        else:
            if nid is not None:
                index[str(nid)] = len(out)
            out.append(dict(note))
    return out


def _find_processed_view(data: dict, view_id: str) -> Optional[dict]:
    """The view with ``view_id`` inside ``data['processed']['views']`` (None when absent)."""
    processed = data.get("processed") if isinstance(data.get("processed"), dict) else {}
    views = processed.get("views") if isinstance(processed.get("views"), list) else []
    return next((v for v in views if isinstance(v, dict) and v.get("id") == view_id), None)


def _upsert_processed_view(
    data: dict, *, view_id: str, kind: str, notes: list[dict],
    source_cursor: Optional[str], params: Optional[dict],
) -> dict:
    """Pure merge of drained copilot notes into the ADDRESSABLE, VERSIONED processed shape
    (release DoD — multi-consumer, meeting-scoped today, mountable by N consumers later):

        data.processed = {"views": [{id, kind, params, doc, source_cursor, updated_at}]}

    Upserts the view keyed by ``id`` — other views (future per-workspace/other processings) are
    preserved untouched; merges ``notes`` into the view's ``doc['notes']`` by note id; stamps
    ``params`` (the processing metadata APPLIED — provider/model/pipeline, stamped by the
    producing worker — reproducibility) only when the drain carried them, so an idle drain never
    erases provenance; ``source_cursor`` records the stream position the view reflects.
    Returns the new ``data`` dict (the caller persists it)."""
    from datetime import datetime, timezone

    out = dict(data)
    processed = dict(out.get("processed")) if isinstance(out.get("processed"), dict) else {}
    views = [dict(v) for v in processed.get("views", []) if isinstance(v, dict)] \
        if isinstance(processed.get("views"), list) else []
    view = next((v for v in views if v.get("id") == view_id), None)
    if view is None:
        view = {"id": view_id, "kind": kind, "params": {}, "doc": {"notes": []}}
        views.append(view)
    doc = dict(view.get("doc")) if isinstance(view.get("doc"), dict) else {}
    existing_notes = doc.get("notes") if isinstance(doc.get("notes"), list) else []
    doc["notes"] = _merge_notes_by_id(list(existing_notes), notes)
    view["doc"] = doc
    view["kind"] = kind
    if params:
        view["params"] = params
    if source_cursor:
        view["source_cursor"] = source_cursor
    view["updated_at"] = datetime.now(timezone.utc).isoformat()
    processed["views"] = views
    out["processed"] = processed
    return out


def _segment_to_api(seg: dict) -> dict:
    """Map a stored/Redis segment to an api.v1 ``TranscriptionSegment`` (start/end/text/language
    required; the optional fields ride along)."""
    out = {
        "start": seg.get("start", seg.get("start_time", 0.0)),
        "end": seg.get("end", seg.get("end_time", 0.0)),
        "text": seg.get("text", ""),
        "language": seg.get("language"),
    }
    for k in ("speaker", "completed", "segment_id", "source", "absolute_start_time", "absolute_end_time", "created_at"):
        if seg.get(k) is not None:
            out[k] = seg[k]
    return out


class SqlAlchemyTranscriptStore:
    """``TranscriptStore`` over a SQLAlchemy-async ``session_factory`` (the ``meetings`` /
    ``transcriptions`` tables; recordings/notes live in ``meeting.data`` JSONB — NO separate
    table). Carve of ``collector/endpoints.py`` SELECT/merge logic."""

    def __init__(self, session_factory, redis_client=None, *, statement_factory=None):
        self._session_factory = session_factory
        self._statement_factory = statement_factory
        # The live Redis hash of in-flight segments (``meeting:{id}:segments``) is merged on read
        # in prod; the merge helper is kept here when a client is provided.
        self._redis = redis_client
        # numeric meeting_id → (native_meeting_id, platform). The id→native map is immutable for a
        # meeting row, so cache it forever once resolved (bounded by the live meeting set).
        self._native_cache: dict[int, tuple[str, str]] = {}

    def _statement(self, sql: str):
        if self._statement_factory is not None:
            return self._statement_factory(sql)
        from sqlalchemy import text

        return text(sql)

    async def _acquire_legacy_read_barrier(self, db, meeting_id: int) -> None:
        """Serialize a content read with withdrawal, TTL purge, and GDPR erasure."""

        await db.execute(
            self._statement(
                "SELECT pg_advisory_xact_lock_shared(:meeting_lock_key)"
            ),
            {"meeting_lock_key": meeting_write_lock_key(meeting_id)},
        )

    async def native_for(self, meeting_id) -> "Optional[tuple[str, str]]":
        """Resolve a NUMERIC meeting_id → (native_meeting_id, platform) from the meetings table.

        Cross-user (the collector is the trusted internal segment consumer — it owns the mapping and
        is NOT user-scoped): the agent-api live-transcript relay re-keys numeric→native off this, so a
        meeting's segments reach the terminal's native channel regardless of which user owns it. Cached
        because the pair is immutable per row. Returns None if the id is unknown (caller keeps numeric)."""
        try:
            mid = int(meeting_id)
        except (TypeError, ValueError):
            return None
        if mid in self._native_cache:
            return self._native_cache[mid]
        from sqlalchemy import select  # lazy: not needed for the in-memory fakes

        from .models import Meeting

        async with self._session_factory() as db:
            m = (await db.execute(select(Meeting).where(Meeting.id == mid))).scalars().first()
            if not m or not m.platform_specific_id:
                return None
            pair = (m.platform_specific_id, m.platform or "google_meet")
            self._native_cache[mid] = pair
            return pair

    async def owner_for(self, meeting_id) -> "Optional[int]":
        """Resolve one numeric meeting row to its immutable database owner."""
        try:
            mid = int(meeting_id)
        except (TypeError, ValueError):
            return None
        if mid <= 0:
            return None

        async with self._session_factory() as db:
            result = await db.execute(
                self._statement("SELECT user_id FROM meetings WHERE id = :meeting_id"),
                {"meeting_id": mid},
            )
            owner = result.scalar()
        return owner if isinstance(owner, int) and not isinstance(owner, bool) and owner > 0 else None

    async def connect_meeting_doc_by_id(self, meeting_id) -> "Optional[dict]":
        """Connect the canonical generated doc to one locked row, deriving its owner in DB."""
        try:
            mid = int(meeting_id)
        except (TypeError, ValueError):
            return None
        if mid <= 0:
            return None

        from ..meeting_writes import content_scopes_are_writable

        async with self._session_factory() as db:
            result = await db.execute(
                self._statement(
                    "SELECT user_id, data FROM meetings "
                    "WHERE id = :meeting_id FOR UPDATE"
                ),
                {"meeting_id": mid},
            )
            row = result.mappings().first()
            if not row:
                return None
            owner = row.get("user_id")
            data = row.get("data") if isinstance(row.get("data"), dict) else {}
            if (
                not isinstance(owner, int)
                or isinstance(owner, bool)
                or owner <= 0
                or not content_scopes_are_writable(data, "summary")
            ):
                return None
            doc = {
                "workspace": str(owner),
                "path": f"kg/entities/meeting/{mid}.md",
                "title": f"Meeting {mid}",
                "kind": "meeting",
            }
            updated = dict(data)
            updated["docs"] = _upsert_doc(list(updated.get("docs", [])), doc)
            await db.execute(
                self._statement(
                    "UPDATE meetings SET data = CAST(:data AS jsonb) WHERE id = :meeting_id"
                ),
                {"meeting_id": mid, "data": json.dumps(updated, sort_keys=True)},
            )
            await db.commit()
            return doc

    async def _transcript_doc(self, db, meeting, *, data=None) -> Optional[dict]:
        """Build the api.v1 ``TranscriptionResponse`` dict for a resolved ``meeting`` ROW — the shared
        body used by BOTH ``get_transcript`` (native → newest row) and ``get_transcript_by_id`` (exact
        row). Reads the row's persisted ``transcriptions`` + merges the live redis in-flight hash, all
        keyed by ``meeting.id`` (the row id) — so a by-id read returns EXACTLY that row's segments/notes,
        never a sibling row's (the wrong-row hydration fix)."""
        from sqlalchemy import select

        from .models import Transcription

        raw_data = meeting.data if isinstance(meeting.data, dict) else {}
        managed = "zaki_capture" in raw_data or "zaki_retention" in raw_data
        data = data if isinstance(data, dict) else (
            legacy_collector_read_projection(raw_data) if managed else raw_data
        )
        if data is None:
            return None

        transcript_stmt = select(Transcription).where(
            Transcription.meeting_id == meeting.id
        )
        if managed:
            census = (await db.execute(
                self._statement(
                    """
                    SELECT COUNT(*) AS segment_count,
                           COALESCE(SUM(
                               OCTET_LENGTH(COALESCE(CAST(segment_id AS text), ''))
                               + OCTET_LENGTH(COALESCE(text, ''))
                               + OCTET_LENGTH(COALESCE(speaker, ''))
                               + OCTET_LENGTH(COALESCE(language, ''))
                               + 96
                           ), 0) AS content_bytes
                    FROM transcriptions
                    WHERE meeting_id = :meeting_id
                    """
                ),
                {"meeting_id": int(meeting.id)},
            )).mappings().first()
            try:
                durable_count = int(census["segment_count"])
                durable_bytes = int(census["content_bytes"])
            except (KeyError, TypeError, ValueError, OverflowError):
                return None
            if (
                durable_count < 0
                or durable_count > MAX_FINAL_TRANSCRIPT_SEGMENTS
                or durable_bytes < 0
                or durable_bytes > MAX_FINAL_TRANSCRIPT_CONTENT_BYTES
            ):
                return None
            transcript_stmt = transcript_stmt.limit(MAX_FINAL_TRANSCRIPT_SEGMENTS + 1)

        seg_rows = (await db.execute(transcript_stmt)).scalars().all()
        if managed and len(seg_rows) > MAX_FINAL_TRANSCRIPT_SEGMENTS:
            return None
        # Postgres-persisted segments (the background db-writer flush path).
        seg_by_id: dict = {}
        order: list = []
        encoded_sizes: dict[object, int] = {}
        content_bytes = 0

        def merge_segment(segment: dict, sid: object) -> bool:
            nonlocal content_bytes
            if not managed:
                if sid not in seg_by_id:
                    order.append(sid)
                seg_by_id[sid] = segment
                return True
            try:
                encoded_size = len(json.dumps(
                    segment,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8"))
            except (TypeError, ValueError, OverflowError):
                return False
            prior_size = encoded_sizes.get(sid, 0)
            next_size = content_bytes - prior_size + encoded_size
            if (
                next_size > MAX_FINAL_TRANSCRIPT_CONTENT_BYTES
                or (sid not in seg_by_id and len(order) >= MAX_FINAL_TRANSCRIPT_SEGMENTS)
            ):
                return False
            if sid not in seg_by_id:
                order.append(sid)
            seg_by_id[sid] = segment
            encoded_sizes[sid] = encoded_size
            content_bytes = next_size
            return True

        for r in seg_rows:
            s = _segment_to_api({
                "start": r.start_time, "end": r.end_time, "text": r.text,
                "language": r.language, "speaker": r.speaker,
                "segment_id": r.segment_id, "completed": True,
            })
            sid = s.get("segment_id") or f"pg-{len(order)}"
            if not merge_segment(s, sid):
                return None
        # Merge the LIVE Redis hash of in-flight segments (``meeting:{id}:segments``) — the source
        # of truth before/until the db-writer flush. The carve had dropped this merge, so a transcript
        # whose segments are still only in Redis (every short/just-finished meeting) read as EMPTY.
        if self._redis is not None:
            if managed:
                hscan = getattr(self._redis, "hscan", None)
                hlen = getattr(self._redis, "hlen", None)
                if not callable(hscan) or not callable(hlen):
                    return None
                try:
                    carrier_count = int(await hlen(f"meeting:{meeting.id}:segments"))
                except Exception:
                    carrier_count = -1
                if carrier_count > MAX_FINAL_TRANSCRIPT_SEGMENTS:
                    return None
                if carrier_count < 0:
                    carrier_count = 0
                cursor = 0
                seen_cursors: set[int] = set()
                carrier_entries = 0
                carrier_bytes = 0
                scan_steps = 0
                durable_snapshot = (
                    dict(seg_by_id),
                    list(order),
                    dict(encoded_sizes),
                    content_bytes,
                )
                redis_available = True
                if carrier_count:
                    while True:
                        scan_steps += 1
                        if scan_steps > _MAX_MANAGED_REDIS_SCAN_STEPS:
                            return None
                        try:
                            next_cursor, raw = await hscan(
                                f"meeting:{meeting.id}:segments",
                                cursor=cursor,
                                count=100,
                            )
                        except Exception:
                            redis_available = False
                            break
                        if not isinstance(raw, dict):
                            return None
                        carrier_entries += len(raw)
                        if carrier_entries > MAX_FINAL_TRANSCRIPT_SEGMENTS:
                            return None
                        for value in raw.values():
                            value_bytes = (
                                bytes(value)
                                if isinstance(value, (bytes, bytearray))
                                else str(value).encode("utf-8")
                            )
                            carrier_bytes += len(value_bytes)
                            if carrier_bytes > MAX_FINAL_TRANSCRIPT_CONTENT_BYTES:
                                return None
                            try:
                                segment = json.loads(value_bytes)
                            except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
                                continue
                            s = _segment_to_api(segment)
                            sid = s.get("segment_id") or f"rh-{len(order)}"
                            if not merge_segment(s, sid):
                                return None
                        try:
                            cursor = int(next_cursor)
                        except (TypeError, ValueError, OverflowError):
                            return None
                        if cursor == 0:
                            break
                        if cursor in seen_cursors:
                            return None
                        seen_cursors.add(cursor)
                if not redis_available:
                    # Redis is a best-effort live tail. A transport failure may omit drafts, but
                    # never relaxes the durable census or request-time privacy decision.
                    seg_by_id, order, encoded_sizes, content_bytes = durable_snapshot
            else:
                try:
                    raw = await self._redis.hgetall(f"meeting:{meeting.id}:segments")
                    for v in (raw.values() if isinstance(raw, dict) else []):
                        try:
                            seg = json.loads(v.decode() if isinstance(v, (bytes, bytearray)) else v)
                        except Exception:
                            continue
                        s = _segment_to_api(seg)
                        sid = s.get("segment_id") or f"rh-{len(order)}"
                        merge_segment(s, sid)
                except Exception:
                    pass
        try:
            segments = sorted(
                (seg_by_id[k] for k in order),
                key=(
                    (lambda s: float(s.get("start") or 0.0))
                    if managed
                    else (lambda s: (s.get("start") or 0.0))
                ),
            )
        except (TypeError, ValueError, OverflowError):
            if managed:
                return None
            raise
        # The dashboard's renderer SKIPS any segment without absolute_start_time
        # (use-vexa-websocket.ts: `if (!seg.absolute_start_time) continue`). Derive it from the
        # meeting start + the relative offset when a producer didn't supply it, so the historical
        # transcript renders (the carve served only relative start/end → the UI dropped every segment).
        from datetime import timedelta
        base = meeting.start_time or meeting.created_at
        if base is not None:
            for s in segments:
                if not s.get("absolute_start_time") and s.get("start") is not None:
                    try:
                        s["absolute_start_time"] = (base + timedelta(seconds=float(s["start"]))).isoformat()
                        s["absolute_end_time"] = (base + timedelta(seconds=float(s.get("end") or s["start"]))).isoformat()
                    except Exception:
                        pass
        if managed:
            data = legacy_collector_read_projection(raw_data)
            if data is None:
                return None
        return {
            "id": meeting.id,
            "platform": meeting.platform,
            "native_meeting_id": meeting.platform_specific_id,
            "constructed_meeting_url": (data.get("constructed_meeting_url")),
            "status": meeting.status,
            "start_time": meeting.start_time.isoformat() if meeting.start_time else None,
            "end_time": meeting.end_time.isoformat() if meeting.end_time else None,
            "recordings": data.get("recordings", []),
            "notes": data.get("notes"),
            "data": data,
            "segments": segments,
        }

    async def get_transcript(self, user_id, platform, native_meeting_id) -> Optional[dict]:
        from sqlalchemy import select  # lazy: SQLAlchemy not needed for the in-memory fakes

        from .models import Meeting  # local re-export of the admin-api models

        async with self._session_factory() as db:
            try:
                selector = (
                    select(Meeting.id)
                    .where(
                        Meeting.user_id == user_id,
                        Meeting.platform == platform,
                        Meeting.platform_specific_id == native_meeting_id,
                    )
                    .order_by(Meeting.created_at.desc())
                    .limit(1)
                )
                meeting_id = (await db.execute(selector)).scalar()
                if meeting_id is None:
                    return None
                await self._acquire_legacy_read_barrier(db, int(meeting_id))
                stmt = select(Meeting).where(
                    Meeting.id == meeting_id,
                    Meeting.user_id == user_id,
                    Meeting.platform == platform,
                    Meeting.platform_specific_id == native_meeting_id,
                ).execution_options(populate_existing=True)
                meeting = (await db.execute(stmt)).scalars().first()
                projected = (
                    legacy_collector_read_projection(meeting.data)
                    if meeting is not None
                    else None
                )
                document = (
                    await self._transcript_doc(db, meeting, data=projected)
                    if meeting is not None and projected is not None
                    else None
                )
                await db.commit()
                return document
            except BaseException:
                await _rollback_or_invalidate_advisory_transaction(db)
                raise

    async def get_transcript_by_id(self, user_id, meeting_id, member_workspaces=None) -> Optional[dict]:
        """Exact-row transcript for ``meeting.id == meeting_id``, authorized by the SAME three-way rule as
        authorize_subscribe: (a) owner, (b) member of the bound workspace, (c) redeemed a transcript-share
        link (``data.transcript_viewers``). Any other caller → ``None`` (→ 404), so it can never leak an
        unrelated tenant's transcript (P0) while letting a shared recipient load the durable feed."""
        from sqlalchemy import select

        from .models import Meeting

        try:
            mid = int(meeting_id)
        except (TypeError, ValueError):
            return None
        async with self._session_factory() as db:
            try:
                await self._acquire_legacy_read_barrier(db, mid)
                meeting = (await db.execute(
                    select(Meeting)
                    .where(Meeting.id == mid)
                    .execution_options(populate_existing=True)
                )).scalars().first()
                if not meeting:
                    await db.commit()
                    return None
                data = meeting.data if isinstance(meeting.data, dict) else {}
                authorized = (
                    meeting.user_id == user_id                                      # (a) owner
                    or user_id in (data.get("transcript_viewers") or [])            # (c) transcript-share
                    or (bool(member_workspaces) and data.get("workspace_id") in member_workspaces)  # (b) bound ws member
                )
                projected = legacy_collector_read_projection(data)
                document = (
                    await self._transcript_doc(db, meeting, data=projected)
                    if authorized and projected is not None
                    else None
                )
                await db.commit()
                return document
            except BaseException:
                await _rollback_or_invalidate_advisory_transaction(db)
                raise

    async def list_meetings(self, user_id, *, status=None, platform=None, limit=None, offset=None, member_workspaces=None):
        from sqlalchemy import cast, func, or_, select
        from sqlalchemy.dialects.postgresql import JSONB

        from .models import Meeting

        async with self._session_factory() as db:
            # ACCESS = owner OR transcript-share viewer OR member of the bound workspace. Shared meetings
            # (owned by someone else) surface in the caller's list so a share recipient can find + open them.
            access = [
                Meeting.user_id == user_id,
                cast(Meeting.data["transcript_viewers"], JSONB).op("@>")(func.to_jsonb(user_id)),
            ]
            if member_workspaces:
                access.append(Meeting.data["workspace_id"].astext.in_(list(member_workspaces)))
            stmt = select(Meeting).where(or_(*access))
            if status:
                stmt = stmt.where(Meeting.status == status)
            if platform:
                stmt = stmt.where(Meeting.platform == platform)
            stmt = stmt.order_by(Meeting.created_at.desc())
            if limit:
                stmt = stmt.limit(limit)
            if offset:
                stmt = stmt.offset(offset)
            try:
                candidates = (await db.execute(stmt)).scalars().all()
                candidate_ids = sorted({int(meeting.id) for meeting in candidates})
                for meeting_id in candidate_ids:
                    await self._acquire_legacy_read_barrier(db, meeting_id)
                if candidate_ids:
                    current_stmt = (
                        select(Meeting)
                        .where(Meeting.id.in_(candidate_ids), or_(*access))
                        .order_by(Meeting.created_at.desc())
                        .execution_options(populate_existing=True)
                    )
                    if status:
                        current_stmt = current_stmt.where(Meeting.status == status)
                    if platform:
                        current_stmt = current_stmt.where(Meeting.platform == platform)
                    rows = (await db.execute(current_stmt)).scalars().all()
                else:
                    rows = []
                projected_rows = []
                for meeting in rows:
                    projected = legacy_collector_read_projection(meeting.data)
                    if projected is not None:
                        projected_rows.append((meeting, projected))
                response = [
                    {
                        "id": m.id,
                        "user_id": m.user_id,
                        "platform": m.platform,
                        "native_meeting_id": m.platform_specific_id,
                        "constructed_meeting_url": projected.get("constructed_meeting_url"),
                        "status": m.status,
                        "bot_container_id": m.bot_container_id,
                        "start_time": m.start_time.isoformat() if m.start_time else None,
                        "end_time": m.end_time.isoformat() if m.end_time else None,
                        "data": projected,
                        "shared": m.user_id != user_id,   # surfaced via a share/membership, not owned by the caller
                        "created_at": m.created_at.isoformat() if m.created_at else None,
                        "updated_at": m.updated_at.isoformat() if m.updated_at else None,
                    }
                    for m, projected in projected_rows
                ]
                await db.commit()
                return response
            except BaseException:
                await _rollback_or_invalidate_advisory_transaction(db)
                raise

    @staticmethod
    def _zaki_read_row(meeting) -> dict:
        data = meeting.data if isinstance(meeting.data, dict) else {}
        return {
            "id": meeting.id,
            "user_id": meeting.user_id,
            "platform": meeting.platform,
            "status": meeting.status,
            "start_time": _utc_iso(meeting.start_time),
            "end_time": _utc_iso(meeting.end_time),
            "data": data,
            "created_at": _utc_iso(meeting.created_at),
            "updated_at": _utc_iso(meeting.updated_at),
        }

    async def _zaki_read_transcript_doc(self, db, meeting) -> dict:
        """Stream one immutable durable transcript within strict producer memory bounds.

        The zaki-read finalization marker is written only after Redis has been flushed, so this
        path intentionally never merges the live hash. SQL projects bounded text fields and uses a
        server-side stream; neither ``.all()`` nor Redis ``HGETALL`` can materialize an unbounded
        meeting before the 4,096-turn / 256-KiB checks run.
        """
        from ..meeting_writes import (
            MAX_FINAL_TRANSCRIPT_CONTENT_BYTES,
            MAX_FINAL_TRANSCRIPT_SEGMENTS,
            MAX_FINAL_TRANSCRIPT_TURN_CHARS,
            TranscriptRevisionBuilder,
            canonical_transcript_segment,
            validated_transcript_finalization_marker,
        )

        data = meeting.data if isinstance(meeting.data, dict) else {}
        expected = validated_transcript_finalization_marker(data)
        if expected is None:
            return {"_zaki_read_invalid": "revision_mismatch", "segments": []}
        stmt = self._statement(
            """
            SELECT LEFT(segment_id, 257) AS segment_id,
                   start_time AS start,
                   end_time AS end,
                   LEFT(text, 65537) AS text,
                   LEFT(speaker, 201) AS speaker,
                   LEFT(language, 36) AS language
            FROM transcriptions
            WHERE meeting_id = :meeting_id
            ORDER BY start_time ASC, segment_id ASC, id ASC
            """
        )
        result = await db.stream(
            stmt,
            {"meeting_id": meeting.id},
            execution_options={"yield_per": 8},
        )
        segments: list[dict] = []
        content_bytes = 0
        revision = TranscriptRevisionBuilder()
        async for row in result.mappings():
            segment = {
                "segment_id": row.get("segment_id"),
                "start": row.get("start"),
                "end": row.get("end"),
                "text": row.get("text"),
                "speaker": row.get("speaker"),
                "language": row.get("language"),
            }
            if (
                len(segments) >= MAX_FINAL_TRANSCRIPT_SEGMENTS
                or isinstance(segment["text"], str)
                and len(segment["text"]) > MAX_FINAL_TRANSCRIPT_TURN_CHARS
            ):
                return {"_zaki_read_invalid": "content_too_large", "segments": []}
            try:
                canonical = canonical_transcript_segment(segment)
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
            try:
                revision.add(canonical)
            except ValueError:
                return {"_zaki_read_invalid": "revision_mismatch", "segments": []}
            segments.append(_segment_to_api(segment))
        actual = revision.marker()
        if (
            actual["segment_count"] != expected["segment_count"]
            or actual["revision"] != expected["revision"]
        ):
            return {"_zaki_read_invalid": "revision_mismatch", "segments": []}
        return {
            "id": meeting.id,
            "platform": meeting.platform,
            "status": meeting.status,
            "start_time": _utc_iso(meeting.start_time),
            "end_time": _utc_iso(meeting.end_time),
            "data": data,
            "segments": segments,
        }

    async def get_zaki_read_snapshot(
        self, user_id, meeting_id, *, include_transcript=False, readable_at=None,
    ) -> Optional[dict]:
        """Read owner metadata and optional durable content under one erasure-safe transaction."""
        from sqlalchemy import select

        from .models import Meeting

        try:
            user_id = int(user_id)
            meeting_id = int(meeting_id)
            lock_key = meeting_write_lock_key(meeting_id)
        except (TypeError, ValueError):
            return None
        async with self._session_factory() as db:
            await db.execute(
                self._statement(
                    "SELECT pg_advisory_xact_lock_shared(:meeting_lock_key)"
                ),
                {"meeting_lock_key": lock_key},
            )
            meeting = (await db.execute(
                select(Meeting).where(
                    Meeting.id == meeting_id,
                    Meeting.user_id == user_id,
                ).with_for_update(read=True)
            )).scalars().first()
            if meeting is None:
                return None
            row = self._zaki_read_row(meeting)
            content_authorized = False
            if include_transcript and isinstance(readable_at, datetime):
                # Evaluate every content authority while the same erasure barrier and row lock are
                # held. An expired/withdrawn/unfinalized item never reaches the segment query.
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
                await self._zaki_read_transcript_doc(db, meeting)
                if content_authorized
                else None
            )
            return {"row": row, "document": document}

    async def list_zaki_read_meetings(
        self, user_id, *, snapshot, visible_at, before_occurred=None, before_id=None,
        inclusive=False, limit=100,
    ) -> list[dict]:
        """Owner-only keyset query used by the non-enumerating cross-spoke read plane."""
        from sqlalchemy import and_, func, or_, select, text

        from .models import Meeting

        async with self._session_factory() as db:
            snapshot_db = _naive_utc(snapshot)
            before_db = _naive_utc(before_occurred)
            occurred = func.coalesce(Meeting.start_time, Meeting.created_at)
            stmt = select(Meeting).where(
                Meeting.user_id == int(user_id),
                Meeting.updated_at <= snapshot_db,
                Meeting.status.in_(("completed", "failed")),
                text("""
                    meetings.data #>> '{zaki_capture,state}' = 'authorized'
                    AND meetings.data #>> '{zaki_capture,bot_name}' = 'ZAKI Notetaker'
                    AND meetings.data #> '{zaki_capture,tenant_attested}' = 'true'::jsonb
                    AND LENGTH(BTRIM(COALESCE(
                      meetings.data #>> '{zaki_capture,tenant_policy_version}', ''
                    ))) BETWEEN 1 AND 80
                    AND CASE
                      WHEN pg_input_is_valid(COALESCE(
                        meetings.data #>> '{zaki_capture,tenant_attested_at}', ''
                      ), 'timestamp with time zone')
                      THEN CAST(
                        meetings.data #>> '{zaki_capture,tenant_attested_at}' AS timestamptz
                      ) <= :visible_at
                      ELSE FALSE
                    END
                    AND meetings.data #>> '{zaki_transcript_finalization,state}' = 'finalized'
                    AND COALESCE(
                      meetings.data #>> '{zaki_transcript_finalization,revision}', ''
                    ) ~ '^sha256:[0-9a-f]{64}$'
                    AND COALESCE(
                      meetings.data #>> '{zaki_transcript_finalization,segment_count}', ''
                    ) ~ '^[0-9]+$'
                    AND CASE
                      WHEN pg_input_is_valid(COALESCE(
                        meetings.data #>> '{zaki_transcript_finalization,finalized_at}', ''
                      ), 'timestamp with time zone')
                      THEN CAST(
                        meetings.data #>> '{zaki_transcript_finalization,finalized_at}' AS timestamptz
                      ) <= :visible_at
                      ELSE FALSE
                    END
                    AND COALESCE(meetings.data #>> '{zaki_retention,state}', 'invalid') = 'open'
                    AND (
                      (
                        NOT (COALESCE(meetings.data #> '{zaki_retention,expired_scopes}', '[]'::jsonb) ? 'transcript')
                        AND CASE
                          WHEN NOT pg_input_is_valid(COALESCE(
                            meetings.data #>> '{zaki_retention,scope_expiries,transcript}', ''
                          ), 'timestamp with time zone') THEN FALSE
                          ELSE CAST(meetings.data #>> '{zaki_retention,scope_expiries,transcript}' AS timestamptz) > :visible_at
                        END
                      ) OR (
                        NOT (COALESCE(meetings.data #> '{zaki_retention,expired_scopes}', '[]'::jsonb) ? 'summary')
                        AND CASE
                          WHEN NOT pg_input_is_valid(COALESCE(
                            meetings.data #>> '{zaki_retention,scope_expiries,summary}', ''
                          ), 'timestamp with time zone') THEN FALSE
                          ELSE CAST(meetings.data #>> '{zaki_retention,scope_expiries,summary}' AS timestamptz) > :visible_at
                        END
                      )
                    )
                """).bindparams(visible_at=visible_at),
            )
            if before_db is not None and before_id is not None:
                older = or_(
                    occurred < before_db,
                    and_(occurred == before_db, Meeting.id < int(before_id)),
                )
                if inclusive:
                    older = or_(older, and_(
                        occurred == before_db,
                        Meeting.id == int(before_id),
                    ))
                stmt = stmt.where(older)
            stmt = stmt.order_by(occurred.desc(), Meeting.id.desc()).limit(
                max(1, min(int(limit), 100))
            )
            rows = (await db.execute(stmt)).scalars().all()
            return [self._zaki_read_row(meeting) for meeting in rows]

    async def get_zaki_read_meeting(self, user_id, meeting_id) -> Optional[dict]:
        from sqlalchemy import select

        from .models import Meeting

        try:
            user_id, meeting_id = int(user_id), int(meeting_id)
        except (TypeError, ValueError):
            return None
        async with self._session_factory() as db:
            meeting = (await db.execute(select(Meeting).where(
                Meeting.id == meeting_id,
                Meeting.user_id == user_id,
            ))).scalars().first()
            return self._zaki_read_row(meeting) if meeting is not None else None

    async def authorize_subscribe(self, user_id, platform, native_meeting_id, member_workspaces=None) -> Optional[int]:
        """Authorize a live-transcript subscribe → the meeting ROW id, or None. TWO branches:
        (a) OWNERSHIP (unchanged) — the meeting's owner may always subscribe;
        (b) MEMBERSHIP (Lane A) — any meeting BOUND (``data.workspace_id``) to a shared workspace the
            caller is a member of. ``member_workspaces`` is the caller's workspace-id set (gateway-injected
            x-user-workspaces). The binding IS the authorization: a member of the bound workspace sees the
            feed. Native-id collisions across tenants are handled by scanning candidates and matching the
            binding, never by picking a row blindly."""
        from sqlalchemy import select

        from .models import Meeting

        async with self._session_factory() as db:
            try:
                owned_id = (await db.execute(
                    select(Meeting.id).where(
                        Meeting.user_id == user_id,
                        Meeting.platform == platform,
                        Meeting.platform_specific_id == native_meeting_id,
                    ).order_by(Meeting.created_at.desc()).limit(1)
                )).scalar()
                if owned_id is not None:
                    await self._acquire_legacy_read_barrier(db, int(owned_id))
                    owned = (await db.execute(
                        select(Meeting)
                        .where(
                            Meeting.id == owned_id,
                            Meeting.user_id == user_id,
                            Meeting.platform == platform,
                            Meeting.platform_specific_id == native_meeting_id,
                        )
                        .execution_options(populate_existing=True)
                    )).scalars().first()
                    authorized_id = (
                        owned.id
                        if owned is not None
                        and legacy_collector_read_projection(owned.data) is not None
                        else None
                    )
                    await db.commit()
                    return authorized_id  # (a) owner, or a non-enumerating denial

                candidate_ids = (await db.execute(
                    select(Meeting.id).where(
                        Meeting.platform == platform,
                        Meeting.platform_specific_id == native_meeting_id,
                    )
                )).scalars().all()
                locked_ids = sorted({int(meeting_id) for meeting_id in candidate_ids})
                for meeting_id in locked_ids:
                    await self._acquire_legacy_read_barrier(db, meeting_id)
                rows = (
                    (await db.execute(
                        select(Meeting)
                        .where(
                            Meeting.id.in_(locked_ids),
                            Meeting.platform == platform,
                            Meeting.platform_specific_id == native_meeting_id,
                        )
                        .execution_options(populate_existing=True)
                    )).scalars().all()
                    if locked_ids
                    else []
                )
                authorized_id = None
                for mtg in rows:
                    data = mtg.data if isinstance(mtg.data, dict) else {}
                    if legacy_collector_read_projection(data) is None:
                        continue
                    if member_workspaces and data.get("workspace_id") in member_workspaces:
                        authorized_id = mtg.id  # (b) member of the bound shared workspace
                        break
                    if user_id in (data.get("transcript_viewers") or []):
                        authorized_id = mtg.id  # (c) redeemed transcript-share link
                        break
                await db.commit()
                return authorized_id
            except BaseException:
                await _rollback_or_invalidate_advisory_transaction(db)
                raise

    async def bind_workspace(self, user_id, platform, native_meeting_id, workspace_id) -> "Optional[str]":
        """OWNER-scoped: bind the meeting to a shared workspace (``data.workspace_id``) so its members can
        subscribe to the live transcript feed (authorize_subscribe branch b). Many meetings → one workspace
        (Amendment 6). Returns the bound workspace_id, or None if the caller owns no such meeting."""
        from sqlalchemy import select
        from sqlalchemy.orm.attributes import flag_modified

        from .models import Meeting

        async with self._session_factory() as db:
            stmt = (
                select(Meeting).where(
                    Meeting.user_id == user_id,
                    Meeting.platform == platform,
                    Meeting.platform_specific_id == native_meeting_id,
                ).order_by(Meeting.created_at.desc()).limit(1).with_for_update()
            )
            meeting = (await db.execute(stmt)).scalars().first()
            if not meeting:
                return None
            data = dict(meeting.data) if isinstance(meeting.data, dict) else {}
            data["workspace_id"] = workspace_id
            meeting.data = data
            flag_modified(meeting, "data")
            await db.commit()
            return workspace_id

    async def mint_transcript_share(self, user_id, platform, native_meeting_id, *,
                                    mode="open", allowed_emails=None, expires_in_sec=86400) -> "Optional[dict]":
        """OWNER-scoped: mint an INDEPENDENT transcript share grant (no workspace needed). Stored in
        ``data.share_grants[]`` as {id, secret_hash, mode, allowed_emails, expires_at, revoked} — only the
        HASH, never the token. Returns {id, token, ...} ONCE (token = ``<meeting_id>.<secret>`` so redeem
        resolves the meeting). None if the caller owns no such meeting."""
        from datetime import timedelta

        from sqlalchemy import select
        from sqlalchemy.orm.attributes import flag_modified

        from .models import Meeting

        async with self._session_factory() as db:
            stmt = (select(Meeting).where(
                Meeting.user_id == user_id, Meeting.platform == platform,
                Meeting.platform_specific_id == native_meeting_id,
            ).order_by(Meeting.created_at.desc()).limit(1).with_for_update())
            meeting = (await db.execute(stmt)).scalars().first()
            if not meeting:
                return None
            secret = secrets.token_urlsafe(24)
            gid = secrets.token_hex(8)
            expires_at = (_now() + timedelta(seconds=int(expires_in_sec))).isoformat()
            grant = {"id": gid, "secret_hash": _sha(secret), "mode": mode,
                     "allowed_emails": list(allowed_emails or []), "expires_at": expires_at, "revoked": False}
            data = dict(meeting.data) if isinstance(meeting.data, dict) else {}
            data["share_grants"] = list(data.get("share_grants", [])) + [grant]
            meeting.data = data
            flag_modified(meeting, "data")
            await db.commit()
            return {"id": gid, "token": f"{meeting.id}.{secret}", "mode": mode, "expires_at": expires_at}

    async def redeem_transcript_share(self, user_id, user_email, token) -> "Optional[dict]":
        """Redeem a transcript share token (any authenticated user) → grants THIS user subscribe access to
        that meeting's live feed (adds them to ``data.transcript_viewers[]``). Token = ``<meeting_id>.<secret>``.
        Returns {meeting_id, ok} on success, {error} on an invalid/expired/not-allowed grant, or None if the
        token is malformed / the meeting is gone. Cross-user by design — the capability token IS the authz."""
        from sqlalchemy import select
        from sqlalchemy.orm.attributes import flag_modified

        from .models import Meeting

        if not token or "." not in token:
            return None
        mid_s, secret = token.split(".", 1)
        try:
            mid = int(mid_s)
        except ValueError:
            return None
        async with self._session_factory() as db:
            meeting = (await db.execute(
                select(Meeting).where(Meeting.id == mid).with_for_update()
            )).scalars().first()
            if not meeting or not isinstance(meeting.data, dict):
                return None
            data = dict(meeting.data)
            h = _sha(secret)
            grant = next((g for g in data.get("share_grants", []) if g.get("secret_hash") == h), None)
            if not grant:
                return {"error": "invalid"}
            err = validate_transcript_grant(grant, user_email)
            if err:
                return {"error": err}
            viewers = list(data.get("transcript_viewers", []))
            if user_id not in viewers:
                viewers.append(user_id)
            data["transcript_viewers"] = viewers
            meeting.data = data
            flag_modified(meeting, "data")
            await db.commit()
            return {"meeting_id": mid, "ok": True}

    @asynccontextmanager
    async def transcript_write_lease(self, meeting_id, *, scopes=("transcript",)):
        """Hold an auto-releasing shared consent transaction across live publications."""
        mid = int(meeting_id)
        lock_params = {"meeting_lock_key": meeting_write_lock_key(mid)}
        async with self._session_factory() as db:
            try:
                await db.execute(
                    self._statement(
                        "SELECT pg_advisory_xact_lock_shared(:meeting_lock_key)"
                    ),
                    lock_params,
                )
                result = await db.execute(
                    self._statement("SELECT data FROM meetings WHERE id = :meeting_id"),
                    {"meeting_id": mid},
                )
                row = result.mappings().first()
                data = row["data"] if row and isinstance(row["data"], dict) else {}
                if (
                    row is None
                    or not content_scopes_are_writable(data, *scopes)
                    or ("transcript" in scopes and transcript_is_finalized(data))
                ):
                    raise TranscriptWriteRefused("meeting is not writable")
                yield _RedisTranscriptBatchWriter(self._redis, mid)
                await db.commit()
            except BaseException:
                await _rollback_or_invalidate_advisory_transaction(db)
                raise

    async def append_segment(self, meeting_id, segment) -> None:
        # Compatibility seam for callers that produce a single segment. Ingest uses one lease for
        # the complete decoded message so consent cannot be withdrawn between segment writes and
        # their live publication.
        async with self.transcript_write_lease(meeting_id) as writer:
            await writer.append_segments([segment])

    @staticmethod
    def _durable_segment_rows(meeting_id: int, segments: list[dict]) -> list[dict]:
        rows = []
        for segment in segments:
            segment_id = segment.get("segment_id")
            if not segment_id:
                continue
            try:
                start = float(segment.get("start", segment.get("start_time", 0.0)) or 0.0)
                end = float(segment.get("end", segment.get("end_time", start)) or start)
            except (TypeError, ValueError):
                continue
            if end < start:
                start, end = end, start
            rows.append({
                "mid": meeting_id,
                "start": start,
                "end": end,
                "text": segment.get("text") or "",
                "speaker": segment.get("speaker"),
                "lang": segment.get("language"),
                "uid": segment.get("session_uid"),
                "segid": str(segment_id),
                "created": datetime.now(timezone.utc).replace(tzinfo=None),
            })
        return rows

    async def _write_durable_segment_rows(self, db, rows: list[dict]) -> None:
        for row in rows:
            await db.execute(
                self._statement("""
                    INSERT INTO transcriptions (meeting_id, start_time, end_time, text, speaker, language, session_uid, segment_id, created_at)
                    VALUES (:mid, :start, :end, :text, :speaker, :lang, :uid, :segid, :created)
                    ON CONFLICT (meeting_id, segment_id) WHERE segment_id IS NOT NULL
                    DO UPDATE SET text = EXCLUDED.text, speaker = EXCLUDED.speaker,
                                  start_time = EXCLUDED.start_time, end_time = EXCLUDED.end_time,
                                  language = EXCLUDED.language, created_at = EXCLUDED.created_at
                """),
                row,
            )

    async def upsert_segments(self, meeting_id, segments) -> None:
        """The db-writer's durable sink — UPSERT a batch of flushed segments into ``transcriptions``
        on the segment identity ``(meeting_id, segment_id)`` (the partial unique index
        ``ix_transcription_meeting_segment`` in the admin-api authoritative schema), exactly the
        parent db-writer's ON CONFLICT statement: idempotent, a re-flushed rewrite lands as an
        UPDATE, never a duplicate row."""
        mid = int(meeting_id)
        rows = self._durable_segment_rows(mid, segments)
        if not rows:
            return
        async with self._session_factory() as db:
            lock_params = {
                "meeting_lock_key": meeting_write_lock_key(mid)
            }
            await db.execute(
                self._statement("SELECT pg_advisory_xact_lock_shared(:meeting_lock_key)"),
                lock_params,
            )
            result = await db.execute(
                self._statement("SELECT data FROM meetings WHERE id = :meeting_id"),
                {"meeting_id": mid},
            )
            meeting = result.mappings().first()
            data = meeting["data"] if meeting and isinstance(meeting["data"], dict) else {}
            if (
                meeting is None
                or not content_scopes_are_writable(data, "transcript")
                or transcript_is_finalized(data)
            ):
                raise TranscriptWriteRefused("meeting is not writable")
            await self._write_durable_segment_rows(db, rows)
            await db.commit()

    async def finalize_transcript(self, redis_client, meeting_id: int):
        """Seal one terminal transcript under an auto-releasing transaction barrier.

        The PostgreSQL advisory lock is transaction-scoped: cancellation or connection failure can
        never return a pooled session carrying a meeting lock. Redis is HSCANed and the durable
        census is server-side streamed under hard launch-read bounds; neither carrier is fully
        materialized before those bounds are enforced.
        """

        from ..meeting_writes import (
            TranscriptRevisionBuilder,
            TranscriptFinalizationOutcome,
            minutes_transcript_is_finalizable,
            validated_transcript_finalization_marker,
        )
        from .carriers import ACTIVE_MEETINGS_KEY, segments_hash_key
        from .db_writer import iter_terminal_segment_batches

        mid = int(meeting_id)
        lock_params = {"meeting_lock_key": meeting_write_lock_key(mid)}
        hash_key = segments_hash_key(mid)
        async with self._session_factory() as db:
            try:
                await db.execute(
                    self._statement("SELECT pg_advisory_xact_lock(:meeting_lock_key)"),
                    lock_params,
                )
                result = await db.execute(
                    self._statement(
                        "SELECT data FROM meetings WHERE id = :meeting_id FOR UPDATE"
                    ),
                    {"meeting_id": mid},
                )
                row = result.mappings().first()
                if row is None:
                    outcome = TranscriptFinalizationOutcome(state="cancelled")
                else:
                    data = row.get("data") if isinstance(row.get("data"), dict) else {}
                    marker = validated_transcript_finalization_marker(data)
                    authorized = minutes_transcript_is_finalizable(data)
                    if marker is not None:
                        outcome = (
                            TranscriptFinalizationOutcome(state="finalized", marker=marker)
                            if authorized
                            else TranscriptFinalizationOutcome(state="cancelled")
                        )
                    elif "zaki_transcript_finalization" in data:
                        # A malformed seal already fences every writer. Never overwrite or publish
                        # it from an unverifiable durable revision; operator/erasure recovery owns
                        # this content-free failure state.
                        raise ValueError("transcript finalization marker is invalid")
                    else:
                        capture_tagged = "zaki_capture" in data
                        if (
                            not capture_tagged
                            and content_scopes_are_writable(data, "transcript")
                        ) or authorized:
                            async for batch in iter_terminal_segment_batches(
                                redis_client, mid
                            ):
                                rows = self._durable_segment_rows(mid, batch)
                                await self._write_durable_segment_rows(db, rows)
                        if authorized:
                            census = await db.stream(
                                self._statement(
                                    "SELECT LEFT(segment_id, 257) AS segment_id, "
                                    "start_time AS start, end_time AS end, "
                                    "LEFT(text, 65537) AS text, "
                                    "LEFT(speaker, 201) AS speaker, "
                                    "LEFT(language, 36) AS language FROM transcriptions "
                                    "WHERE meeting_id = :meeting_id "
                                    "ORDER BY start_time ASC, segment_id ASC, id ASC"
                                ),
                                {"meeting_id": mid},
                                execution_options={"yield_per": 100},
                            )
                            revision = TranscriptRevisionBuilder()
                            async for item in census.mappings():
                                revision.add(item)
                            marker = revision.marker()
                            data = {**data, "zaki_transcript_finalization": marker}
                            await db.execute(
                                self._statement(
                                    "UPDATE meetings SET data = CAST(:data AS jsonb) "
                                    "WHERE id = :meeting_id"
                                ),
                                {
                                    "meeting_id": mid,
                                    "data": json.dumps(data, sort_keys=True),
                                },
                            )
                            outcome = TranscriptFinalizationOutcome(
                                state="finalized", marker=marker
                            )
                        else:
                            outcome = TranscriptFinalizationOutcome(state="cancelled")
                # Commit releases pg_advisory_xact_lock. Late writers must then observe the durable
                # Minutes marker and refuse before publishing; Redis cleanup is safe to retry.
                await db.commit()
            except BaseException:
                rollback = getattr(db, "rollback", None)
                if callable(rollback):
                    try:
                        await rollback()
                    except BaseException:
                        # If rollback itself is uncertain, invalidate the physical connection so
                        # it cannot return to the pool carrying an open transaction/lock.
                        invalidate = getattr(db, "invalidate", None)
                        if callable(invalidate):
                            try:
                                await invalidate()
                            except BaseException:
                                pass
                raise
        await redis_client.delete(hash_key)
        await redis_client.srem(ACTIVE_MEETINGS_KEY, str(mid))
        return outcome

    async def processed_view_cursor(self, meeting_id, view_id) -> Optional[str]:
        """The ``source_cursor`` of the ``view_id`` view inside ``meeting.data['processed']['views']``
        — the last ``proc:meeting:{id}`` stream entry already durable; the db-writer resumes after it."""
        from sqlalchemy import select

        from .models import Meeting

        async with self._session_factory() as db:
            m = (await db.execute(select(Meeting).where(Meeting.id == int(meeting_id)))).scalars().first()
            if not m or not isinstance(m.data, dict):
                return None
            view = _find_processed_view(m.data, view_id)
            return view.get("source_cursor") if view else None

    async def merge_processed_view(
        self, meeting_id, *, view_id, kind, notes, source_cursor, params=None,
    ) -> None:
        """Persist drained copilot notes into the meeting row's ``data['processed']['views']``
        JSONB (the documented meeting.data home — the same pattern recordings/notes/docs use; NO
        schema change), in the ADDRESSABLE, VERSIONED multi-consumer shape (release DoD):
        the view keyed ``view_id`` is upserted (other views preserved), its ``doc['notes']`` merged
        by note id, ``params`` = the processing metadata APPLIED, ``source_cursor`` = the stream
        position the view reflects. The shared meeting-write barrier is acquired before the row
        lock, matching transcript and recording writers, so durable withdrawal wins against every
        transcript-derived PostgreSQL mutation."""
        async with self._session_factory() as db:
            lock_params = {
                "meeting_lock_key": meeting_write_lock_key(int(meeting_id))
            }
            await db.execute(
                self._statement("SELECT pg_advisory_xact_lock_shared(:meeting_lock_key)"),
                lock_params,
            )
            result = await db.execute(
                self._statement("SELECT data FROM meetings WHERE id = :meeting_id FOR UPDATE"),
                {"meeting_id": int(meeting_id)},
            )
            row = result.mappings().first()
            data = row["data"] if row and isinstance(row["data"], dict) else {}
            if row is None or not content_scopes_are_writable(
                data, "transcript", "summary"
            ):
                raise TranscriptWriteRefused("meeting is not writable")
            updated = _upsert_processed_view(
                data, view_id=view_id, kind=kind, notes=notes,
                source_cursor=source_cursor, params=params,
            )
            await db.execute(
                self._statement(
                    "UPDATE meetings SET data = CAST(:data AS jsonb) WHERE id = :meeting_id"
                ),
                {
                    "meeting_id": int(meeting_id),
                    "data": json.dumps(updated, sort_keys=True),
                },
            )
            await db.commit()

    async def _mutate_docs(self, user_id, platform, native_meeting_id, mutator):
        """Owner-scoped atomic read→modify→write of ``meeting.data['docs']`` under ONE
        ``SELECT … FOR UPDATE`` row lock. Returns the updated docs list, or ``None`` when the
        user owns no such meeting."""
        from sqlalchemy import select
        from sqlalchemy.orm.attributes import flag_modified

        from .models import Meeting

        async with self._session_factory() as db:
            stmt = (
                select(Meeting)
                .where(
                    Meeting.user_id == user_id,
                    Meeting.platform == platform,
                    Meeting.platform_specific_id == native_meeting_id,
                )
                .order_by(Meeting.created_at.desc())
                .limit(1)
                .with_for_update()
            )
            meeting = (await db.execute(stmt)).scalars().first()
            if not meeting:
                return None
            data = dict(meeting.data) if isinstance(meeting.data, dict) else {}
            docs = mutator(list(data.get("docs", [])))
            data["docs"] = docs
            meeting.data = data
            flag_modified(meeting, "data")
            await db.commit()
            return docs

    async def connect_doc(self, user_id, platform, native_meeting_id, doc):
        return await self._mutate_docs(
            user_id, platform, native_meeting_id, lambda docs: _upsert_doc(docs, doc)
        )

    async def disconnect_doc(self, user_id, platform, native_meeting_id, path):
        return await self._mutate_docs(
            user_id, platform, native_meeting_id, lambda docs: _remove_doc(docs, path)
        )

    async def set_intent(self, user_id, platform, native_meeting_id, status, scheduled_at=None):
        """Owner-scoped atomic write of the INTENT status (``idle`` / ``scheduled``) onto the
        ``meetings.status`` column under ONE ``SELECT … FOR UPDATE`` row lock. Stamps / clears
        ``meeting.data['scheduled_at']``. NEVER touches the bot FSM."""
        from sqlalchemy import select
        from sqlalchemy.orm.attributes import flag_modified

        from .models import Meeting

        async with self._session_factory() as db:
            stmt = (
                select(Meeting)
                .where(
                    Meeting.user_id == user_id,
                    Meeting.platform == platform,
                    Meeting.platform_specific_id == native_meeting_id,
                )
                .order_by(Meeting.created_at.desc())
                .limit(1)
                .with_for_update()
            )
            meeting = (await db.execute(stmt)).scalars().first()
            if not meeting:
                return None
            data = dict(meeting.data) if isinstance(meeting.data, dict) else {}
            prev_status = meeting.status
            prev_at = data.get("scheduled_at")
            new_at = scheduled_at if status == "scheduled" else None
            meeting.status = status
            if status == "scheduled":
                data["scheduled_at"] = new_at
            else:
                data.pop("scheduled_at", None)
            meeting.data = data
            flag_modified(meeting, "data")
            await db.commit()
            changed = (prev_status != status) or (prev_at != new_at)
            return {
                "id": meeting.id,
                "user_id": user_id,
                "platform": platform,
                "native_id": native_meeting_id,
                "status": status,
                "scheduled_at": new_at,
                "changed": changed,
            }

    @staticmethod
    def _planned_row(m) -> dict:
        """One meeting ORM row → the ``list_meetings`` dict shape (owner context: shared=False)."""
        return {
            "id": m.id,
            "user_id": m.user_id,
            "platform": m.platform,
            "native_meeting_id": m.platform_specific_id,
            "constructed_meeting_url": (m.data or {}).get("constructed_meeting_url")
            if isinstance(m.data, dict) else None,
            "status": m.status,
            "bot_container_id": m.bot_container_id,
            "start_time": m.start_time.isoformat() if m.start_time else None,
            "end_time": m.end_time.isoformat() if m.end_time else None,
            "data": m.data if isinstance(m.data, dict) else {},
            "shared": False,
            "created_at": m.created_at.isoformat() if m.created_at else None,
            "updated_at": m.updated_at.isoformat() if m.updated_at else None,
        }

    async def create_planned_meeting(self, user_id, *, platform, native_meeting_id,
                                     title=None, scheduled_at=None, meeting_url=None,
                                     workspace_id=None, auto_join=True, calendar_uid=None,
                                     workspace_source=None, attendees=None) -> dict:
        """Insert a PLANNED row (intent status, no bot). Takes the SAME per-user advisory lock as
        ``bot_spawn.create_meeting_guarded`` so planned-create serializes with concurrent spawns
        and calendar sync; the unique partial index remains the DB-level backstop (→ duplicate)."""
        from sqlalchemy import bindparam, select, text
        from sqlalchemy.exc import IntegrityError

        from .models import Meeting

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
        status = "scheduled" if scheduled_at else "idle"

        async with self._session_factory() as db:
            await db.execute(
                text("SELECT pg_advisory_xact_lock(:uid)").bindparams(bindparam("uid", user_id))
            )
            if native_meeting_id is not None:
                dup = (await db.execute(
                    select(Meeting.id).where(
                        Meeting.user_id == user_id,
                        Meeting.platform == platform,
                        Meeting.platform_specific_id == native_meeting_id,
                        Meeting.status.notin_(("completed", "failed")),
                    )
                )).scalars().first()
                if dup is not None:
                    return {"error": "duplicate"}
            m = Meeting(
                user_id=user_id, platform=platform, platform_specific_id=native_meeting_id,
                status=status, data=data,
            )
            db.add(m)
            try:
                await db.commit()
            except IntegrityError:
                await db.rollback()
                return {"error": "duplicate"}
            await db.refresh(m)
            return self._planned_row(m)

    async def update_planned_meeting(self, user_id, meeting_id, updates) -> "Optional[dict]":
        """ROW-id-addressed PATCH of a planned row (intent status only). ``updates`` carries only
        the keys the caller sent — presence means apply (None clears where documented)."""
        from sqlalchemy import bindparam, select, text
        from sqlalchemy.exc import IntegrityError
        from sqlalchemy.orm.attributes import flag_modified

        from .models import Meeting

        async with self._session_factory() as db:
            await db.execute(
                text("SELECT pg_advisory_xact_lock(:uid)").bindparams(bindparam("uid", user_id))
            )
            meeting = (await db.execute(
                select(Meeting).where(Meeting.id == meeting_id, Meeting.user_id == user_id)
                .with_for_update()
            )).scalars().first()
            if meeting is None:
                return None
            if meeting.status not in ("idle", "scheduled"):
                return {"error": "conflict"}
            data = dict(meeting.data) if isinstance(meeting.data, dict) else {}

            if "native_meeting_id" in updates:
                new_platform = updates.get("platform") or meeting.platform
                new_native = updates["native_meeting_id"]
                if new_native is not None:
                    dup = (await db.execute(
                        select(Meeting.id).where(
                            Meeting.user_id == user_id,
                            Meeting.platform == new_platform,
                            Meeting.platform_specific_id == new_native,
                            Meeting.status.notin_(("completed", "failed")),
                            Meeting.id != meeting_id,
                        )
                    )).scalars().first()
                    if dup is not None:
                        return {"error": "duplicate"}
                meeting.platform = new_platform
                meeting.platform_specific_id = new_native
            if "constructed_meeting_url" in updates:
                if updates["constructed_meeting_url"]:
                    data["constructed_meeting_url"] = updates["constructed_meeting_url"]
                else:
                    data.pop("constructed_meeting_url", None)
            if "title" in updates:
                if updates["title"]:
                    data["title"] = updates["title"]
                else:
                    data.pop("title", None)
            if "scheduled_at" in updates:
                if updates["scheduled_at"]:
                    data["scheduled_at"] = updates["scheduled_at"]
                    meeting.status = "scheduled"
                else:
                    data.pop("scheduled_at", None)
                    meeting.status = "idle"
            if "workspace_id" in updates:
                if updates["workspace_id"]:
                    # an explicit bind is the USER's choice — it also lifts any series tombstone
                    data["workspace_id"] = updates["workspace_id"]
                    data["workspace_source"] = "user"
                    data.pop("workspace_unbound", None)
                else:
                    # explicit unbind tombstones the series row so sync never re-inherits it
                    data.pop("workspace_id", None)
                    data.pop("workspace_source", None)
                    if (data.get("calendar_uid")):
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

            meeting.data = data
            flag_modified(meeting, "data")
            try:
                await db.commit()
            except IntegrityError:
                await db.rollback()
                return {"error": "duplicate"}
            await db.refresh(meeting)
            return self._planned_row(meeting)

    async def delete_planned_meeting(self, user_id, meeting_id) -> "Optional[bool]":
        from sqlalchemy import select

        from .models import Meeting

        async with self._session_factory() as db:
            meeting = (await db.execute(
                select(Meeting).where(Meeting.id == meeting_id, Meeting.user_id == user_id)
                .with_for_update()
            )).scalars().first()
            if meeting is None:
                return None
            if meeting.status not in ("idle", "scheduled"):
                return False
            await db.delete(meeting)
            await db.commit()
            return True


class RedisStreamBus:
    """``RedisBus`` over a ``redis.asyncio`` client — XREADGROUP the segments stream, XACK,
    PUBLISH ``tc:meeting:{id}:mutable``. Carve of ``collector/consumer.py`` + ``processors.py``."""

    def __init__(self, client):
        self._client = client

    async def read_segments(self, *, group, consumer, stream, count=10):
        try:
            await self._client.xgroup_create(name=stream, groupname=group, id="0", mkstream=True)
        except Exception:
            pass  # BUSYGROUP — group already exists
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
            return accepted
        return await self._client.publish(channel, data)

    async def xadd(self, stream, payload):
        """Append one row-scoped feed entry, fenced atomically against retention erasure."""
        if stream.startswith("tc:meeting:"):
            try:
                meeting_id = int(stream.removeprefix("tc:meeting:"))
                if transcript_stream_key(meeting_id) != stream:
                    raise ValueError
            except (TypeError, ValueError):
                raise TranscriptWriteRefused("meeting carrier identity is invalid") from None
            scopes = (
                ("raw", "processed")
                if payload.get("type") == "session_end"
                else ("raw",)
            )
            accepted = await xadd_if_carrier_writable(
                self._client,
                meeting_id,
                scope=scopes,
                stream=stream,
                fields={"payload": json.dumps(payload)},
            )
            if not accepted:
                raise TranscriptWriteRefused("meeting carrier is no longer writable")
            return accepted
        return await self._client.xadd(stream, {"payload": json.dumps(payload)})

    async def xadd_many(self, stream, payloads):
        """Append a bounded transcript batch atomically against the row's raw fence."""
        if not payloads:
            return []
        if stream.startswith("tc:meeting:"):
            try:
                meeting_id = int(stream.removeprefix("tc:meeting:"))
                if transcript_stream_key(meeting_id) != stream:
                    raise ValueError
            except (TypeError, ValueError):
                raise TranscriptWriteRefused("meeting carrier identity is invalid") from None
            accepted = await xadd_many_if_carrier_writable(
                self._client,
                meeting_id,
                scope="raw",
                stream=stream,
                entries=[{"payload": json.dumps(payload)} for payload in payloads],
            )
            if not accepted:
                raise TranscriptWriteRefused("meeting carrier is no longer writable")
            return accepted
        async with self._client.pipeline(transaction=True) as pipe:
            for payload in payloads:
                pipe.xadd(stream, {"payload": json.dumps(payload)})
            return await pipe.execute()


def build_production_app(
    *,
    database_url: Optional[str] = None,
    redis_url: Optional[str] = None,
):
    """Construct the collector app with real SQLAlchemy-async + redis adapters from env.

    Lazy-imports SQLAlchemy + redis so the package can be imported (and unit-tested with fakes)
    without those runtime deps installed in the gate venv.
    """
    import redis.asyncio as aioredis
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from .app import create_app

    database_url = database_url or os.getenv(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@postgres:5432/vexa"
    )
    redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis:6379/0")

    engine = create_async_engine(database_url, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    redis_client = aioredis.from_url(
        redis_url, decode_responses=True, **redis_client_options()
    )

    store = SqlAlchemyTranscriptStore(session_factory, redis_client=redis_client)
    bus = RedisStreamBus(redis_client)
    return create_app(store, bus)
