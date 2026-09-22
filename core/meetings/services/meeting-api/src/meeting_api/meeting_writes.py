"""Shared meeting-write barrier identity and durable capture-state predicate."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re
from typing import Literal


MEETING_WRITE_LOCK_NAMESPACE = 23115
_RETENTION_SCOPES = frozenset({"audio", "transcript", "summary"})
_MAX_SIGNED_BIGINT = 9_223_372_036_854_775_807
_FINAL_REVISION = re.compile(r"^sha256:[0-9a-f]{64}$")
MAX_FINAL_TRANSCRIPT_SEGMENTS = 4_096
MAX_FINAL_TRANSCRIPT_CONTENT_BYTES = 256 * 1_024
MAX_FINAL_TRANSCRIPT_TURN_CHARS = 65_536


@dataclass(frozen=True)
class TranscriptFinalizationOutcome:
    """Content-free result consumed by the platform finalized outbox."""

    state: Literal["finalized", "cancelled"]
    marker: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        if self.state not in {"finalized", "cancelled"}:
            raise ValueError("transcript finalization outcome is invalid")
        if self.state == "finalized" and not isinstance(self.marker, Mapping):
            raise ValueError("finalized transcript marker is required")
        if self.state == "cancelled" and self.marker is not None:
            raise ValueError("cancelled transcript cannot carry a marker")


def meeting_write_lock_key(meeting_id: object) -> int:
    """Map a public meeting id to its PostgreSQL one-key advisory barrier.

    PostgreSQL's two-argument advisory-lock functions accept two signed 32-bit
    integers, while meeting ids use the complete positive signed-bigint domain.
    A negative one-key lock preserves that domain and keeps meeting locks
    disjoint from the positive one-key user locks used by bot spawning.
    """

    if (
        type(meeting_id) is not int
        or meeting_id <= 0
        or meeting_id > _MAX_SIGNED_BIGINT
    ):
        raise ValueError("meeting id is invalid")
    return -meeting_id


def minutes_transcript_is_finalizable(data: object) -> bool:
    """Whether a terminal row still has authority to publish a Minutes transcript event."""

    capture = data.get("zaki_capture") if isinstance(data, Mapping) else None
    return (
        isinstance(capture, Mapping)
        and capture.get("state") == "authorized"
        and content_scopes_are_writable(data, "transcript")
    )


def transcript_is_finalized(data: object) -> bool:
    marker = data.get("zaki_transcript_finalization") if isinstance(data, Mapping) else None
    return isinstance(marker, Mapping) and marker.get("state") == "finalized"


def validated_transcript_finalization_marker(data: object) -> dict[str, object] | None:
    marker = data.get("zaki_transcript_finalization") if isinstance(data, Mapping) else None
    if not isinstance(marker, Mapping) or set(marker) != {
        "state", "revision", "finalized_at", "segment_count"
    }:
        return None
    revision = marker.get("revision")
    finalized_at = marker.get("finalized_at")
    count = marker.get("segment_count")
    if (
        marker.get("state") != "finalized"
        or not isinstance(revision, str)
        or _FINAL_REVISION.fullmatch(revision) is None
        or not isinstance(finalized_at, str)
        or type(count) is not int
        or count < 0
        or count > MAX_FINAL_TRANSCRIPT_SEGMENTS
    ):
        return None
    try:
        stamp = datetime.fromisoformat(finalized_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        return None
    return dict(marker)


def canonical_transcript_segment(segment: object) -> dict[str, object]:
    """Validate and project one durable row without retaining arbitrary source fields."""

    if not isinstance(segment, Mapping):
        raise ValueError("durable transcript segment is invalid")
    segment_id = segment.get("segment_id")
    start = segment.get("start", segment.get("start_time"))
    end = segment.get("end", segment.get("end_time"))
    text = segment.get("text")
    speaker = segment.get("speaker")
    language = segment.get("language")
    if (
        not isinstance(segment_id, str)
        or not segment_id
        or len(segment_id) > 256
        or isinstance(start, bool)
        or not isinstance(start, (int, float))
        or not math.isfinite(float(start))
        or isinstance(end, bool)
        or not isinstance(end, (int, float))
        or not math.isfinite(float(end))
        or not isinstance(text, str)
        or len(text) > MAX_FINAL_TRANSCRIPT_TURN_CHARS
        or (speaker is not None and (not isinstance(speaker, str) or len(speaker) > 200))
        or (language is not None and (not isinstance(language, str) or len(language) > 35))
    ):
        raise ValueError("durable transcript segment is invalid")
    return {
        "segment_id": segment_id,
        "start": float(start),
        "end": float(end),
        "text": text,
        "speaker": speaker,
        "language": language,
    }


class TranscriptRevisionBuilder:
    """Incrementally hash an already ordered durable census within launch read bounds."""

    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(b"[")
        self._count = 0
        self._content_bytes = 0
        self._last_key: tuple[float, str] | None = None

    def add(self, segment: object) -> None:
        canonical = canonical_transcript_segment(segment)
        key = (canonical["start"], canonical["segment_id"])
        if self._last_key is not None and key < self._last_key:
            raise ValueError("durable transcript census is not ordered")
        if self._count >= MAX_FINAL_TRANSCRIPT_SEGMENTS:
            raise ValueError("durable transcript exceeds safe bounds")
        encoded = json.dumps(
            canonical,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if self._content_bytes + len(encoded) > MAX_FINAL_TRANSCRIPT_CONTENT_BYTES:
            raise ValueError("durable transcript exceeds safe bounds")
        if self._count:
            self._digest.update(b",")
        self._digest.update(encoded)
        self._count += 1
        self._content_bytes += len(encoded)
        self._last_key = key

    def marker(self, *, finalized_at: datetime | None = None) -> dict[str, object]:
        digest = self._digest.copy()
        digest.update(b"]")
        return _transcript_finalization_marker(
            digest.hexdigest(), self._count, finalized_at=finalized_at
        )


def _transcript_finalization_marker(
    digest: str, segment_count: int, *, finalized_at: datetime | None = None
) -> dict[str, object]:
    stamp = finalized_at or datetime.now(timezone.utc)
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("transcript finalization time is invalid")
    stamp = stamp.astimezone(timezone.utc)
    return {
        "state": "finalized",
        "revision": "sha256:" + digest,
        "finalized_at": stamp.isoformat().replace("+00:00", "Z"),
        "segment_count": segment_count,
    }


def build_transcript_finalization_marker(
    segments: object, *, finalized_at: datetime | None = None
) -> dict[str, object]:
    """Hash a bounded canonical durable transcript projection used by the owner read plane."""

    if not isinstance(segments, (list, tuple)):
        raise ValueError("durable transcript segments are invalid")
    if len(segments) > MAX_FINAL_TRANSCRIPT_SEGMENTS:
        raise ValueError("durable transcript exceeds safe bounds")
    canonical = [canonical_transcript_segment(segment) for segment in segments]
    canonical.sort(key=lambda row: (row["start"], row["segment_id"]))
    builder = TranscriptRevisionBuilder()
    for segment in canonical:
        builder.add(segment)
    return builder.marker(finalized_at=finalized_at)


def capture_is_withdrawn(data: object) -> bool:
    """True only for an explicitly withdrawn ZAKI capture; ordinary Vexa meetings stay writable."""
    capture = data.get("zaki_capture") if isinstance(data, Mapping) else None
    return isinstance(capture, Mapping) and capture.get("state") == "withdrawn"


def content_scopes_are_writable(data: object, *scopes: str) -> bool:
    """Whether content in ``scopes`` may still be created for one meeting.

    Ordinary upstream Vexa meetings have neither ZAKI capture nor retention metadata and remain
    writable. A capture-tagged row must carry a valid retention record; its state, expired-scope
    list, and wall-clock deadlines are authorization inputs. Missing/malformed authority, erasure,
    or an already-expired requested scope fails closed.
    """

    if capture_is_withdrawn(data):
        return False
    if not scopes or any(scope not in _RETENTION_SCOPES for scope in scopes):
        return False
    if isinstance(data, Mapping) and "zaki_capture" in data:
        capture = data.get("zaki_capture")
        if not isinstance(capture, Mapping) or capture.get("state") != "authorized":
            return False
    retention = data.get("zaki_retention") if isinstance(data, Mapping) else None
    if retention is None:
        return not isinstance(data, Mapping) or (
            "zaki_capture" not in data and "zaki_retention" not in data
        )
    if not isinstance(retention, Mapping) or retention.get("state") != "open":
        return False
    expired = retention.get("expired_scopes", [])
    if not isinstance(expired, list) or any(
        not isinstance(scope, str) for scope in expired
    ):
        return False
    if len(expired) != len(set(expired)) or any(
        scope not in _RETENTION_SCOPES for scope in expired
    ):
        return False
    if any(scope in expired for scope in scopes):
        return False
    expiries = retention.get("scope_expiries")
    if not isinstance(expiries, Mapping):
        return False
    now = datetime.now(timezone.utc)
    for scope in scopes:
        value = expiries.get(scope)
        if not isinstance(value, str):
            return False
        try:
            expiry = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
        if (
            expiry.tzinfo is None
            or expiry.utcoffset() != timedelta(0)
            or expiry <= now
        ):
            return False
    return True


def purge_scope_data_carriers(data: dict[str, object], *scopes: str) -> int:
    """Remove persisted JSON carriers for expired Minutes scopes.

    The returned count preserves the TTL adapter's existing content-free receipt semantics:
    generated summary entries and processed views count as units, while metadata containers do
    not. Audio object counts are supplied by object storage before this helper clears their JSON
    discovery metadata.
    """

    selected = set(scopes)
    if not selected or not selected.issubset(_RETENTION_SCOPES):
        raise ValueError("retention purge scope is invalid")

    deleted = 0
    if "summary" in selected:
        summaries = data.pop("summaries", None)
        if isinstance(summaries, list):
            deleted += len(summaries)
        if data.pop("summary", None) is not None:
            deleted += 1
        data.pop("notes", None)
        data.pop("docs", None)

    if selected.intersection({"summary", "transcript"}):
        processed = data.pop("processed", None)
        views = processed.get("views") if isinstance(processed, Mapping) else None
        if isinstance(views, list):
            deleted += len(views)

    if "transcript" in selected:
        data.pop("zaki_transcript_finalization", None)

    if "audio" in selected:
        data["recordings"] = []
        data.pop("zaki_recording_prefixes", None)

    return deleted


def legacy_collector_read_projection(data: object) -> dict[str, object] | None:
    """Project one row for the ordinary collector without bypassing Minutes retention.

    Rows with neither Minutes metadata key are upstream Vexa rows and retain their historical
    payload.  Any Minutes-tagged row must have current transcript authority to appear on this
    transcript-oriented plane.  Audio and summary fields are independently removed at their
    request-time deadlines, even if the asynchronous TTL purge has not run yet.
    """

    source = dict(data) if isinstance(data, Mapping) else {}
    managed = "zaki_capture" in source or "zaki_retention" in source
    if not managed:
        return source
    if "zaki_capture" not in source or "zaki_retention" not in source:
        return None
    if not content_scopes_are_writable(source, "transcript"):
        return None

    projected = dict(source)
    if not content_scopes_are_writable(source, "audio"):
        purge_scope_data_carriers(projected, "audio")
    if not content_scopes_are_writable(source, "summary"):
        purge_scope_data_carriers(projected, "summary")
    return projected


def capture_authority_is_stale(incoming: object, prior: object) -> bool:
    """Reject capture authority that does not post-date a durable scope withdrawal.

    A malformed withdrawal tombstone fails closed: once the durable state says ``withdrawn``, only
    parseable evidence of a strictly newer authorization may reopen the same capture scope.
    """
    if not capture_is_withdrawn(prior):
        return False
    incoming_capture = incoming.get("zaki_capture") if isinstance(incoming, Mapping) else None
    prior_capture = prior.get("zaki_capture") if isinstance(prior, Mapping) else None
    if not isinstance(incoming_capture, Mapping) or not isinstance(prior_capture, Mapping):
        return True
    try:
        authorized_at = datetime.fromisoformat(incoming_capture["authorized_at"])
        withdrawn_at = datetime.fromisoformat(prior_capture["withdrawn_at"])
    except (KeyError, TypeError, ValueError):
        return True
    if (
        authorized_at.tzinfo is None
        or authorized_at.utcoffset() is None
        or withdrawn_at.tzinfo is None
        or withdrawn_at.utcoffset() is None
    ):
        return True
    return authorized_at <= withdrawn_at
