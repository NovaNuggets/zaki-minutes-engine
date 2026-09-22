"""The recordings flow — chunk upload + finalize → master in ``meeting.data`` JSONB.

Port of the parent ``recordings.internal_upload_recording`` + ``recording_finalizer`` CORE:

  * ``upload_chunk(...)`` — verify the MeetingToken, resolve the bot's ``MeetingSession`` by
    ``(meeting_id, session_uid)``, upload the chunk to object storage, fold it into the recording's JSONB payload
    (``jsonb.apply_chunk_to_recording``) under a read-modify-write on ``meeting.data['recordings']``,
    and return the upload receipt.
  * ``finalize_master(...)`` — concatenate a recording media-file's chunks into a master via the
    golden-locked ``build_recording_master`` codec, upload the master, and stamp the JSONB media-file
    (``storage_path`` → master key, ``finalized_by``, ``is_final``, ``playback_url``).

The codec itself (``meeting_api.build_recording_master``, recording.v1) is already ported +
golden-locked — this module only orchestrates the IO + the JSONB bookkeeping around it.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import hmac
import re
from typing import Any, Optional

from ..bot_spawn.invocation import verify_meeting_token
from ..obs import log_event
from ..recording_codec import build_recording_master
from .jsonb import (
    apply_chunk_to_recording,
    chunk_storage_key,
    master_storage_key,
    recording_numeric_id_for_session,
)
from .ports import RecordingRepo, RecordingWriteRefused, Storage

# Media content types (parent ``recording_codec._media_content_type``, reduced to the core set).
_CONTENT_TYPES = {
    "webm": "video/webm",
    "wav": "audio/wav",
    "mp4": "video/mp4",
    "mkv": "video/x-matroska",
}
_MEDIA_TYPES = frozenset({"audio", "video"})
_MEDIA_FORMATS = frozenset(_CONTENT_TYPES)
_KEY_SEGMENT = re.compile(r"^[A-Za-z0-9._:-]+$")
_MAX_CHUNK_SEQ = 2_147_483_647
DEFAULT_RECORDING_MAX_CHUNKS = 4_096
# Finalization currently holds source chunks, the assembled master, and may transiently hand a
# third representation to the storage adapter. Keep one recording small enough that the explicitly
# bounded concurrent finalizers fit beneath the meeting-api's 1 GiB pod limit.
DEFAULT_RECORDING_MAX_TOTAL_BYTES = 64 * 1024 * 1024


def _content_type(media_format: str) -> str:
    return _CONTENT_TYPES.get(media_format, "application/octet-stream")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class SessionNotFound(Exception):
    """The upload's ``session_uid`` matches no MeetingSession AND it is the final chunk → 404."""


class InvalidRecordingMetadata(ValueError):
    """An upload field could produce an unsafe or unsupported object-storage key."""


class RecordingChunkConflict(RuntimeError):
    """A deterministic chunk sequence already exists with different bytes."""


class RecordingNotReady(RuntimeError):
    """The bounded, contiguous, final recording manifest is not ready for publication."""


class RecordingLimitExceeded(RuntimeError):
    """A recording exceeds its configured chunk-count or aggregate-byte budget."""


def _validate_limits(*, max_chunks: int, max_total_bytes: int) -> None:
    if type(max_chunks) is not int or max_chunks < 1:
        raise ValueError("recording chunk limit must be positive")
    if type(max_total_bytes) is not int or max_total_bytes < 1:
        raise ValueError("recording byte limit must be positive")


def _media_manifest(media_file: Optional[Mapping[str, object]]) -> tuple[dict[int, int], int | None]:
    """Return one strictly validated private chunk manifest from recording JSONB."""

    if media_file is None:
        return {}, None
    metadata = media_file.get("metadata")
    if not isinstance(metadata, Mapping):
        raise RecordingNotReady("recording manifest is unavailable")
    raw_sizes = metadata.get("zaki_chunk_sizes")
    if not isinstance(raw_sizes, Mapping):
        raise RecordingNotReady("recording manifest is unavailable")
    sizes: dict[int, int] = {}
    for raw_sequence, raw_size in raw_sizes.items():
        if (
            not isinstance(raw_sequence, str)
            or not 1 <= len(raw_sequence) <= 10
            or not raw_sequence.isdecimal()
        ):
            raise RecordingNotReady("recording manifest is invalid")
        sequence = int(raw_sequence)
        if raw_sequence != str(sequence) or not 0 <= sequence <= _MAX_CHUNK_SEQ:
            raise RecordingNotReady("recording manifest is invalid")
        if type(raw_size) is not int or raw_size < 0:
            raise RecordingNotReady("recording manifest is invalid")
        sizes[sequence] = raw_size
    final_sequence = metadata.get("zaki_final_chunk_seq")
    if final_sequence is not None and (
        type(final_sequence) is not int or not 0 <= final_sequence <= _MAX_CHUNK_SEQ
    ):
        raise RecordingNotReady("recording manifest is invalid")
    if final_sequence is not None and any(sequence > final_sequence for sequence in sizes):
        raise RecordingNotReady("recording manifest is invalid")
    return sizes, final_sequence


def _is_finalized_master(media_file: Mapping[str, object]) -> bool:
    path = media_file.get("storage_path")
    return (
        isinstance(path, str)
        and path.rsplit("/", 1)[-1].startswith("master.")
        and media_file.get("finalized_by") == "recording_finalizer.master"
    )


def _validate_upload_metadata(
    *, session_uid: str, media_type: str, media_format: str, chunk_seq: int
) -> None:
    if (
        not isinstance(session_uid, str)
        or not _KEY_SEGMENT.fullmatch(session_uid)
        or session_uid in {".", ".."}
    ):
        raise InvalidRecordingMetadata("invalid session identity")
    if media_type not in _MEDIA_TYPES:
        raise InvalidRecordingMetadata("invalid media type")
    if media_format not in _MEDIA_FORMATS:
        raise InvalidRecordingMetadata("invalid media format")
    if (
        isinstance(chunk_seq, bool)
        or not isinstance(chunk_seq, int)
        or not 0 <= chunk_seq <= _MAX_CHUNK_SEQ
    ):
        raise InvalidRecordingMetadata("invalid chunk sequence")


async def upload_chunk(
    repo: RecordingRepo,
    storage: Storage,
    *,
    token_meeting_id: int,
    session_uid: str,
    data: bytes,
    media_type: str = "audio",
    media_format: str = "wav",
    chunk_seq: int = 0,
    is_final: bool = True,
    duration_seconds: Optional[float] = None,
    sample_rate: Optional[int] = None,
    max_chunks: int = DEFAULT_RECORDING_MAX_CHUNKS,
    max_total_bytes: int = DEFAULT_RECORDING_MAX_TOTAL_BYTES,
) -> dict:
    """Process ONE recording chunk upload. ``token_meeting_id`` is the verified MeetingToken's
    meeting_id (the route verifies the token before calling this).

    Returns ``{recording_id, media_file_id, storage_path, status, chunk_seq}``. When the session is
    not yet known and the chunk is non-final, returns ``{"status": "pending"}`` (the bot retries).
    """
    _validate_upload_metadata(
        session_uid=session_uid,
        media_type=media_type,
        media_format=media_format,
        chunk_seq=chunk_seq,
    )
    _validate_limits(max_chunks=max_chunks, max_total_bytes=max_total_bytes)
    session = await repo.find_session(
        meeting_id=token_meeting_id,
        session_uid=session_uid,
    )
    if session is None:
        if not is_final:
            return {"status": "pending"}
        raise SessionNotFound(f"no MeetingSession for session_uid {session_uid}")

    meeting_id = token_meeting_id

    # The lease spans BOTH object upload and JSONB mutation. Erasure takes the exclusive side of the
    # same gate, waits for this block to drain, persists the non-writable state, then sweeps objects.
    async with repo.recording_write(meeting_id):
        owner = await repo.owner_of(meeting_id)
        if owner is None:
            raise RecordingWriteRefused("meeting is not writable")

        # Find / start the bot recording for this session.
        recordings = await repo.get_recordings(meeting_id)
        existing_rec = next(
            (r for r in recordings if r.get("session_uid") == session_uid and r.get("source") == "bot"),
            None,
        )
        recording_id = existing_rec["id"] if existing_rec else recording_numeric_id_for_session(
            user_id=owner,
            meeting_id=meeting_id,
            session_uid=session_uid,
        )

        # One cross-process manifest lock spans limit validation, object IO and the JSONB fold. A
        # finalize-on-read takes the same lock, so it either sees the complete final manifest or
        # refuses before touching storage; it can never publish a pre-final snapshot.
        async with repo.manifest_write(recording_id, media_type):
            recordings = await repo.get_recordings(meeting_id)
            existing_rec = next(
                (
                    r
                    for r in recordings
                    if r.get("session_uid") == session_uid and r.get("source") == "bot"
                ),
                None,
            )
            existing_media = next(
                (
                    mf
                    for mf in (existing_rec or {}).get("media_files", [])
                    if mf.get("type") == media_type
                ),
                None,
            )
            try:
                chunk_sizes, declared_final = _media_manifest(existing_media)
            except RecordingNotReady:
                raise RecordingChunkConflict("recording manifest conflicts with upload") from None
            if existing_media is not None and existing_media.get("format") != media_format:
                raise RecordingChunkConflict("recording media format conflicts with upload")
            if declared_final is not None:
                if chunk_seq > declared_final or (is_final and chunk_seq != declared_final):
                    raise RecordingChunkConflict("recording final chunk conflicts with manifest")
            proposed_final = chunk_seq if is_final else declared_final
            if declared_final is not None and proposed_final != declared_final:
                raise RecordingChunkConflict("recording final chunk conflicts with manifest")
            prior_size = chunk_sizes.get(chunk_seq)
            proposed_count = len(chunk_sizes) + (0 if prior_size is not None else 1)
            proposed_bytes = sum(chunk_sizes.values()) - (prior_size or 0) + len(data)
            if (
                chunk_seq >= max_chunks
                or proposed_count > max_chunks
                or proposed_bytes > max_total_bytes
            ):
                raise RecordingLimitExceeded("recording exceeds configured limits")

            key = chunk_storage_key(
                user_id=owner,
                recording_id=recording_id,
                session_uid=session_uid,
                media_type=media_type,
                media_format=media_format,
                chunk_seq=chunk_seq,
            )

            # A stamped master is immutable. Only an exact replay of a chunk already represented in
            # its final manifest is accepted, and that replay cannot mutate storage or JSONB.
            if existing_media is not None and _is_finalized_master(existing_media):
                if prior_size is None or not await storage.exists(key):
                    raise RecordingChunkConflict("recording master is already finalized")
                if await storage.size(key) != len(data):
                    raise RecordingChunkConflict("recording master is already finalized")
                stored_data = await storage.get(key)
                if not hmac.compare_digest(stored_data, data):
                    raise RecordingChunkConflict("recording master is already finalized")
                return {
                    "recording_id": recording_id,
                    "media_file_id": existing_media.get("id"),
                    "storage_path": key,
                    "status": (existing_rec or {}).get("status", "completed"),
                    "chunk_seq": chunk_seq,
                }

            # Upload the chunk idempotently. The narrow prefix is durable before object creation so
            # erasure can still discover a carrier if the later JSONB fold and compensation fail.
            prefix = key.rsplit("/", 2)[0] + "/"
            await repo.register_recording_prefix(meeting_id, prefix)
            async with repo.chunk_write(key):
                object_already_present = await storage.exists(key)
                try:
                    if object_already_present:
                        if await storage.size(key) != len(data):
                            raise RecordingChunkConflict(
                                "recording chunk conflicts with its existing sequence"
                            )
                        stored_data = await storage.get(key)
                        if not hmac.compare_digest(stored_data, data):
                            raise RecordingChunkConflict(
                                "recording chunk conflicts with its existing sequence"
                            )
                    else:
                        await storage.upload(key, data, content_type=_content_type(media_format))

                    def _fold(recs):
                        ex = next(
                            (
                                r
                                for r in recs
                                if r.get("session_uid") == session_uid
                                and r.get("source") == "bot"
                            ),
                            None,
                        )
                        rid = ex["id"] if ex else recording_id
                        payload, transitioned_ = apply_chunk_to_recording(
                            ex,
                            recording_id=rid,
                            meeting_id=meeting_id,
                            user_id=owner,
                            session_uid=session_uid,
                            media_type=media_type,
                            media_format=media_format,
                            storage_path=key,
                            file_size=len(data),
                            chunk_seq=chunk_seq,
                            is_final=is_final,
                            duration_seconds=duration_seconds,
                            sample_rate=sample_rate,
                        )
                        others = [r for r in recs if r.get("id") != rid]
                        return others + [payload], (payload, transitioned_)

                    rec_payload, transitioned = await repo.mutate_recordings(meeting_id, _fold)
                except BaseException:
                    if not object_already_present:
                        try:
                            await storage.delete(key)
                        except BaseException:
                            raise RuntimeError(
                                "recording upload compensation requires retry"
                            ) from None
                    raise
        recording_id = rec_payload["id"]

        media_file = next((mf for mf in rec_payload["media_files"] if mf["type"] == media_type), {})
        if transitioned:
            log_event(
                "recording_completed", audience="user", span="recordings.upload",
                user_id=owner, meeting_id=str(meeting_id),
                fields={"recording_id": recording_id, "media_type": media_type},
            )
        return {
            "recording_id": recording_id,
            "media_file_id": media_file.get("id"),
            "storage_path": key,
            "status": rec_payload["status"],
            "chunk_seq": chunk_seq,
        }


async def finalize_master(
    repo: RecordingRepo,
    storage: Storage,
    *,
    meeting_id: int,
    recording_id: int,
    media_type: str = "audio",
    max_chunks: int = DEFAULT_RECORDING_MAX_CHUNKS,
    max_total_bytes: int = DEFAULT_RECORDING_MAX_TOTAL_BYTES,
) -> Optional[str]:
    """Build + upload the master for a recording media-file and stamp the JSONB. Idempotent: if the
    master already exists in storage it is reused. Returns the master storage key, or ``None`` when
    there is nothing to finalize.
    """
    _validate_limits(max_chunks=max_chunks, max_total_bytes=max_total_bytes)
    async with repo.recording_write(meeting_id):
        async with repo.manifest_write(recording_id, media_type):
            return await _finalize_master_under_lease(
                repo,
                storage,
                meeting_id=meeting_id,
                recording_id=recording_id,
                media_type=media_type,
                max_chunks=max_chunks,
                max_total_bytes=max_total_bytes,
            )


async def _finalize_master_under_lease(
    repo: RecordingRepo,
    storage: Storage,
    *,
    meeting_id: int,
    recording_id: int,
    media_type: str,
    max_chunks: int,
    max_total_bytes: int,
) -> Optional[str]:
    recordings = await repo.get_recordings(meeting_id)
    rec = next((r for r in recordings if r.get("id") == recording_id), None)
    if rec is None:
        return None
    mf = next((m for m in rec.get("media_files", []) if m.get("type") == media_type), None)
    if mf is None:
        return None

    media_format = mf.get("format")
    storage_path = mf.get("storage_path")
    if media_format not in _MEDIA_FORMATS or not isinstance(storage_path, str):
        raise RecordingNotReady("recording manifest is invalid")
    chunk_sizes, final_sequence = _media_manifest(mf)
    if final_sequence is None:
        raise RecordingNotReady("recording has no declared final chunk")
    if final_sequence >= max_chunks or len(chunk_sizes) > max_chunks:
        raise RecordingLimitExceeded("recording exceeds configured limits")
    expected_sequences = set(range(final_sequence + 1))
    if chunk_sizes.keys() != expected_sequences:
        raise RecordingNotReady("recording manifest is not contiguous")
    aggregate_bytes = sum(chunk_sizes.values())
    if aggregate_bytes > max_total_bytes:
        raise RecordingLimitExceeded("recording exceeds configured limits")
    if (
        type(mf.get("chunk_count")) is not int
        or mf.get("chunk_count") != len(chunk_sizes)
        or type(mf.get("file_size_bytes")) is not int
        or mf.get("file_size_bytes") != aggregate_bytes
    ):
        raise RecordingNotReady("recording manifest census does not match")

    if _is_finalized_master(mf):
        return storage_path

    prefix = storage_path.rsplit("/", 1)[0]
    master_key = master_storage_key(storage_path, media_format)
    chunk_keys = [
        f"{prefix}/{sequence:06d}.{media_format}" for sequence in range(final_sequence + 1)
    ]

    # Validate the complete census and aggregate bound before fetching any body. The finalizer never
    # enumerates the object prefix, so hostile or stray objects cannot create an unbounded list.
    for sequence, key in enumerate(chunk_keys):
        if not await storage.exists(key):
            raise RecordingNotReady("recording chunk is unavailable")
        actual_size = await storage.size(key)
        if actual_size != chunk_sizes[sequence]:
            raise RecordingNotReady("recording chunk census does not match")

    chunks: list[bytes] = []
    for sequence, key in enumerate(chunk_keys):
        chunk = await storage.get(key)
        if len(chunk) != chunk_sizes[sequence]:
            raise RecordingNotReady("recording chunk census does not match")
        chunks.append(chunk)
    try:
        master_bytes = build_recording_master(chunks, media_format)
    except ValueError:
        raise RecordingNotReady("recording media is invalid") from None

    if await storage.exists(master_key):
        if await storage.size(master_key) != len(master_bytes):
            raise RecordingNotReady("existing recording master conflicts with manifest")
        existing_master = await storage.get(master_key)
        if not hmac.compare_digest(existing_master, master_bytes):
            raise RecordingNotReady("existing recording master conflicts with manifest")
    else:
        await storage.upload(master_key, master_bytes, content_type=_content_type(media_format))

    # G3 — stamp the media-file finalized ATOMICALLY (read→modify→write under one row lock), so a late
    # concurrent chunk upload can't clobber the finalized master pointer (the master bytes are already
    # uploaded above, idempotently by key). The mutator re-reads the LIVE recording.
    def _stamp(recs):
        r = next((x for x in recs if x.get("id") == recording_id), None)
        if r is None:
            return recs, None
        m = next((x for x in r.get("media_files", []) if x.get("type") == media_type), None)
        if m is None:
            return recs, None
        live_sizes, live_final = _media_manifest(m)
        if live_sizes != chunk_sizes or live_final != final_sequence:
            raise RecordingNotReady("recording manifest changed during finalization")
        m["storage_path"] = master_key
        m["is_final"] = True
        m["finalized_at"] = _now_iso()
        m["finalized_by"] = "recording_finalizer.master"
        existing_pb = r.get("playback_url") or {}
        r["playback_url"] = {
            "audio": existing_pb.get("audio")
            or (f"/recordings/{recording_id}/master?type=audio" if media_type == "audio" else None),
            "video": existing_pb.get("video")
            or (f"/recordings/{recording_id}/master?type=video" if media_type == "video" else None),
        }
        others = [x for x in recs if x.get("id") != recording_id]
        return others + [r], master_key

    return await repo.mutate_recordings(meeting_id, _stamp)


def _verify_meeting_token(token: str, *, secret: Optional[str] = None) -> dict[str, Any]:
    """Compatibility export for the recording-purpose strict verifier."""
    return verify_meeting_token(token, purpose="recording", secret=secret)
