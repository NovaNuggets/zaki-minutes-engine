# recordings — chunk upload + finalize → `meeting.data` JSONB

Ported from the parent `recordings.internal_upload_recording` + `recording_finalizer` +
`recording_jsonb`. The bot streams recording chunks (authenticated by the MeetingToken it carries);
each chunk lands in object storage and is folded into the recording's JSONB payload under
`meeting.data['recordings']` — there is **NO separate recordings table**. Finalize concatenates a
recording's chunks into a master via the golden-locked `build_recording_master` codec (recording.v1)
and stamps the JSONB media-file.

Every upload resolves the session by the pair `(meeting_id, session_uid)`. The per-spawn
MeetingToken supplies both identities from strictly verified signed claims, and the submitted
`session_uid` must match before any carrier read or write. Platform-wide internal credentials are
not accepted on this content edge.

## Front door
- `build_router(repo, storage)` — the mountable routes (the unified app mounts them): POST
  `/internal/recordings/upload`, GET `/recordings`, GET `/recordings/{id}/master`.
- `upload_chunk(...)` / `finalize_master(...)` — the flow core (callable directly in tests).
- `apply_chunk_to_recording` / `chunk_storage_key` / `master_storage_key` /
  `new_recording_numeric_id` — the pure JSONB record materializers (no IO/DB).
- `Storage` / `RecordingRepo` ports + `SessionNotFound`.
- `adapters.build_production_router(...)` — wire with real MinIO/S3 + SQLAlchemy.
- `fakes` — `InMemoryStorage` / `InMemoryRecordingRepo` (offline drivers).

`upload_chunk` and `finalize_master` hold `RecordingRepo.recording_write(meeting_id)` for their
entire object-storage + JSONB mutation. The SQLAlchemy adapter implements that lease with a shared
transaction-level PostgreSQL advisory lock and refuses meetings whose durable
`data.zaki_retention.state` is `erasing`, whose audio scope has expired, or whose ZAKI capture has
been withdrawn. Retention erasure and capture withdrawal use the exclusive side of the same lock.
A cancelled boto3 offload is awaited before the lease exits, preventing a worker thread from
creating a ghost object after erasure sweeps the prefix. Before the first object write, the narrow
session prefix is durably deduplicated in `data.zaki_recording_prefixes`; this remains discoverable
even if the later recording JSONB fold and compensating exact-object delete both fail. Routes map
write refusal to a content-free `409`.

The upload route reads at most `RECORDING_CHUNK_MAX_BYTES + 1` bytes and returns `413` before any
carrier mutation when the chunk is larger. The operator setting defaults to 8 MiB; the deployment
proxy/body limit must use the same value or a slightly larger multipart-envelope allowance.

Every media file also carries a private deterministic manifest in
`metadata.zaki_chunk_sizes` plus `metadata.zaki_final_chunk_seq`. A cross-process per-manifest lock
serializes chunk folds and finalize-on-read. Finalization requires a declared final sequence and
the exact contiguous census `0..final`, generates those bounded object keys directly (never an
unbounded prefix listing), validates every object size before fetching a body, and only then builds
the master. `RECORDING_MAX_CHUNKS` (4,096) and `RECORDING_MAX_TOTAL_BYTES` (64 MiB) are enforced on
upload and again before assembly. Finalization is additionally capped at two concurrent assemblies;
startup rejects any size/concurrency combination whose estimated three-copy working set reaches the
configured 512 MiB recording budget or the 1 GiB pod limit. Raw reads cap each Range at 8 MiB before
fetching object bytes. Missing/inconsistent manifests return a content-free `409`; over-limit
manifests return `413`. Once stamped, the master is immutable: only byte-identical replays of
already-manifested chunks are accepted.

Recording reads treat audio retention as authorization at request time. The repository filters
past-expiry, withdrawn, erasing, or malformed capture/retention rows before list/detail/master/raw
resolution, even if the asynchronous TTL deleter has not yet removed the object. An unavailable
record is exposed uniformly as `404`, and no object-store read is attempted.

## The JSONB shape
`meeting.data['recordings']` is a list of recording dicts (`id`, `session_uid`, `source="bot"`,
`status`, `media_files[]`). Each `media_files[]` entry tracks per-type cumulative
`file_size_bytes` / `chunk_count`, the chunk/master `storage_path`, and `is_final` / `finalized_by`
(Pack U.7 master-preserve + sticky-COMPLETED status are ported verbatim). `completed` is reached
only when the declared final manifest is contiguous; merely receiving a sparse final sequence is
not completion authority.

## Remaining seam
Lifecycle-driven server-side finalization is not wired; this carve finalizes lazily on read via
`GET /recordings/{id}/master`. The raw route serves only a stamped master, requires a cheap bounded
size check, and supports single HTTP byte ranges without falling back to an unbounded full fetch.

Tests: `../../../tests/test_recordings.py`. Codec golden: `../../../tests/test_recording_golden.py`.
