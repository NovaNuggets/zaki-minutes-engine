# retention — raw meeting erasure core

Owner-scoped orchestration for deleting one meeting across Minutes-owned carriers. The repository's
`begin_erasure` boundary accepts only an owned terminal (`completed`/`failed`) meeting, atomically
blocks new content writes, and drains in-flight writes before it returns a stable plan. Callers must
durably withdraw capture and confirm bot teardown before the meeting becomes terminal; this slice
has no runtime-stop port and never assumes an active transcript producer has stopped. The core then
deletes every object under validated recording prefixes, purges Redis carriers, commits transcript /
summary / meeting-row deletion last, and returns content-free counts.

The managed HTTP orchestrator first obtains an exact fresh `erasure.v1` Agent receipt and stores
that proof unchanged on the fenced plan. After the recording census it signs and durably stores a
Minutes `erasure.v1` candidate before any object deletion. Retries and concurrent candidates reuse
those exact bytes; the final PostgreSQL transaction checks actual row counts, inserts the same
receipt, and deletes the meeting. A receipt/signature/count conflict aborts instead of issuing a
new or false proof.

This module owns no HTTP route, scheduler, database schema or policy default. `adapters.py` supplies
the production PostgreSQL repository and S3/MinIO prefix adapter. Every recording writer shares the
same PostgreSQL advisory-lock namespace with erasure: chunk upload and master finalization hold a
shared session lock across object + JSONB mutation; `begin_erasure` waits on the exclusive
transaction lock, persists the non-writable state, then releases it. A state-only check without the
lease is invalid.

Before the first object deletion, the core paginates a prefix census and persists the count in the
durable erasure metadata. Deletion repeats bounded 1,000-object batches until the prefix is empty and
the core verifies zero current-object residue. Deleting objects before the database commit keeps
retries safe: a storage failure leaves the database in the non-writable erasing state; a later
database failure retries over an already-empty prefix with the original census count. On an S3
bucket with versioning enabled or suspended, the census and delete cover every object version and
delete marker. Object Lock, legal holds, bucket versioning status and backup expiry remain explicit
Infra launch-drill inputs; a storage refusal leaves the durable plan retryable rather than claiming
erasure.

Prefixes are derived from both committed `data.recordings[]` media paths and the durable
`data.zaki_recording_prefixes[]` pre-upload intent list. Both sources must agree with the meeting
owner/recording/session identity; broad or mismatched entries fail before object I/O.

Public surface: `erase_meeting`, `ErasureReceipt`, `SignedErasureReceipt`, `ErasureFailed`. Ports live in `ports.py`; offline
fakes in `fakes.py`; focused two-tenant proof is `tests/test_zaki_retention.py`; production-boundary
proof is `tests/test_zaki_retention_adapters.py`. `managed_erasure.py` mounts the owner-scoped route
only when repository, storage, Agent verifier/client, and Minutes signing boundary are all supplied.

The raw legacy receipt covers Minutes-owned carriers only; the managed signed receipt additionally
binds the already-verified Agent counts into the cross-spoke GDPR completion proof.
Agent-owned `unit:agent-meet-{row}:out`, workspace/Brain derivatives, and their provenance counts stay
behind the Agent ownership seam. Before activation, sealed S09 must expose the Agent-owned purge
contract: durably tombstone/stop meeting processing, purge those linked derivatives with counts, then
invoke or retry this Minutes erasure after the meeting is terminal. Minutes must not delete Agent
workspace/Brain state directly or bypass agent-only-write ownership.

`ttl.py` is the scheduler-free S02a policy core. It validates explicit UTC expiry instants for audio,
transcript and summary independently, prevents an already-stored expiry from moving later, and runs
an injected-clock batch capped at 500 opaque due scopes. It returns only per-scope counts; candidate
identity and adapter errors never enter the receipt.

`ttl_adapters.py` is the S02b production composition. It selects a deterministic bounded batch from
terminal meetings whose materialized `data.zaki_retention.scope_expiries` are due, excluding erasing
meetings and scopes already recorded in `expired_scopes`. Every mutation takes the same exclusive
meeting advisory lock as full erasure and revalidates the owner/status/unchanged deadline. Audio
object prefixes are validated and emptied before recording metadata is cleared; the marker also
makes later recording writers fail closed. Transcript/summary expiry validates under the meeting
lock, releases PostgreSQL for bounded Redis I/O, then reacquires the lock and revalidates before
removing PostgreSQL transcript/derived JSON and stamping `expired_scopes`. Full erasure similarly
purges Redis only after the durable `erasing` fence and outside its database transaction, then
revalidates before deleting the meeting row. Row-private keys and shared collection members are
deleted narrowly, then re-deleted and verified in bounded passes after source cleanup. The shared
source stream uses bounded target-only scan/XDEL passes; unattributable rows or a producer that keeps
recreating target/source rows fail the operation for retry instead of issuing a false receipt. Redis
lifecycle deletion creates no content and does not change the P23 semantic writers: meeting-api
alone XADDs `tc`, agent-worker alone XADDs `proc`.

Expiry selection validates canonical UTC strings before any PostgreSQL timestamp cast. A missing,
malformed, or non-UTC scope deadline on a retention-tagged meeting is selected as immediately due,
marked as invalid on the opaque candidate, and revalidated under the meeting lock before purge; a
generic aggregate warning is emitted without meeting or user identity. A failed candidate stores a
five-minute per-scope `ttl_retry_after` in the same meeting JSONB. Selection honors that durable
backoff so a permanently failing oldest batch cannot continuously starve later healthy due scopes;
successful expiry removes its retry marker.

Before deleting a selected Redis carrier class, retention permanently and monotonically sets the
content-free row tombstone `zaki:retention:meeting:{numeric_row_id}:fence`: `raw=1` fences source /
transcript carriers and `processed=1` fences summaries, processed notes, processing flags/cursors,
and delayed finalization. The hash is explicitly made persistent, never cleared, idempotent, and
isolated by numeric row id. Consent withdrawal installs both scopes immediately after its durable
database transition and before workload teardown; TTL/full erasure idempotently install the selected
scope again before deletion. A withdrawal fence failure still attempts authoritative workload
teardown but returns the content-free retryable pending result instead of completion. Every external
producer serializes its fence check with its Redis write:
the bot gates source XADD + mutable PUBLISH in one Lua command; meeting-api gates row-feed batches,
terminal `session_end`, and `processed_pending`; Agent gates processing activation/re-arm, processed
notes/cursors, and meeting output. Redis/script errors fail closed. The surviving tombstone is what
turns a bounded empty-carrier observation into a durable no-resurrection guarantee for delayed bot
buffers, lifecycle callbacks, and Agent final beats. A failed Redis/object/DB carrier remains due or
`erasing` for retry, and receipts contain counts only.

The Redis fence does not claim cross-storage atomicity. Agent workspace/Brain derivatives and a
model request already in flight remain S09's explicit responsibility: authoritative worker stop /
tombstone plus Agent-owned derivative purge and counts are still required for the cross-spoke GDPR
receipt.

S02b still owns no scheduler, HTTP route, deployment resource, retention default, authority-layer
resolution, or restoration-horizon claim. Those product/operations choices must compose the store
explicitly and supply already-materialized per-scope deadlines. Its public
`run_production_ttl_once` boundary requires an explicit boolean operator decision and bounded batch
limit; the disabled path returns an empty content-free receipt without touching PostgreSQL or object
storage. S08 may schedule that one-shot boundary later, but this module never enables itself.

## S08 activation gate — shared-source hygiene and boundedness

Before any S08 retention scheduler or Minutes activation is enabled, Infra must census the live
`transcription_segments` stream and retain aggregate evidence that all of the following are true:

- the source contains no more than 125,000 entries, the fail-closed erasure scan ceiling;
- every entry has one valid JSON `payload` attributable to a positive numeric `meeting_id`; and
- a restore/erasure drill proves one selected meeting can be purged without deleting another
  meeting's shared-source entries, and that the permanent fence prevents resurrection.

Any legacy, malformed or unattributable entry must be drained or migrated before activation. A
stream above the ceiling must be reduced and the producing/consumer lag explained before the gate
is signed. Do not raise the scan cap or weaken the unattributable-row refusal to make the drill pass:
those fail-closed checks prevent an incomplete GDPR receipt. The producer's approximate
`MAXLEN ~ 100000` trim is only a storage bound; it is not a substitute for this preactivation census
or for per-meeting erasure proof. S09's Agent-owned stop/tombstone and derivative-purge receipt is a
separate activation gate and remains required.
