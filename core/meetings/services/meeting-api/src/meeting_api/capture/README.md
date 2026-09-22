# ZAKI capture profile

This module is the fail-closed service boundary for a ZAKI-managed Minutes capture. It does not add
an HTTP route or deployment flag. `request_capture(...)` intersects four independently supplied
authorities before it touches the meeting repository or runtime:

1. the operator has enabled Minutes capture;
2. the tenant has enabled capture and recorded a versioned lawful-capture attestation;
3. the user requested this capture; and
4. quota permits it.

The grant is bound to one tenant, user, platform/native meeting identity, and—when supplied—an exact
SHA-256 of the validated meeting URL. It is valid for at most five minutes and single-use: the
meeting row stores only a SHA-256 of its opaque grant id, and the atomic spawn transaction rejects
replay within that tenant even after withdrawal moves the first row out of the active set. Spawn and
withdrawal use the same per-user transaction lock; the latest tenant/user/meeting scope withdrawal
is a monotonic tombstone that also rejects a different grant unless its authorization is strictly
newer than the withdrawal. A different tenant neither consumes the grant identity nor inherits the
tombstone, even when user and meeting identifiers coincide. The allowed
path always joins as **ZAKI Notetaker**, enables recording and transcription, stores only content-free
policy evidence under `meeting.data.zaki_capture`, and materializes immutable UTC
audio/transcript/summary expiries under `meeting.data.zaki_retention`. Explicit meeting URLs must use
the approved host for their declared platform. Callers cannot override the bot name or inject
evidence. Missing, malformed, expired, mismatched, or disabled authority returns a stable
`CaptureDenial` before repository/runtime mutation.

Those three stored expiries remain independent deletion deadlines; none is moved later when the
meeting ends. Their earliest absolute instant is also the managed capture cutoff. The spawn contract
threads that instant as `invocation.v1.captureExpiresAt` and derives the runtime workload's
`maxLifetimeSec` from the same frozen authority. The bot starts a bounded graceful guard before the
instant, synchronously latches its transcript and recording sinks closed, permanently fences both
Redis carrier scopes, then stops capture. The local latch means even a Redis outage/fence timeout
cannot let a recovered connection accept the pipeline's final flush; the permanent fence covers
other/delayed producers. The atomic transcript write also compares Redis `TIME` with the absolute
cutoff, so a command queued before local revocation cannot append after expiry. Long deadlines are
scheduled in bounded one-day chunks to avoid Node's signed-32-bit timer overflow. The runtime
lifetime is the hard-stop backstop if graceful teardown wedges. PostgreSQL/object-storage writers
still recheck their own scope's wall-clock expiry, so a delayed recording upload is refused even if
the worker misses its graceful window.

The runtime kernel still rechecks owner quota. If that defense-in-depth check rejects after the
meeting row was reserved, the row is made terminal (`failed`) and its capture evidence becomes the
named `quota_exhausted` non-capture state; it never remains an active `requested` orphan.

`withdraw_capture(...)` is the second S03 tracer. It is tenant/user/meeting scoped and takes the
exclusive meeting-write barrier before storing `zaki_capture.state=withdrawn`, the original UTC
withdrawal instant, `withdrawal_reason=consent_withdrawn`, and `stop_requested=true`. A
non-terminal row first stores `teardown_state=pending`; an already-terminal row is durable stop
evidence and stores `teardown_state=confirmed` immediately without runtime I/O. Every non-terminal
withdrawal immediately installs the permanent numeric-row Redis tombstone with both `raw=1` and
`processed=1`, before any runtime teardown. `RedisCaptureCarrierFencer` is the production adapter
over the shared carrier-fence contract. This atomically closes the bot's source-XADD + mutable-
PUBLISH egress and the Agent's processing activation/write paths even when a buffered final beat
wakes after withdrawal. A fence error is recorded without its diagnostic text, does not skip the
authoritative hard delete, and still raises the stable content-free pending outcome so a repeated
withdrawal retries the monotonic fence without re-deleting a confirmed workload.

Every non-terminal withdrawal without prior durable confirmation hard-deletes the known workload
as the primary stop action; the kernel may perform its own graceful stop before returning. Only
afterward is the bot leave command published as a best-effort courtesy. Even a positive Redis
subscriber count proves neither that the intended bot handled the command nor that it stopped. A
missing runtime/workload, non-2xx response, failed delete, or unconfirmed Redis fence raises the
stable, content-free
`CaptureTeardownUnconfirmed` while preserving the first durable withdrawal timestamp for retry.
The teardown state remains pending when stop proof is missing, but may already be confirmed when
only the idempotent Redis fence needs retry. A confirmed
2xx hard delete is recorded through one narrow, retrying CAS as `teardown_state=confirmed` and the
terminal consent-stop projection: preserve an existing `completed`/`failed` state, otherwise set
`status=completed`, `completion_reason=stopped`, and `end_time`. The capture's
`withdrawal_reason=consent_withdrawn` keeps the more specific attribution. This closes terminal-only
erasure without waiting for an impossible callback from the deleted workload. Repeated withdrawal
preserves the first timestamp, retries pending teardown, and never re-deletes a confirmed workload.
Physical deletion without a successfully persisted CAS still returns the stable unconfirmed outcome;
GDPR erasure remains blocked until both teardown and terminal evidence are durable.
The spawn path performs the same CAS and refreshes its response when withdrawal races workload
creation, so it cannot report that a concurrently withdrawn capture started.

Recording and durable meeting transcript writers take the shared side of the same cross-process
barrier and refuse entry once withdrawal is durable. A write already holding the lease drains; a
later write cannot touch PostgreSQL, object storage, or recording JSONB. The permanent Redis fence
separately covers bot and Agent producers that cannot share the PostgreSQL lease. Buffered Redis
transcript data is purged when its durable flush observes withdrawal rather than being retained for retry. Late
non-terminal bot callbacks cannot move the terminal meeting back to `active` or enter the durable
audit trail; a genuine terminal callback remains an idempotent acknowledgement and retains the
capture attribution.

No public withdrawal route is introduced here. Hub/BFF routing, settings persistence, secrets,
charts, and activation belong to later slices.
