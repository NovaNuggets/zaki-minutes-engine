# invocation.v2 — managed Minutes bot constructor

`invocation.v2` is a separate, managed-capture constructor. It does not tighten the sealed v1
schema in place. It adds an explicit `contractVersion`, a canonical decimal-string `meeting_id`,
the absolute `captureExpiresAt` fence, and the complete per-scope `managedRetention` authority.
Unlike v1, it has no `redisUrl`: the ephemeral managed bot receives only a spawn-scoped
MeetingToken and the authenticated `transcriptIngestUrl` / `retentionFenceUrl`. Meeting-api derives
the meeting from that token and its authoritative session row, then performs the existing
retention-guarded Redis/database write with its own service credential.

The managed constructor is deliberately complete: the MeetingToken, immutable session/native
identity, lifecycle callback, funded STT edge, recording upload edge, and enabled capture modes are
required at schema validation. Managed recording and transcription are always on; voice acts are
off until a separately authenticated, session-scoped command contract exists.

Ordinary bots continue to receive `invocation.v1` on the `meeting-bot` runtime profile. A managed
producer may send v2 only through the independently enabled `meeting-bot-v2` profile. The workload
also carries `VEXA_INVOCATION_CONTRACT=invocation.v2`, so a version-aware bot selects this schema at
boot. If either the meeting-api gate or runtime profile is absent, capture fails closed; no v2
payload is sent to an old `meeting-bot` profile.

`captureExpiresAt` must equal the earliest of the audio, transcript, and summary scope deadlines.
The schema pins the shapes; both producer and consumer enforce that cross-field invariant.
