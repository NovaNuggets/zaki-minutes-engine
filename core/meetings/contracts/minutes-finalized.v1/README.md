# minutes-finalized.v1

> **SEALED CANDIDATE.** Review the exact schema and vectors, then add its hash with the repository's
> deliberate `seal:contracts` step before merge.

`minutes-finalized.v1` is the dedicated operator-owned notification that one Minutes transcript
has been durably finalized. It is independent of `webhook.v1`: it never uses a user's webhook URL,
secret, event subscriptions, or legacy bearer header, so existing strict webhook consumers never
receive a new event vocabulary under an old contract.

The envelope is deliberately content-free. It carries only a stable event id, creation time,
canonical meeting-row identity, artifact/state constants, and an idempotency key. `meeting_id` is
canonical positive decimal text, not a JSON number, so JavaScript receivers cannot silently round a
PostgreSQL bigint. Transcript text, summaries, recordings, prompts, speaker names, URLs, and user or
workspace identity are forbidden by the exact schemas (`additionalProperties: false`).

The sender serializes the envelope as canonical compact JSON and signs
`<X-Webhook-Timestamp>.<raw-body>` with HMAC-SHA-256. Every delivery has exactly these four headers:

- `Content-Type: application/json`
- `X-Webhook-Key-Id`
- `X-Webhook-Timestamp`
- `X-Webhook-Signature: sha256=<hex>`

`Authorization` is absent and rejected. The key id selects an operator-owned verification key; the
secret is process-local and never enters the outbox or wire body. Pending events are re-signed with
the current key on each delivery attempt, which supports overlap-based key rotation.

The Meeting API builder validates the envelope at construction, and the sink validates the exact
header set before transport. Goldens are validated by `validate.mjs` through `gate:schema`.
