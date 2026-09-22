# erasure.v1

`erasure.v1` is the candidate content-free proof shared by the Minutes and Agent erasure
owners. A receipt binds its owner, meeting/account scope, subject identifiers, deletion
counts, issue time, signing-key id, and nonce. It never contains transcript, summary,
recording, prompt, or workspace content.

Subjects encode every `user_id` / `meeting_id` as canonical positive decimal text. JSON numbers are
invalid, preventing signed-bigint precision loss at JavaScript boundaries. Count manifests are exact:

| Receipt owner | Meeting/account count keys |
| --- | --- |
| `agent` | `agent_unit_streams`, `agent_workspace_documents`, `agent_brain_records` |
| `minutes` | the three Agent keys plus `meeting_rows`, `transcript_rows`, `summary_documents`, `recording_objects` |

Scope independently fixes the subject: meeting receipts require exactly `user_id` + `meeting_id`;
account receipts require exactly `user_id`. Subsets, extras, or an owner/scope-shaped mismatch are
invalid even when every individual field has the right scalar type.

The canonical algorithm is deterministic:

1. Serialize all fields except `digest` and `signature` as UTF-8 JSON with recursively
   sorted keys and no insignificant whitespace.
2. Set `digest` to `sha256=<hex SHA-256>` of those bytes.
3. Serialize that object plus `digest` the same way, then set `signature` to
   `sha256=<hex HMAC-SHA-256(key, bytes)>`.

Verification must bind the expected owner, scope, and subject from the authenticated
request; signature validity alone is insufficient. A live exchange may additionally
enforce an injected-clock age/future window and a durable nonce set. Historical audit
verification deliberately leaves the age limit unset so an old deletion receipt does
not become unverifiable.

The Agent→Minutes meeting handoff is a durable idempotent completion proof, not a one-time
authorization token. Agent returns the same signed bytes on retries, including retries after an
outage longer than five minutes. Minutes therefore enforces signature, a configured verification
key id, exact
owner/scope/subject/count policy, and a future-time bound, but deliberately applies no maximum age
and no nonce-consumption rule to that same-row retry.

Signing-key rotation uses a bounded one-key overlap, not receipt rewriting. The active key signs new
receipts, while the verifier accepts the current key plus exactly one previous key so an in-flight or
completed durable receipt survives one rotation. The previous verifier can be retired only after all
receipts that reference it clear their retention/audit window; a second rotation must wait or first
prove that no durable receipt still depends on the older key. Agent verification keys are a separate
trust domain from both the Minutes signing key and the ordinary internal-service authentication secret.

The Python reference primitives live in
`services/meeting-api/src/meeting_api/erasure_receipts.py`. The managed meeting deletion route
uses them at both boundaries: it verifies the exact durable Agent proof, then persists the exact
Minutes proof before object deletion and publishes it in the database-delete transaction. The
route remains fail-closed in production until independent managed key sources and the coordinated
Agent signer are configured; the legacy unsigned Agent response is deliberately not accepted.

The executable vectors use `erasure-golden-secret-v1` only as a public test key. It is
not a deployable credential. The validator fixes time at `2026-07-15T12:30:00Z` and covers all four
owner/scope variants plus exact negative owner, scope, count-subset, count-extra, tamper, and stale
replay cases. Authenticated callers still bind their expected owner/scope/subject separately.
