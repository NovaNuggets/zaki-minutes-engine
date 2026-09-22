# minutes-api.v1

`minutes-api.v1` is the exact user-facing managed Minutes boundary. Internal lifecycle
storage may use `needs_help`; every public Minutes response and event uses
`needs_human_help`.

| Method and path | Success | State |
| --- | --- | --- |
| `POST /minutes/captures` | `201 CaptureResponse` | mounted; FastAPI response model bound |
| `DELETE /minutes/captures/{platform}/{nativeMeetingId}` | `200 WithdrawalResponse` | mounted; FastAPI response model bound |
| `GET /minutes/meetings/{meetingId}/status` | `200 StatusResponse` | mounted; owner-scoped row-read port and exact FastAPI response model bound |
| `DELETE /minutes/meetings/{meetingId}` | `200 ErasureResponse` (`scope=meeting`) | mounted; exact signed response model and pre-commit durable receipt path bound |
| `DELETE /minutes/account` | `200 ErasureResponse` (`scope=account`) | reserved contract; account fan-out remains an Identity-owned orchestration handoff |

The capture response deliberately contains only the canonical database row id and product
status; consent evidence, retention deadlines, internal workload ids, webhook credentials,
and meeting data stay server-side. Withdrawal is content-free and retry-safe.

Every database `id` / `meeting_id` on this JSON boundary is canonical positive decimal text, never
a JSON number. This preserves distinct signed-bigint rows in JavaScript; the producer converts its
internal integer explicitly and rejects noncanonical or out-of-range text. `StatusResponse` is a
three-way exact union: nonterminal states carry no terminal attribution, `completed` carries only
`completion_reason`, and `failed` carries only `failure_stage`.

`ErasureResponse` references only the Minutes meeting/account variants of the sealed `erasure.v1`
receipt; an Agent-owned receipt is not a user-facing Minutes erasure response. The managed meeting
route accepts only a fresh, subject-bound signed
Agent receipt, persists an exact Minutes-signed receipt before destructive carrier work, and
atomically stores that same receipt with the final database deletion. Production activation still
requires independent operator key sources and an Agent endpoint that emits the signed contract;
the current legacy Agent response is rejected fail closed. No hard-coded signing secret is used.

Both coded product errors and bounded compatibility `detail` errors are exact. New error codes
require a versioned contract change. `meeting_url_invalid` is the stable public denial for a
rejected managed meeting URL; no internal capture-denial name crosses this boundary. Executable
goldens include negative vectors for internal status vocabulary, wrong terminal attribution,
numeric/overflow identifiers, Agent-owned erasure receipts, and surplus response fields.
