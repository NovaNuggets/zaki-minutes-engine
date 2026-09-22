# collector — the folded-in transcript backend

The transcript read-side + segment-ingestion the gateway proxies `/transcripts` + `/meetings` +
`/ws/authorize-subscribe` to. **Relocated VERBATIM** from the standalone `transcription_collector`
service into `meeting_api.collector` (P2 unification) — the same shipped code, now a front-doored
sub-package of the one meeting-api modular monolith. Mounted by `meeting_api.app.create_app`
alongside lifecycle / bot_spawn / recordings. Import direction is one-way: the gateway conformance
harness imports this sub-package to drive the shipped collector; this package imports nothing from
conformance.

- **`create_app(store, redis, ...)`** / **`build_router(store, redis, ...)`** — `app.py`. GET
  `/transcripts/{platform}/{native_meeting_id}` (api.v1 `TranscriptionResponse`), GET `/meetings`
  (api.v1 `MeetingListResponse`), POST `/ws/authorize-subscribe` (the gateway `/ws` authorizer hop).
  The narrow internal `GET /internal/meetings/{row}/owner` edge requires `X-Internal-Secret` and
  returns only the exact canonical `{meeting_id,user_id}` pair the agent watcher needs for attribution;
  it never exposes meeting content or trusts producer-supplied owner hints.
  `POST /internal/meetings/{row}/docs` uses the same secret boundary and exact row identity to link the
  generated meeting doc. The store locks that row and derives both `workspace=<row owner>` and the
  native-id-backed path server-side; no global user API key or caller-supplied owner/native can select it.
  `build_router` is the mountable `APIRouter` the unified app composes in (one app, one `/health`);
  `create_app` is the standalone app the conformance harness + this module's tests still drive.
  Identity arrives as the gateway-injected `x-user-id` header (missing → 401).
  Ordinary Vexa rows with no `zaki_capture`/`zaki_retention` tags retain the legacy behavior. Every
  Minutes-tagged owner/share/workspace detail, list, and live-subscription authorization is instead
  evaluated under the shared per-meeting erasure barrier at request time: withdrawn, erasing,
  malformed, or transcript-expired rows are non-enumerating 404/omissions before Redis is touched.
  Audio and summary fields are independently redacted at their deadlines. Managed transcript reads
  preflight a bounded PostgreSQL census and scan (never `HGETALL`) a count/byte-bounded Redis live
  tail, so a retained pre-finalization carrier cannot force unbounded materialization.
- **`ingest` / `consume_segments`** — `ingest.py`. `transcription_segments` stream → `store` →
  publish `tc:meeting:{id}:mutable`. No background loop — the caller drives it (eval `tick`). The
  production store takes one shared meeting-write lease around the complete decoded message: one
  Redis persistence transaction plus its mutable and transcript-stream publications. Once ZAKI
  capture withdrawal is durable, a later message is dropped without persistence or live
  publication; buffered transcript and processed-note PII is purged rather than retried. Retention
  authorization also fails closed on a non-open/malformed record or a materialized scope deadline
  that has already elapsed, even before the TTL sweep stamps its marker. Each PostgreSQL flush takes
  the same shared barrier. The always-on consumer loop is a P3 seam.
- **`carriers.py`** — the one numeric-row key registry for `tc`, live segment hashes, processed
  stream/flag/cursor, and their shared discovery memberships. It performs lifecycle deletion only:
  meeting-api remains the semantic `tc` writer and agent-worker remains the semantic `proc` writer
  (P23). Before deletion it installs the permanent content-free
  `zaki:retention:meeting:{row_id}:fence` hash (`raw` / `processed`). Bot, meeting-api, and Agent
  atomically check that row fence with each carrier write, so a delayed producer cannot recreate PII
  after bounded purge quiescence; Redis authority errors fail closed. TTL/full erasure delete
  carriers before committing their durable marker; a Redis failure therefore leaves the operation
  retryable. Because two consumer groups share the global
  `transcription_segments` source, normal consumption never XDELs after only one group's ACK.
  Lifecycle erasure instead performs a bounded, meeting-targeted scan/XDEL outside PostgreSQL and
  fails for retry when rows cannot be attributed or the target producer does not quiesce. The bot's
  `MAXLEN ~ 100000` is storage defense-in-depth only: under extreme consumer lag it may evict an
  unread row, so it is not an exact-erasure mechanism or a lossless-delivery guarantee.
- **`ports.py`** — `TranscriptStore`, `RedisBus`, `PubSub` (Protocols; real adapters + fakes both
  satisfy them structurally).
- **`adapters.py`** — the real SQLAlchemy-async + redis wiring (lazy imports).
- **`models.py`** — re-exports the shared SQLAlchemy mirror from `meeting_api.sessions.models` (ONE
  `Base` per monolith).
- **`fakes.py`** — `InMemoryTranscriptStore` + `FakeRedisBus` (offline).
- **`obs.py`** — `logevent.v1` trace emitter, bound to `service="transcription-collector"` (the
  collector hop identity is preserved); reads the gateway-forwarded `X-Trace-Id` so this hop's logs
  join the same trace.

Tests (relocated into meeting-api's suite): `../../../../tests/test_collector_api.py`,
`test_ingest.py`, `test_collector_health.py` (+ the `collector_contracts.py` api.v1 oracle).
