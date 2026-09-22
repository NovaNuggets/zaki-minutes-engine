# deploy/compose/tests — the gate:compose stack-readiness proof (P5)

`stack_test.py` brings up the **real** v0.12 compose stack (see `../docker-compose.yml`), proves it
is ready to run the vexa bot, and tears it all down (`down -v`) in a guaranteed finally — the
"fully tested / proven ready" deliverable, run via `make -C deploy/compose stack-test`.

`minutes-config-contract.sh` is a no-stack render check: it proves policy-owning APIs are hard-pinned
Minutes-off, the bundled Agent receives only forced-false gates and no URL/credential, intent reaches
the code-78 startup guard, operator overrides cannot activate meeting/admin, and `.env.example`
contains no Hub/read/erasure credential value. It also proves the dedicated Gateway identity proof
reaches only Gateway + Agent, runtime cannot broker it into workers, and Redis has authenticated,
bounded, no-eviction memory semantics. Its optional previous key is independently proven
verifier-only on Agent; Gateway and all other services never receive it.

The `stack` session fixture (`conftest.py`) owns the whole lifecycle: `docker compose up -d --build`
under an isolated project name, a bounded wait for every service `healthy`, then `down -v` on exit.
Everything polls with bounded timeouts — never sleep-and-hope. Absent docker → the module self-skips
(green-or-skip).

## What it proves

| step | proof | always-on? |
|------|-------|------------|
| 1  | `/health` 200 on gateway·meeting-api·runtime·admin-api; Redis refuses anonymous `PING` with `NOAUTH` and accepts the operator-authenticated probe | yes |
| 2  | admin-api mints a scoped token; gateway accepts it (200), rejects missing/invalid (401) + out-of-scope (403); a proxied call reaches meeting-api | yes |
| 4  | XADD golden segments → the collector consumer stores them (live segment hash) + publishes `tc:meeting:{id}:mutable` → a `/ws` client (through the gateway) receives the live frame | yes |
| 5  | upload a chunk via the bot's `/internal/recordings/upload` → the object lands in minio; finalize → a master is assembled in minio | yes |
| 6c | `continue_meeting` reuses the meeting row + accumulates a session; the prior transcript survives | yes |
| 6d | a user at `max_concurrent_bots` gets `429` on the N+1; a freed slot admits the next | yes |
| 6b | the join-retry re-spawn wiring is present in the live image; the **backoff proof leans on the offline P3 `test_join_retry.py`** (forcing a real transient join-failure on a live bot is slow/flaky) | yes (wiring) |
| 3  | `POST /bots` → meeting-api spawns via runtime → a real `vexa-mtg-…` container appears in `docker ps` AND the meeting advances to `joining`; then the bot is stopped + cleaned | **`COMPOSE_BOT=1`** |
| 6a | start-then-stop a real bot → terminal; the leave-command channel wiring is asserted | **`COMPOSE_BOT=1`** |

## Run

```bash
make -C deploy/compose stack-test        # always-on subset (what gate:compose runs)
make -C deploy/compose stack-test-bot    # + the real bot-spawn proof (COMPOSE_BOT=1, slow ~7GB image)
```

On a host where other services hold the default ports (or a dev stack is already up), run the proof
beside them with `COMPOSE_DYNAMIC_PORTS=1` so the gate binds free random host ports.

### Always-on vs `COMPOSE_BOT`-gated — why

Steps 3 and 6a drive a **real `vexaai/vexa-bot:v012` container** (Playwright browser, ~7GB image)
through its lifecycle (the published `vexaai/vexa-bot:dev` is the old 0.10 line and fails the
`lifecycle.v1` handshake; a source-built bot from `make -C deploy/compose bot` also works). That
is slow and inherently flaky for a routine gate (browser boot, a dummy
meeting URL, callback timing). The fast steps (1·2·4·5·6c·6d·6b) exercise the entire control plane —
auth, the transcript bus, recordings→object-store, and the lifecycle/scheduling wiring — with no real
bot, deterministically, so they are the always-on `gate:compose`. The bot-spawn proof is **runnable**
(`COMPOSE_BOT=1`) and is the thing that finally says "a real bot spawns and reaches `joining`".

### RESOLVED — the real bot-spawn (step 3) now passes

The three carve gaps that once blocked a live `COMPOSE_BOT=1` bot spawn are all fixed; a `POST /bots`
on the live stack now spawns a real `vexa-mtg-…` container (verified end-to-end):

1. **meeting-api `MEETING_TOKEN_SECRET`** — fixed and separated. `../docker-compose.yml` projects the
   dedicated signer only into meeting-api; `ADMIN_TOKEN` is absent there. Startup refuses a missing or
   reused signer, so `POST /bots` can mint a scoped MeetingToken without sharing administrative auth.
2. **`updated_at` tz mismatch** — fixed. The repo writes tz-naive (`datetime.now(timezone.utc).replace(tzinfo=None)`)
   and the column uses a server-side `onupdate=func.now()`, so there is no offset-aware/naive subtraction.
3. **runtime image `docker` CLI** — fixed (was a misdiagnosis). The `DockerBackend` talks to
   `/var/run/docker.sock` over the **socket HTTP API** (`requests_unixsocket`), needing no `docker`
   client and no daemon in the image; `runtime/Dockerfile` installs no docker package at all.

The always-on subset (never spawns a real container) remains fully green on the live stack.

### Note on `GET /transcripts` vs the `:mutable` frame (step 4)

In this carve the collector's live-path stores in-flight segments in the redis hash
`meeting:{id}:segments` and publishes the `:mutable` update; `GET /transcripts` reads the postgres
`transcriptions` table (the parent's background db-writer that flushes the hash → table is out of this
carve's scope). So step 4 proves the **store** side via the redis hash and the **publish** side via the
live `/ws` frame — the two observable halves the running stack actually produces.
