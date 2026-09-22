# terminal — the browser-CLI workbench (Next.js)

## Purpose

The user-facing client for the agent domain: a browser "terminal" that renders a
[dockview](https://dockview.dev) workbench over a registry of surfaces (chat, meeting,
workspace, routines, sessions, tasks). It owns no business logic — every surface talks to
agent-api through thin `/api/*` Next route proxies that keep the backend host (and any key)
server-side. Next.js because the workbench is a rich client UI and the proxies want a
same-origin server runtime (SSE relay, no CORS).

## Seams

| Direction | Neighbour | Via | What crosses |
|---|---|---|---|
| calls | agent-api | `POST /api/chat` (SSE proxy → `${AGENT_API}/api/chat`) | a chat now-dispatch; SSE relay of the agent's output stream |
| calls | agent-api | `GET /api/sessions?subject=` | a subject's chat-session list (resume) |
| calls | agent-api | `GET/POST /api/routines`, `PATCH /api/routines/{name}/enabled`, `DELETE /api/routines/{id}` | list / create / enable·disable / delete a `routine.v1` cron job |
| produces | agent-api | `POST /api/events` (→ `${AGENT_API}/events`) | an `event.v1` Event → a `unit.v1` Invocation → Dispatcher |
| calls | meeting-api (via gateway) | `GET /meetings` + `WS /ws` (`u:{user}:meetings`) | the user's meetings (live + past); live status deltas over the socket (no poll) |
| calls | agent-api | `GET /api/meeting/stream?meeting_id=&session_uid=` (SSE, `EventSource`) | live transcript + copilot output wire |
| calls | identity (via gateway) | `GET/PUT /user/minutes` | browser-scoped operator availability, user capture/read consent, per-scope retention, and bounded repair state |
| calls | meeting-api (via gateway) | `PUT /meetings/{platform}/{native}/intent` | schedule or cancel a planned meeting; generic Vexa `/bots` APIs remain available to non-Minutes clients but the launch UI does not bypass the managed capture edge |
| calls | agent-api | `GET /api/workspace/{tree,file,git}?subject=` (git polled 5s) | workspace tree, file content, the agent's real git state |
| consumes | browser | dockview workbench + surfaces registry (`src/surfaces/index.tsx`) | LEFT lists, CENTER tab-kinds, RIGHT context-kinds, `/`-skill commands |

All upstreams resolve to `AGENT_API_URL` (default `http://127.0.0.1:18100`).

This reference Terminal is not deployed in the managed Minutes topology and receives no Hub
credential. Managed capture, status, withdrawal, and erasure controls are therefore absent from the
UI, and retained compatibility helpers fail locally without issuing a request. Those controls live in
the ZAKI Hub. The catch-all rejects both `/api/minutes` and every nested managed path before proxy
authentication, body buffering, or network I/O; a browser user key is never substituted for the
dedicated Hub credential. The Terminal continues to expose the human's browser-scoped consent and
retention panel through `/api/user/minutes`.

The frozen meetings `api.v1` still represents row IDs as JSON integers. Terminal rejects any value
outside JavaScript's safe-integer range before it reaches state or an action; the `api.v2` handoff must
represent positive decimal row IDs as strings end-to-end before the backing sequence can cross that range.

## Proxy authentication

REST, chat, workspace, meeting-stream, and `/ws` proxy requests use the signed-in user's httpOnly
`vexa-token` cookie. Without it they return `401` locally, before a request body is consumed or an
upstream connection is opened. Deployment service credentials, including `VEXA_BOT_API_KEY`, are
never browser identity.

For a loopback-only self-host, an operator may explicitly set
`VEXA_TERMINAL_SHARED_KEY_MODE=true` plus a user-scoped `VEXA_API_KEY`. The flag defaults off and the
server refuses to start when shared mode is paired with a missing key, a missing/public
`NEXTAUTH_URL`/`TERMINAL_URL`, or a non-loopback listener/Docker publication. Compose and Lite pass
the real host bind as `VEXA_TERMINAL_HOST_BIND`; setting only a localhost URL does not make a public
bind safe. The shared user key is still validated by admin-api before Terminal-owned STT and API-token
routes spend operator credentials or resolve the user.

Composer dictation is a sensitive, credentialed edge: `/api/stt` requires that validated user,
accepts at most 4 MiB per request (about two minutes of 16 kHz mono PCM), refuses redirects, bounds
the upstream response, and marks every response `no-store`. Per-user STT billing quota/rate accounting
is a separate launch follow-up; authentication and byte bounds are not a substitute for it.

## Contracts

**Owns:** none — the terminal defines no `*.v1`; it is a pure client of the agent domain.
**Consumes:** `core/agent/contracts/event.v1` (the `/api/events` ingress shape), `routine.v1`
(routines CRUD), `unit.v1` (chat + SSE relay), and the meeting/workspace surfaces of
`core/agent/services/agent-api`. Schemas are sealed in `contracts.seal.json` (repo root) — the
proxies forward bodies verbatim, they do not re-declare schemas.

## Terminal modes

`NEXT_PUBLIC_TERMINAL_MODE` (build-time public env — inlined by `next build`, changing it requires a
rebuild; see `src/app/mode.ts`):

- unset / empty (default) — every surface registers.
- `meetings` — a meetings-only terminal: only the **Meetings** list, the **meeting/canvas** tabs, and
  the **API Tokens** surface register. The agent surfaces (chat, sessions, workspace/knowledge,
  routines) and their palette commands never register, and the server proxy refuses agent-api paths
  with 404 (`src/app/api/proxyMode.ts`), so no agent traffic is possible from this deployment.

Pass it as a Docker build arg (`--build-arg NEXT_PUBLIC_TERMINAL_MODE=meetings`) or via the
commented example on the `terminal` service in `deploy/compose/docker-compose.yml`.

## API tokens (self-serve)

The **API Tokens** left list (`src/surfaces/tokens.tsx`) lets the logged-in user list, mint
(scopes `bot`/`tx`/`browser`, optional name + expiry) and revoke their own tokens. The `/api/tokens`
routes call admin-api with the server's `VEXA_ADMIN_API_KEY` (admin tier, like the login flow) and
scope every operation to the user resolved from the httpOnly auth cookies — a `user_id` from the
client is never accepted. The minted token value is returned once, at creation.

## Isolated evaluation

Standalone tests plus build/typecheck:

```bash
pnpm install && pnpm test && pnpm build
pnpm dev                        # next dev -p 3000 — drive surfaces against a live agent-api (L4)
```

## Status

- ✅ delivered — dockview workbench + surfaces registry (chat / meeting / workspace / routines / sessions / tasks)
- ✅ delivered — `/api/chat` SSE proxy + resumable chat sessions (`/api/sessions`)
- ✅ delivered — routines board over `/api/routines` CRUD
- ✅ delivered — workspace files + docs viewer + git source-control panel (5s poll)
- ✅ delivered — live meeting surface: `/meetings` + `/ws` status (no poll) + `/api/meeting/stream` SSE; unsafe api.v1 numeric row IDs are rejected before state/actions
- ✅ delivered — browser-scoped Minutes settings: operator availability separated from user capture consent, agent-read opt-in, per-scope retention, and one-click bounded repair
- ⬜ external Hub — managed Minutes capture/status/withdrawal/erasure (deliberately unavailable here; no Hub credential is projected)
- ✅ delivered — generic event ingress proxy (`/api/events` → `event.v1`)
- ✅ delivered — cookie-only authenticated identity for hosted mode; no hardcoded subject or browser service-key fallback
- ✅ delivered — OAuth login plus an explicit default-off, exact-allowlist, loopback-only direct-login test mode
- ✅ delivered — real owner-scoped meetings list (live + past) with a recorded view
- ⬜ planned — routines type-toggle (agent | meeting)
- ⬜ planned — meeting ↔ doc cross-links
- ✅ delivered — a single owner-scoped gateway WS client for meeting status deltas
