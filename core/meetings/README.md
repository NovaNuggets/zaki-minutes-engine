# meetings — the capture domain (join → capture → transcript)

The CAPTURE domain: it joins a meeting, captures + transcribes it, and emits a speaker-attributed
**`transcript.v1`**. It owns the bot (TS, the realtime capture/STT pipeline), the cloud
control-plane (`meeting-api`, Python), and a single-process desktop host — the same `modules/` bricks
composed three ways (desktop process · bot container · split cloud services). TS where realtime audio
+ browser automation live; Python for the control-plane seams.

## Boundary (SoC)

**This domain is about:** joining meetings, capturing + transcribing them, the meeting row + bot
lifecycle, meeting status, and the **transcript** — it is the *single writer* of the transcript carrier
(P23). **It is never about:** the copilot, chat, the agent's workspace, or what gets *extracted* from a
transcript — that is the **agent** domain. Cross-spoke traffic stays on explicit published contracts:
Agent reads Minutes through `zaki-read.v1`, Agent control uses `agent-control.v1`, and coordinated
deletion returns content-free `erasure.v1` evidence. Neither spoke writes the other's carriers. See
[`docs/docs/architecture/control-plane.mdx`](../../docs/docs/architecture/control-plane.mdx).

## Seams

| Direction | Neighbour | Via | What crosses |
|---|---|---|---|
| produces | agent · dashboard · collector | `meetings/contracts/transcript.v1` | speaker-attributed segments + the live mutable bundle |
| consumes | runtime kernel | `runtime/contracts/runtime.v1` | the bot is spawned as a `runtime.v1` workload (`POST /bots` → kernel) |
| consumes | runtime kernel | `runtime/contracts/schedule.v1` | scheduled-meeting triggers into `bot_spawn` |
| produces | the meeting-bot | `meetings/contracts/invocation.v1` | the bot's constructor (`VEXA_BOT_CONFIG` env, minted at spawn) |
| produces | managed Minutes bot | `meetings/contracts/invocation.v2` | isolated v2 constructor with bigint-safe id + retention authority |
| consumes | the meeting-bot | `meetings/contracts/lifecycle.v1` | bot domain-status events → `POST /bots/internal/callback/lifecycle` FSM |
| publishes | the meeting-bot | `meetings/contracts/acts.v1` | control→bot commands over redis `bot_commands:meeting:{id}` |
| produces | subscribers | `meetings/contracts/webhook.v1` | signed outbound delivery envelope (`meeting.*`, `bot.failed`, …) |
| produces | operator Minutes Hub sink | `meetings/contracts/minutes-finalized.v1` | content-free, bigint-safe transcript-finalized event + exact non-bearer HMAC headers |
| produces | gateway / dashboard | `gateway/contracts/api.v1` | `GET /transcripts/{platform}/{native}` · `GET /meetings` · `POST /ws/authorize-subscribe` |
| produces | eval / replay | `meetings/contracts/captured-signal.v1` | raw capture signal teed at the bot's capture bridge |
| produces | replay routing | `meetings/contracts/flagged-issue.v1` | a flagged transcript/attribution bug → its captured signal |
| produces | Agent control plane | `meetings/contracts/agent-control.v1` | exact-row owner resolution + canonical generated-doc attachment |
| produces | Agent worker | `meetings/contracts/zaki-read.v1` | bounded owner-scoped `minutes_read` metadata/content profile |
| produces | gateway / Terminal | `meetings/contracts/minutes-api.v1` | exact managed capture, withdrawal, status, error, and erasure shapes |
| produces | Agent / Identity erasure orchestration | `meetings/contracts/erasure.v1` | content-free signed meeting/account deletion evidence |

## Contracts

**Owns:** [`transcript.v1`](contracts/transcript.v1) · [`lifecycle.v1`](contracts/lifecycle.v1) ·
[`acts.v1`](contracts/acts.v1) · [`webhook.v1`](contracts/webhook.v1) ·
[`captured-signal.v1`](contracts/captured-signal.v1) · [`flagged-issue.v1`](contracts/flagged-issue.v1) ·
[`invocation.v1`](contracts/invocation.v1) · [`invocation.v2`](contracts/invocation.v2) ·
[`agent-control.v1`](contracts/agent-control.v1) ·
[`zaki-read.v1`](contracts/zaki-read.v1) · [`erasure.v1`](contracts/erasure.v1) ·
[`minutes-api.v1`](contracts/minutes-api.v1) ·
[`minutes-finalized.v1`](contracts/minutes-finalized.v1) (the existing contracts are sealed in the
root `contracts.seal.json`; `minutes-finalized.v1` is the reviewed candidate for the next deliberate
seal update).
**Consumes:** [`runtime/contracts/runtime.v1`](../runtime/contracts/runtime.v1) ·
[`runtime/contracts/schedule.v1`](../runtime/contracts/schedule.v1) ·
[`gateway/contracts/api.v1`](../gateway/contracts/api.v1).

## Isolated evaluation

- **L1 contract** — `node contracts/<c>.v1/validate.mjs` (goldens ≡ schema, `gate:schema`).
- **L2 unit / L3 integration** — Python `meeting-api`: `cd services/meeting-api && uv run pytest -q`
  (the whole modular monolith proves out with in-memory stores + fakeredis, no docker/bot).
  TS bot/desktop: `pnpm --filter @vexa/bot test`.
- **L2 replay (offline)** — `pnpm --filter @vexa/bot run replay` (deterministic captured-signal.v1
  replay, `gate:replay`); `node eval/flag.test.mjs` (flag→store→route).
- **L4 live+eval** — [`eval/`](eval/README.md): real bots join a live meeting, speak known clips, and
  score the resulting transcript (`./bin/eval.sh launch|drive|judge`).

## Status

- ✅ delivered — 11 owned contracts published + sealed; `minutes-finalized.v1` is a sealed candidate
- ✅ delivered — meeting-api unified modular monolith (lifecycle FSM · bot_spawn · folded-in collector · recordings)
- ✅ delivered — bot capture pipeline (gmeet/teams/zoom capture · whisper STT · recording · capture bridge tee)
- ✅ delivered — desktop single-process host (gmeet subset)
- ✅ delivered — L4 live eval harness + deterministic offline replay + flag→replay routing
- 🟡 partial — webhooks + scheduling library bricks present, not yet on the unified app's HTTP path
