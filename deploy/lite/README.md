# Vexa Lite (v0.12)

The whole v0.12 control plane in **one container**. The simplest way to self-host — `make lite`
from the repo root provisions PostgreSQL + MinIO and runs everything else in a single image.

## Why

Everything except the datastores runs in one container — gateway, admin, meeting-api, runtime,
agent control plane, redis, and the X11/audio stack. No Docker socket, no per-service
containers. The runtime uses the **process backend**: meeting bots and agent workers run as
**child processes** inside the container, not socket-spawned containers.

- One app container instead of eight + on-demand workers
- Full API + terminal + meeting bots + agent
- No GPU required — transcription runs via an external API (or your own GPU service)

> **Minutes launch boundary:** Lite is a local/reference topology, not a supported Minutes launch
> topology. Its entrypoint rejects every Minutes setting, including false feature flags, read/Hub
> credentials, historical erasure keys, finalized delivery, retention, and Minutes runtime-profile
> overrides. Use the managed Helm profile with the bundled Agent/Gateway/Terminal off and external
> Nullalis for Minutes launch. Ordinary upstream meeting bots remain available when no Minutes
> configuration is supplied.

## Quick start

From the repo root:

```bash
make lite
```

Provisions PostgreSQL + MinIO sidecars, pulls/builds the Lite image, starts them on a private
bridge with loopback-published front doors, and probes those front doors. Set `TRANSCRIPTION_SERVICE_URL` /
`TRANSCRIPTION_SERVICE_TOKEN` in the repo-root `.env` for transcripts (get a token at
`vexa.ai/account`, or self-host the transcription service on a GPU).

After it finishes:

- **Terminal:** `http://localhost:3001` (the agent-domain browser-CLI workbench)
- **API:** `http://localhost:8056` (the gateway — auth, routing) · docs at `/docs`
- **Agent API:** `http://localhost:8100`

Lite publishes all three front doors on `127.0.0.1` by default. For remote use, keep that default and
use an SSH tunnel. A deliberate network-facing bind (`make lite HOST_BIND=0.0.0.0
TERMINAL_PUBLIC_URL=https://vexa.example.com`) must also disable
`VEXA_TERMINAL_SHARED_KEY_MODE`, configure normal OAuth/cookie authentication, replace every
development secret, and put TLS/reverse-proxy controls in front. The Terminal refuses shared-key or
direct-email login on a public bind even if its declared URL says localhost.

To stop: `make lite-down` (data volumes are kept; `docker volume rm vexa-lite-pgdata
vexa-lite-miniodata` to wipe).

## What's inside

Supervised by `supervisord`:

| Service | Port | Role |
|---|---|---|
| gateway | **8056** | the one front door — auth, scopes, routing, `/ws` fan-out |
| admin-api | 8001 | users + API keys + `/internal/validate` |
| meeting-api | 8080 | bots, transcripts, recordings (→ MinIO) |
| runtime | 8090 | spawns bot + agent workers as **child processes** (process backend) |
| agent-api | **8100** | the agent control plane — dispatch, chat (SSE), routines |
| terminal | **3001** | agent-domain browser-CLI workbench (Next.js + custom `server.mjs` SSE/`/ws` relay) |
| redis | 6379 | bus + scheduler + per-dispatch streams (internal) |
| Xvfb · fluxbox · PulseAudio | :99 | display + audio for the headful bot browser |
| x11vnc · noVNC | 5900 / 6080 | browser view (debugging) |

External (the `make lite` sidecars): **PostgreSQL** (metadata) and **MinIO** (recordings +
agent workspaces).

### Architecture

```
+--------------------------------------------------------------+
|                    Vexa Lite container                       |
|                                                              |
|  gateway  admin-api  meeting-api  runtime                    |
|   :8056     :8001      :8080       :8090                      |
|                                                              |
|  agent-api   redis   Xvfb  fluxbox  PulseAudio  noVNC        |
|   :8100      :6379    :99                        :6080       |
|                                                              |
|  bot processes (Playwright)  +  agent workers (Claude Code)  |
|     ← runtime spawns as child processes (process backend)    |
+--------------------------------------------------------------+
        |                    |                    |
        v                    v                    v
   Transcription        PostgreSQL             MinIO
     (external)         (sidecar)             (sidecar)
```

In [compose mode](../compose/README.md) the runtime spawns each bot/agent in its **own
container** via the Docker socket; in lite they are child processes sharing one display/audio.
Lite still separates workload code from its root control plane: every meeting bot crosses the
trusted launcher into its own ephemeral numeric uid (with the `vexa-bot` primary group and only
`pulse-access` supplemental access), an empty capability set, and `no_new_privs`. Concurrent bots
therefore cannot read one another's MeetingToken through `/proc`. The bot can read `/app`, write `/tmp`, and use the
shared Xvfb/system PulseAudio services, but cannot list or traverse the root-only `/run/vexa`
operator-secret tier.
Lite's POSIX Agent mapping accepts canonical numeric subject ids `0..99999`; larger platform ids fail
closed in Lite instead of colliding with reserved workload uid/gid ranges. Hosted deployments use
container/pod isolation and do not have this single-host uid limitation.

## Configuration

The repo-root `.env` (auto-seeded from `deploy/compose/.env` if present, else minimal):

| Variable | Default | Description |
|---|---|---|
| `TRANSCRIPTION_SERVICE_URL` / `_TOKEN` | — | STT endpoint + key. Unset → bots capture, no transcript |
| `ADMIN_TOKEN` / `ADMIN_API_TOKEN` | `changeme` | Identity-admin credential. The entrypoint stores it in a root-only file and projects it only to admin-api, Terminal, and the local Terminal key provisioner; meeting-api never receives it. |
| `INTERNAL_API_SECRET` | `lite-internal-secret` | Platform service-to-service credential, exec-projected to admin-api, agent-api, meeting-api, gateway, and Terminal—but not runtime. |
| `RUNTIME_CONTROL_SECRET` | `lite-runtime-control-secret` | Dedicated runtime controller credential, exec-projected only to runtime and its agent/meeting controllers. |
| `RUNTIME_CALLBACK_SECRET` | `lite-runtime-callback-secret` | Dedicated runtime callback credential, projected at exec only into runtime and meeting-api. Runtime trusts exactly `http://localhost:8080`. |
| `MEETING_TOKEN_SECRET` | `lite-meeting-token-secret` | Dedicated MeetingToken signer/verifier, projected at exec only into meeting-api; never reuse the admin token. |
| `GATEWAY_IDENTITY_SECRET` | `lite-gateway-identity-secret-local-v1` | Dedicated 32..512 printable-ASCII Gateway → Agent identity proof. Only the Gateway and Agent exec wrappers reference it; no internal-secret fallback. |
| `GATEWAY_IDENTITY_PREVIOUS_SECRET` | — | Optional distinct 32..512-character verifier-only overlap. Only the Agent exec wrapper exports it; Gateway never signs with it. |
| `REDIS_MAXMEMORY` | `512mb` | Positive internal Redis dataset ceiling. Keep container memory above it for allocator and AOF rewrite headroom; `noeviction` rejects writes at the ceiling. |
| `IMAGE_TAG` | `latest` | the `vexaai/vexa-lite` tag to pull (a local `vexa-lite:dev` build wins) |
| `HOST_BIND` (make variable) | `127.0.0.1` | Real host publication for gateway, Terminal, and agent-api. Prefer an SSH tunnel instead of changing it. |
| `TERMINAL_PUBLIC_URL` (make variable) | `http://localhost:3001` | Declared Terminal origin used for cookies/OAuth and checked alongside `HOST_BIND`. |
| `VEXA_TERMINAL_SHARED_KEY_MODE` | `true` | Lite-only zero-login mode backed by its provisioned user key. Set `false` to require login cookies. Shared mode requires both a loopback public origin and loopback `HOST_BIND`. |
| Any `ZAKI_MINUTES_*`, `ZAKI_READ_TOKEN_MINUTES`, `ZAKI_AGENT_ERASURE_*`, `MINUTES_TTL_*`, `MINUTES_BOT_COMMAND`, or `MINUTES_BROWSER_IMAGE` setting | unsupported | Rejected before filesystem or datastore setup, even when empty or set to `false`. Minutes configuration belongs in the managed Helm topology. |

`ADMIN_API_TOKEN`, `RUNTIME_CALLBACK_SECRET`, `MEETING_TOKEN_SECRET`,
`RUNTIME_CONTROL_SECRET`, `INTERNAL_API_SECRET`, and `GATEWAY_IDENTITY_SECRET` must be pairwise
distinct; when configured, `GATEWAY_IDENTITY_PREVIOUS_SECRET` must also be distinct from every
current trust-domain credential. Lite rejects an alias
before starting any service.
The entrypoint removes all current credentials plus the optional previous verifier (including
Agent-name aliases) from supervisord's parent
environment after writing mode-`0600` files below mode-`0700` `/run/vexa`. Each named service's exec
wrapper references only its configured files. Spawned meeting bots and agent workers receive a minimum
allowlisted environment and run as unprivileged per-workload identities, so they cannot traverse the
operator-secret directory.

This is configuration hygiene, not isolation between supervised control-plane services. Lite is one
trusted OS process domain: those services (including the root runtime required for per-workload
`setuid`/`chown`) can access root-owned files if compromised. `GATEWAY_IDENTITY_PREVIOUS_SECRET` is
therefore Agent-only in normal environment projection, not protected from a hostile peer root
process. Use Compose or managed Helm when service-to-service kernel isolation is required.

Lite Redis uses AOF `appendfsync=always`, `noeviction`, and a positive `REDIS_MAXMEMORY` ceiling.
When the bounded dataset is exhausted, writes fail closed until capacity is restored; acknowledged
privacy fences are not evicted. Leave container-memory headroom above that ceiling for allocator
overhead and AOF rewrite copy-on-write.

Agent inference is BYO — point the runtime at your endpoint via `ANTHROPIC_*` / `VEXA_AGENT_MODEL`
in `.env`; the runtime brokers credentials into spawned workers (nothing leaves the network).

## Debugging

```bash
docker logs -f vexa-lite                          # container logs
docker exec vexa-lite supervisorctl status        # all supervised services
docker exec vexa-lite supervisorctl restart meeting-api
docker exec vexa-lite ps aux | grep dist/index.js # running bot processes
```

## Lite vs. Compose

| | Lite | Compose |
|---|---|---|
| Bot / agent isolation | dedicated unprivileged bot uid; per-subject agent uid, 0700 tiers, per-share gids | separate containers (per-mount binds) |
| Docker socket | not needed | required (runtime spawns over it) |
| Datastores | postgres + minio sidecars | in-stack |
| Setup | `make lite` | `make all` |

Outgrow lite? Switch to [compose](../compose/README.md) — same images, same contracts.

## Known limitations

| Issue | Note |
|---|---|
| Shared X11 display | bots share one Xvfb (`:99`) — best for one browser session at a time |
| Ephemeral redis | internal redis is in-container; mount `/var/lib/redis` for persistence |
| Agent ↔ gateway | the agent control plane is reached directly on `:8100` (gateway-fronting is roadmap) |
