# deploy/compose — the v0.12 control-plane stack (P4)

`docker-compose.yml` brings up the v0.12 control plane: the infra (`postgres:17-alpine`,
`redis:7-alpine`, `minio` + `minio-init`) and the long-running services below, each building its own
slim image from `<service>/Dockerfile`:

| service      | build context                          | host port | entrypoint                         |
|--------------|----------------------------------------|-----------|------------------------------------|
| admin-api    | `core/identity/services/admin-api`     | 18057     | `python -m admin_api`              |
| runtime      | `core/runtime`                         | 18090     | `python -m runtime_kernel`         |
| meeting-api  | `core/meetings/services/meeting-api`   | 18080     | `python -m meeting_api`            |
| agent-api    | `core/agent/services/agent-api`        | 18100     | `uvicorn control_plane.api`        |
| gateway      | `core/gateway/services/gateway`        | 18056     | `python -m gateway`                |
| terminal     | `clients/terminal`                     | 13000     | Next.js custom server              |

Every service answers `GET /health` and carries a compose healthcheck; `depends_on` waits on
`condition: service_healthy` so the bring-up is ordered. The `runtime` mounts
`/var/run/docker.sock` and spawns the bot (`BROWSER_IMAGE=vexaai/vexa-bot:v012`, published — a
reference, never built here; never point it at the published `vexaai/vexa-bot:dev`, which is the
old 0.10 line and incompatible with this stack's `lifecycle.v1`) on demand and the per-dispatch
agent worker (`vexaai/v012-agent-worker:v012`, a `build-only` compose profile); neither is a
long-running compose service.

## Usage

```bash
cp .env.example .env            # edit secrets/ports/DOCKER_GID
docker compose -f deploy/compose/docker-compose.yml build
docker compose -f deploy/compose/docker-compose.yml up -d
# poll until healthy, then:
curl -sf http://localhost:18056/health   # gateway
docker compose -f deploy/compose/docker-compose.yml down -v
```

`.env.example` documents every variable (faithful to the 0.11 `deploy/compose` names: `DB_*`,
`REDIS_URL`, `ADMIN_TOKEN`, `INTERNAL_API_SECRET`, `RUNTIME_CONTROL_SECRET`,
`RUNTIME_CALLBACK_SECRET`, `MEETING_TOKEN_SECRET`, `GATEWAY_IDENTITY_SECRET`,
`GATEWAY_IDENTITY_PREVIOUS_SECRET`, `REDIS_PASSWORD`, `MINIO_*`,
`BROWSER_IMAGE`/`AGENT_IMAGE`,
`DOCKER_GID`, `*_HOST_PORT`).

Runtime control and callback authentication are separate boundaries. `RUNTIME_CONTROL_SECRET` is
projected into runtime plus its meeting/Agent controllers; `RUNTIME_CALLBACK_SECRET` is projected
only into runtime and meeting-api, and runtime will attach it only to the exact meeting-api origin.
Runtime and agent-api use the shared `.env` only to retain their intended model-provider inputs.
Their service-level environment explicitly clears every unrelated credential inherited through
that file: runtime keeps only runtime control/callback plus brokered model credentials; agent-api
keeps its namespaced internal/runtime controls plus model, STT, dispatch, and bot-service keys.
Admin, MeetingToken, Redis-password, session, OAuth-client, database, object-store, and Terminal-user
credentials therefore cannot hitchhike through the provider `env_file`.

Redis requires authentication in every Compose profile. The operator-owned `REDIS_PASSWORD` is
projected raw only into Redis; gateway, runtime, meeting-api, and agent-api receive authenticated
internal URLs. The Redis healthcheck refuses to pass unless anonymous `PING` returns `NOAUTH` and
the authenticated probe returns `PONG`. Use a high-entropy 16..512-character URI-userinfo-safe
password (`A-Z a-z 0-9 . _ ~ -`); Redis fails before boot if it cannot be embedded safely. MeetingToken
signing is independently scoped through `MEETING_TOKEN_SECRET`, projected only into meeting-api;
meeting-api never receives the administrative `ADMIN_TOKEN`.

Gateway-to-Agent identity attestation uses `GATEWAY_IDENTITY_SECRET`, an independent 32..512
unpadded printable-ASCII proof projected only to those two services. It never reuses the
cluster-wide `INTERNAL_API_SECRET` and is explicitly scrubbed from runtime's shared provider
environment, so it cannot be brokered into workers. There is no fallback to another credential.
An optional `GATEWAY_IDENTITY_PREVIOUS_SECRET` supports a bounded rotation overlap: it is projected
only to Agent as verifier material and never to Gateway, so it cannot sign new requests.

This bundled Compose stack is deliberately **not** a Minutes launch topology. Its Agent and spawned
workers need the general-purpose Redis URL for ordinary control-plane work, which would bypass the
Minutes consent/retention namespace boundary. Compose therefore hard-pins every Minutes producer,
read, Hub-auth, finalized-delivery, receipt-rotation, and TTL setting off; clears every Minutes credential inherited from
`.env`; and exits agent-api with code 78 when `ZAKI_MINUTES_CAPTURE_ENABLED` or
`ZAKI_MINUTES_READ_ENABLED` is requested. Meeting/admin remain inert even during that failed start,
so capture cannot come up partially. Use Helm with its bundled Agent disabled, the complete
TTL/erasure boundary, and external Nullalis over the bounded HTTP erasure fan-out.

`ZAKI_MINUTES_MANAGED_ONLY=false` remains forced on meeting-api because this is the ordinary Vexa
reference topology, not a partially managed Minutes deployment; upstream `POST /bots` therefore
continues to work. The reserved Minutes variables stay documented in
`.env.example` for migration/config-contract visibility, but Compose ignores their activation values
and projects no Hub/read bearer or erasure signer. The sole historical exception is one previous
Agent receipt verifier, projected only to agent-api and meeting-api so either can replay durable
receipts across rotation; no previous signing variable exists and runtime/workers never receive it.
The optional pair is valid only alongside the complete active current erasure boundary. Standard
Compose does not provide that boundary, so leave it blank; partial rotation input fails service boot.
zaki-infra separately projects the read bearer and current Agent receipt signer to external Nullalis;
Compose never does.

Redis runs with AOF `appendfsync=always`, `maxmemory-policy=noeviction`, and a positive bounded
`REDIS_MAXMEMORY` (`512mb` by default): acknowledged consent and erasure fences are never selected
for eviction. Hitting the dataset limit therefore rejects writes until capacity is restored; this
fail-closed behavior is intentional and must be alerted on. Size the container/host above the Redis
ceiling so allocator overhead and AOF rewrite copy-on-write retain headroom.
