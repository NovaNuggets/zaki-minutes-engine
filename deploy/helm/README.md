# deploy/helm — the v0.12 control-plane chart (Kubernetes)

The `helm` target of the lite/compose/helm trio: the full v0.12 stack as a Kubernetes release —
the control plane **gateway · admin-api · meeting-api · runtime · agent-api**, the **terminal** web
UI, and infra (`postgres:17` · `redis:7` · `minio` + a `minio-init` bucket Job). The **terminal** is
the human front door (Next.js; proxies `/ws` → gateway and REST/login → agent-api/admin-api
server-side); the gateway stays the API front door for programmatic use. The difference from compose
is the **spawn substrate**: on k8s the `runtime` launches the bot and agent-worker as **Pods** (via
`kubectl`, under a chart-provided ServiceAccount/RBAC), selected by `RUNTIME_BACKEND=k8s` — not the
host Docker socket.

## Chart

[`charts/vexa`](charts/vexa/) — the full multi-service deployment. Production-hardened scaffolding
carried from the 0.10.6.3 baseline: zero-downtime `RollingUpdate` (maxSurge 1 / maxUnavailable 0),
PodDisruptionBudgets on enabled stateless services, default-deny NetworkPolicies, restricted Pod
Security, the Redis durability paired invariant, secret-sourced DB/admin/provider credentials,
optional PgBouncer for managed Postgres, and an idempotent migration hook.

## Quick start (any cluster)

```bash
# 1. Pin the image tag your build produced (build-once promotion), fill secrets.
helm upgrade --install vexa deploy/helm/charts/vexa -n vexa --create-namespace \
  --set global.imageTag=YYMMDD-HHMM \
  --set secrets.adminApiToken=$ADMIN_TOKEN \
  --set secrets.internalApiSecret=$INTERNAL_API_SECRET \
  --set secrets.runtimeControlSecret=$RUNTIME_CONTROL_SECRET \
  --set secrets.runtimeCallbackSecret=$RUNTIME_CALLBACK_SECRET \
  --set secrets.meetingTokenSecret=$MEETING_TOKEN_SECRET \
  --set secrets.redisPassword=$REDIS_PASSWORD \
  --set secrets.transcriptionServiceToken=$STT_TOKEN \
  --wait --timeout 10m

# 2. Watch it come up, then probe the front door.
kubectl -n vexa rollout status deploy/vexa-vexa-gateway
kubectl -n vexa port-forward svc/vexa-vexa-gateway 8000:8000 &
curl -sf localhost:8000/health
```

## Local k3s smoke (no registry)

```bash
make -C deploy/helm test     # static gate:helm — lint + render assertions, no cluster
make -C deploy/helm smoke    # build 5 images → import into k3s containerd → install → status
make -C deploy/helm down     # uninstall + drop namespace
```

`smoke` needs `sudo` (k3s writes a root-only kubeconfig at `/etc/rancher/k3s/k3s.yaml`) and a local
Docker to build the images. It proves the control plane stands up and `/health` is green.

## Configuration that matters

| Knob | Default | Notes |
|---|---|---|
| `global.imageTag` / component `image.digest` | `""` | Local installs may use ordinary tags. Hosted and managed Minutes renders require either a lowercase `sha256` digest or an immutable `YYMMDD-<git-sha>` build tag for every enabled application, dependency, and spawned profile. `global.imageTag` promotes first-party Vexa images only; upstream MinIO/mc retain their own explicit `RELEASE.*` tags or digests. The staging overlay is intentionally non-renderable until Infra supplies real refs. |
| `runtime.backend` | `k8s` | `k8s` spawns Pods via RBAC (real cloud); `docker` mounts the host socket (single-node only) and requires the exact host-socket GID in `runtime.dockerSocketGroup` for the non-root runtime; `process` runs child processes. |
| `runtime.workloadNamespace` | `""` | Local compatibility falls back to the release namespace. Hosted and managed Minutes installs require a distinct, pre-created, secret-free namespace with `pod-security.kubernetes.io/enforce=restricted` and `enforce-version=latest`. Run `make -C deploy/helm verify-workload-namespace WORKLOAD_NAMESPACE=... KUBE_CONTEXT=...` before install/upgrade. The chart creates only a narrow Pod Role/RoleBinding and namespace-wide default-deny plus labeled-workload allow policy there; Infra owns Namespace lifecycle and any audited workload-only PVC. `pods/log` and `watch` are not granted. |
| `runtime.spawnedPods.{botIdentity,agentIdentity}` | dedicated UID/GID/fsGroup | Bot and Agent-worker Pods do not share a Unix identity. Agent workers additionally use a read-only root filesystem with only their per-workspace mounts, `/tmp`, and `/home/vexa` writable. GPU requests equal limits; `maxGpu` is admission policy, not an over-allocation. |
| `global.deploymentProfile` | `local` | `local`, `staging`, or `production`. Staging/production, ingress, and public Terminal origins reject blank/known fixture inline auth/signing secrets. A referenced `secrets.existingSecretName` is allowed because Helm cannot inspect its data. |
| `secrets.existingSecretRevision` | `""` | Required with `existingSecretName` in hosted/managed profiles. Bump this opaque value whenever Secret data changes; its hash is stamped into every consumer Pod template. Inline chart Secret data is checksummed directly. Gateway proof rotation projects `GATEWAY_IDENTITY_PREVIOUS_SECRET` only to Agent as a verifier; Gateway retains current-only signing authority. |
| `secrets.*` | placeholders | `adminApiToken`, `internalApiSecret`, `runtimeControlSecret`, `runtimeCallbackSecret`, `meetingTokenSecret`, `redisPassword`, `minioAccessKey`, `minioSecretKey`, `transcriptionServiceToken`, `dispatchSigningKey`, `nextauthSecret`, `anthropic*`, and enabled OAuth application credentials. MeetingToken signing is projected only into meeting-api; admin auth is absent there. Runtime callback auth is projected only into runtime + meeting-api. MinIO access/root credentials reach meeting-api, MinIO, and minio-init only through `secretKeyRef`, never literal workload values. Or set `secrets.existingSecretName` (must carry the corresponding fixed keys, including `RUNTIME_CALLBACK_SECRET`, `MEETING_TOKEN_SECRET`, and `REDIS_URL`; add `REDIS_PASSWORD` when the chart deploys Redis and `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` when it deploys MinIO). Managed Minutes needs the meeting-api-only `ZAKI_MINUTES_HUB_TOKEN`; read additionally needs `ZAKI_READ_TOKEN_MINUTES`; erasure needs `ZAKI_AGENT_ERASURE_VERIFICATION_SECRET` and `ZAKI_MINUTES_ERASURE_SIGNING_SECRET`, plus optional current-domain previous-verifier keys during rotation. Every Minutes Hub, read, current/previous erasure, and finalized-delivery credential must be independently generated, 32..512 unpadded printable-ASCII characters, and distinct from every platform signing, provider, database, object-storage, and other Minutes credential. Helm enforces this for inline values; operators using `existingSecretName` must preserve it because Helm cannot inspect Secret data, and meeting-api rechecks projected credentials at activation. The chart projects Hub/read tokens and Agent verifiers only into meeting-api. zaki-infra separately projects the matching read token, current Agent receipt signer, and optional previous Agent verifier into external Nullalis; previous Agent material never gains signing authority. |
| `minutes.*` activation + historical erasure | off | Capture, invocation-v2, read, TTL, finalized delivery, or retained erasure material requires `meetingApi.enabled=true`, `adminApi.enabled=true`, `runtime.enabled=true`, `agentApi.enabled=false`, `gateway.enabled=false`, `terminal.enabled=false`, `minutes.ttl.enabled=true`, a dedicated Hub/BFF token, both current erasure key IDs/secrets, and an explicit `minutes.nullalisErasureBaseUrl`. Identity and the Minutes-only runtime remain present in rollback so status, withdrawal, and workload teardown stay available. meeting-api uses the Nullalis URL only for the bounded, redirect-refusing erasure fan-out. The bundled Agent and its runtime worker image profile are absent. The bundled gateway has no agentless profile, and the in-chart Terminal is not the Minutes UI; zaki-prod's external BFF/UI must wire directly to the meeting-api edge in zaki-infra, receive the matching Hub token through its own operator Secret, and send it as `X-Zaki-Minutes-Token`. Active or historical Minutes forces managed-only bot spawning and calendar auto-join off; the ordinary default chart leaves upstream `POST /bots` available. Roll back in two stages: first disable capture while keeping this topology up, then drain/withdraw and erase active meetings; remove runtime only when the Minutes topology and historical controls are removed. |
| `minutes.ttl.*` | `enabled=false`, `intervalSeconds=60`, `batchSize=100` | Retention worker controls. The chart constrains the interval to `(0, 86400]` seconds and the integer batch to `1..500` before deployment. |
| `minutes.previousAgentErasureKeyId` / `secrets.agentErasurePreviousVerificationSecret` and `minutes.previousMinutesErasureKeyId` / `secrets.minutesErasurePreviousVerificationSecret` | empty pairs | Optional verifier-only one-key overlap per receipt domain. Configure the relevant pair during signer rotation so durable in-flight erasure receipts remain replayable; new receipts still use only the current signer. |
| `redis.maxmemory` / `redis.maxmemoryPolicy` / `redis.durability.appendfsync` | `512mb` / `noeviction` / `always` | Privacy-fence durability is enforced at render time. The positive dataset ceiling retains headroom below the 1Gi pod limit for allocator/AOF rewrite overhead. Unsafe unbounded memory, eviction, or fsync overrides are rejected. Capacity exhaustion returns write errors and requires operator recovery; it never evicts acknowledged consent/erasure state. |
| `postgres.enabled` / `redis.enabled` / `minio.enabled` | `true` | Disable only the in-chart backing service being replaced. Managed Redis either uses an operator Secret containing the complete `REDIS_URL`, or inline structured `redisConfig.scheme/host/port/database/username` plus `secrets.redisPassword`; complex provider URLs should use the Secret path. |
| `postgres.createCredentialsSecret` / `postgres.credentialsSecretName` / `postgres.credentialsSecretRevision` | `true` / `postgres-credentials` / `""` | Local development may let Helm create `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD` from `database.*`. Hosted profiles should set creation to `false`, pre-create the named Secret, and bump its independent revision on every data change so Postgres and all DB consumers roll. If a staging/production profile deliberately keeps chart creation on, the inline password must be URI-userinfo-safe, non-default, and at least 16 characters. |
| `pgbouncer.enabled` | `false` | Transaction pooler for managed Postgres with a fixed slot budget. |
| `migrations.enabled` | `false` locally | Mandatory in hosted and managed Minutes profiles. Runs the authoritative idempotent `admin_api.migrate` command as `post-install,pre-upgrade` before workload promotion. |
| `networkPolicy.*` | enabled | Release Pods start default-deny. A distinct dedicated workload namespace is namespace-wide default-deny, including unlabeled Pods; local same-namespace compatibility selects only `runtime.managed=true` to avoid governing unrelated Pods. The chart admits release-local authenticated flows, DNS, and declared public HTTPS/WebRTC ports. Private managed DB/STT/object-store or nonstandard ingress flows must be added explicitly with the `platformAdditional*` / `workloadAdditionalEgress` rule lists. |
| `adminApi.userModelAllowedHosts` / `adminApi.userTranscriptionAllowedHosts` | `""` | Exact public HTTPS host allowlists for personal model/STT endpoints. Empty denies personal egress; operator endpoints remain independent. |
| `terminal.enabled` | `true` | The web UI. Set `terminal.publicUrl` (NEXTAUTH_URL/TERMINAL_URL) when fronted by ingress. Hosted/staging renders require at least one explicit `terminal.oauth.*.enabled`; chart-owned credentials, internal origins, tokens, and auth-mode variables cannot be shadowed through plaintext `terminal.extraEnv`. |
| `terminal.sharedKeyMode` | `false` | Unsupported by this chart: a Kubernetes Service is not a provable host-loopback publication, so render fails. Use loopback Compose/Lite for the zero-login local profile. |
| `terminal.directLoginAllowedEmails` | `[]` | Unsupported by this chart for the same listener reason, so any non-empty value fails render. Use OAuth, or loopback Compose/Lite for exact-allowlist debug login. |
| `ingress.enabled` | `false` | Fronts the **terminal** by default; set `host`/`className`/`tls`. Add a second path to `gateway` to also expose the raw API. |
| `minio.service.type` | `ClusterIP` | `NodePort` to reach presigned download URLs browser-side on dev clusters. |

## Known boundaries (v0.12)

Gateway-to-Agent identity attestation uses a dedicated `GATEWAY_IDENTITY_SECRET`, projected only to
those two services. Inline values must be independently generated 32..512 unpadded printable-ASCII
characters and distinct from every platform, provider, storage, and Minutes credential. An
operator-managed `secrets.existingSecretName` must contain that fixed key and preserve the same
separation; there is no fallback to `INTERNAL_API_SECRET`, and workers never receive the proof.

Rotate that proof with verifier overlap; never swap current and previous in one release:

1. Keep `GATEWAY_IDENTITY_SECRET=old`, set `GATEWAY_IDENTITY_PREVIOUS_SECRET=new`, bump
   `secrets.existingSecretRevision`, upgrade, and wait for the Agent rollout. Gateway still signs
   old; every Agent accepts old and new.
2. Set current to `new` and previous to `old`, bump the revision, upgrade, and wait for both Gateway
   and Agent rollouts. Old/new Pods remain mutually compatible throughout this rollout.
3. After the overlap window and rollout convergence, remove previous, bump the revision, upgrade,
   and wait for Agent. Gateway never receives the previous verifier in any stage.

The previous value is a verifier credential with the same uniqueness requirements as current. A
single-release current/previous swap is unsafe because a new Gateway can reach an old Agent before
the latter accepts the new key.

Redis authentication is mandatory. The chart-generated `REDIS_URL` lives only in the Kubernetes
Secret; gateway, runtime, meeting-api, and agent-api reference it through `secretKeyRef`, so
password-bearing URLs are absent from Deployment manifests. Redis itself receives only
`REDIS_PASSWORD`, and readiness requires anonymous `PING` to return `NOAUTH` before authenticated
`PING` may return `PONG`. Inline passwords are restricted to high-entropy URI-userinfo-safe
characters; use an operator-managed `REDIS_URL` Secret for arbitrary provider credential formats.

Chart upgrades must rename legacy `minio.accessKey` / `minio.secretKey` overrides to
`secrets.minioAccessKey` / `secrets.minioSecretKey` (or put `MINIO_ACCESS_KEY` and
`MINIO_SECRET_KEY` in `secrets.existingSecretName`). Legacy paths fail rendering explicitly so an
upgrade cannot silently rotate the object-store identity to local defaults.

Hosted deployments that retain the bundled Postgres should pre-create
`postgres.credentialsSecretName` with `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD`, then
set `postgres.createCredentialsSecret=false`. The chart references that Secret from Postgres and
every database consumer without recreating it; `postgres.credentialsSecretRevision` is hashed into
those Pod templates because Helm cannot observe external Secret data. This separates operator-owned infrastructure
credentials from user-facing configuration and keeps the password out of Helm release history.

Redis, Postgres, MinIO, and the MinIO initializer run non-root with `RuntimeDefault` seccomp,
privilege escalation disabled, all Linux capabilities dropped, read-only root filesystems, and only
their data/socket/scratch paths writable. The application Deployments and migration Job follow the
same restricted pattern. PDB templates skip disabled components, so an absent Deployment cannot
leave a drain-blocking orphan policy. The staging overlay disables the in-chart Terminal because
the external launch BFF/UI owns that edge.

Every Minutes activation and historical-erasure mode fails chart rendering while
`agentApi.enabled=true`, `gateway.enabled=true`, or `terminal.enabled=true`. The bundled Agent
receives no Minutes flag, read URL/token, or erasure credential; runtime registers no Agent/worker
image when the bundled Agent is disabled; and `extraEnv` cannot reintroduce those paths. The
external zaki-prod BFF/UI must receive an explicit meeting-api route from zaki-infra. Read/capture
also cannot render without the complete TTL/erasure fan-out required by sealed issue #25.

`minutes.nullalisErasureBaseUrl` is mandatory for an active/historical profile and may not point at
the chart's Agent Service or contain credentials, query, or fragment. meeting-api uses it through
the bounded HTTP client (10-second timeout, redirects refused, 16 KiB response cap, signed durable
receipt verification). The read bearer and Agent verification key stay producer/verifier-side in
meeting-api; zaki-infra separately projects the read bearer and matching receipt signer to external
Nullalis. No direct Redis sharing with Nullalis is part of this profile.

Component `extraEnv` lists are extension-only. They cannot duplicate chart-owned credentials,
credential-bearing service origins, database/object-store endpoints, runtime backends/images, or
Minutes controls. Configure those through their typed values and operator Secret so Kubernetes
never receives ambiguous duplicate environment entries or plaintext Secret bypasses.

- **Bot spawn** works on k8s (the bot's config arrives as one env var). The hardened runtime targets
  a separate namespace; this removes the Pod-create-to-platform-Secret escalation path. Its
  namespace-wide NetworkPolicy also contains deliberately unlabeled Pods, while restricted Pod
  Security Admission blocks privileged/hostPath escape even if Pod-create authority is compromised.
  The chart cannot own or inspect that external namespace without cluster-scoped authority, so the
  mandatory operator preflight verifies both PSA labels and the absence of Secrets. **Agent-worker** Pods mount
  the workspace store with **per-mount tenant isolation**: one `subPath` + `readOnly` volumeMount per
  granted workspace against the store PVC (`runtime_kernel/mounts.py:k8s_volume_mounts`) — a worker's
  filesystem contains only its dispatch's workspaces. Multi-node clusters need an **RWX** storage
  class for the store PVC (NFS/Longhorn; k3s `local-path` is RWO-only — single node works), with
  `agentApi.workspaces.accessMode: ReadWriteMany`. When the workload namespace is separate, Infra
  must pre-provision the same audited workspace claim there against the shared RWX volume; the chart
  deliberately does not create a Namespace or cross-namespace storage authority.
- The staging overlay declares the funded `vexa-staging` transcription gateway as a private,
  selector-scoped TCP/8084 egress exception for both platform and spawned workload Pods. If Infra
  changes that Service's backing labels, update the two typed NetworkPolicy rules in the same
  change; do not replace them with private-CIDR-wide egress.
- The `runtime` image bundles `kubectl` for the k8s backend; the docker/process backends ignore it.

## Contracts

This is a composition layer — it owns no service code and consumes none of the `*.v1` schemas
directly (each service vendors its own). It mirrors the [`deploy/compose`](../compose/) env contract.
