# vexa — v0.12 control-plane Helm chart

Deploys the full v0.12 stack to Kubernetes: the control plane **gateway · admin-api · meeting-api ·
runtime · agent-api**, the **terminal** web UI, and infra (`postgres` · `redis` · `minio` + a
`minio-init` bucket Job). The `runtime` spawns the bot and agent-worker as on-demand Pods
(`RUNTIME_BACKEND=k8s`, under the chart's ServiceAccount/RBAC); they are not long-running services.

```
            ┌──────────┐
  client ──>│ gateway  │──> admin-api ──┐
            └────┬─────┘                ├─> postgres
                 └────> meeting-api ────┘
                          │  └─> minio (recordings)
                          └─> runtime ──(kubectl run)──> bot Pod / agent-worker Pod
            agent-api ──> runtime                        redis (streams/pubsub)
```

## Install

```bash
helm upgrade --install vexa . -n vexa --create-namespace \
  --set global.imageTag=YYMMDD-HHMM \
  --set secrets.adminApiToken=$ADMIN_TOKEN \
  --set secrets.internalApiSecret=$INTERNAL_API_SECRET \
  --set secrets.runtimeControlSecret=$RUNTIME_CONTROL_SECRET \
  --set secrets.runtimeCallbackSecret=$RUNTIME_CALLBACK_SECRET \
  --set secrets.meetingTokenSecret=$MEETING_TOKEN_SECRET \
  --set secrets.redisPassword=$REDIS_PASSWORD
```

See [`../../README.md`](../../README.md) for the cookbook (local k3s smoke, managed backing,
ingress) and the values table. Key knobs: `global.imageTag`, `runtime.backend`
(`k8s`|`docker`|`process`), `secrets.*` (or `secrets.existingSecretName`), `postgres/redis/minio.enabled`,
`pgbouncer.enabled`, `ingress.*`.

Hosted/managed installs are intentionally fail-closed: set `runtime.workloadNamespace` to a
distinct, pre-created, secret-free namespace with restricted Pod Security Admission; run
`../../bin/verify-workload-namespace.sh <namespace>` before install/upgrade; enable the migration
hook; keep NetworkPolicy on;
pin every enabled image/spawn profile by sha256 digest or immutable build tag; and bump
`secrets.existingSecretRevision` whenever operator Secret data changes. The chart binds only Pod
`create/delete/get/list` in the workload namespace (no Secret, log, or watch authority) and never
creates the Namespace itself. The workload default-deny selects every Pod in that dedicated
namespace (including unlabeled Pods); only labeled runtime-managed Pods receive explicit DNS,
in-stack, and configured public-media/provider egress.

Minutes is a separate topology: turn the bundled `agentApi`, `gateway`, and `terminal` off; configure
the complete TTL/erasure boundary; keep `adminApi` on for Identity-owned authority and `runtime` on
for withdrawal/teardown; and set
`minutes.nullalisErasureBaseUrl` to external Nullalis. The
chart rejects every active or historical-erasure mode with the general-purpose bundled Agent or its
Agent-dependent gateway/Terminal, removes the runtime Agent worker profile, and prevents `extraEnv`
from restoring those paths. zaki-prod's external BFF/UI owns the user edge. meeting-api calls
Nullalis only through its bounded HTTP erasure client. Every Minutes credential must be independently
generated, 32..512 unpadded printable-ASCII characters, and domain-separated. The Hub/read bearers and current/previous
Agent verifiers stay in meeting-api; zaki-infra separately projects the read bearer, current receipt
signer, and optional previous Agent verifier to Nullalis. The external Hub sends
`X-Zaki-Minutes-Token` using the matching token from its own operator Secret; previous receipt keys
are verifier-only and cannot sign new receipts.
MinIO access/root credentials use the same chart Secret boundary: meeting-api, MinIO, and minio-init
receive `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` only through `secretKeyRef`. When
`secrets.existingSecretName` is set and in-chart MinIO is enabled, that operator Secret must contain
both keys; no credential is copied into a workload manifest.
Legacy `minio.accessKey` / `minio.secretKey` overrides fail rendering with the replacement paths;
this prevents an upgrade from silently ignoring the old credentials and selecting local defaults.
Bundled Postgres supports the same operator boundary without copying credentials: hosted profiles
set `postgres.createCredentialsSecret=false` and pre-create `postgres.credentialsSecretName` with
`POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD`. Postgres and every database consumer then
reference that Secret directly, and the chart does not create or overwrite it. The checked-in
staging overlay uses this mode.
External Postgres (`postgres.enabled=false`) requires that same pre-created-Secret mode and a
structured `database.host`; the chart rejects the incomplete combination instead of installing
Pods that reference a missing Secret. External object-store routing is not yet a typed chart path:
while meeting-api is enabled, `minio.enabled=false` is rejected, and storage/DB aliases cannot be
smuggled through `extraEnv`. Minutes/historical-erasure topology additionally requires the exact
`runtime.backend=k8s` value and disables this chart's Ingress because zaki-prod owns the user edge.
That managed topology also forces `ZAKI_MINUTES_MANAGED_ONLY=true`, including during historical-
erasure rollback, so legacy `POST /bots` cannot bypass consent. The ordinary default chart renders
it `false` and keeps the upstream bot-create path available without requiring a Minutes Hub token.
Hosted operator-owned Postgres credentials additionally require
`postgres.credentialsSecretRevision`; its checksum rolls every DB consumer independently of the
platform Secret revision.
Rollback is two-stage: disable capture first while retaining Identity, meeting-api, runtime, TTL,
and erasure; drain/withdraw and erase active meetings; remove runtime only when the managed Minutes
topology and its historical controls are removed.
In the ordinary bundled topology, Gateway identity attestation uses a dedicated
`GATEWAY_IDENTITY_SECRET`, projected only to Gateway and Agent. It is never the cluster-wide
`INTERNAL_API_SECRET`, never reaches a worker, and must be an independently generated 32..512
unpadded printable-ASCII value. Operator-managed Secrets must preserve that separation.
During rotation, `GATEWAY_IDENTITY_PREVIOUS_SECRET` is verifier-only and reaches Agent alone;
Gateway continues signing exclusively with the current key. Use three upgrades and wait for rollout
between them: `(current=old, previous=new)` → `(current=new, previous=old)` →
`(current=new, previous=empty)`, bumping `secrets.existingSecretRevision` each time. This overlap
keeps old/new Gateway and Agent Pods compatible; an atomic swap can yield transient 401s.
Redis, Postgres, MinIO, minio-init, applications, and migration workloads run under restricted Pod
Security with read-only root filesystems and explicit scratch/data mounts. Spawned Agent workers
use their own UID/GID/fsGroup, a read-only root, and only workspace/HOME/tmp writable mounts; GPU
requests and limits are identical. PDBs render only for enabled components, and the staging overlay
leaves Terminal disabled because the external launch UI/BFF owns the user edge.
`extraEnv` remains available for true extensions, but chart-owned credentials, service origins,
storage routes, runtime profiles, and Minutes controls are reserved against duplicate overrides.

## Validate (no cluster)

```bash
helm lint .
helm template vexa . -n vexa -f values-test.yaml
```
