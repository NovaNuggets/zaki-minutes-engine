#!/usr/bin/env bash
# Launch-specific deployment policy: these checks are intentionally separate from the broad
# compatibility render gate so every fail-closed production invariant has one readable proof.
set -euo pipefail

HELM_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "$HELM_DIR/../.." && pwd)"
CHART="$HELM_DIR/charts/vexa"
VALUES="$CHART/values-test.yaml"
fail=0
STAGING_RENDER_ARGS=(
  --set-string secrets.existingSecretRevision=test-revision-20260716
  --set-string postgres.credentialsSecretRevision=test-db-revision-20260716
  --set-string global.imageTag=260716-abcdef
  --set-string postgres.image=postgres@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  --set-string redis.image=redis@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
)

expect_fail() {
  local label="$1"; shift
  if helm template vexa "$CHART" -n vexa -f "$VALUES" "$@" >/dev/null 2>&1; then
    echo "  FAIL: $label"; fail=1
  else
    echo "  OK: $label"
  fi
}

MINUTES_BASE=(
  --set agentApi.enabled=false
  --set gateway.enabled=false
  --set terminal.enabled=false
  --set postgres.createCredentialsSecret=false
  --set-string postgres.credentialsSecretName=operator-db
  --set-string postgres.credentialsSecretRevision=db-rev-20260716
  --set-string secrets.existingSecretName=operator-platform
  --set-string secrets.existingSecretRevision=minutes-rev-20260716
  --set migrations.enabled=true
  --set-string runtime.workloadNamespace=vexa-workloads
  --set-string global.imageTag=260716-abcdef
  --set-string postgres.image=postgres@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  --set-string redis.image=redis@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
  --set minutes.ttl.enabled=true
  --set-string minutes.nullalisErasureBaseUrl=https://nullalis.internal.example
  --set-string minutes.agentErasureKeyId=agent-erasure-2026-07
  --set-string minutes.minutesErasureKeyId=minutes-erasure-2026-07
)

echo "=== gate:helm-launch-hardening ==="

# Merely retaining historical Minutes erasure authority is production-sensitive. It must activate
# the same fixture-secret guard as a public/staging deployment, even when deploymentProfile=local.
expect_fail "Minutes activation rejects local/default platform credentials" \
  --set agentApi.enabled=false --set gateway.enabled=false --set terminal.enabled=false \
  --set minutes.ttl.enabled=true \
  --set-string minutes.nullalisErasureBaseUrl=https://nullalis.internal.example \
  --set-string minutes.agentErasureKeyId=agent-erasure-2026-07 \
  --set-string minutes.minutesErasureKeyId=minutes-erasure-2026-07 \
  --set-string secrets.minutesHubToken=minutes-hub-token-0123456789abcdef \
  --set-string secrets.agentErasureHmacSecret=agent-erasure-secret-0123456789abcdef \
  --set-string secrets.minutesErasureSigningSecret=minutes-erasure-secret-0123456789abcdef

expect_fail "Minutes in-chart Redis requires a PVC" \
  "${MINUTES_BASE[@]}" --set redis.persistence.enabled=false
for component in adminApi meetingApi runtime; do
  expect_fail "Minutes rejects zero $component replicas" \
    "${MINUTES_BASE[@]}" --set "$component.replicaCount=0"
done

# Managed Postgres TLS reaches both applications and the authoritative schema-convergence job.
TLS_ARGS=(
  --set postgres.enabled=false
  --set postgres.createCredentialsSecret=false
  --set-string postgres.credentialsSecretName=operator-db
  --set-string postgres.credentialsSecretRevision=db-rev-20260716
  --set-string database.host=db.internal.example
  --set database.sslMode=require
  --set migrations.enabled=true
  --set-string global.imageTag=260716-abcdef
)
for template in deployment-admin-api.yaml deployment-meeting-api.yaml job-migrations.yaml; do
  rendered="$(helm template vexa "$CHART" -n vexa -f "$VALUES" "${TLS_ARGS[@]}" --show-only "templates/$template")"
  if printf '%s\n' "$rendered" | grep -A1 -m1 -- '- name: DB_SSL_MODE' | grep -q 'value: "require"'; then
    echo "  OK: $template receives validated DB SSL mode"
  else
    echo "  FAIL: $template drops database.sslMode"; fail=1
  fi
done
expect_fail "invalid database.sslMode is rejected" --set-string database.sslMode=trust-me
expect_fail "PgBouncer explicitly rejects unsupported managed-Postgres TLS" \
  "${TLS_ARGS[@]}" --set pgbouncer.enabled=true

MIGRATION="$(helm template vexa "$CHART" -n vexa -f "$VALUES" "${TLS_ARGS[@]}" \
  --show-only templates/job-migrations.yaml)"
if grep -A3 -m1 -- 'command:' <<<"$MIGRATION" | grep -q -- '- admin_api.migrate' \
  && grep -q 'image: "vexaai/v012-admin-api:260716-abcdef"' <<<"$MIGRATION" \
  && grep -q '"helm.sh/hook": post-install,pre-upgrade' <<<"$MIGRATION" \
  && ! grep -q 'meeting_api.database' <<<"$MIGRATION"; then
  echo "  OK: migration job runs the authoritative idempotent schema command"
else
  echo "  FAIL: migration job is not executable/authoritative/tag-pinned"; fail=1
fi

# Spawned bot/worker pods inherit the runtime Deployment's placement/private-registry policy.
RUNTIME="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
  --set-json 'global.imagePullSecrets=[{"name":"private-registry"}]' \
  --set-json 'global.affinity={"nodeAffinity":{"preferredDuringSchedulingIgnoredDuringExecution":[]}}' \
  --show-only templates/deployment-runtime.yaml)"
for env_name in RUNTIME_K8S_NODE_SELECTOR_JSON RUNTIME_K8S_TOLERATIONS_JSON \
  RUNTIME_K8S_AFFINITY_JSON RUNTIME_K8S_IMAGE_PULL_SECRETS_JSON \
  RUNTIME_K8S_DEFAULT_CPU RUNTIME_K8S_DEFAULT_MEMORY_MB RUNTIME_K8S_MAX_CPU \
  RUNTIME_K8S_MAX_MEMORY_MB RUNTIME_K8S_MAX_GPU RUNTIME_K8S_RUN_AS_USER; do
  if grep -q -- "- name: $env_name" <<<"$RUNTIME"; then
    echo "  OK: runtime exports $env_name to its backend"
  else
    echo "  FAIL: runtime omits $env_name"; fail=1
  fi
done

# Every application image has its own numeric identity; the chart reinforces it and a read-only
# root filesystem at runtime. Stateful dependencies are deliberately outside this list.
components=(gateway admin-api meeting-api runtime agent-api terminal)
templates=(deployment-gateway.yaml deployment-admin-api.yaml deployment-meeting-api.yaml \
  deployment-runtime.yaml deployment-agent-api.yaml deployment-terminal.yaml)
uids=(10001 10002 10003 10004 10005 10006)
mount_counts=(1 1 1 1 2 2)
empty_dir_counts=(1 1 1 1 1 2)
for i in "${!components[@]}"; do
  rendered="$(helm template vexa "$CHART" -n vexa -f "$VALUES" --show-only "templates/${templates[$i]}")"
  if grep -q 'runAsNonRoot: true' <<<"$rendered" \
    && grep -q "runAsUser: ${uids[$i]}" <<<"$rendered" \
    && grep -q 'readOnlyRootFilesystem: true' <<<"$rendered" \
    && grep -q 'allowPrivilegeEscalation: false' <<<"$rendered" \
    && grep -A3 -m1 'capabilities:' <<<"$rendered" | grep -q -- '- ALL' \
    && grep -q "fsGroup: ${uids[$i]}" <<<"$rendered" \
    && grep -q 'type: RuntimeDefault' <<<"$rendered"; then
    echo "  OK: ${components[$i]} has a dedicated restricted runtime identity"
  else
    echo "  FAIL: ${components[$i]} pod security context is incomplete"; fail=1
  fi
  if [ "$(grep -c 'mountPath:' <<<"$rendered" || true)" -eq "${mount_counts[$i]}" ] \
    && [ "$(grep -c 'emptyDir:' <<<"$rendered" || true)" -eq "${empty_dir_counts[$i]}" ] \
    && grep -q 'mountPath: /tmp' <<<"$rendered"; then
    echo "  OK: ${components[$i]} exposes only its required writable paths"
  else
    echo "  FAIL: ${components[$i]} writable paths are incomplete or over-broad"; fail=1
  fi
done

# App hardening is a platform invariant, not an escape hatch in global values. A caller may add
# tighter fields, but cannot restore privilege escalation or capabilities on launch services.
OVERRIDE_PROBE="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
  --set global.securityContext.allowPrivilegeEscalation=true \
  --set-json 'global.securityContext.capabilities={"add":["SYS_ADMIN"]}' \
  --show-only templates/deployment-gateway.yaml)"
if grep -q 'allowPrivilegeEscalation: false' <<<"$OVERRIDE_PROBE" \
  && grep -A3 -m1 'capabilities:' <<<"$OVERRIDE_PROBE" | grep -q -- '- ALL' \
  && ! grep -q 'SYS_ADMIN' <<<"$OVERRIDE_PROBE"; then
  echo "  OK: global values cannot relax application container privileges"
else
  echo "  FAIL: global values relaxed application container privileges"; fail=1
fi

dockerfiles=(
  core/gateway/services/gateway/Dockerfile
  core/identity/services/admin-api/Dockerfile
  core/meetings/services/meeting-api/Dockerfile
  core/runtime/Dockerfile
  core/agent/services/agent-api/Dockerfile
  clients/terminal/Dockerfile
)
for i in "${!dockerfiles[@]}"; do
  file="$ROOT/${dockerfiles[$i]}"
  if grep -Eq "^USER ${uids[$i]}(:${uids[$i]})?$" "$file" \
    && grep -Eq "(useradd|adduser).*(^|[^0-9])${uids[$i]}([^0-9]|$)" "$file"; then
    echo "  OK: ${dockerfiles[$i]} declares dedicated numeric user ${uids[$i]}"
  else
    echo "  FAIL: ${dockerfiles[$i]} still runs as root/shared user"; fail=1
  fi
done

MINIO="$(helm template vexa "$CHART" -n vexa -f "$VALUES")"
if grep -q 'minio/minio:RELEASE.2025-09-07T16-13-09Z' <<<"$MINIO" \
  && grep -q 'minio/mc:RELEASE.2025-08-13T08-35-41Z' <<<"$MINIO" \
  && ! grep -Eq 'minio/(minio|mc):latest' <<<"$MINIO"; then
  echo "  OK: MinIO server and client use explicit release tags"
else
  echo "  FAIL: MinIO server/client images are not pinned"; fail=1
fi
PROMOTED_MINIO="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
  --set-string global.imageTag=260716-abcdef)"
if grep -q 'minio/minio:RELEASE.2025-09-07T16-13-09Z' <<<"$PROMOTED_MINIO" \
  && grep -q 'minio/mc:RELEASE.2025-08-13T08-35-41Z' <<<"$PROMOTED_MINIO" \
  && ! grep -Eq 'minio/(minio|mc):260716-abcdef' <<<"$PROMOTED_MINIO"; then
  echo "  OK: first-party promotion tags do not rewrite MinIO release images"
else
  echo "  FAIL: global.imageTag rewrote an upstream MinIO release image"; fail=1
fi
MINIO_DIGEST="sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
if MINIO_DIGEST_ONLY="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
  --set-string minio.image.digest="$MINIO_DIGEST" \
  --set-string minio.image.tag= \
  --set-string minio.clientImage.digest="$MINIO_DIGEST" \
  --set-string minio.clientImage.tag=)" \
  && grep -q "minio/minio@$MINIO_DIGEST" <<<"$MINIO_DIGEST_ONLY" \
  && grep -q "minio/mc@$MINIO_DIGEST" <<<"$MINIO_DIGEST_ONLY"; then
  echo "  OK: MinIO server and client accept digest-only image references"
else
  echo "  FAIL: MinIO digest-only image references require redundant tags"; fail=1
fi
for path in minio.image.tag minio.clientImage.tag; do
  expect_fail "hardened profile rejects $path=latest" \
    --set global.deploymentProfile=staging \
    --set-string secrets.existingSecretName=operator-platform \
    --set postgres.createCredentialsSecret=false \
    --set-string postgres.credentialsSecretName=operator-db \
    --set-string "$path=latest"
  expect_fail "Minutes rejects an empty $path" "${MINUTES_BASE[@]}" --set-string "$path="
done
expect_fail "enabled PgBouncer rejects latest" --set pgbouncer.enabled=true \
  --set-string pgbouncer.image=edoburu/pgbouncer:latest

# The Helm-only Docker substrate mounts the host socket. Its non-root runtime identity must receive
# the exact operator-supplied socket group; silently defaulting to root or omitting the group makes
# the advertised backend either privileged or unusable.
expect_fail "Helm Docker backend requires an explicit socket group" \
  --set runtime.backend=docker
DOCKER_RUNTIME="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
  --set runtime.backend=docker --set-string runtime.dockerSocketGroup=998 \
  --show-only templates/deployment-runtime.yaml)"
if grep -A3 -m1 'supplementalGroups:' <<<"$DOCKER_RUNTIME" | grep -q -- '- 998'; then
  echo "  OK: Helm Docker backend grants only the configured socket group"
else
  echo "  FAIL: Helm Docker backend omits the configured socket group"; fail=1
fi

# A Pod creator can reference any Secret/PVC in its target namespace even without Secret RBAC.
# Hardened deployments therefore require an externally-created, secret-free workload namespace;
# the runtime's binding lives there and carries only the Pod lifecycle verbs the backend uses.
expect_fail "hardened k8s runtime requires an explicit workload namespace" \
  --set global.deploymentProfile=staging \
  --set-string secrets.existingSecretName=operator-platform \
  --set-string secrets.existingSecretRevision=rev-20260716 \
  --set migrations.enabled=true --set terminal.oauth.google.enabled=true
expect_fail "hardened workload namespace must differ from the platform namespace" \
  --set global.deploymentProfile=staging \
  --set-string secrets.existingSecretName=operator-platform \
  --set-string secrets.existingSecretRevision=rev-20260716 \
  --set migrations.enabled=true --set terminal.oauth.google.enabled=true \
  --set-string runtime.workloadNamespace=vexa
ISOLATED_RBAC="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
  --set-string runtime.workloadNamespace=vexa-workloads \
  --show-only templates/rbac-runtime.yaml)"
if [ "$(grep -c '^  namespace: vexa-workloads$' <<<"$ISOLATED_RBAC" || true)" -ge 2 ] \
  && grep -q 'resources: \["pods"\]' <<<"$ISOLATED_RBAC" \
  && grep -q 'verbs: \["create", "delete", "get", "list"\]' <<<"$ISOLATED_RBAC" \
  && ! grep -q 'pods/log' <<<"$ISOLATED_RBAC" \
  && ! grep -q '"watch"' <<<"$ISOLATED_RBAC"; then
  echo "  OK: runtime Pod RBAC is bound only in the isolated workload namespace"
else
  echo "  FAIL: runtime Pod RBAC remains over-broad or platform-namespace scoped"; fail=1
fi

# Default-deny applies to both the platform and runtime-managed workload Pods. Explicit policies
# then admit DNS, declared in-stack service flows, and the configured bot egress surface.
NETWORK_POLICIES="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
  --set-string runtime.workloadNamespace=vexa-workloads 2>/dev/null || true)"
for policy in platform-default-deny platform-explicit-flows workload-default-deny workload-explicit-flows; do
  if grep -q "name: vexa-vexa-$policy" <<<"$NETWORK_POLICIES"; then
    echo "  OK: NetworkPolicy $policy renders"
  else
    echo "  FAIL: NetworkPolicy $policy is missing"; fail=1
  fi
done
if grep -A8 -m1 'name: vexa-vexa-workload-default-deny' <<<"$NETWORK_POLICIES" \
  | grep -q 'namespace: vexa-workloads'; then
  echo "  OK: spawned workload default-deny lands in the isolated namespace"
else
  echo "  FAIL: spawned workload default-deny is not namespace-isolated"; fail=1
fi
WORKLOAD_DEFAULT_DENY="$(awk '
  /name: vexa-vexa-workload-default-deny/ { capture=1 }
  capture { print }
  capture && /^---$/ { exit }
' <<<"$NETWORK_POLICIES")"
if grep -q '^  podSelector: {}$' <<<"$WORKLOAD_DEFAULT_DENY"; then
  echo "  OK: workload namespace default-deny covers unlabeled Pods"
else
  echo "  FAIL: an unlabeled Pod can bypass workload namespace default-deny"; fail=1
fi
LOCAL_NETWORK_POLICIES="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
  --show-only templates/networkpolicy.yaml)"
LOCAL_WORKLOAD_DEFAULT_DENY="$(awk '
  /name: vexa-vexa-workload-default-deny/ { capture=1 }
  capture { print }
  capture && /^---$/ { exit }
' <<<"$LOCAL_NETWORK_POLICIES")"
if grep -q 'runtime.managed: "true"' <<<"$LOCAL_WORKLOAD_DEFAULT_DENY" \
  && ! grep -q '^  podSelector: {}$' <<<"$LOCAL_WORKLOAD_DEFAULT_DENY"; then
  echo "  OK: local shared-namespace fallback does not quarantine unrelated Pods"
else
  echo "  FAIL: local shared namespace received a namespace-wide deny"; fail=1
fi

STAGING_NETWORK_POLICIES="$(helm template vexa "$CHART" -n vexa \
  -f "$CHART/values-staging.yaml" "${STAGING_RENDER_ARGS[@]}" \
  --show-only templates/networkpolicy.yaml)"
if [ "$(grep -c 'kubernetes.io/metadata.name: vexa-staging' <<<"$STAGING_NETWORK_POLICIES" || true)" -ge 2 ] \
  && [ "$(grep -c 'app.kubernetes.io/component: transcription-gateway' <<<"$STAGING_NETWORK_POLICIES" || true)" -ge 2 ] \
  && [ "$(grep -c 'port: 8084' <<<"$STAGING_NETWORK_POLICIES" || true)" -ge 2 ]; then
  echo "  OK: staging platform and workload Pods can reach only the selected STT service"
else
  echo "  FAIL: staging STT cross-namespace egress is blocked or over-broad"; fail=1
fi

# Spawn identities are profile-specific. Agent workers get a read-only root filesystem and only
# workspace/HOME/tmp writable mounts; the runtime exports all three identity fields per profile.
RUNTIME_IDENTITIES="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
  --show-only templates/deployment-runtime.yaml)"
for env_name in RUNTIME_K8S_BOT_RUN_AS_USER RUNTIME_K8S_BOT_RUN_AS_GROUP \
  RUNTIME_K8S_BOT_FS_GROUP RUNTIME_K8S_AGENT_RUN_AS_USER \
  RUNTIME_K8S_AGENT_RUN_AS_GROUP RUNTIME_K8S_AGENT_FS_GROUP; do
  if grep -q -- "- name: $env_name" <<<"$RUNTIME_IDENTITIES"; then
    echo "  OK: runtime exports $env_name"
  else
    echo "  FAIL: runtime omits $env_name"; fail=1
  fi
done
expect_fail "spawned bot and Agent profiles require distinct UIDs" \
  --set runtime.spawnedPods.agentIdentity.runAsUser=10003
expect_fail "spawned Agent identity rejects a root fsGroup" \
  --set runtime.spawnedPods.agentIdentity.fsGroup=0

# Stateful dependencies and hook Jobs run under the restricted Pod Security profile with an
# immutable root filesystem and narrowly declared writable mounts.
for template in deployment-redis.yaml statefulset-postgres.yaml deployment-minio.yaml job-minio-init.yaml; do
  restricted="$(helm template vexa "$CHART" -n vexa -f "$VALUES" --show-only "templates/$template")"
  if grep -q 'runAsNonRoot: true' <<<"$restricted" \
    && grep -q 'readOnlyRootFilesystem: true' <<<"$restricted" \
    && grep -q 'allowPrivilegeEscalation: false' <<<"$restricted" \
    && grep -A3 -m1 'capabilities:' <<<"$restricted" | grep -Eq -- '- ALL|drop: \["ALL"\]' \
    && grep -q 'type: RuntimeDefault' <<<"$restricted" \
    && grep -q 'mountPath: /tmp' <<<"$restricted"; then
    echo "  OK: $template uses restricted Pod Security with explicit scratch"
  else
    echo "  FAIL: $template lacks restricted Pod Security or scratch mounts"; fail=1
  fi
done

# Managed and hosted installs may not rely on mutable tags. Every enabled application/dependency
# and every spawned profile has an explicit digest; clearing any one digest fails rendering.
DIGEST="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
HARDENED_IMAGES=(
  --set global.deploymentProfile=staging
  --set-string secrets.existingSecretName=operator-platform
  --set-string secrets.existingSecretRevision=rev-20260716
  --set migrations.enabled=true
  --set terminal.oauth.google.enabled=true
  --set-string runtime.workloadNamespace=vexa-workloads
  --set postgres.createCredentialsSecret=false
  --set-string postgres.credentialsSecretName=operator-db
  --set-string postgres.credentialsSecretRevision=db-rev-20260716
  --set-string gateway.image.digest="$DIGEST"
  --set-string adminApi.image.digest="$DIGEST"
  --set-string meetingApi.image.digest="$DIGEST"
  --set-string runtime.image.digest="$DIGEST"
  --set-string agentApi.image.digest="$DIGEST"
  --set-string terminal.image.digest="$DIGEST"
  --set-string minio.image.digest="$DIGEST"
  --set-string minio.image.tag=
  --set-string minio.clientImage.digest="$DIGEST"
  --set-string minio.clientImage.tag=
  --set-string postgres.image="postgres@$DIGEST"
  --set-string redis.image="redis@$DIGEST"
  --set-string runtime.browserImage="vexaai/vexa-bot@$DIGEST"
  --set-string runtime.agentImage="vexaai/v012-agent-api@$DIGEST"
  --set-string runtime.agentWorkerImage="vexaai/v012-agent-worker@$DIGEST"
)
if helm template vexa "$CHART" -n vexa -f "$VALUES" "${HARDENED_IMAGES[@]}" >/dev/null 2>&1; then
  echo "  OK: hardened deployment accepts digest-pinned images"
else
  echo "  FAIL: hardened digest-pinned deployment did not render"; fail=1
fi
expect_fail "hardened deployment rejects a mutable gateway image" \
  "${HARDENED_IMAGES[@]}" --set-string gateway.image.digest=
expect_fail "hardened deployment rejects a mutable spawned bot image" \
  "${HARDENED_IMAGES[@]}" --set-string runtime.browserImage=vexaai/vexa-bot:v012
expect_fail "hardened external database Secret requires an explicit revision" \
  "${HARDENED_IMAGES[@]}" --set-string postgres.credentialsSecretRevision=

# Gateway proof rotation is verifier-overlap, not an atomic current/previous swap. All three
# intermediate Secret states must render while Gateway receives only current and Agent receives the
# optional previous verifier. The chart README gives the matching wait-for-rollout runbook.
GATEWAY_OLD='gateway-identity-old-0123456789abcdef'
GATEWAY_NEW='gateway-identity-new-0123456789abcdef'
rotation_stages=("$GATEWAY_OLD|$GATEWAY_NEW" "$GATEWAY_NEW|$GATEWAY_OLD" "$GATEWAY_NEW|")
rotation_fail=0
for stage in "${rotation_stages[@]}"; do
  current="${stage%%|*}"
  previous="${stage#*|}"
  ROTATION_AGENT="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
    --set-string "secrets.gatewayIdentitySecret=$current" \
    --set-string "secrets.gatewayIdentityPreviousSecret=$previous" \
    --show-only templates/deployment-agent-api.yaml)"
  ROTATION_GATEWAY="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
    --set-string "secrets.gatewayIdentitySecret=$current" \
    --set-string "secrets.gatewayIdentityPreviousSecret=$previous" \
    --show-only templates/deployment-gateway.yaml)"
  ROTATION_SECRET="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
    --set-string "secrets.gatewayIdentitySecret=$current" \
    --set-string "secrets.gatewayIdentityPreviousSecret=$previous" \
    --show-only templates/secret.yaml)"
  if ! grep -q 'key: GATEWAY_IDENTITY_SECRET' <<<"$ROTATION_AGENT" \
    || ! grep -q 'key: GATEWAY_IDENTITY_SECRET' <<<"$ROTATION_GATEWAY" \
    || grep -q 'GATEWAY_IDENTITY_PREVIOUS_SECRET' <<<"$ROTATION_GATEWAY" \
    || ! grep -qF "  GATEWAY_IDENTITY_SECRET: \"$current\"" <<<"$ROTATION_SECRET"; then
    echo "  FAIL: Gateway rotation stage breaks signer/verifier authority"; fail=1; rotation_fail=1
  fi
  if [ -n "$previous" ]; then
    if ! grep -qF "  GATEWAY_IDENTITY_PREVIOUS_SECRET: \"$previous\"" <<<"$ROTATION_SECRET"; then
      echo "  FAIL: Gateway rotation stage omits or changes the previous verifier"; fail=1; rotation_fail=1
    fi
  elif grep -q 'GATEWAY_IDENTITY_PREVIOUS_SECRET:' <<<"$ROTATION_SECRET"; then
    echo "  FAIL: final Gateway rotation stage retains the previous verifier"; fail=1; rotation_fail=1
  fi
done
if [ "$rotation_fail" -eq 0 ]; then
  echo "  OK: three-stage Gateway verifier overlap states preserve authority"
fi
expect_fail "previous Gateway verifier rejects unrelated credential reuse" \
  --set-string secrets.transcriptionServiceToken=shared-rotation-secret-0123456789abcdef \
  --set-string secrets.gatewayIdentityPreviousSecret=shared-rotation-secret-0123456789abcdef

# Inline Secret data is checksummed directly. External Secret data cannot be read by Helm, so its
# operator-controlled revision is mandatory and changing it must change the Pod-template checksum.
expect_fail "hardened existingSecret requires an explicit revision" \
  --set global.deploymentProfile=staging \
  --set-string secrets.existingSecretName=operator-platform \
  --set migrations.enabled=true --set terminal.oauth.google.enabled=true \
  --set-string runtime.workloadNamespace=vexa-workloads
REV_ONE="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
  --set-string secrets.existingSecretName=operator-platform \
  --set-string secrets.existingSecretRevision=rev-one \
  --show-only templates/deployment-meeting-api.yaml | grep 'checksum/platform-secret' || true)"
REV_TWO="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
  --set-string secrets.existingSecretName=operator-platform \
  --set-string secrets.existingSecretRevision=rev-two \
  --show-only templates/deployment-meeting-api.yaml | grep 'checksum/platform-secret' || true)"
if [ -n "$REV_ONE" ] && [ -n "$REV_TWO" ] && [ "$REV_ONE" != "$REV_TWO" ]; then
  echo "  OK: external Secret revision drives workload rollout"
else
  echo "  FAIL: external Secret revision does not alter Pod-template checksum"; fail=1
fi
DB_REV_ONE="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
  --set postgres.createCredentialsSecret=false --set-string postgres.credentialsSecretName=operator-db \
  --set-string postgres.credentialsSecretRevision=db-rev-one \
  --show-only templates/deployment-meeting-api.yaml | grep 'checksum/database-secret' || true)"
DB_REV_TWO="$(helm template vexa "$CHART" -n vexa -f "$VALUES" \
  --set postgres.createCredentialsSecret=false --set-string postgres.credentialsSecretName=operator-db \
  --set-string postgres.credentialsSecretRevision=db-rev-two \
  --show-only templates/deployment-meeting-api.yaml | grep 'checksum/database-secret' || true)"
if [ -n "$DB_REV_ONE" ] && [ -n "$DB_REV_TWO" ] && [ "$DB_REV_ONE" != "$DB_REV_TWO" ]; then
  echo "  OK: external database Secret revision drives consumer rollout"
else
  echo "  FAIL: external database Secret revision does not alter consumer Pod templates"; fail=1
fi

# Schema convergence is a mandatory lifecycle gate for every hosted or managed Minutes install.
expect_fail "staging requires the migration hook" \
  --set global.deploymentProfile=staging \
  --set-string secrets.existingSecretName=operator-platform \
  --set-string secrets.existingSecretRevision=rev-20260716 \
  --set terminal.oauth.google.enabled=true \
  --set-string runtime.workloadNamespace=vexa-workloads

# PDBs must never select absent Deployments, and the staging overlay intentionally omits the
# in-chart Terminal because the external launch BFF/UI owns that edge.
NO_GATEWAY="$(helm template vexa "$CHART" -n vexa -f "$VALUES" --set gateway.enabled=false)"
if grep -q 'name: vexa-vexa-gateway-pdb' <<<"$NO_GATEWAY"; then
  echo "  FAIL: disabled gateway still receives a PDB"; fail=1
else
  echo "  OK: disabled components receive no PDB"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-staging.yaml" \
  "${STAGING_RENDER_ARGS[@]}" \
  --show-only templates/deployment-terminal.yaml 2>/dev/null | grep -q '^kind: Deployment'; then
  echo "  FAIL: staging still deploys the in-chart Terminal"; fail=1
else
  echo "  OK: staging leaves the user edge to the external launch UI/BFF"
fi

[ "$fail" -eq 0 ] && { echo "gate:helm-launch-hardening PASS"; exit 0; }
echo "gate:helm-launch-hardening FAIL"; exit 1
