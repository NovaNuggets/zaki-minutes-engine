#!/usr/bin/env bash
# Render the v0.12 vexa chart (no cluster required) and assert the carved control plane is present:
# 5 service Deployments, postgres + minio StatefulSets, redis, minio-init Job, runtime SA/Role/
# RoleBinding (k8s backend), agent-workspaces PVC. This is the gate:helm static proof.
set -euo pipefail

HELM_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CHART="$HELM_DIR/charts/vexa"
STAGING_RENDER_ARGS=(
  --set-string secrets.existingSecretRevision=test-revision-20260716
  --set-string postgres.credentialsSecretRevision=test-db-revision-20260716
  --set-string global.imageTag=260716-abcdef
  --set-string postgres.image=postgres@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  --set-string redis.image=redis@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
)

if ! command -v helm >/dev/null 2>&1; then
  echo "SKIP: helm not installed"; exit 0
fi

RENDER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml")"

fail=0
need() {  # need <count> <grep-pattern> <label>
  local want="$1" pat="$2" label="$3" got
  got="$(printf '%s\n' "$RENDER" | grep -cE "$pat" || true)"
  if [ "$got" -ge "$want" ]; then echo "  OK: $label ($got)"; else echo "  FAIL: $label — want >=$want got $got"; fail=1; fi
}

echo "=== gate:helm — template render assertions ==="
# 6 long-running services (+ terminal) + redis = 7 Deployments
need 7 '^kind: Deployment'    "Deployments"
need 2 '^kind: StatefulSet'   "StatefulSets (postgres+minio)"
need 9 '^kind: Service$'      "Services"
need 1 'name: vexa-vexa-terminal' "terminal present"
need 1 '^kind: ServiceAccount' "runtime ServiceAccount"
need 1 '^kind: Role$'         "runtime Role"
need 1 '^kind: RoleBinding'   "runtime RoleBinding"
need 1 '^kind: Job'           "minio-init Job"
need 2 '^kind: PersistentVolumeClaim' "PVCs (redis+workspaces)"
need 1 'name: vexa-vexa-agent-api' "agent-api present"
need 1 'RUNTIME_BACKEND'      "runtime backend env"
need 1 'serviceAccountName: vexa-vexa-runtime' "runtime SA bound"
# model-auth wiring: worker creds ride the dispatch spec env FROM agent-api, so agent-api must
# carry the optional secret refs (values-test leaves auth unset — CI has no creds; render + boot
# must stay green, the env ref is optional:true).
need 1 'key: CLAUDE_CODE_OAUTH_TOKEN' "agent-api CLAUDE_CODE_OAUTH_TOKEN secret ref"
need 2 'key: ANTHROPIC_AUTH_TOKEN'    "ANTHROPIC_AUTH_TOKEN secret refs (agent-api + runtime)"

component_need() {  # component_need <template> <grep-pattern> <label>
  local template="$1" pat="$2" label="$3" got
  got="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --show-only "templates/$template" | grep -cE "$pat" || true)"
  if [ "$got" -ge 1 ]; then echo "  OK: $label ($got)"; else echo "  FAIL: $label — got $got"; fail=1; fi
}

# Dynamic Settings + credential-test parity. Each server that resolves or tests STT must receive
# the same internal admin edge and operator-owned env fallback in a default chart render.
component_need deployment-meeting-api.yaml 'name: ADMIN_API_URL' "meeting-api admin settings URL"
component_need deployment-admin-api.yaml 'name: VEXA_USER_MODEL_ALLOWED_HOSTS' "admin-api personal model host allowlist"
component_need deployment-agent-api.yaml 'name: VEXA_ADMIN_API_URL' "agent-api admin settings URL"
component_need deployment-agent-api.yaml 'name: VEXA_MEETING_API_URL' "agent-api meeting owner authority URL"
component_need deployment-agent-api.yaml 'name: VEXA_INTERNAL_API_SECRET' "agent-api internal settings secret"
component_need deployment-agent-api.yaml 'name: VEXA_REQUIRE_GATEWAY_IDENTITY' "agent-api requires gateway-proven identity"
component_need deployment-agent-api.yaml 'key: GATEWAY_IDENTITY_SECRET' "agent-api dedicated gateway identity verifier"
component_need deployment-gateway.yaml 'key: GATEWAY_IDENTITY_SECRET' "gateway dedicated identity signer"
component_need deployment-agent-api.yaml 'key: GATEWAY_IDENTITY_PREVIOUS_SECRET' "agent-api optional previous gateway verifier"
GATEWAY_IDENTITY_RENDER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --show-only templates/deployment-gateway.yaml)"
if grep -q 'GATEWAY_IDENTITY_PREVIOUS_SECRET' <<<"$GATEWAY_IDENTITY_RENDER"; then
  echo "  FAIL: gateway received its verifier-only previous signing key"; fail=1
else
  echo "  OK: gateway signs with the current identity key only"
fi
component_need deployment-agent-api.yaml 'name: TRANSCRIPTION_SERVICE_URL' "agent-api STT fallback URL"
component_need deployment-agent-api.yaml 'key: TRANSCRIPTION_SERVICE_TOKEN' "agent-api STT fallback token"
component_need deployment-terminal.yaml 'name: VEXA_INTERNAL_API_SECRET' "terminal internal settings secret"
component_need deployment-terminal.yaml 'name: TRANSCRIPTION_SERVICE_URL' "terminal dictation STT URL"
component_need deployment-terminal.yaml 'key: TRANSCRIPTION_SERVICE_TOKEN' "terminal dictation STT token"
component_need deployment-terminal.yaml 'name: VEXA_TERMINAL_SHARED_KEY_MODE' "terminal explicit shared-key mode flag"
component_need deployment-terminal.yaml 'key: NEXTAUTH_SECRET' "terminal NextAuth signing secret ref"
component_need deployment-runtime.yaml 'name: RUNTIME_CONTROL_SECRET' "runtime control credential"
component_need deployment-runtime.yaml 'name: RUNTIME_CALLBACK_SECRET' "runtime callback credential"
component_need deployment-meeting-api.yaml 'name: RUNTIME_CONTROL_SECRET' "meeting-api runtime controller credential"
component_need deployment-meeting-api.yaml 'name: RUNTIME_CALLBACK_SECRET' "meeting-api runtime callback verifier credential"
component_need deployment-agent-api.yaml 'name: VEXA_RUNTIME_CONTROL_SECRET' "agent-api runtime controller credential"
component_need deployment-meeting-api.yaml 'name: MEETING_TOKEN_SECRET' "meeting-api dedicated MeetingToken signer"
component_need deployment-meeting-api.yaml 'key: MINIO_ACCESS_KEY' "meeting-api MinIO access credential secret ref"
component_need deployment-meeting-api.yaml 'key: MINIO_SECRET_KEY' "meeting-api MinIO secret credential secret ref"
component_need deployment-minio.yaml 'key: MINIO_ACCESS_KEY' "MinIO root user secret ref"
component_need deployment-minio.yaml 'key: MINIO_SECRET_KEY' "MinIO root password secret ref"
component_need job-minio-init.yaml 'key: MINIO_ACCESS_KEY' "minio-init access credential secret ref"
component_need job-minio-init.yaml 'key: MINIO_SECRET_KEY' "minio-init secret credential secret ref"

# Stateful workloads have a safe anti-affinity default, but an operator-wide affinity policy must
# replace it through one canonical YAML key. Duplicate affinity keys are especially dangerous here:
# Kubernetes' YAML decoder silently keeps one and drops the other.
CUSTOM_AFFINITY='{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"vexa.ai/custom-placement","operator":"Exists"}]}]}}}'
for template in deployment-redis.yaml statefulset-postgres.yaml deployment-minio.yaml; do
  AFFINITY_RENDER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-json "global.affinity=$CUSTOM_AFFINITY" --show-only "templates/$template")"
  AFFINITY_KEYS="$(grep -cE '^[[:space:]]+affinity:$' <<<"$AFFINITY_RENDER" || true)"
  if [ "$AFFINITY_KEYS" -eq 1 ] \
    && grep -qF 'vexa.ai/custom-placement' <<<"$AFFINITY_RENDER" \
    && ! grep -qF 'podAntiAffinity:' <<<"$AFFINITY_RENDER"; then
    echo "  OK: $template renders one operator affinity policy"
  else
    echo "  FAIL: $template duplicated or ignored global.affinity"; fail=1
  fi
done

for template in deployment-admin-api.yaml deployment-meeting-api.yaml deployment-runtime.yaml \
  deployment-terminal.yaml; do
  GATEWAY_IDENTITY_NON_OWNER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --show-only "templates/$template")"
  if grep -qF 'GATEWAY_IDENTITY_SECRET' <<<"$GATEWAY_IDENTITY_NON_OWNER"; then
    echo "  FAIL: $template receives the gateway identity proof"; fail=1
  else
    echo "  OK: $template receives no gateway identity proof"
  fi
done

for invalid in short ' leading-space-gateway-identity-secret-1234567890' \
  "$(printf 'x%.0s' {1..513})"; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "secrets.gatewayIdentitySecret=$invalid" >/dev/null 2>&1; then
    echo "  FAIL: gateway identity proof accepted an invalid 32..512 printable-ASCII value"; fail=1
  else
    echo "  OK: gateway identity proof enforces its bounded printable-ASCII contract"
  fi
done
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set-string secrets.gatewayIdentitySecret=vexa-internal-secret >/dev/null 2>&1; then
  echo "  FAIL: gateway identity proof reused the internal API credential"; fail=1
else
  echo "  OK: gateway identity proof is distinct from the internal API credential"
fi

for template in deployment-meeting-api.yaml deployment-minio.yaml job-minio-init.yaml; do
  MINIO_CONSUMER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --show-only "templates/$template")"
  if grep -Eq 'value: "vexa-(access|secret)-key"' <<<"$MINIO_CONSUMER"; then
    echo "  FAIL: $template renders literal MinIO credentials"; fail=1
  else
    echo "  OK: $template renders no literal MinIO credentials"
  fi
done

# Bundled Postgres may consume a pre-created credentials Secret in hosted environments. The chart
# must not recreate/overwrite it or leave staging on the local `postgres` password.
POSTGRES_OPERATOR_SECRET='operator-postgres-credentials'
PRECREATED_POSTGRES_ARGS=(
  --set postgres.createCredentialsSecret=false
  --set-string "postgres.credentialsSecretName=$POSTGRES_OPERATOR_SECRET"
)
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set postgres.enabled=false \
  --set-string database.host=db.example.internal >/dev/null 2>&1; then
  echo "  FAIL: external Postgres rendered while its credential Secret was neither created nor declared"; fail=1
else
  echo "  OK: external Postgres rejects the incomplete chart-created Secret mode"
fi
for collision in \
  'postgres.credentialsSecretName=vexa-vexa-secrets' \
  'secrets.existingSecretName=postgres-credentials'; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "$collision" >/dev/null 2>&1; then
    echo "  FAIL: chart-created Postgres Secret collided with the platform Secret ($collision)"; fail=1
  else
    echo "  OK: chart-created Postgres Secret cannot overwrite the platform Secret ($collision)"
  fi
done
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set postgres.createCredentialsSecret=false \
  --set-string postgres.credentialsSecretName= >/dev/null 2>&1; then
  echo "  FAIL: pre-created Postgres mode accepted no Secret name"; fail=1
else
  echo "  OK: pre-created Postgres mode requires an explicit Secret name"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set postgres.createCredentialsSecret=false \
  --set-string postgres.credentialsSecretName=' Invalid Secret ' >/dev/null 2>&1; then
  echo "  FAIL: pre-created Postgres mode accepted a non-canonical Secret name"; fail=1
else
  echo "  OK: Postgres credentials Secret name is canonical"
fi
PRECREATED_POSTGRES_SECRETS="$(helm template vexa "$CHART" -n vexa \
  -f "$CHART/values-test.yaml" "${PRECREATED_POSTGRES_ARGS[@]}" \
  --show-only templates/secret.yaml)"
if grep -qF "  name: $POSTGRES_OPERATOR_SECRET" <<<"$PRECREATED_POSTGRES_SECRETS"; then
  echo "  FAIL: chart recreated the operator-owned Postgres Secret"; fail=1
else
  echo "  OK: chart does not recreate the operator-owned Postgres Secret"
fi
for template in deployment-meeting-api.yaml deployment-admin-api.yaml; do
  POSTGRES_CONSUMER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    "${PRECREATED_POSTGRES_ARGS[@]}" --show-only "templates/$template")"
  if printf '%s\n' "$POSTGRES_CONSUMER" | grep -B2 -A2 -m1 -- 'key: POSTGRES_PASSWORD' \
    | grep -Eq "name: \"?$POSTGRES_OPERATOR_SECRET\"?$"; then
    echo "  OK: $template uses the operator-owned Postgres Secret"
  else
    echo "  FAIL: $template does not use the operator-owned Postgres Secret"; fail=1
  fi
done
POSTGRES_STATEFULSET="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${PRECREATED_POSTGRES_ARGS[@]}" --show-only templates/statefulset-postgres.yaml)"
if grep -Eq "name: \"?$POSTGRES_OPERATOR_SECRET\"?$" <<<"$POSTGRES_STATEFULSET"; then
  echo "  OK: bundled Postgres uses the operator-owned credentials Secret"
else
  echo "  FAIL: bundled Postgres does not use the operator-owned credentials Secret"; fail=1
fi
EXTERNAL_POSTGRES_ARGS=(
  --set postgres.enabled=false
  --set postgres.createCredentialsSecret=false
  --set-string "postgres.credentialsSecretName=$POSTGRES_OPERATOR_SECRET"
  --set-string database.host=db.example.internal
)
EXTERNAL_POSTGRES_RENDER="$(helm template vexa "$CHART" -n vexa \
  -f "$CHART/values-test.yaml" "${EXTERNAL_POSTGRES_ARGS[@]}")"
if grep -q 'app.kubernetes.io/component: postgres' <<<"$EXTERNAL_POSTGRES_RENDER" \
  || grep -qF "  name: $POSTGRES_OPERATOR_SECRET" <<<"$EXTERNAL_POSTGRES_RENDER"; then
  echo "  FAIL: external Postgres rendered a bundled database or recreated its operator Secret"; fail=1
else
  echo "  OK: external Postgres renders no bundled database and preserves its operator Secret"
fi
for template in deployment-meeting-api.yaml deployment-admin-api.yaml; do
  EXTERNAL_POSTGRES_CONSUMER="$(helm template vexa "$CHART" -n vexa \
    -f "$CHART/values-test.yaml" "${EXTERNAL_POSTGRES_ARGS[@]}" \
    --show-only "templates/$template")"
  if printf '%s\n' "$EXTERNAL_POSTGRES_CONSUMER" | grep -B2 -A2 -m1 -- 'key: POSTGRES_PASSWORD' \
    | grep -Eq "name: \"?$POSTGRES_OPERATOR_SECRET\"?$"; then
    echo "  OK: $template uses the external Postgres operator Secret"
  else
    echo "  FAIL: $template lost the external Postgres operator Secret"; fail=1
  fi
done
for template in deployment-meeting-api.yaml deployment-admin-api.yaml; do
  TLS_POSTGRES_CONSUMER="$(helm template vexa "$CHART" -n vexa \
    -f "$CHART/values-test.yaml" "${EXTERNAL_POSTGRES_ARGS[@]}" \
    --set database.sslMode=verify-full --show-only "templates/$template")"
  if printf '%s\n' "$TLS_POSTGRES_CONSUMER" | grep -A1 -m1 -- '- name: DB_SSL_MODE' \
    | grep -qF 'value: "verify-full"'; then
    echo "  OK: $template receives the typed external Postgres TLS policy"
  else
    echo "  FAIL: $template lost database.sslMode"; fail=1
  fi
done
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set database.sslMode=require >/dev/null 2>&1; then
  echo "  FAIL: bundled Postgres accepted a service-side TLS policy it cannot terminate"; fail=1
else
  echo "  OK: bundled Postgres rejects unsupported service-side TLS"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${EXTERNAL_POSTGRES_ARGS[@]}" --set pgbouncer.enabled=true \
  --set database.sslMode=require >/dev/null 2>&1; then
  echo "  FAIL: PgBouncer accepted TLS without typed client/server certificate config"; fail=1
else
  echo "  OK: PgBouncer rejects TLS until its certificate policy is typed"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${EXTERNAL_POSTGRES_ARGS[@]}" --set database.sslMode=prefer >/dev/null 2>&1; then
  echo "  FAIL: database.sslMode accepted a downgrade-capable mode"; fail=1
else
  echo "  OK: database.sslMode is a strict fail-closed enum"
fi
MIGRATION_RENDER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set migrations.enabled=true --set-string global.imageTag=migration-cut \
  --show-only templates/job-migrations.yaml)"
if grep -qF '"helm.sh/hook": post-install,pre-upgrade' <<<"$MIGRATION_RENDER" \
  && grep -qF 'image: "vexaai/v012-admin-api:migration-cut"' <<<"$MIGRATION_RENDER" \
  && grep -A3 -m1 -- 'command:' <<<"$MIGRATION_RENDER" | grep -qF -- '- admin_api.migrate'; then
  echo "  OK: migration Job uses the supported admin schema entrypoint and safe Helm lifecycle"
else
  echo "  FAIL: migration Job still targets a missing module, wrong image/tag, or unsafe hook"; fail=1
fi
if grep -q 'POSTGRES_PASSWORD: "postgres"' <<<"$(helm template vexa "$CHART" -n vexa \
  -f "$CHART/values-staging.yaml" "${STAGING_RENDER_ARGS[@]}")"; then
  echo "  FAIL: staging renders the local Postgres password"; fail=1
else
  echo "  OK: staging renders no local Postgres password"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-staging.yaml" \
  "${STAGING_RENDER_ARGS[@]}" \
  --set postgres.createCredentialsSecret=true \
  --set-string database.password=postgres >/dev/null 2>&1; then
  echo "  FAIL: hardened chart accepted the local Postgres password"; fail=1
else
  echo "  OK: hardened chart rejects the local Postgres password"
fi

EXTERNAL_MINIO_SECRET='operator-platform-secrets'
for key in minioAccessKey minioSecretKey; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "secrets.$key=" >/dev/null 2>&1; then
    echo "  FAIL: in-chart MinIO accepted a missing secrets.$key"; fail=1
  else
    echo "  OK: in-chart MinIO requires secrets.$key"
  fi
  if ! helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "secrets.existingSecretName=$EXTERNAL_MINIO_SECRET" \
    --set-string "secrets.$key=" >/dev/null 2>&1; then
    echo "  FAIL: operator Secret could not supply $key"; fail=1
  else
    echo "  OK: operator Secret may supply $key without an inline value"
  fi
done
for legacy_key in accessKey secretKey; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "minio.$legacy_key=legacy-credential" >/dev/null 2>&1; then
    echo "  FAIL: legacy minio.$legacy_key override was silently ignored"; fail=1
  else
    echo "  OK: legacy minio.$legacy_key fails with an explicit migration"
  fi
done
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set minio.enabled=false >/dev/null 2>&1; then
  echo "  FAIL: meeting-api accepted a dangling unsupported external object-store path"; fail=1
else
  echo "  OK: meeting-api fails closed when the typed in-chart object store is disabled"
fi

for template in deployment-meeting-api.yaml deployment-minio.yaml job-minio-init.yaml; do
  MINIO_CONSUMER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "secrets.existingSecretName=$EXTERNAL_MINIO_SECRET" \
    --show-only "templates/$template")"
  for key in MINIO_ACCESS_KEY MINIO_SECRET_KEY; do
    if printf '%s\n' "$MINIO_CONSUMER" | grep -B2 -A2 -m1 -- "key: $key" \
      | grep -qF "name: $EXTERNAL_MINIO_SECRET"; then
      echo "  OK: $template sources $key from the operator Secret"
    else
      echo "  FAIL: $template does not source $key from the operator Secret"; fail=1
    fi
  done
done

component_env_need() {  # component_env_need <template> <env-name> <value> <label>
  local template="$1" env_name="$2" want="$3" label="$4" rendered
  rendered="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --show-only "templates/$template")"
  if printf '%s\n' "$rendered" | grep -A1 -m1 -- "- name: $env_name" | grep -qF -- "value: \"$want\""; then
    echo "  OK: $label"
  else
    echo "  FAIL: $label — expected $env_name=$want"; fail=1
  fi
}

# Minutes stays inert until an operator enables it. The bundled Agent is deliberately not a
# Minutes consumer: it has an unscoped Redis connection and cannot safely share the Minutes
# topology until scoped mediation exists.
component_env_need deployment-admin-api.yaml ZAKI_MINUTES_CAPTURE_ENABLED false "admin-api Minutes capture is default-off"
component_env_need deployment-admin-api.yaml ZAKI_MINUTES_READ_ENABLED false "admin-api Minutes read is default-off"
component_env_need deployment-meeting-api.yaml ZAKI_MINUTES_CAPTURE_ENABLED false "meeting-api Minutes capture is default-off"
component_env_need deployment-meeting-api.yaml ZAKI_MINUTES_INVOCATION_V2_ENABLED false "meeting-api invocation v2 producer is default-off"
component_env_need deployment-meeting-api.yaml ZAKI_MINUTES_READ_ENABLED false "meeting-api Minutes read is default-off"
component_env_need deployment-meeting-api.yaml ZAKI_MINUTES_AUTO_JOIN_ENABLED false "meeting-api calendar auto-join is forced off"
component_env_need deployment-meeting-api.yaml ZAKI_MINUTES_MANAGED_ONLY false "ordinary meeting-api keeps the upstream bot-create path"
component_env_need deployment-meeting-api.yaml ZAKI_MINUTES_FINALIZED_ENABLED false "meeting-api platform finalized delivery is default-off"
component_env_need deployment-meeting-api.yaml MINUTES_TTL_ENABLED false "meeting-api Minutes TTL is default-off"
component_env_need deployment-meeting-api.yaml MINUTES_TTL_INTERVAL_S 60 "meeting-api Minutes TTL interval defaults to 60s"
component_env_need deployment-meeting-api.yaml MINUTES_TTL_BATCH_SIZE 100 "meeting-api Minutes TTL batch defaults to 100"
component_env_need deployment-meeting-api.yaml AGENT_API_URL http://vexa-vexa-agent-api:8100 "meeting-api uses the in-chart agent service"
component_env_need deployment-runtime.yaml RUNTIME_AGENT_PROFILE_ENABLED true "default runtime enables its bundled Agent profile"
component_env_need deployment-runtime.yaml RUNTIME_CALLBACK_TRUSTED_ORIGINS 'http://vexa-vexa-meeting-api:8080' "runtime callback credential is limited to the exact meeting-api origin"

RUNTIME_MINUTES_DEFAULT="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --show-only templates/deployment-runtime.yaml)"
if grep -q 'name: MINUTES_BROWSER_IMAGE' <<<"$RUNTIME_MINUTES_DEFAULT"; then
  echo "  FAIL: default runtime registers the managed Minutes bot profile"; fail=1
else
  echo "  OK: default runtime registers no managed Minutes bot profile"
fi

AGENT_MINUTES_DEFAULT="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --show-only templates/deployment-agent-api.yaml)"
if grep -Eq 'name: (ZAKI_MINUTES_|ZAKI_READ_TOKEN_MINUTES|ZAKI_AGENT_ERASURE_)' \
  <<<"$AGENT_MINUTES_DEFAULT"; then
  echo "  FAIL: bundled agent-api receives Minutes flags or credentials"; fail=1
else
  echo "  OK: bundled agent-api receives no Minutes flags or credentials"
fi
for override in \
  ZAKI_MINUTES_CAPTURE_ENABLED \
  ZAKI_MINUTES_INVOCATION_V2_ENABLED \
  ZAKI_MINUTES_READ_ENABLED \
  ZAKI_MINUTES_READ_BASE_URL \
  ZAKI_MINUTES_ERASURE_SIGNING_SECRET \
  ZAKI_MINUTES_FINALIZED_SECRET \
  ZAKI_MINUTES_HUB_TOKEN \
  ZAKI_READ_TOKEN_MINUTES \
  ZAKI_AGENT_ERASURE_SIGNING_KEY_ID \
  ZAKI_AGENT_ERASURE_SIGNING_SECRET \
  ZAKI_AGENT_ERASURE_VERIFICATION_SECRET \
  ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET \
  MINUTES_TTL_ENABLED \
  MINUTES_BROWSER_IMAGE; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "agentApi.extraEnv[0].name=$override" \
    --set-string 'agentApi.extraEnv[0].value=plaintext-bypass' >/dev/null 2>&1; then
    echo "  FAIL: agentApi.extraEnv reintroduced reserved Minutes setting $override"; fail=1
  else
    echo "  OK: agentApi.extraEnv cannot reintroduce reserved Minutes setting $override"
  fi
done
for override in \
  'minutes.captureEnabled=true' \
  'minutes.invocationV2Enabled=true' \
  'minutes.botImage=vexaai/zaki-minutes-bot:v2' \
  'minutes.readEnabled=true' \
  'minutes.ttl.enabled=true' \
  'minutes.finalized.enabled=true' \
  'minutes.agentErasureKeyId=historical-agent-erasure'; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set "$override" >/dev/null 2>&1; then
    echo "  FAIL: bundled Agent accepted Minutes mode $override"; fail=1
  else
    echo "  OK: bundled Agent rejects Minutes mode $override"
  fi
done
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set agentApi.enabled=false \
  --set-string minutes.botImage=vexaai/zaki-minutes-bot:v2 >/dev/null 2>&1; then
  echo "  FAIL: runtime accepted a dangling Minutes bot image without invocation-v2"; fail=1
else
  echo "  OK: Minutes bot image cannot register without invocation-v2"
fi

RUNTIME_AUTH_DEFAULT="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --show-only templates/deployment-runtime.yaml)"
if grep -q -- 'name: INTERNAL_API_SECRET' <<<"$RUNTIME_AUTH_DEFAULT"; then
  echo "  FAIL: runtime still receives the platform-internal credential"; fail=1
else
  echo "  OK: runtime receives no platform-internal credential"
fi
for template in deployment-runtime.yaml deployment-meeting-api.yaml; do
  CALLBACK_CONSUMER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --show-only "templates/$template")"
  if printf '%s\n' "$CALLBACK_CONSUMER" | grep -A4 -m1 -- '- name: RUNTIME_CALLBACK_SECRET' \
    | grep -q 'key: RUNTIME_CALLBACK_SECRET'; then
    echo "  OK: $template receives callback auth through secretKeyRef"
  else
    echo "  FAIL: $template does not receive callback auth through secretKeyRef"; fail=1
  fi
done
for template in deployment-agent-api.yaml deployment-admin-api.yaml deployment-gateway.yaml deployment-terminal.yaml; do
  NON_CALLBACK_CONSUMER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --show-only "templates/$template")"
  if grep -q -- 'RUNTIME_CALLBACK_SECRET' <<<"$NON_CALLBACK_CONSUMER"; then
    echo "  FAIL: $template receives the runtime callback credential"; fail=1
  else
    echo "  OK: $template receives no runtime callback credential"
  fi
done
if ! helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set-string secrets.runtimeCallbackSecret=vexa-internal-secret >/dev/null 2>&1 \
  && ! helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set-string secrets.runtimeCallbackSecret=vexa-runtime-control-secret >/dev/null 2>&1; then
  echo "  OK: inline callback credential must be distinct from internal and control credentials"
else
  echo "  FAIL: inline callback credential reused another authentication secret"; fail=1
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set-string secrets.adminApiToken=shared-platform-auth-secret \
  --set-string secrets.internalApiSecret=shared-platform-auth-secret >/dev/null 2>&1; then
  echo "  FAIL: inline admin and internal authentication domains accepted one credential"; fail=1
else
  echo "  OK: inline platform trust domains require distinct credentials"
fi
for override in \
  'runtime.extraEnv[0].name=RUNTIME_CALLBACK_SECRET' \
  'runtime.extraEnv[0].name=RUNTIME_CALLBACK_TRUSTED_ORIGINS' \
  'meetingApi.extraEnv[0].name=RUNTIME_CALLBACK_SECRET'; do
  base="${override%%.name=*}"
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "$override" --set-string "$base.value=plaintext-bypass" >/dev/null 2>&1; then
    echo "  FAIL: extraEnv overrode reserved callback authentication setting $override"; fail=1
  else
    echo "  OK: extraEnv cannot override reserved callback setting $override"
  fi
done
for override in \
  'runtime.extraEnv[0].name=AGENT_IMAGE' \
  'runtime.extraEnv[0].name=AGENT_WORKER_IMAGE' \
  'runtime.extraEnv[0].name=RUNTIME_AGENT_PROFILE_ENABLED' \
  'runtime.extraEnv[0].name=MINUTES_BROWSER_IMAGE' \
  'meetingApi.extraEnv[0].name=AGENT_API_URL' \
  'meetingApi.extraEnv[0].name=ZAKI_MINUTES_READ_ENABLED' \
  'meetingApi.extraEnv[0].name=ZAKI_READ_TOKEN_MINUTES' \
  'meetingApi.extraEnv[0].name=ZAKI_MINUTES_HUB_TOKEN' \
  'meetingApi.extraEnv[0].name=ZAKI_AGENT_ERASURE_VERIFICATION_SECRET' \
  'meetingApi.extraEnv[0].name=ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET' \
  'meetingApi.extraEnv[0].name=MINUTES_TTL_ENABLED' \
  'adminApi.extraEnv[0].name=ZAKI_MINUTES_CAPTURE_ENABLED' \
  'adminApi.extraEnv[0].name=ZAKI_MINUTES_READ_ENABLED'; do
  base="${override%%.name=*}"
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "$override" --set-string "$base.value=plaintext-bypass" >/dev/null 2>&1; then
    echo "  FAIL: extraEnv reintroduced Agent/Minutes topology setting $override"; fail=1
  else
    echo "  OK: extraEnv cannot reintroduce Agent/Minutes topology setting $override"
  fi
done

for override in \
  'adminApi.extraEnv[0].name=ADMIN_API_TOKEN' \
  'adminApi.extraEnv[0].name=DB_HOST' \
  'adminApi.extraEnv[0].name=DB_SSL_MODE' \
  'adminApi.extraEnv[0].name=DATABASE_URL' \
  'adminApi.extraEnv[0].name=VEXA_USER_MODEL_ALLOWED_HOSTS' \
  'agentApi.extraEnv[0].name=VEXA_INTERNAL_API_SECRET' \
  'agentApi.extraEnv[0].name=VEXA_RUNTIME_CONTROL_SECRET' \
  'agentApi.extraEnv[0].name=VEXA_GATEWAY_URL' \
  'agentApi.extraEnv[0].name=ANTHROPIC_AUTH_TOKEN' \
  'gateway.extraEnv[0].name=INTERNAL_API_SECRET' \
  'gateway.extraEnv[0].name=ADMIN_API_URL' \
  'meetingApi.extraEnv[0].name=INTERNAL_API_SECRET' \
  'meetingApi.extraEnv[0].name=DB_HOST' \
  'meetingApi.extraEnv[0].name=DB_SSL_MODE' \
  'meetingApi.extraEnv[0].name=DATABASE_URL' \
  'meetingApi.extraEnv[0].name=MINIO_ENDPOINT' \
  'meetingApi.extraEnv[0].name=S3_ENDPOINT' \
  'meetingApi.extraEnv[0].name=S3_ACCESS_KEY' \
  'meetingApi.extraEnv[0].name=S3_SECRET_KEY' \
  'meetingApi.extraEnv[0].name=RECORDING_BUCKET' \
  'meetingApi.extraEnv[0].name=STORAGE_BACKEND' \
  'meetingApi.extraEnv[0].name=TRANSCRIPTION_SERVICE_URL' \
  'runtime.extraEnv[0].name=INTERNAL_API_SECRET' \
  'runtime.extraEnv[0].name=RUNTIME_BACKEND' \
  'runtime.extraEnv[0].name=ANTHROPIC_AUTH_TOKEN' \
  'runtime.extraEnv[0].name=ZAKI_READ_TOKEN_MINUTES'; do
  base="${override%%.name=*}"
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "$override" --set-string "$base.value=plaintext-or-routing-bypass" \
    >/dev/null 2>&1; then
    echo "  FAIL: extraEnv overrode chart-owned credential/routing setting $override"; fail=1
  else
    echo "  OK: extraEnv cannot override chart-owned setting $override"
  fi
done

MEETING_AUTH_DEFAULT="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --show-only templates/deployment-meeting-api.yaml)"
if printf '%s\n' "$MEETING_AUTH_DEFAULT" | grep -A4 -m1 -- '- name: MEETING_TOKEN_SECRET' \
  | grep -q 'key: MEETING_TOKEN_SECRET' \
  && ! grep -q -- 'name: ADMIN_TOKEN' <<<"$MEETING_AUTH_DEFAULT"; then
  echo "  OK: meeting-api receives only the dedicated MeetingToken secret"
else
  echo "  FAIL: meeting-api MeetingToken/admin credential separation is broken"; fail=1
fi
for template in deployment-runtime.yaml deployment-agent-api.yaml deployment-admin-api.yaml \
  deployment-gateway.yaml deployment-terminal.yaml; do
  NON_MEETING_TOKEN_CONSUMER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --show-only "templates/$template")"
  if grep -q -- 'MEETING_TOKEN_SECRET' <<<"$NON_MEETING_TOKEN_CONSUMER"; then
    echo "  FAIL: $template receives the MeetingToken signer"; fail=1
  else
    echo "  OK: $template receives no MeetingToken signer"
  fi
done
for reused in test-admin-token vexa-internal-secret vexa-runtime-control-secret \
  vexa-runtime-callback-secret vexa-redis-password; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "secrets.meetingTokenSecret=$reused" >/dev/null 2>&1; then
    echo "  FAIL: MeetingToken signer reused another credential ($reused)"; fail=1
  else
    echo "  OK: MeetingToken signer rejects credential reuse ($reused)"
  fi
done

REDIS_DEFAULT="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --show-only templates/deployment-redis.yaml)"
if grep -q -- '- "always"' <<<"$REDIS_DEFAULT" \
  && grep -q -- '- "noeviction"' <<<"$REDIS_DEFAULT" \
  && grep -q -- '- "512mb"' <<<"$REDIS_DEFAULT" \
  && grep -q -- 'key: REDIS_PASSWORD' <<<"$REDIS_DEFAULT" \
  && grep -q -- 'requirepass' <<<"$REDIS_DEFAULT" \
  && grep -q -- 'NOAUTH' <<<"$REDIS_DEFAULT" \
  && grep -q -- 'REDISCLI_AUTH' <<<"$REDIS_DEFAULT"; then
  echo "  OK: Redis is durable, password-authenticated, and probes anonymous rejection"
else
  echo "  FAIL: Redis durability/authentication defaults are not hardened"; fail=1
fi
for override in \
  'redis.maxmemory=0' \
  'redis.durability.appendonly=no' \
  'redis.durability.appendfsync=everysec' \
  'redis.maxmemoryPolicy=allkeys-lru'; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "$override" >/dev/null 2>&1; then
    echo "  FAIL: Redis accepted unsafe override $override"; fail=1
  else
    echo "  OK: Redis rejects unsafe override $override"
  fi
done
for backend in typo K8S; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "runtime.backend=$backend" >/dev/null 2>&1; then
    echo "  FAIL: runtime accepted ambiguous backend $backend"; fail=1
  else
    echo "  OK: runtime backend is an exact lowercase enum ($backend rejected)"
  fi
done

for pair in \
  'deployment-runtime.yaml:REDIS_URL' \
  'deployment-meeting-api.yaml:REDIS_URL' \
  'deployment-gateway.yaml:REDIS_URL' \
  'deployment-agent-api.yaml:VEXA_REDIS_URL'; do
  template="${pair%%:*}"
  env_name="${pair#*:}"
  REDIS_CONSUMER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --show-only "templates/$template")"
  if printf '%s\n' "$REDIS_CONSUMER" | grep -A4 -m1 -- "- name: $env_name" \
    | grep -q 'key: REDIS_URL' \
    && ! grep -q -- 'redis://' <<<"$REDIS_CONSUMER"; then
    echo "  OK: $template sources $env_name from Secret without rendering credentials"
  else
    echo "  FAIL: $template Redis credential projection is unsafe"; fail=1
  fi
done
if grep -q '^  REDIS_PASSWORD: "vexa-redis-password"' <<<"$RENDER" \
  && grep -q '^  REDIS_URL: "redis://:vexa-redis-password@vexa-vexa-redis.vexa.svc.cluster.local:6379/0"' <<<"$RENDER"; then
  echo "  OK: the local chart Secret owns authenticated in-chart Redis connection material"
else
  echo "  FAIL: the chart Secret does not contain authenticated in-chart Redis material"; fail=1
fi
for override in \
  'runtime.extraEnv[0].name=REDIS_URL' \
  'meetingApi.extraEnv[0].name=REDIS_URL' \
  'gateway.extraEnv[0].name=REDIS_URL' \
  'agentApi.extraEnv[0].name=VEXA_REDIS_URL'; do
  base="${override%%.name=*}"
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "$override" --set-string "$base.value=redis://anonymous-bypass" >/dev/null 2>&1; then
    echo "  FAIL: extraEnv overrode Secret-backed Redis setting $override"; fail=1
  else
    echo "  OK: extraEnv cannot override Secret-backed Redis setting $override"
  fi
done

if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set redis.enabled=false >/dev/null 2>&1; then
  echo "  FAIL: inline managed Redis accepted a missing host"; fail=1
else
  echo "  OK: inline managed Redis requires structured connection metadata"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set redis.enabled=false \
  --set-string redisConfig.host='cache.internal@attacker.example' >/dev/null 2>&1; then
  echo "  FAIL: inline managed Redis accepted a userinfo-injecting host"; fail=1
else
  echo "  OK: inline managed Redis rejects unsafe host syntax"
fi
MANAGED_REDIS_INLINE="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set redis.enabled=false \
  --set-string redisConfig.scheme=rediss \
  --set-string redisConfig.host=cache.example.internal \
  --set redisConfig.port=6380 \
  --set redisConfig.database=1 \
  --set-string redisConfig.username=default \
  --set-string secrets.redisPassword=managed-redis-password)"
if grep -q '^  REDIS_URL: "rediss://default:managed-redis-password@cache.example.internal:6380/1"' \
  <<<"$MANAGED_REDIS_INLINE"; then
  echo "  OK: inline managed Redis builds its authenticated URL only inside the Secret"
else
  echo "  FAIL: inline managed Redis Secret construction is incomplete"; fail=1
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set redis.enabled=false \
  --set-string secrets.existingSecretName=operator-owned-secrets >/dev/null 2>&1; then
  echo "  OK: managed Redis accepts an operator Secret carrying REDIS_URL"
else
  echo "  FAIL: managed Redis rejected an operator-owned REDIS_URL Secret"; fail=1
fi

MEETING_DEFAULT="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --show-only templates/deployment-meeting-api.yaml)"
AGENT_DEFAULT="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --show-only templates/deployment-agent-api.yaml)"
if printf '%s\n%s\n' "$MEETING_DEFAULT" "$AGENT_DEFAULT" | grep -q 'name: ZAKI_READ_TOKEN_MINUTES'; then
  echo "  FAIL: default-off Minutes read consumers still receive the cross-spoke token"; fail=1
else
  echo "  OK: default-off Minutes read consumers receive no cross-spoke token"
fi
for key in ZAKI_MINUTES_HUB_TOKEN ZAKI_AGENT_ERASURE_SIGNING_SECRET \
  ZAKI_AGENT_ERASURE_VERIFICATION_SECRET ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET \
  ZAKI_MINUTES_ERASURE_SIGNING_SECRET ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET \
  ZAKI_MINUTES_FINALIZED_SECRET; do
  if printf '%s\n%s\n' "$MEETING_DEFAULT" "$AGENT_DEFAULT" | grep -q "name: $key"; then
    echo "  FAIL: default-off Minutes still projects $key"; fail=1
  else
    echo "  OK: default-off Minutes omits $key"
  fi
done

NULLALIS_ERASURE_URL='https://nullalis.internal.example'
MINUTES_BOT_IMAGE='vexaai/zaki-minutes-bot@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd'
ERASURE_BOUNDARY=(
  --set agentApi.enabled=false
  --set gateway.enabled=false
  --set terminal.enabled=false
  # Managed/historical Minutes is hardened even in the local profile. Keep this inline fixture
  # deliberately production-shaped so tests below fail for the boundary they are exercising,
  # rather than short-circuiting on unrelated default credentials.
  --set-string secrets.adminApiToken=minutes-test-admin-7f2a9b3c
  --set-string secrets.internalApiSecret=minutes-test-internal-4d8e1f6a
  --set-string secrets.runtimeControlSecret=minutes-test-runtime-control-6e9c2a4b
  --set-string secrets.runtimeCallbackSecret=minutes-test-runtime-callback-8b3d7e5a
  --set-string secrets.meetingTokenSecret=minutes-test-meeting-token-5a8c2d7e
  --set-string secrets.gatewayIdentitySecret=minutes-test-gateway-identity-7a4c9e2b6d1f8a3c
  --set-string secrets.redisPassword=minutes-test-redis-password-1f4b9c6d
  --set-string secrets.minioAccessKey=minutes-test-minio-access-8d3f6a1c
  --set-string secrets.minioSecretKey=minutes-test-minio-secret-6b2e9d4f
  --set-string secrets.dispatchSigningKey=minutes-test-dispatch-9c5b2e7d
  --set-string secrets.nextauthSecret=minutes-test-nextauth-3a6f8d1c
  --set-string database.password=minutes-test-database-password-7f2a9b3c
  --set migrations.enabled=true
  --set-string runtime.workloadNamespace=vexa-workloads
  --set-string global.imageTag=260716-abcdef
  --set-string postgres.image=postgres@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  --set-string redis.image=redis@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
  --set minutes.ttl.enabled=true
  --set-string minutes.nullalisErasureBaseUrl="$NULLALIS_ERASURE_URL"
  --set-string minutes.agentErasureKeyId=agent-erasure-2026-07
  --set-string minutes.minutesErasureKeyId=minutes-erasure-2026-07
  --set-string secrets.minutesHubToken=minutes-hub-bearer-8d4f1a7c9e2b6
  --set-string secrets.agentErasureHmacSecret=agent-shared-hmac-7f2a9b3c5e8d1c
  --set-string secrets.minutesErasureSigningSecret=minutes-independent-hmac-4d8e1f6a
)
READ_KEYS=(
  "${ERASURE_BOUNDARY[@]}"
  --set minutes.readEnabled=true
)

if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set agentApi.enabled=false --set minutes.readEnabled=true \
  --set-string secrets.minutesReadToken=local-minutes-read-7f2a9b3c5e8d1c >/dev/null 2>&1; then
  echo "  FAIL: Minutes read accepted no complete erasure/TTL boundary"; fail=1
else
  echo "  OK: Minutes read requires the complete erasure/TTL boundary"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" \
  --set-string minutes.nullalisErasureBaseUrl= --set minutes.readEnabled=true \
  --set-string secrets.minutesReadToken=local-minutes-read-7f2a9b3c5e8d1c >/dev/null 2>&1; then
  echo "  FAIL: Minutes read accepted no external Nullalis erasure URL"; fail=1
else
  echo "  OK: Minutes read requires an explicit external Nullalis erasure URL"
fi
for bad_url in \
  'http://user:password@nullalis.internal.example' \
  'https://nullalis.internal.example?redirect=attacker' \
  'https://nullalis.internal.example:99999' \
  'http://vexa-vexa-agent-api:8100'; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    "${READ_KEYS[@]}" --set-string "minutes.nullalisErasureBaseUrl=$bad_url" \
    --set-string secrets.minutesReadToken=local-minutes-read-7f2a9b3c5e8d1c >/dev/null 2>&1; then
    echo "  FAIL: Minutes accepted unsafe/non-external Nullalis URL $bad_url"; fail=1
  else
    echo "  OK: Minutes rejects unsafe/non-external Nullalis URL $bad_url"
  fi
done
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${READ_KEYS[@]}" >/dev/null 2>&1; then
  echo "  FAIL: Minutes read accepted a missing read token"; fail=1
else
  echo "  OK: Minutes read rejects a missing read token"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${READ_KEYS[@]}" --set-string secrets.minutesReadToken=CHANGE_ME >/dev/null 2>&1; then
  echo "  FAIL: Minutes read accepted a placeholder read token"; fail=1
else
  echo "  OK: Minutes read rejects a placeholder read token"
fi
SHORT_MINUTES_SECRET='1234567890123456789012345678901'
NON_ASCII_MINUTES_SECRET='minutes-credential-12345678901234é'
OVERSIZED_MINUTES_SECRET="$(printf 'a%.0s' {1..513})"
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${READ_KEYS[@]}" --set-string "secrets.minutesReadToken=$SHORT_MINUTES_SECRET" >/dev/null 2>&1; then
  echo "  FAIL: Minutes read accepted a 31-character token"; fail=1
else
  echo "  OK: Minutes read token requires at least 32 characters"
fi
for bad_read_token in "$NON_ASCII_MINUTES_SECRET" "$OVERSIZED_MINUTES_SECRET"; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    "${READ_KEYS[@]}" --set-string "secrets.minutesReadToken=$bad_read_token" >/dev/null 2>&1; then
    echo "  FAIL: Minutes read accepted a non-ASCII/oversized token"; fail=1
  else
    echo "  OK: Minutes read token matches the runtime's printable-ASCII size contract"
  fi
done
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${READ_KEYS[@]}" --set-string 'secrets.minutesReadToken= local-minutes-read-7f2a9b3c5e8d1c ' >/dev/null 2>&1; then
  echo "  FAIL: Minutes read accepted a whitespace-wrapped token"; fail=1
else
  echo "  OK: Minutes read token rejects ambiguous outer whitespace"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${READ_KEYS[@]}" --set-string secrets.existingSecretName=change-me >/dev/null 2>&1; then
  echo "  FAIL: Minutes read accepted a placeholder operator Secret reference"; fail=1
else
  echo "  OK: Minutes read rejects a placeholder operator Secret reference"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${READ_KEYS[@]}" --set-string 'secrets.existingSecretName= vexa-minutes-secrets ' >/dev/null 2>&1; then
  echo "  FAIL: Minutes read accepted a whitespace-wrapped operator Secret name"; fail=1
else
  echo "  OK: operator Secret name rejects ambiguous outer whitespace"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${READ_KEYS[@]}" --set-string secrets.existingSecretName=vexa..minutes >/dev/null 2>&1; then
  echo "  FAIL: Minutes read accepted an invalid Kubernetes Secret name"; fail=1
else
  echo "  OK: operator Secret reference requires canonical DNS labels"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${READ_KEYS[@]}" \
  --set-string secrets.existingSecretName=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  >/dev/null 2>&1; then
  echo "  FAIL: Minutes read accepted a 64-character Kubernetes Secret label"; fail=1
else
  echo "  OK: operator Secret reference enforces the 63-character DNS-label limit"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${READ_KEYS[@]}" --set agentApi.enabled=true \
  --set-string secrets.minutesReadToken=local-minutes-read-7f2a9b3c5e8d1c >/dev/null 2>&1; then
  echo "  FAIL: Minutes read co-deployed the bundled Agent with unscoped Redis"; fail=1
else
  echo "  OK: Minutes read rejects the bundled Agent topology"
fi

READ_INLINE="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${READ_KEYS[@]}" \
  --set-string secrets.minutesReadToken=local-minutes-read-7f2a9b3c5e8d1c)"
if printf '%s\n' "$READ_INLINE" | grep -A4 -m1 -- '- name: ZAKI_READ_TOKEN_MINUTES' \
  | grep -q 'key: ZAKI_READ_TOKEN_MINUTES'; then
  echo "  OK: enabled Minutes read sources its token via secretKeyRef"
else
  echo "  FAIL: enabled Minutes read does not source its token via secretKeyRef"; fail=1
fi
for template in deployment-meeting-api.yaml; do
  READ_CONSUMER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    "${READ_KEYS[@]}" \
    --set-string secrets.minutesReadToken=local-minutes-read-7f2a9b3c5e8d1c \
    --show-only "templates/$template")"
  if printf '%s\n' "$READ_CONSUMER" | grep -A4 -m1 -- '- name: ZAKI_READ_TOKEN_MINUTES' \
    | grep -q 'key: ZAKI_READ_TOKEN_MINUTES'; then
    echo "  OK: $template receives the dedicated read token"
  else
    echo "  FAIL: $template does not receive the dedicated read token"; fail=1
  fi
done
if printf '%s\n' "$READ_CONSUMER" | grep -A1 -m1 -- '- name: AGENT_API_URL' \
  | grep -qF -- "value: \"$NULLALIS_ERASURE_URL\""; then
  echo "  OK: meeting-api erasure fan-out targets external Nullalis over bounded HTTP"
else
  echo "  FAIL: meeting-api does not target the explicit external Nullalis erasure URL"; fail=1
fi
READ_RUNTIME="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${READ_KEYS[@]}" --set-string secrets.minutesReadToken=local-minutes-read-7f2a9b3c5e8d1c \
  --show-only templates/deployment-runtime.yaml)"
if grep -Eq 'name: AGENT_(IMAGE|WORKER_IMAGE)' <<<"$READ_RUNTIME" \
  || ! printf '%s\n' "$READ_RUNTIME" | grep -A1 -m1 -- '- name: RUNTIME_AGENT_PROFILE_ENABLED' \
    | grep -q 'value: "false"'; then
  echo "  FAIL: external Minutes profile still registers a bundled Agent worker image"; fail=1
else
  echo "  OK: external Minutes profile registers no bundled Agent worker image"
fi
if grep -q 'name: vexa-vexa-agent-api' <<<"$READ_INLINE"; then
  echo "  FAIL: enabled Minutes read still deploys the bundled Agent"; fail=1
else
  echo "  OK: enabled Minutes read has no bundled Agent deployment"
fi
if grep -q '^  ZAKI_READ_TOKEN_MINUTES: "local-minutes-read-7f2a9b3c5e8d1c"' <<<"$READ_INLINE"; then
  echo "  OK: enabled inline Minutes token lands in the chart Secret"
else
  echo "  FAIL: enabled inline Minutes token is missing from the chart Secret"; fail=1
fi

STAGING_MEETING_OFF="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-staging.yaml" \
  "${STAGING_RENDER_ARGS[@]}" \
  --show-only templates/deployment-meeting-api.yaml 2>/dev/null || true)"
if printf '%s\n' "$STAGING_MEETING_OFF" | grep -q 'name: ZAKI_READ_TOKEN_MINUTES'; then
  echo "  FAIL: staging default-off Minutes read still receives its token"; fail=1
else
  echo "  OK: staging default-off Minutes read receives no token"
fi
STAGING_MEETING_ON="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-staging.yaml" \
  "${STAGING_RENDER_ARGS[@]}" \
  --set agentApi.enabled=false --set gateway.enabled=false --set terminal.enabled=false \
  --set minutes.readEnabled=true --set minutes.ttl.enabled=true \
  --set-string minutes.nullalisErasureBaseUrl="$NULLALIS_ERASURE_URL" \
  --set-string minutes.agentErasureKeyId=agent-erasure-2026-07 \
  --set-string minutes.minutesErasureKeyId=minutes-erasure-2026-07 \
  --show-only templates/deployment-meeting-api.yaml 2>/dev/null || true)"
if printf '%s\n' "$STAGING_MEETING_ON" | grep -A4 -m1 -- '- name: ZAKI_READ_TOKEN_MINUTES' \
  | grep -q 'name: vexa-v012-secrets' \
  && printf '%s\n' "$STAGING_MEETING_ON" | grep -A4 -m1 -- '- name: ZAKI_MINUTES_HUB_TOKEN' \
    | grep -q 'name: vexa-v012-secrets'; then
  echo "  OK: staging Minutes read and Hub auth source the operator Secret"
else
  echo "  FAIL: staging Minutes read/Hub auth do not source the operator Secret"; fail=1
fi

MINUTES_HOSTED_BASE=(
  --set agentApi.enabled=false
  --set gateway.enabled=false
  --set global.deploymentProfile=staging
  --set terminal.enabled=false
  --set migrations.enabled=true
  --set-string runtime.workloadNamespace=vexa-workloads
  --set-string global.imageTag=260716-abcdef
  --set-string postgres.image=postgres@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  --set-string redis.image=redis@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
  --set-string secrets.adminApiToken=prod-admin-7f2a9b3c
  --set-string secrets.internalApiSecret=prod-internal-4d8e1f6a
  --set-string secrets.runtimeControlSecret=prod-runtime-control-6e9c2a4b
  --set-string secrets.runtimeCallbackSecret=prod-runtime-callback-8b3d7e5a
  --set-string secrets.meetingTokenSecret=prod-meeting-token-5a8c2d7e
  --set-string secrets.gatewayIdentitySecret=prod-gateway-identity-7a4c9e2b6d1f8a3c
  --set-string secrets.redisPassword=prod-redis-password-1f4b9c6d
  --set-string secrets.minioAccessKey=prod-minio-access-8d3f6a1c
  --set-string secrets.minioSecretKey=prod-minio-secret-6b2e9d4f
  --set-string secrets.dispatchSigningKey=prod-dispatch-9c5b2e7d
  --set-string secrets.nextauthSecret=prod-nextauth-3a6f8d1c
  --set-string database.password=prod-database-password-7f2a9b3c
  --set-string secrets.minutesHubToken=prod-minutes-hub-4e8b2c7d9f1a6c3b
  --set minutes.ttl.enabled=true
  --set-string minutes.nullalisErasureBaseUrl="$NULLALIS_ERASURE_URL"
  --set-string minutes.agentErasureKeyId=agent-erasure-2026-07
  --set-string minutes.minutesErasureKeyId=minutes-erasure-2026-07
  --set-string secrets.agentErasureHmacSecret=prod-agent-erasure-2b7d9f4a6c3a8
  --set-string secrets.minutesErasureSigningSecret=prod-minutes-erasure-6c3a8e1d4f7
  --set minutes.readEnabled=true
)
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${MINUTES_HOSTED_BASE[@]}" >/dev/null 2>&1; then
  echo "  FAIL: hosted Minutes read accepted no Secret value/ref"; fail=1
else
  echo "  OK: hosted Minutes read rejects a missing Secret value/ref"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${MINUTES_HOSTED_BASE[@]}" \
  --set-string secrets.minutesReadToken=prod-minutes-read-7f2a9b3c5e8d1c >/dev/null 2>&1; then
  echo "  OK: hosted Minutes read accepts a non-placeholder Secret value"
else
  echo "  FAIL: hosted Minutes read rejected a non-placeholder Secret value"; fail=1
fi

minutes_value_must_fail() { # minutes_value_must_fail <label> <helm override>
  local label="$1" override="$2"
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set "$override" >/dev/null 2>&1; then
    echo "  FAIL: Minutes accepted $label"; fail=1
  else
    echo "  OK: Minutes rejects $label"
  fi
}
minutes_value_must_fail "a zero-second TTL interval" 'minutes.ttl.intervalSeconds=0'
minutes_value_must_fail "a TTL interval above one day" 'minutes.ttl.intervalSeconds=86401'
minutes_value_must_fail "a zero-row TTL batch" 'minutes.ttl.batchSize=0'
minutes_value_must_fail "a TTL batch above meeting-api's 500-row limit" 'minutes.ttl.batchSize=501'
minutes_value_must_fail "a fractional TTL batch" 'minutes.ttl.batchSize=1.5'

PLATFORM_SECRET_PATHS=(
  secrets.adminApiToken
  secrets.internalApiSecret
  secrets.runtimeControlSecret
  secrets.runtimeCallbackSecret
  secrets.meetingTokenSecret
  secrets.gatewayIdentitySecret
  secrets.redisPassword
  secrets.transcriptionServiceToken
  secrets.dispatchSigningKey
  secrets.botApiKey
  secrets.terminalSharedApiKey
  secrets.nextauthSecret
  secrets.googleClientSecret
  secrets.microsoftClientSecret
  secrets.anthropicApiKey
  secrets.anthropicAuthToken
  secrets.claudeCodeOauthToken
  database.password
  secrets.minioAccessKey
  secrets.minioSecretKey
)
PLATFORM_DOMAIN_SECRET='platform-domain-secret-1234567890abcdef'
for platform_path in "${PLATFORM_SECRET_PATHS[@]}"; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    "${READ_KEYS[@]}" --set-string "$platform_path=$PLATFORM_DOMAIN_SECRET" \
    --set-string "secrets.minutesReadToken=$PLATFORM_DOMAIN_SECRET" >/dev/null 2>&1; then
    echo "  FAIL: Minutes read reused platform credential $platform_path"; fail=1
  else
    echo "  OK: Minutes read token rejects platform credential reuse ($platform_path)"
  fi
done

if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set agentApi.enabled=false --set minutes.captureEnabled=true \
  --set minutes.invocationV2Enabled=true \
  --set-string minutes.botImage=vexaai/zaki-minutes-bot:v2 \
  --set-string minutes.nullalisErasureBaseUrl="$NULLALIS_ERASURE_URL" \
  --set-string minutes.agentErasureKeyId=agent-erasure-2026-07 \
  --set-string minutes.minutesErasureKeyId=minutes-erasure-2026-07 \
  --set-string secrets.minutesHubToken=minutes-hub-bearer-8d4f1a7c9e2b6 \
  --set-string secrets.agentErasureHmacSecret=agent-shared-hmac-7f2a9b3c5e8d1c \
  --set-string secrets.minutesErasureSigningSecret=minutes-independent-hmac-4d8e1f6a \
  >/dev/null 2>&1; then
  echo "  FAIL: Minutes capture rendered without the retention worker"; fail=1
else
  echo "  OK: Minutes capture requires the retention worker"
fi

CAPTURE_KEYS=(
  "${ERASURE_BOUNDARY[@]}"
  --set minutes.captureEnabled=true
  --set minutes.invocationV2Enabled=true
  --set-string minutes.botImage="$MINUTES_BOT_IMAGE"
)
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" --set ingress.enabled=true >/dev/null 2>&1; then
  echo "  FAIL: managed Minutes rendered this chart's dead Terminal ingress"; fail=1
else
  echo "  OK: managed Minutes requires the external zaki-prod user edge"
fi
for backend in docker process; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    "${CAPTURE_KEYS[@]}" --set-string "runtime.backend=$backend" >/dev/null 2>&1; then
    echo "  FAIL: managed Minutes accepted unsupported runtime backend $backend"; fail=1
  else
    echo "  OK: managed Minutes requires runtime.backend=k8s ($backend rejected)"
  fi
done
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" --set-string secrets.minutesHubToken= >/dev/null 2>&1; then
  echo "  FAIL: managed Minutes rendered without a Hub/BFF credential"; fail=1
else
  echo "  OK: managed Minutes requires a dedicated Hub/BFF credential"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" --set meetingApi.enabled=false >/dev/null 2>&1; then
  echo "  FAIL: Minutes retention rendered without meeting-api"; fail=1
else
  echo "  OK: every Minutes data/erasure mode requires meeting-api"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" --set runtime.enabled=false >/dev/null 2>&1; then
  echo "  FAIL: managed/historical Minutes rendered without withdrawal runtime"; fail=1
else
  echo "  OK: every managed/historical Minutes mode retains withdrawal runtime"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" --set adminApi.enabled=false >/dev/null 2>&1; then
  echo "  FAIL: managed/historical Minutes rendered without Identity settings"; fail=1
else
  echo "  OK: every managed/historical Minutes mode requires Identity-owned settings"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${READ_KEYS[@]}" --set adminApi.enabled=false \
  --set-string secrets.minutesReadToken=local-minutes-read-7f2a9b3c5e8d1c >/dev/null 2>&1; then
  echo "  FAIL: Minutes read rendered without Identity settings"; fail=1
else
  echo "  OK: Minutes read requires Identity-owned settings"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" --set gateway.enabled=true \
  --set terminal.enabled=false >/dev/null 2>&1; then
  echo "  FAIL: Minutes rendered the gateway's phantom bundled-Agent route"; fail=1
else
  echo "  OK: managed Minutes rejects the bundled-Agent gateway topology"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" --set gateway.enabled=false \
  --set terminal.enabled=true >/dev/null 2>&1; then
  echo "  FAIL: Minutes rendered the in-chart Terminal against an absent Agent"; fail=1
else
  echo "  OK: managed Minutes requires the external launch UI/BFF"
fi
for key in minutesHubToken agentErasureHmacSecret minutesErasureSigningSecret; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    "${CAPTURE_KEYS[@]}" --set-string "secrets.$key=$SHORT_MINUTES_SECRET" >/dev/null 2>&1; then
    echo "  FAIL: $key accepted a 31-character credential"; fail=1
  else
    echo "  OK: $key requires at least 32 characters"
  fi
done
for key in agentErasureHmacSecret minutesErasureSigningSecret; do
  for bad_erasure_secret in "$NON_ASCII_MINUTES_SECRET" "$OVERSIZED_MINUTES_SECRET"; do
    if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
      "${CAPTURE_KEYS[@]}" --set-string "secrets.$key=$bad_erasure_secret" >/dev/null 2>&1; then
      echo "  FAIL: $key accepted a non-ASCII/oversized credential"; fail=1
    else
      echo "  OK: $key matches the runtime's printable-ASCII size contract"
    fi
  done
done
for bad_hub_token in 'minutes-hub-bearer-8d4f1a7c9e2bé' "$OVERSIZED_MINUTES_SECRET"; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    "${CAPTURE_KEYS[@]}" --set-string "secrets.minutesHubToken=$bad_hub_token" >/dev/null 2>&1; then
    echo "  FAIL: Minutes accepted a non-ASCII/oversized Hub credential"; fail=1
  else
    echo "  OK: Hub/BFF credential stays within the runtime's printable-ASCII size contract"
  fi
done
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" --set-string 'minutes.botImage= vexaai/zaki-minutes-bot:v2 ' >/dev/null 2>&1; then
  echo "  FAIL: Minutes accepted a whitespace-wrapped bot image"; fail=1
else
  echo "  OK: Minutes bot image rejects ambiguous outer whitespace"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string 'secrets.minutesHubToken= minutes-hub-bearer-8d4f1a7c9e2b6 ' >/dev/null 2>&1; then
  echo "  FAIL: Minutes accepted a whitespace-wrapped Hub/BFF credential"; fail=1
else
  echo "  OK: Hub/BFF credential rejects ambiguous outer whitespace"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string secrets.minutesHubToken=agent-shared-hmac-7f2a9b3c5e8d1c >/dev/null 2>&1; then
  echo "  FAIL: Minutes Hub/BFF bearer reused an erasure credential"; fail=1
else
  echo "  OK: Minutes Hub/BFF bearer is independent from erasure credentials"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" --set-string 'minutes.agentErasureKeyId= agent-erasure-2026-07 ' >/dev/null 2>&1; then
  echo "  FAIL: Minutes accepted a whitespace-wrapped erasure key id"; fail=1
else
  echo "  OK: Minutes erasure key ids reject ambiguous outer whitespace"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" --set-string 'minutes.nullalisErasureBaseUrl= https://nullalis.internal.example ' >/dev/null 2>&1; then
  echo "  FAIL: Minutes accepted a whitespace-wrapped Nullalis URL"; fail=1
else
  echo "  OK: Nullalis erasure URL rejects ambiguous outer whitespace"
fi
for platform_path in "${PLATFORM_SECRET_PATHS[@]}"; do
  for key in minutesHubToken agentErasureHmacSecret minutesErasureSigningSecret; do
    if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
      "${CAPTURE_KEYS[@]}" --set-string "$platform_path=$PLATFORM_DOMAIN_SECRET" \
      --set-string "secrets.$key=$PLATFORM_DOMAIN_SECRET" >/dev/null 2>&1; then
      echo "  FAIL: $key reused platform credential $platform_path"; fail=1
    else
      echo "  OK: $key rejects platform credential reuse ($platform_path)"
    fi
  done
done
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set agentApi.enabled=false --set minutes.captureEnabled=true \
  --set minutes.invocationV2Enabled=true \
  --set-string minutes.botImage=vexaai/zaki-minutes-bot:v2 \
  --set minutes.ttl.enabled=true \
  --set-string minutes.nullalisErasureBaseUrl="$NULLALIS_ERASURE_URL" >/dev/null 2>&1; then
  echo "  FAIL: Minutes capture accepted missing erasure receipt keys"; fail=1
else
  echo "  OK: Minutes capture requires erasure receipt keys"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" --set agentApi.enabled=true >/dev/null 2>&1; then
  echo "  FAIL: Minutes capture co-deployed the bundled Agent with unscoped Redis"; fail=1
else
  echo "  OK: Minutes capture rejects the bundled Agent topology"
fi

ERASURE_KEYS_WITHOUT_TTL=(
  --set agentApi.enabled=false
  --set-string minutes.nullalisErasureBaseUrl="$NULLALIS_ERASURE_URL"
  --set-string minutes.agentErasureKeyId=agent-erasure-2026-07
  --set-string minutes.minutesErasureKeyId=minutes-erasure-2026-07
  --set-string secrets.minutesHubToken=minutes-hub-bearer-8d4f1a7c9e2b6
  --set-string secrets.agentErasureHmacSecret=agent-shared-hmac-7f2a9b3c5e8d1c
  --set-string secrets.minutesErasureSigningSecret=minutes-independent-hmac-4d8e1f6a
)
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_KEYS_WITHOUT_TTL[@]}" >/dev/null 2>&1; then
  echo "  FAIL: capture-off erasure configuration rendered without the retention worker"; fail=1
else
  echo "  OK: retained erasure configuration still requires the retention worker"
fi

ERASURE_ONLY_RENDER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}")"
ERASURE_ONLY_MEETING="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" --show-only templates/deployment-meeting-api.yaml)"
ERASURE_ONLY_RUNTIME="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" --show-only templates/deployment-runtime.yaml)"
if grep -q 'key: ZAKI_AGENT_ERASURE_VERIFICATION_SECRET' <<<"$ERASURE_ONLY_MEETING" \
  && grep -qF "value: \"$NULLALIS_ERASURE_URL\"" <<<"$ERASURE_ONLY_MEETING" \
  && printf '%s\n' "$ERASURE_ONLY_MEETING" | grep -A1 -m1 -- '- name: ZAKI_MINUTES_MANAGED_ONLY' \
    | grep -q 'value: "true"' \
  && ! grep -Eq 'name: AGENT_(IMAGE|WORKER_IMAGE)' <<<"$ERASURE_ONLY_RUNTIME" \
  && printf '%s\n' "$ERASURE_ONLY_RUNTIME" | grep -A1 -m1 -- '- name: RUNTIME_AGENT_PROFILE_ENABLED' \
    | grep -q 'value: "false"' \
  && grep -q 'value: "false"' <<<"$ERASURE_ONLY_MEETING"; then
  echo "  OK: historical erasure-only mode retains fan-out without bundled Agent profiles"
else
  echo "  FAIL: historical erasure-only mode is not safely preserved"; fail=1
fi

if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" --set minutes.readEnabled=true \
  --set-string secrets.minutesReadToken=agent-shared-hmac-7f2a9b3c5e8d1c >/dev/null 2>&1; then
  echo "  FAIL: Minutes read accepted an erasure credential"; fail=1
else
  echo "  OK: Minutes read token must be distinct from erasure credentials"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string secrets.internalApiSecret=platform-internal-auth-1234567890abcdef \
  --set-string secrets.minutesErasureSigningSecret=platform-internal-auth-1234567890abcdef >/dev/null 2>&1; then
  echo "  FAIL: Minutes signer accepted the internal API credential"; fail=1
else
  echo "  OK: Minutes signer must be distinct from the internal API credential"
fi

CAPTURE_RENDER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}")"
if ! grep -q '^  ZAKI_AGENT_ERASURE_SIGNING_SECRET:' <<<"$CAPTURE_RENDER" \
  && grep -q '^  ZAKI_MINUTES_HUB_TOKEN: "minutes-hub-bearer-8d4f1a7c9e2b6"' <<<"$CAPTURE_RENDER" \
  && grep -q '^  ZAKI_AGENT_ERASURE_VERIFICATION_SECRET: "agent-shared-hmac-7f2a9b3c5e8d1c"' <<<"$CAPTURE_RENDER" \
  && grep -q '^  ZAKI_MINUTES_ERASURE_SIGNING_SECRET: "minutes-independent-hmac-4d8e1f6a"' <<<"$CAPTURE_RENDER"; then
  echo "  OK: capture keeps the Agent signer in external Nullalis and only stores its verifier here"
else
  echo "  FAIL: capture erasure key projections are incomplete"; fail=1
fi
CAPTURE_MEETING="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" --show-only templates/deployment-meeting-api.yaml)"
CAPTURE_RUNTIME="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" --show-only templates/deployment-runtime.yaml)"
CAPTURE_ADMIN="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" --show-only templates/deployment-admin-api.yaml)"
if ! grep -q 'kind: Deployment' <<<"$(helm template vexa "$CHART" -n vexa \
    -f "$CHART/values-test.yaml" "${CAPTURE_KEYS[@]}" \
    --show-only templates/deployment-agent-api.yaml 2>/dev/null || true)" \
  && grep -q 'key: ZAKI_MINUTES_HUB_TOKEN' <<<"$CAPTURE_MEETING" \
  && grep -q 'key: ZAKI_AGENT_ERASURE_VERIFICATION_SECRET' <<<"$CAPTURE_MEETING" \
  && grep -q 'key: ZAKI_MINUTES_ERASURE_SIGNING_SECRET' <<<"$CAPTURE_MEETING" \
  && printf '%s\n' "$CAPTURE_MEETING" | grep -A1 -m1 -- '- name: ZAKI_MINUTES_MANAGED_ONLY' \
    | grep -q 'value: "true"' \
  && ! grep -q 'key: ZAKI_AGENT_ERASURE_SIGNING_SECRET' <<<"$CAPTURE_MEETING" \
  && ! grep -q 'ZAKI_MINUTES_HUB_TOKEN' <<<"$CAPTURE_RUNTIME" \
  && ! grep -q 'ZAKI_MINUTES_HUB_TOKEN' <<<"$CAPTURE_ADMIN"; then
  echo "  OK: Hub auth and erasure keys are meeting-api-only; no bundled Agent is deployed"
else
  echo "  FAIL: erasure secret service isolation is broken"; fail=1
fi
if grep -Eq 'name: AGENT_(IMAGE|WORKER_IMAGE)' <<<"$CAPTURE_RUNTIME" \
  || grep -Eq 'name: (ANTHROPIC_|CLAUDE_CODE_OAUTH_TOKEN)' <<<"$CAPTURE_RUNTIME" \
  || ! printf '%s\n' "$CAPTURE_RUNTIME" | grep -A1 -m1 -- '- name: MINUTES_BROWSER_IMAGE' \
    | grep -qF "value: \"$MINUTES_BOT_IMAGE\""; then
  echo "  FAIL: capture runtime profile isolation is broken"; fail=1
else
  echo "  OK: capture keeps only the managed Minutes bot profile, never Agent images or provider auth"
fi

if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string minutes.previousAgentErasureKeyId=agent-erasure-2026-06 \
  >/dev/null 2>&1; then
  echo "  FAIL: previous Agent verifier accepted a missing secret"; fail=1
else
  echo "  OK: previous Agent verifier key and secret must be configured together"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string minutes.previousAgentErasureKeyId=agent-erasure-2026-06 \
  --set-string "secrets.agentErasurePreviousVerificationSecret=$SHORT_MINUTES_SECRET" \
  >/dev/null 2>&1; then
  echo "  FAIL: previous Agent verifier accepted a 31-character credential"; fail=1
else
  echo "  OK: previous Agent verifier requires at least 32 characters"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string minutes.previousAgentErasureKeyId=agent-erasure-2026-06 \
  --set-string "secrets.agentErasurePreviousVerificationSecret=$OVERSIZED_MINUTES_SECRET" \
  >/dev/null 2>&1; then
  echo "  FAIL: previous Agent verifier accepted an oversized credential"; fail=1
else
  echo "  OK: previous Agent verifier matches the runtime's printable-ASCII size contract"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string secrets.agentErasurePreviousVerificationSecret=agent-previous-verifier-6a3d9c2e7f4b8d \
  >/dev/null 2>&1; then
  echo "  FAIL: previous Agent verifier secret accepted a missing key id"; fail=1
else
  echo "  OK: previous Agent verifier secret cannot exist without its key id"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string minutes.previousAgentErasureKeyId=agent-erasure-2026-07 \
  --set-string secrets.agentErasurePreviousVerificationSecret=agent-previous-verifier-6a3d9c2e7f4b8d \
  >/dev/null 2>&1; then
  echo "  FAIL: previous Agent verifier reused the current key id"; fail=1
else
  echo "  OK: previous Agent verifier key id is independent from current receipt keys"
fi
AGENT_ROTATION_RENDER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string minutes.previousAgentErasureKeyId=agent-erasure-2026-06 \
  --set-string secrets.agentErasurePreviousVerificationSecret=agent-previous-verifier-6a3d9c2e7f4b8d)"
AGENT_ROTATION_MEETING="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string minutes.previousAgentErasureKeyId=agent-erasure-2026-06 \
  --set-string secrets.agentErasurePreviousVerificationSecret=agent-previous-verifier-6a3d9c2e7f4b8d \
  --show-only templates/deployment-meeting-api.yaml)"
if grep -q '^  ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET: "agent-previous-verifier-6a3d9c2e7f4b8d"' <<<"$AGENT_ROTATION_RENDER" \
  && grep -q 'name: ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID' <<<"$AGENT_ROTATION_MEETING" \
  && grep -q 'key: ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET' <<<"$AGENT_ROTATION_MEETING" \
  && ! grep -q 'ZAKI_AGENT_ERASURE_PREVIOUS_SIGNING' <<<"$AGENT_ROTATION_RENDER"; then
  echo "  OK: one previous Agent receipt key is projected to meeting-api as verifier-only"
else
  echo "  FAIL: previous Agent receipt verifier projection is incomplete or gained signing authority"; fail=1
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string minutes.previousAgentErasureKeyId=agent-erasure-2026-06 \
  --set-string secrets.dispatchSigningKey=platform-dispatch-signing-1234567890abcdef \
  --set-string secrets.agentErasurePreviousVerificationSecret=platform-dispatch-signing-1234567890abcdef \
  >/dev/null 2>&1; then
  echo "  FAIL: previous Agent verifier reused a platform credential"; fail=1
else
  echo "  OK: previous Agent verifier rejects platform credential reuse"
fi

if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string minutes.previousMinutesErasureKeyId=minutes-erasure-2026-06 \
  >/dev/null 2>&1; then
  echo "  FAIL: previous Minutes verifier accepted a missing secret"; fail=1
else
  echo "  OK: previous Minutes verifier key and secret must be configured together"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string minutes.previousMinutesErasureKeyId=minutes-erasure-2026-06 \
  --set-string "secrets.minutesErasurePreviousVerificationSecret=$SHORT_MINUTES_SECRET" \
  >/dev/null 2>&1; then
  echo "  FAIL: previous Minutes verifier accepted a 31-character credential"; fail=1
else
  echo "  OK: previous Minutes verifier requires at least 32 characters"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string minutes.previousMinutesErasureKeyId=minutes-erasure-2026-06 \
  --set-string "secrets.minutesErasurePreviousVerificationSecret=$NON_ASCII_MINUTES_SECRET" \
  >/dev/null 2>&1; then
  echo "  FAIL: previous Minutes verifier accepted a non-ASCII credential"; fail=1
else
  echo "  OK: previous Minutes verifier matches the runtime's printable-ASCII size contract"
fi
ROTATION_RENDER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string minutes.previousMinutesErasureKeyId=minutes-erasure-2026-06 \
  --set-string secrets.minutesErasurePreviousVerificationSecret=minutes-previous-hmac-6a3d9c2e7f)"
if grep -q '^  ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET: "minutes-previous-hmac-6a3d9c2e7f"' <<<"$ROTATION_RENDER" \
  && grep -q 'name: ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_KEY_ID' <<<"$ROTATION_RENDER"; then
  echo "  OK: one previous Minutes receipt key is projected verifier-only during rotation"
else
  echo "  FAIL: previous Minutes receipt verifier projection is incomplete"; fail=1
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${CAPTURE_KEYS[@]}" \
  --set-string minutes.previousMinutesErasureKeyId=minutes-erasure-2026-06 \
  --set-string secrets.dispatchSigningKey=platform-dispatch-signing-1234567890abcdef \
  --set-string secrets.minutesErasurePreviousVerificationSecret=platform-dispatch-signing-1234567890abcdef \
  >/dev/null 2>&1; then
  echo "  FAIL: previous Minutes verifier reused a platform credential"; fail=1
else
  echo "  OK: previous Minutes verifier rejects platform credential reuse"
fi

if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" --set minutes.finalized.enabled=true >/dev/null 2>&1; then
  echo "  FAIL: platform finalized delivery accepted missing Hub config"; fail=1
else
  echo "  OK: platform finalized delivery requires Hub config"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" --set minutes.finalized.enabled=true \
  --set-string minutes.finalized.url=http://hub-api:8080/internal/minutes/finalized \
  --set-string minutes.finalized.keyId=minutes-platform-2026-07 \
  --set-string "secrets.minutesFinalizedSecret=$SHORT_MINUTES_SECRET" >/dev/null 2>&1; then
  echo "  FAIL: platform finalization accepted a 31-character signer"; fail=1
else
  echo "  OK: platform finalization signer requires at least 32 characters"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" --set minutes.finalized.enabled=true \
  --set-string minutes.finalized.url=http://hub-api:8080/internal/minutes/finalized \
  --set-string minutes.finalized.keyId=minutes-platform-2026-07 \
  --set-string "secrets.minutesFinalizedSecret=$OVERSIZED_MINUTES_SECRET" >/dev/null 2>&1; then
  echo "  FAIL: platform finalization accepted an oversized signer"; fail=1
else
  echo "  OK: platform finalization signer matches the runtime's printable-ASCII size contract"
fi
for bad_url in \
  ' http://hub-api:8080/internal/minutes/finalized ' \
  'http://user:password@hub-api:8080/internal/minutes/finalized' \
  'http://hub-api:8080/internal/minutes/finalized?next=attacker' \
  'http://hub-api:99999/internal/minutes/finalized'; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    "${ERASURE_BOUNDARY[@]}" --set minutes.finalized.enabled=true \
    --set-string "minutes.finalized.url=$bad_url" \
    --set-string minutes.finalized.keyId=minutes-platform-2026-07 \
    --set-string secrets.minutesFinalizedSecret=platform-finalized-hmac-9c5b2e7d >/dev/null 2>&1; then
    echo "  FAIL: platform finalization accepted unsafe URL $bad_url"; fail=1
  else
    echo "  OK: platform finalization rejects unsafe URL $bad_url"
  fi
done
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" \
  --set minutes.finalized.enabled=true \
  --set-string minutes.finalized.url=http://hub-api:8080/internal/minutes/finalized \
  --set-string minutes.finalized.keyId=minutes-platform-2026-07 \
  --set-string secrets.internalApiSecret=platform-internal-auth-1234567890abcdef \
  --set-string secrets.minutesFinalizedSecret=platform-internal-auth-1234567890abcdef >/dev/null 2>&1; then
  echo "  FAIL: platform finalization accepted the internal API credential"; fail=1
else
  echo "  OK: platform finalization signer is independently scoped"
fi
FINALIZED_RENDER="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" \
  --set minutes.finalized.enabled=true \
  --set-string minutes.finalized.url=http://hub-api:8080/internal/minutes/finalized \
  --set-string minutes.finalized.keyId=minutes-platform-2026-07 \
  --set-string secrets.minutesFinalizedSecret=platform-finalized-hmac-9c5b2e7d)"
if grep -q '^  ZAKI_MINUTES_FINALIZED_SECRET: "platform-finalized-hmac-9c5b2e7d"' <<<"$FINALIZED_RENDER" \
  && grep -q 'name: ZAKI_MINUTES_FINALIZED_ENABLED' <<<"$FINALIZED_RENDER"; then
  echo "  OK: platform finalized delivery is Secret-backed and explicitly enabled"
else
  echo "  FAIL: platform finalized deployment projection is incomplete"; fail=1
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${ERASURE_BOUNDARY[@]}" --set minutes.finalized.enabled=true \
  --set-string minutes.finalized.url=http://hub-api:8080/internal/minutes/finalized \
  --set-string minutes.finalized.keyId=minutes-platform-2026-07 \
  --set-string secrets.nextauthSecret=platform-session-signing-1234567890abcdef \
  --set-string secrets.minutesFinalizedSecret=platform-session-signing-1234567890abcdef >/dev/null 2>&1; then
  echo "  FAIL: platform finalization signer reused a platform credential"; fail=1
else
  echo "  OK: platform finalization signer rejects platform credential reuse"
fi

AGENT_DEFAULT="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --show-only templates/deployment-agent-api.yaml)"
if printf '%s\n' "$AGENT_DEFAULT" | grep -A1 'name: VEXA_AGENT_DEFAULT_SUBJECT' | grep -q 'u_live'; then
  echo "  FAIL: production agent-api still collapses live meetings into u_live"; fail=1
else
  echo "  OK: production agent-api has no u_live owner fallback"
fi

TERMINAL_DEFAULT="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --show-only templates/deployment-terminal.yaml)"
if printf '%s\n' "$TERMINAL_DEFAULT" | grep -qE 'name: VEXA_BOT_API_KEY'; then
  echo "  FAIL: terminal must not receive the bot service key"; fail=1
else
  echo "  OK: terminal does not receive the bot service key"
fi
if printf '%s\n' "$TERMINAL_DEFAULT" | grep -qE 'name: VEXA_DIRECT_LOGIN_ALLOWED_EMAILS'; then
  echo "  FAIL: direct login must be default-off"; fail=1
else
  echo "  OK: direct login is absent by default"
fi

if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set 'terminal.directLoginAllowedEmails[0]=local-test@example.com' >/dev/null 2>&1; then
  echo "  FAIL: loopback-only direct login rendered behind a cluster Service"; fail=1
else
  echo "  OK: direct login is refused by the cluster chart"
fi

if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set terminal.sharedKeyMode=true --set secrets.terminalSharedApiKey=self-host-key >/dev/null 2>&1; then
  echo "  FAIL: loopback-only shared-key mode rendered behind a cluster Service"; fail=1
else
  echo "  OK: shared-key mode is refused by the cluster chart"
fi

# auth unset (values-test) → the chart Secret must NOT carry the key; auth set → it must.
if grep -qE '^  CLAUDE_CODE_OAUTH_TOKEN:' <<<"$RENDER"; then
  echo "  FAIL: CLAUDE_CODE_OAUTH_TOKEN rendered into the Secret with auth UNSET"; fail=1
else
  echo "  OK: Secret omits CLAUDE_CODE_OAUTH_TOKEN when unset"
fi
RENDER_AUTH="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set secrets.claudeCodeOauthToken=sk-test-oauth)"
if grep -qE '^  CLAUDE_CODE_OAUTH_TOKEN: "sk-test-oauth"' <<<"$RENDER_AUTH"; then
  echo "  OK: CLAUDE_CODE_OAUTH_TOKEN lands in the Secret when set"
else
  echo "  FAIL: CLAUDE_CODE_OAUTH_TOKEN missing from the Secret when set"; fail=1
fi

# Public/staging renders may reference an operator-managed existing Secret. When the chart renders
# inline values, however, every auth/signing boundary must be explicitly strong: no blank, fixture,
# or historical dev default may survive into a public deployment.
STRONG_SECRETS=(
  --set migrations.enabled=true
  --set-string runtime.workloadNamespace=vexa-workloads
  --set-string global.imageTag=260716-abcdef
  --set-string postgres.image=postgres@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  --set-string redis.image=redis@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
  --set-string secrets.adminApiToken=prod-admin-7f2a9b3c
  --set-string secrets.internalApiSecret=prod-internal-4d8e1f6a
  --set-string secrets.runtimeControlSecret=prod-runtime-control-6e9c2a4b
  --set-string secrets.runtimeCallbackSecret=prod-runtime-callback-8b3d7e5a
  --set-string secrets.meetingTokenSecret=prod-meeting-token-5a8c2d7e
  --set-string secrets.gatewayIdentitySecret=prod-gateway-identity-7a4c9e2b6d1f8a3c
  --set-string secrets.redisPassword=prod-redis-password-1f4b9c6d
  --set-string secrets.minioAccessKey=prod-minio-access-8d3f6a1c
  --set-string secrets.minioSecretKey=prod-minio-secret-6b2e9d4f
  --set-string secrets.dispatchSigningKey=prod-dispatch-9c5b2e7d
  --set-string secrets.nextauthSecret=prod-nextauth-3a6f8d1c
  --set-string database.password=prod-database-password-7f2a9b3c
)

# A hosted Terminal without OAuth has no login path: cluster direct-login/shared-key modes are
# intentionally forbidden. Keep local/port-forward defaults provider-free, but fail every public
# or staging render until an operator deliberately enables a Secret-backed OAuth provider.
if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${STRONG_SECRETS[@]}" --set ingress.enabled=true >/dev/null 2>&1; then
  echo "  FAIL: public render accepted a Terminal with zero login providers"; fail=1
else
  echo "  OK: public render rejects a Terminal with zero login providers"
fi

if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${STRONG_SECRETS[@]}" --set ingress.enabled=true \
  --set terminal.oauth.google.enabled=true >/dev/null 2>&1; then
  echo "  FAIL: enabled Google OAuth accepted missing operator credentials"; fail=1
else
  echo "  OK: enabled Google OAuth requires operator credentials"
fi

if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${STRONG_SECRETS[@]}" --set ingress.enabled=true \
  --set terminal.oauth.google.enabled=true \
  --set-string secrets.googleClientId=public-google-client \
  --set-string secrets.googleClientSecret=public-google-secret \
  --set terminal.extraEnv[0].name=GOOGLE_CLIENT_SECRET \
  --set-string terminal.extraEnv[0].value=plaintext-bypass >/dev/null 2>&1; then
  echo "  FAIL: terminal.extraEnv bypassed the Secret-backed OAuth contract"; fail=1
else
  echo "  OK: terminal.extraEnv cannot override auth credentials"
fi

TERMINAL_RESERVED_ENV=(
  NODE_ENV PORT HOST
  GATEWAY_URL AGENT_API_URL VEXA_ADMIN_API_URL VEXA_API_URL
  NEXTAUTH_URL TERMINAL_URL TRANSCRIPTION_SERVICE_URL
  VEXA_ADMIN_API_KEY VEXA_INTERNAL_API_SECRET TRANSCRIPTION_SERVICE_TOKEN
  VEXA_API_KEY VEXA_BOT_API_KEY NEXTAUTH_SECRET
  GOOGLE_CLIENT_ID GOOGLE_CLIENT_SECRET
  MICROSOFT_CLIENT_ID MICROSOFT_CLIENT_SECRET MICROSOFT_TENANT_ID
  VEXA_TERMINAL_SHARED_KEY_MODE VEXA_DIRECT_LOGIN_ALLOWED_EMAILS VEXA_TERMINAL_HOST_BIND
)
for key in "${TERMINAL_RESERVED_ENV[@]}"; do
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    --set-string "terminal.extraEnv[0].name=$key" \
    --set-string terminal.extraEnv[0].value=plaintext-or-origin-bypass \
    >/dev/null 2>&1; then
    echo "  FAIL: terminal.extraEnv overrode reserved $key"; fail=1
  fi
done
if [ "$fail" -eq 0 ]; then
  echo "  OK: terminal.extraEnv cannot shadow chart-owned credentials, origins, or auth modes"
fi

if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${STRONG_SECRETS[@]}" --set ingress.enabled=true \
  --set terminal.oauth.google.enabled=true \
  --set-string secrets.googleClientId=public-google-client \
  --set-string secrets.googleClientSecret=public-google-secret >/dev/null 2>&1; then
  echo "  OK: public render accepts explicit secrets + OAuth"
else
  echo "  FAIL: public render rejected explicit secrets + OAuth"; fail=1
fi

PUBLIC_TERMINAL="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${STRONG_SECRETS[@]}" --set ingress.enabled=true \
  --set terminal.oauth.google.enabled=true \
  --set-string secrets.googleClientId=public-google-client \
  --set-string secrets.googleClientSecret=public-google-secret \
  --show-only templates/deployment-terminal.yaml)"
for key in GOOGLE_CLIENT_ID GOOGLE_CLIENT_SECRET NEXTAUTH_SECRET; do
  if printf '%s\n' "$PUBLIC_TERMINAL" | grep -q "key: $key"; then
    echo "  OK: public terminal sources $key from Secret"
  else
    echo "  FAIL: public terminal does not source $key from Secret"; fail=1
  fi
done

MICROSOFT_TERMINAL="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  "${STRONG_SECRETS[@]}" --set ingress.enabled=true \
  --set terminal.oauth.microsoft.enabled=true \
  --set-string secrets.microsoftClientId=public-microsoft-client \
  --set-string secrets.microsoftClientSecret=public-microsoft-secret \
  --show-only templates/deployment-terminal.yaml)"
for key in MICROSOFT_CLIENT_ID MICROSOFT_CLIENT_SECRET; do
  if printf '%s\n' "$MICROSOFT_TERMINAL" | grep -q "key: $key"; then
    echo "  OK: public terminal sources $key from Secret"
  else
    echo "  FAIL: public terminal does not source $key from Secret"; fail=1
  fi
done

weak_secret_must_fail() { # weak_secret_must_fail <label> <helm override>
  local label="$1" override="$2"
  if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
    "${STRONG_SECRETS[@]}" --set ingress.enabled=true \
    --set terminal.oauth.google.enabled=true \
    --set-string secrets.googleClientId=public-google-client \
    --set-string secrets.googleClientSecret=public-google-secret \
    --set-string "$override" >/dev/null 2>&1; then
    echo "  FAIL: public render accepted $label"; fail=1
  else
    echo "  OK: public render rejects $label"
  fi
}

weak_secret_must_fail "CHANGE_ME admin token" 'secrets.adminApiToken=CHANGE_ME'
weak_secret_must_fail "blank admin token" 'secrets.adminApiToken='
weak_secret_must_fail "known internal API secret" 'secrets.internalApiSecret=vexa-internal-secret'
weak_secret_must_fail "blank internal API secret" 'secrets.internalApiSecret='
weak_secret_must_fail "known runtime control secret" 'secrets.runtimeControlSecret=vexa-runtime-control-secret'
weak_secret_must_fail "blank runtime control secret" 'secrets.runtimeControlSecret='
weak_secret_must_fail "known runtime callback secret" 'secrets.runtimeCallbackSecret=vexa-runtime-callback-secret'
weak_secret_must_fail "blank runtime callback secret" 'secrets.runtimeCallbackSecret='
weak_secret_must_fail "known MeetingToken secret" 'secrets.meetingTokenSecret=vexa-meeting-token-secret'
weak_secret_must_fail "blank MeetingToken secret" 'secrets.meetingTokenSecret='
weak_secret_must_fail "short MeetingToken secret" 'secrets.meetingTokenSecret=too-short'
weak_secret_must_fail "known gateway identity secret" 'secrets.gatewayIdentitySecret=vexa-gateway-identity-secret-local-v1'
weak_secret_must_fail "blank gateway identity secret" 'secrets.gatewayIdentitySecret='
weak_secret_must_fail "known Redis password" 'secrets.redisPassword=vexa-redis-password'
weak_secret_must_fail "blank Redis password" 'secrets.redisPassword='
weak_secret_must_fail "known MinIO access key" 'secrets.minioAccessKey=vexa-access-key'
weak_secret_must_fail "known MinIO secret key" 'secrets.minioSecretKey=vexa-secret-key-local'
weak_secret_must_fail "blank MinIO access key" 'secrets.minioAccessKey='
weak_secret_must_fail "blank MinIO secret key" 'secrets.minioSecretKey='
weak_secret_must_fail "known dispatch signing key" 'secrets.dispatchSigningKey=dev-dispatch-signing-key'
weak_secret_must_fail "blank dispatch signing key" 'secrets.dispatchSigningKey='
weak_secret_must_fail "known NextAuth secret" 'secrets.nextauthSecret=dev-nextauth-secret'
weak_secret_must_fail "blank NextAuth secret" 'secrets.nextauthSecret='
weak_secret_must_fail "placeholder Google OAuth secret" 'secrets.googleClientSecret=CHANGE_ME'

if helm template vexa "$CHART" -n vexa -f "$CHART/values-test.yaml" \
  --set global.deploymentProfile=staging \
  --set terminal.oauth.google.enabled=true \
  --set-string secrets.googleClientId=staging-google-client \
  --set-string secrets.googleClientSecret=staging-google-secret >/dev/null 2>&1; then
  echo "  FAIL: staging profile accepted inline fixture secrets"; fail=1
else
  echo "  OK: staging profile rejects inline fixture secrets"
fi
if helm template vexa "$CHART" -n vexa -f "$CHART/values-staging.yaml" \
  "${STAGING_RENDER_ARGS[@]}" >/dev/null 2>&1; then
  echo "  OK: staging profile accepts a pre-created Secret reference"
else
  echo "  FAIL: staging existing-Secret render failed"; fail=1
fi

STAGING_TERMINAL="$(helm template vexa "$CHART" -n vexa -f "$CHART/values-staging.yaml" \
  "${STAGING_RENDER_ARGS[@]}" \
  --show-only templates/deployment-terminal.yaml 2>/dev/null || true)"
if [ -z "$STAGING_TERMINAL" ]; then
  echo "  OK: staging deploys no in-chart Terminal (external launch edge owns UI/BFF)"
else
  echo "  FAIL: staging unexpectedly rendered the in-chart Terminal"; fail=1
fi

[ "$fail" -eq 0 ] && { echo "gate:helm PASS"; exit 0; } || { echo "gate:helm FAIL"; exit 1; }
