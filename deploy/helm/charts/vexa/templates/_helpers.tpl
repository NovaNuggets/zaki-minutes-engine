{{/*
Common template helpers
*/}}

{{ define "vexa.name" -}}
{{ default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{ end -}}

{{ define "vexa.fullname" -}}
{{ if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "vexa.name" . -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "vexa.labels" -}}
app.kubernetes.io/name: {{ include "vexa.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "vexa.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vexa.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "vexa.componentName" -}}
{{- $root := index . 0 -}}
{{- $component := index . 1 -}}
{{- printf "%s-%s" (include "vexa.fullname" $root) $component | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "vexa.redisUrl" -}}
{{- $password := required "secrets.redisPassword is required for inline Redis connection construction" .Values.secrets.redisPassword -}}
{{- if .Values.redis.enabled -}}
{{- printf "redis://:%s@%s.%s.svc.%s:%d/0" $password (include "vexa.componentName" (list . "redis")) .Release.Namespace .Values.global.clusterDomain (.Values.redis.service.port | int) -}}
{{- else -}}
{{- $host := required "redisConfig.host is required for inline managed Redis when redis.enabled=false" .Values.redisConfig.host -}}
{{- $scheme := .Values.redisConfig.scheme | default "redis" -}}
{{- $username := .Values.redisConfig.username | default "" -}}
{{- $userinfo := printf ":%s" $password -}}
{{- if $username -}}
{{- $userinfo = printf "%s:%s" $username $password -}}
{{- end -}}
{{- printf "%s://%s@%s:%d/%d" $scheme $userinfo $host (.Values.redisConfig.port | int) (.Values.redisConfig.database | int) -}}
{{- end -}}
{{- end -}}

{{- define "vexa.redisHost" -}}
{{- if .Values.redis.enabled -}}
{{- printf "%s.%s.svc.%s" (include "vexa.componentName" (list . "redis")) .Release.Namespace .Values.global.clusterDomain -}}
{{- else -}}
{{- required "redisConfig.host is required when redis.enabled=false" .Values.redisConfig.host -}}
{{- end -}}
{{- end -}}

{{- define "vexa.redisPort" -}}
{{- if .Values.redis.enabled -}}
{{- .Values.redis.service.port | int -}}
{{- else -}}
{{- required "redisConfig.port is required when redis.enabled=false" .Values.redisConfig.port -}}
{{- end -}}
{{- end -}}

{{- define "vexa.dbHost" -}}
{{- if .Values.postgres.enabled -}}
{{- include "vexa.componentName" (list . "postgres") -}}
{{- else -}}
{{- required "database.host is required when postgres.enabled=false" .Values.database.host -}}
{{- end -}}
{{- end -}}

{{- /*
  vexa.dbHostEffective — the host every service SHOULD point at for DB.
  When pgbouncer.enabled=true, routes through the pgbouncer Service.
  Otherwise falls through to vexa.dbHost (direct Postgres). PgBouncer's
  own Deployment bypasses this helper and uses vexa.dbHost directly to
  avoid pointing at itself.
*/ -}}
{{- define "vexa.dbHostEffective" -}}
{{- if .Values.pgbouncer.enabled -}}
{{- include "vexa.componentName" (list . "pgbouncer") -}}
{{- else -}}
{{- include "vexa.dbHost" . -}}
{{- end -}}
{{- end -}}

{{- define "vexa.dbPortEffective" -}}
{{- if .Values.pgbouncer.enabled -}}
{{- .Values.pgbouncer.service.port | default 5432 -}}
{{- else -}}
{{- .Values.database.port -}}
{{- end -}}
{{- end -}}

{{- define "vexa.adminTokenSecretName" -}}
{{- if .Values.secrets.existingSecretName -}}
{{- .Values.secrets.existingSecretName -}}
{{- else -}}
{{- include "vexa.componentName" (list . "secrets") -}}
{{- end -}}
{{- end -}}

{{/* Build one application/dependency image ref. A digest wins over both the local tag and the
global build-promotion tag. Call with (list $root $imageValues). */}}
{{- define "vexa.imageRef" -}}
{{- $root := index . 0 -}}
{{- $image := index . 1 -}}
{{- $repository := required "image.repository is required" $image.repository -}}
{{- $digest := trim (toString ($image.digest | default "")) -}}
{{- if $digest -}}
{{- if not (regexMatch "^sha256:[0-9a-f]{64}$" $digest) -}}
{{- fail "image.digest must be sha256 followed by exactly 64 lowercase hexadecimal characters" -}}
{{- end -}}
{{- printf "%s@%s" $repository $digest -}}
{{- else -}}
{{- $tag := $root.Values.global.imageTag | default $image.tag -}}
{{- printf "%s:%s" $repository (required "image.tag is required when image.digest is empty" $tag) -}}
{{- end -}}
{{- end -}}

{{/* Upstream dependency images have their own release cadence. A first-party global.imageTag must
never rewrite them to a tag that does not exist in the dependency registry. */}}
{{- define "vexa.dependencyImageRef" -}}
{{- $image := index . 0 -}}
{{- $repository := required "dependency image.repository is required" $image.repository -}}
{{- $digest := trim (toString ($image.digest | default "")) -}}
{{- if $digest -}}
{{- if not (regexMatch "^sha256:[0-9a-f]{64}$" $digest) -}}
{{- fail "dependency image.digest must be sha256 followed by exactly 64 lowercase hexadecimal characters" -}}
{{- end -}}
{{- printf "%s@%s" $repository $digest -}}
{{- else -}}
{{- printf "%s:%s" $repository (required "dependency image.tag is required when image.digest is empty" $image.tag) -}}
{{- end -}}
{{- end -}}

{{/* Secret data changes roll every consumer. Inline values hash the rendered Secret. External
Secret data is opaque to Helm, so the operator bumps secrets.existingSecretRevision. */}}
{{- define "vexa.secretRolloutChecksum" -}}
{{- if .Values.secrets.existingSecretName -}}
{{- printf "%s:%s" .Values.secrets.existingSecretName .Values.secrets.existingSecretRevision | sha256sum -}}
{{- else -}}
{{- include (print .Template.BasePath "/secret.yaml") . | sha256sum -}}
{{- end -}}
{{- end -}}

{{- define "vexa.databaseSecretRolloutChecksum" -}}
{{- if .Values.postgres.createCredentialsSecret -}}
{{- include (print .Template.BasePath "/secret.yaml") . | sha256sum -}}
{{- else -}}
{{- printf "%s:%s" .Values.postgres.credentialsSecretName .Values.postgres.credentialsSecretRevision | sha256sum -}}
{{- end -}}
{{- end -}}

{{- define "vexa.workloadNamespace" -}}
{{- .Values.runtime.workloadNamespace | default .Release.Namespace -}}
{{- end -}}

{{/* The on-demand bot image the runtime spawns (BROWSER_IMAGE). The bot is published, never built by
this chart. runtime.browserImage is the explicit value; global.imageTag (set) pins the standard repo. */}}
{{- define "vexa.botImage" -}}
{{- if .Values.runtime.browserImage -}}
{{- .Values.runtime.browserImage -}}
{{- else if .Values.global.imageTag -}}
{{- printf "vexaai/vexa-bot:%s" .Values.global.imageTag -}}
{{- else -}}
vexaai/vexa-bot:v012
{{- end -}}
{{- end -}}

{{/* The agent-api image ref (AGENT_IMAGE the runtime spawns workers from). global.imageTag wins. */}}
{{- define "vexa.agentImage" -}}
{{- if and .Values.runtime.agentImage (regexMatch "@sha256:[0-9a-f]{64}$" .Values.runtime.agentImage) -}}
{{- .Values.runtime.agentImage -}}
{{- else if .Values.agentApi.image.digest -}}
{{- include "vexa.imageRef" (list . .Values.agentApi.image) -}}
{{- else if .Values.global.imageTag -}}
{{- printf "%s:%s" .Values.agentApi.image.repository .Values.global.imageTag -}}
{{- else -}}
{{- .Values.runtime.agentImage | default (printf "%s:%s" .Values.agentApi.image.repository .Values.agentApi.image.tag) -}}
{{- end -}}
{{- end -}}

{{/* The agent-worker image ref (AGENT_WORKER_IMAGE; the dedicated worker build — core/agent/worker/Dockerfile — NOT the agent-api image). */}}
{{- define "vexa.agentWorkerImage" -}}
{{- if and .Values.runtime.agentWorkerImage (regexMatch "@sha256:[0-9a-f]{64}$" .Values.runtime.agentWorkerImage) -}}
{{- .Values.runtime.agentWorkerImage -}}
{{- else if .Values.global.imageTag -}}
{{- printf "vexaai/v012-agent-worker:%s" .Values.global.imageTag -}}
{{- else -}}
{{- .Values.runtime.agentWorkerImage | default "vexaai/v012-agent-worker:v012" -}}
{{- end -}}
{{- end -}}

{{- define "vexa.postgresCredentialsSecretName" -}}
{{- required "postgres.credentialsSecretName must name a Secret with POSTGRES_PASSWORD, POSTGRES_USER, and POSTGRES_DB" .Values.postgres.credentialsSecretName -}}
{{- end -}}

{{- define "vexa.deploymentStrategy" -}}
{{/*
v0.10.5.3 Pack H — zero-downtime rolling update.

Pre-fix: maxSurge: 0, maxUnavailable: 1. With replicaCount: 1, this killed
the OLD pod before creating the NEW pod, causing 502s during any image
bump (e.g. the v0.10.5.2 cycle outage where dashboard + webapp went 502
because new image tags didn't exist on the registry — old pods were
already killed by the time helm upgrade tried to create the new pods).

Post-fix: maxSurge: 1, maxUnavailable: 0. NEW pod is created first;
helm waits until it's Ready before killing the OLD. With --atomic --wait
on the helm upgrade call (release-helm-upgrade-safe Make target),
failed image pulls auto-rollback without ever exposing the outage.

Works on replicaCount=1 (1 old -> 1 old + 1 new -> 1 new) and
replicaCount>1 (rolling progresses one extra at a time).
*/}}
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 1
    maxUnavailable: 0
{{- end -}}

{{/*
Redis privacy-state durability invariant.

AOF with appendfsync=always is the per-ack durability mechanism used for Minutes consent,
withdrawal, ownership, and erasure fences. noeviction prevents ordinary cache pressure from
silently removing those keys; capacity exhaustion therefore returns write errors and callers
must fail closed while operators restore capacity.
`stop-writes-on-bgsave-error: no` allows writes to continue when the
snapshot mechanism fails (block-volume hiccup, disk-full, fsync stall) —
which is non-blocking when AOF is on. Setting `stop-writes-on-bgsave-error: yes`
WITHOUT `appendonly: yes` would create a write-loss window: Redis would
accept writes that aren't durable anywhere if BGSAVE fails. Refuse to render.

The 2026-04-21 redis-storage-cascade incident was triggered by exactly
this anti-pattern: BGSAVE failed, default `stop-writes-on-bgsave-error: yes`
froze writes for 46 min. With AOF + bgsave-error: no, BGSAVE failures
become non-blocking. This render invariant does not substitute for staging crash, volume, and
backup/restore verification.
*/}}
{{- define "vexa.validateRedisDurability" -}}
{{- $aof := .Values.redis.durability.appendonly | default "yes" -}}
{{- $fsync := .Values.redis.durability.appendfsync | default "always" -}}
{{- $bgsaveBlocks := .Values.redis.durability.stopWritesOnBgsaveError | default "no" -}}
{{- $eviction := .Values.redis.maxmemoryPolicy | default "noeviction" -}}
{{- $maxmemoryRaw := toString (.Values.redis.maxmemory | default "") -}}
{{- $maxmemory := trim $maxmemoryRaw -}}
{{- if or (ne $maxmemoryRaw $maxmemory) (not (regexMatch "^[1-9][0-9]*(b|kb|mb|gb)$" $maxmemory)) -}}
{{- required "INVALID Redis privacy durability: redis.maxmemory must be a positive lowercase b/kb/mb/gb quantity so noeviction fails writes before pod memory exhaustion." "" -}}
{{- end -}}
{{- if ne $aof "yes" -}}
{{- required "INVALID Redis privacy durability: redis.durability.appendonly must be yes because Minutes consent and erasure fences are acknowledged durable state." "" -}}
{{- end -}}
{{- if ne $fsync "always" -}}
{{- required "INVALID Redis privacy durability: redis.durability.appendfsync must be always so acknowledged Minutes fences have no configured fsync loss window." "" -}}
{{- end -}}
{{- if ne $eviction "noeviction" -}}
{{- required "INVALID Redis privacy durability: redis.maxmemoryPolicy must be noeviction so consent and erasure fences cannot be evicted; capacity exhaustion must fail writes closed." "" -}}
{{- end -}}
{{- if and (eq $bgsaveBlocks "yes") (ne $aof "yes") -}}
{{- required "INVALID redis.durability config: stopWritesOnBgsaveError=yes requires appendonly=yes (paired AOF + BGSAVE durability invariant — see v0.10.5 Pack C.5). Without AOF, blocking writes on BGSAVE failure means writes that arrive while BGSAVE is failing have no durable record anywhere." "" -}}
{{- end -}}
{{- end -}}
