#!/usr/bin/env bash
# Rendered Compose contract for the operator-owned, default-off Minutes controls.
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$HERE/docker-compose.yml"
ENV_EXAMPLE="$HERE/.env.example"

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  echo "SKIP: docker compose not installed"
  exit 0
fi

need_example() {
  local line="$1" label="$2"
  if grep -qxF -- "$line" "$ENV_EXAMPLE"; then
    echo "  OK: $label"
  else
    echo "  FAIL: $label" >&2
    exit 1
  fi
}

need_example 'ZAKI_MINUTES_CAPTURE_ENABLED=false' 'capture flag is documented default-off'
need_example 'ZAKI_MINUTES_INVOCATION_V2_ENABLED=false' 'invocation v2 producer is documented default-off'
need_example 'MINUTES_BROWSER_IMAGE=' 'v2 bot image has no implicit default'
need_example 'ZAKI_MINUTES_READ_ENABLED=false' 'read flag is documented default-off'
need_example 'ZAKI_MINUTES_AUTO_JOIN_ENABLED=false' 'calendar auto-join is documented default-off'
need_example 'ZAKI_MINUTES_MANAGED_ONLY=false' 'non-Minutes Compose keeps the ordinary bot path'
need_example 'ZAKI_MINUTES_READ_BASE_URL=' 'reserved Agent read origin carries no Compose default'
need_example 'ZAKI_READ_TOKEN_MINUTES=' 'read token example is blank'
need_example 'ZAKI_MINUTES_HUB_TOKEN=' 'Hub/BFF bearer example is blank'
need_example 'ZAKI_AGENT_ERASURE_SIGNING_SECRET=' 'Agent receipt signer example is blank'
need_example 'ZAKI_AGENT_ERASURE_VERIFICATION_SECRET=' 'Agent receipt verifier example is blank'
need_example 'ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET=' 'previous Agent receipt verifier example is blank'
need_example '# It is not a standalone historical mode: service startup requires the complete current boundary.' 'previous Agent verifier dependency is documented'
need_example 'ZAKI_MINUTES_ERASURE_SIGNING_SECRET=' 'Minutes receipt signer example is blank'
need_example 'ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET=' 'previous Minutes receipt verifier example is blank'
need_example 'ZAKI_MINUTES_FINALIZED_ENABLED=false' 'platform finalized delivery is documented default-off'
need_example 'ZAKI_MINUTES_FINALIZED_SECRET=' 'platform finalized signer example is blank'
need_example 'MINUTES_TTL_ENABLED=false' 'TTL worker is documented default-off'
need_example 'MINUTES_TTL_INTERVAL_S=60' 'TTL interval default is documented'
need_example 'MINUTES_TTL_BATCH_SIZE=100' 'TTL batch default is documented'
need_example 'RUNTIME_CALLBACK_SECRET=vexa-runtime-callback-secret' 'dedicated runtime callback secret is documented'
need_example 'MEETING_TOKEN_SECRET=vexa-meeting-token-secret' 'dedicated MeetingToken secret is documented'
need_example 'GATEWAY_IDENTITY_SECRET=vexa-gateway-identity-secret-local-v1' 'dedicated gateway identity proof is documented'
need_example 'GATEWAY_IDENTITY_PREVIOUS_SECRET=' 'previous gateway verifier example is blank'
need_example 'REDIS_PASSWORD=vexa-redis-password' 'authenticated Redis secret is documented'
need_example 'REDIS_MAXMEMORY=512mb' 'bounded Redis memory policy is documented'

render() {
  ZAKI_MINUTES_CAPTURE_ENABLED="${ZAKI_MINUTES_CAPTURE_ENABLED:-}" \
  ZAKI_MINUTES_INVOCATION_V2_ENABLED="${ZAKI_MINUTES_INVOCATION_V2_ENABLED:-}" \
  MINUTES_BROWSER_IMAGE="${MINUTES_BROWSER_IMAGE:-}" \
  ZAKI_MINUTES_READ_ENABLED="${ZAKI_MINUTES_READ_ENABLED:-}" \
  ZAKI_MINUTES_AUTO_JOIN_ENABLED="${ZAKI_MINUTES_AUTO_JOIN_ENABLED:-}" \
  ZAKI_MINUTES_MANAGED_ONLY="${ZAKI_MINUTES_MANAGED_ONLY:-}" \
  ZAKI_MINUTES_READ_BASE_URL="${ZAKI_MINUTES_READ_BASE_URL:-}" \
  ZAKI_READ_TOKEN_MINUTES="${ZAKI_READ_TOKEN_MINUTES:-}" \
  ZAKI_MINUTES_HUB_TOKEN="${ZAKI_MINUTES_HUB_TOKEN:-}" \
  ZAKI_AGENT_ERASURE_SIGNING_KEY_ID="${ZAKI_AGENT_ERASURE_SIGNING_KEY_ID:-}" \
  ZAKI_AGENT_ERASURE_SIGNING_SECRET="${ZAKI_AGENT_ERASURE_SIGNING_SECRET:-}" \
  ZAKI_AGENT_ERASURE_VERIFICATION_KEY_ID="${ZAKI_AGENT_ERASURE_VERIFICATION_KEY_ID:-}" \
  ZAKI_AGENT_ERASURE_VERIFICATION_SECRET="${ZAKI_AGENT_ERASURE_VERIFICATION_SECRET:-}" \
  ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID="${ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID:-}" \
  ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET="${ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET:-}" \
  ZAKI_MINUTES_ERASURE_SIGNING_KEY_ID="${ZAKI_MINUTES_ERASURE_SIGNING_KEY_ID:-}" \
  ZAKI_MINUTES_ERASURE_SIGNING_SECRET="${ZAKI_MINUTES_ERASURE_SIGNING_SECRET:-}" \
  ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_KEY_ID="${ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_KEY_ID:-}" \
  ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET="${ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET:-}" \
  ZAKI_MINUTES_FINALIZED_ENABLED="${ZAKI_MINUTES_FINALIZED_ENABLED:-}" \
  ZAKI_MINUTES_FINALIZED_URL="${ZAKI_MINUTES_FINALIZED_URL:-}" \
  ZAKI_MINUTES_FINALIZED_KEY_ID="${ZAKI_MINUTES_FINALIZED_KEY_ID:-}" \
  ZAKI_MINUTES_FINALIZED_SECRET="${ZAKI_MINUTES_FINALIZED_SECRET:-}" \
  MINUTES_TTL_ENABLED="${MINUTES_TTL_ENABLED:-}" \
  MINUTES_TTL_INTERVAL_S="${MINUTES_TTL_INTERVAL_S:-}" \
  MINUTES_TTL_BATCH_SIZE="${MINUTES_TTL_BATCH_SIZE:-}" \
  RUNTIME_CALLBACK_SECRET="${RUNTIME_CALLBACK_SECRET:-}" \
  MEETING_TOKEN_SECRET="${MEETING_TOKEN_SECRET:-}" \
  GATEWAY_IDENTITY_SECRET="${GATEWAY_IDENTITY_SECRET:-}" \
  GATEWAY_IDENTITY_PREVIOUS_SECRET="${GATEWAY_IDENTITY_PREVIOUS_SECRET:-}" \
  REDIS_PASSWORD="${REDIS_PASSWORD:-}" \
  DB_PASSWORD="${DB_PASSWORD:-}" \
    docker compose -f "$COMPOSE_FILE" config --format json
}

DEFAULT_JSON="$(render)"
CONFIG_JSON="$DEFAULT_JSON" python3 - <<'PY'
import json
import os

services = json.loads(os.environ["CONFIG_JSON"])["services"]
admin = services["admin-api"]["environment"]
meeting = services["meeting-api"]["environment"]
agent = services["agent-api"]["environment"]
runtime = services["runtime"]["environment"]
gateway = services["gateway"]["environment"]
redis_command = services["redis"]["command"]
agent_command = services["agent-api"]["command"]
redis_health = " ".join(services["redis"]["healthcheck"]["test"])
redis_env = services["redis"]["environment"]

assert admin["ZAKI_MINUTES_CAPTURE_ENABLED"] == "false"
assert admin["ZAKI_MINUTES_READ_ENABLED"] == "false"
assert "ZAKI_READ_TOKEN_MINUTES" not in admin
assert meeting["ZAKI_MINUTES_CAPTURE_ENABLED"] == "false"
assert meeting["ZAKI_MINUTES_INVOCATION_V2_ENABLED"] == "false"
assert runtime["MINUTES_BROWSER_IMAGE"] == ""
assert meeting["ZAKI_MINUTES_READ_ENABLED"] == "false"
assert meeting["ZAKI_MINUTES_AUTO_JOIN_ENABLED"] == "false"
assert meeting["ZAKI_MINUTES_MANAGED_ONLY"] == "false"
assert meeting["ZAKI_READ_TOKEN_MINUTES"] == ""
assert meeting["ZAKI_MINUTES_HUB_TOKEN"] == ""
assert agent["ZAKI_MINUTES_READ_ENABLED"] == "false"
assert agent["ZAKI_MINUTES_CAPTURE_ENABLED"] == "false"
assert agent["ZAKI_MINUTES_INVOCATION_V2_ENABLED"] == "false"
assert agent["MINUTES_BROWSER_IMAGE"] == ""
assert agent["ZAKI_MINUTES_AUTO_JOIN_ENABLED"] == "false"
assert agent["ZAKI_MINUTES_MANAGED_ONLY"] == "false"
assert agent["ZAKI_MINUTES_READ_BASE_URL"] == ""
assert agent["ZAKI_READ_TOKEN_MINUTES"] == ""
assert agent["ZAKI_MINUTES_HUB_TOKEN"] == ""
assert agent["ZAKI_AGENT_ERASURE_SIGNING_SECRET"] == ""
assert agent["ZAKI_MINUTES_FINALIZED_ENABLED"] == "false"
assert agent["MINUTES_TTL_ENABLED"] == "false"
assert agent["MINUTES_TTL_INTERVAL_S"] == "60"
assert agent["MINUTES_TTL_BATCH_SIZE"] == "100"
assert runtime["ZAKI_MINUTES_CAPTURE_ENABLED"] == "false"
assert runtime["ZAKI_MINUTES_INVOCATION_V2_ENABLED"] == "false"
assert runtime["ZAKI_MINUTES_READ_ENABLED"] == "false"
assert runtime["ZAKI_MINUTES_AUTO_JOIN_ENABLED"] == "false"
assert runtime["ZAKI_MINUTES_MANAGED_ONLY"] == "false"
assert runtime["ZAKI_MINUTES_HUB_TOKEN"] == ""
assert runtime["ZAKI_MINUTES_FINALIZED_ENABLED"] == "false"
assert runtime["MINUTES_TTL_ENABLED"] == "false"
assert agent["VEXA_DEPLOY_MINUTES_CAPTURE_REQUESTED"] == "false"
assert agent["VEXA_DEPLOY_MINUTES_READ_REQUESTED"] == "false"
assert meeting["ZAKI_AGENT_ERASURE_VERIFICATION_SECRET"] == ""
assert meeting["ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET"] == ""
assert meeting["ZAKI_MINUTES_ERASURE_SIGNING_SECRET"] == ""
assert meeting["ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET"] == ""
assert meeting["ZAKI_MINUTES_FINALIZED_ENABLED"] == "false"
assert meeting["ZAKI_MINUTES_FINALIZED_SECRET"] == ""
assert meeting["MINUTES_TTL_ENABLED"] == "false"
assert meeting["MINUTES_TTL_INTERVAL_S"] == "60"
assert meeting["MINUTES_TTL_BATCH_SIZE"] == "100"
assert meeting["AGENT_API_URL"] == "http://agent-api:8100"
assert runtime.get("INTERNAL_API_SECRET", "") == ""
assert runtime["RUNTIME_CONTROL_SECRET"]
assert runtime["RUNTIME_CALLBACK_SECRET"]
assert runtime["RUNTIME_CALLBACK_SECRET"] != runtime["RUNTIME_CONTROL_SECRET"]
assert meeting["RUNTIME_CONTROL_SECRET"] == runtime["RUNTIME_CONTROL_SECRET"]
assert meeting["RUNTIME_CALLBACK_SECRET"] == runtime["RUNTIME_CALLBACK_SECRET"]
assert agent["VEXA_RUNTIME_CONTROL_SECRET"] == runtime["RUNTIME_CONTROL_SECRET"]
assert runtime["RUNTIME_CALLBACK_TRUSTED_ORIGINS"] == "http://meeting-api:8080"
assert meeting["MEETING_TOKEN_SECRET"] == "vexa-meeting-token-secret"
assert meeting.get("ADMIN_TOKEN", "") == ""
assert meeting["MEETING_TOKEN_SECRET"] != admin["ADMIN_API_TOKEN"]
expected_gateway_identity = "vexa-gateway-identity-secret-local-v1"
assert gateway["GATEWAY_IDENTITY_SECRET"] == expected_gateway_identity
assert agent["GATEWAY_IDENTITY_SECRET"] == expected_gateway_identity
assert agent["GATEWAY_IDENTITY_PREVIOUS_SECRET"] == ""
assert redis_env["REDIS_PASSWORD"] == "vexa-redis-password"
assert redis_env["REDIS_MAXMEMORY"] == "512mb"
expected_redis_url = "redis://:vexa-redis-password@redis:6379/0"
assert runtime["REDIS_URL"] == expected_redis_url
assert meeting["REDIS_URL"] == expected_redis_url
assert agent["VEXA_REDIS_URL"] == expected_redis_url
assert services["gateway"]["environment"]["REDIS_URL"] == expected_redis_url
for name, service in services.items():
    environment = service.get("environment") or {}
    if name not in {"runtime", "meeting-api"}:
        assert environment.get("RUNTIME_CONTROL_SECRET", "") == ""
        assert environment.get("RUNTIME_CALLBACK_SECRET", "") == ""
    if name != "agent-api":
        assert environment.get("VEXA_RUNTIME_CONTROL_SECRET", "") == ""
    if name != "meeting-api":
        assert environment.get("MEETING_TOKEN_SECRET", "") == ""
    if name not in {"gateway", "agent-api"}:
        assert environment.get("GATEWAY_IDENTITY_SECRET", "") == ""
    if name != "agent-api":
        assert environment.get("GATEWAY_IDENTITY_PREVIOUS_SECRET", "") == ""
    if name != "redis":
        assert environment.get("REDIS_PASSWORD", "") == ""
redis_command_text = " ".join(redis_command) if isinstance(redis_command, list) else redis_command
agent_command_text = " ".join(agent_command) if isinstance(agent_command, list) else agent_command
assert "--appendonly" in redis_command_text and "yes" in redis_command_text
assert "--appendfsync" in redis_command_text and "always" in redis_command_text
assert "--maxmemory-policy" in redis_command_text and "noeviction" in redis_command_text
assert "--maxmemory" in redis_command_text and "REDIS_MAXMEMORY" in redis_command_text
assert "^[1-9][0-9]*(b|kb|mb|gb)" in redis_command_text
assert "redis_password_length" in redis_command_text
assert "[!A-Za-z0-9._~-]" in redis_command_text
assert "--requirepass" in redis_command_text and "REDIS_PASSWORD" in redis_command_text
assert "NOAUTH" in redis_health and "REDISCLI_AUTH" in redis_health
assert "VEXA_DEPLOY_MINUTES_CAPTURE_REQUESTED" in agent_command_text
assert "VEXA_DEPLOY_MINUTES_READ_REQUESTED" in agent_command_text
assert "exit 78" in agent_command_text
for name, service in services.items():
    assert (service.get("environment") or {}).get("ZAKI_READ_TOKEN_MINUTES", "") == ""
PY
echo "  OK: rendered Compose defaults keep Minutes inert and the bundled Agent credential-free"

SPECIAL_DB_JSON="$(DB_PASSWORD='generated/db%pass@word:value' render)"
CONFIG_JSON="$SPECIAL_DB_JSON" python3 - <<'PY'
import json
import os

services = json.loads(os.environ["CONFIG_JSON"])["services"]
for service in ("admin-api", "meeting-api"):
    assert services[service]["environment"]["DB_PASSWORD"] == "generated/db%pass@word:value"
PY
echo "  OK: raw generated Postgres secrets reach both URL-encoding composition roots unchanged"

set +e
BAD_REDIS_OUTPUT="$(REDIS_PASSWORD='this/is-not-uri-safe' \
  docker compose -f "$COMPOSE_FILE" run --rm --no-deps redis 2>&1)"
BAD_REDIS_STATUS=$?
set -e
if [ "$BAD_REDIS_STATUS" -eq 64 ] \
  && grep -q 'REDIS_PASSWORD must be 16..512 URI-userinfo-safe characters' <<<"$BAD_REDIS_OUTPUT"; then
  echo "  OK: Redis refuses a password that would corrupt every client URL before startup"
else
  echo "  FAIL: Redis accepted or misreported a URI-unsafe password" >&2
  exit 1
fi

# runtime and agent-api intentionally source model-provider credentials from the shared .env file.
# Compose merges every env_file entry into the container, so prove that explicit environment
# projections scrub unrelated platform credentials even when the operator's file is fully populated.
ENV_FILE_FIXTURE_DIR="$(mktemp -d)"
trap 'rm -rf "$ENV_FILE_FIXTURE_DIR"' EXIT
cp "$COMPOSE_FILE" "$ENV_FILE_FIXTURE_DIR/docker-compose.yml"
cp "$ENV_EXAMPLE" "$ENV_FILE_FIXTURE_DIR/.env"
printf '%s\n' \
  'GOOGLE_CLIENT_SECRET=fixture-google-secret' \
  'MICROSOFT_CLIENT_SECRET=fixture-microsoft-secret' \
  'TRANSCRIPTION_SERVICE_TOKEN=fixture-stt-secret' \
  'VEXA_LLM_API_KEY=fixture-model-secret' \
  'CLAUDE_CODE_OAUTH_TOKEN=fixture-claude-secret' \
  'ANTHROPIC_AUTH_TOKEN=fixture-anthropic-secret' \
  'VEXA_BOT_API_KEY=fixture-bot-service-secret' \
  'VEXA_API_KEY=fixture-terminal-user-secret' \
  'GATEWAY_IDENTITY_PREVIOUS_SECRET=fixture-gateway-identity-previous-secret-v0' \
  'ZAKI_MINUTES_CAPTURE_ENABLED=true' \
  'ZAKI_MINUTES_INVOCATION_V2_ENABLED=true' \
  'MINUTES_BROWSER_IMAGE=fixture/minutes-bot:v2' \
  'ZAKI_MINUTES_READ_ENABLED=true' \
  'ZAKI_MINUTES_AUTO_JOIN_ENABLED=true' \
  'ZAKI_MINUTES_MANAGED_ONLY=false' \
  'ZAKI_MINUTES_READ_BASE_URL=https://minutes.example.invalid' \
  'ZAKI_READ_TOKEN_MINUTES=fixture-minutes-read-secret' \
  'ZAKI_MINUTES_HUB_TOKEN=fixture-hub-secret' \
  'ZAKI_AGENT_ERASURE_SIGNING_KEY_ID=fixture-agent-key' \
  'ZAKI_AGENT_ERASURE_SIGNING_SECRET=fixture-agent-signing-secret' \
  'ZAKI_AGENT_ERASURE_VERIFICATION_KEY_ID=fixture-agent-key' \
  'ZAKI_AGENT_ERASURE_VERIFICATION_SECRET=fixture-agent-verifier-secret' \
  'ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID=fixture-previous-agent-key' \
  'ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET=fixture-previous-agent-secret' \
  'ZAKI_MINUTES_ERASURE_SIGNING_KEY_ID=fixture-minutes-key' \
  'ZAKI_MINUTES_ERASURE_SIGNING_SECRET=fixture-minutes-signing-secret' \
  'ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_KEY_ID=fixture-previous-key' \
  'ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET=fixture-previous-secret' \
  'ZAKI_MINUTES_FINALIZED_ENABLED=true' \
  'ZAKI_MINUTES_FINALIZED_URL=https://hub.example.invalid/finalized' \
  'ZAKI_MINUTES_FINALIZED_KEY_ID=fixture-finalized-key' \
  'ZAKI_MINUTES_FINALIZED_SECRET=fixture-finalized-secret' \
  'MINUTES_TTL_ENABLED=true' \
  'MINUTES_TTL_INTERVAL_S=15' \
  'MINUTES_TTL_BATCH_SIZE=500' \
  >> "$ENV_FILE_FIXTURE_DIR/.env"
ENV_FILE_JSON="$(docker compose -f "$ENV_FILE_FIXTURE_DIR/docker-compose.yml" config --format json)"
CONFIG_JSON="$ENV_FILE_JSON" python3 - <<'PY'
import json
import os

services = json.loads(os.environ["CONFIG_JSON"])["services"]
runtime = services["runtime"]["environment"]
agent = services["agent-api"]["environment"]
meeting = services["meeting-api"]["environment"]

runtime_forbidden = {
    "ADMIN_TOKEN",
    "INTERNAL_API_SECRET",
    "MEETING_TOKEN_SECRET",
    "GATEWAY_IDENTITY_SECRET",
    "GATEWAY_IDENTITY_PREVIOUS_SECRET",
    "REDIS_PASSWORD",
    "VEXA_DISPATCH_SIGNING_KEY",
    "NEXTAUTH_SECRET",
    "DB_PASSWORD",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "TRANSCRIPTION_SERVICE_TOKEN",
    "GOOGLE_CLIENT_SECRET",
    "MICROSOFT_CLIENT_SECRET",
    "VEXA_BOT_API_KEY",
    "VEXA_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
}
agent_forbidden = {
    "ADMIN_TOKEN",
    "INTERNAL_API_SECRET",
    "RUNTIME_CONTROL_SECRET",
    "RUNTIME_CALLBACK_SECRET",
    "MEETING_TOKEN_SECRET",
    "REDIS_PASSWORD",
    "NEXTAUTH_SECRET",
    "DB_PASSWORD",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "GOOGLE_CLIENT_SECRET",
    "MICROSOFT_CLIENT_SECRET",
    "VEXA_API_KEY",
}
for key in runtime_forbidden:
    assert runtime.get(key, "") == "", f"runtime inherited {key} from shared env_file"
for key in agent_forbidden:
    assert agent.get(key, "") == "", f"agent-api inherited {key} from shared env_file"

# Keep the narrowly intended model-provider and service credentials intact.
assert runtime["VEXA_LLM_API_KEY"] == "fixture-model-secret"
assert runtime["ANTHROPIC_AUTH_TOKEN"] == "fixture-anthropic-secret"
assert agent["VEXA_LLM_API_KEY"] == "fixture-model-secret"
assert agent["CLAUDE_CODE_OAUTH_TOKEN"] == "fixture-claude-secret"
assert agent["ANTHROPIC_AUTH_TOKEN"] == "fixture-anthropic-secret"
assert agent["TRANSCRIPTION_SERVICE_TOKEN"] == "fixture-stt-secret"
assert agent["VEXA_BOT_API_KEY"] == "fixture-bot-service-secret"
assert agent["GATEWAY_IDENTITY_SECRET"] == "vexa-gateway-identity-secret-local-v1"
assert agent["GATEWAY_IDENTITY_PREVIOUS_SECRET"] == "fixture-gateway-identity-previous-secret-v0"

minutes_false = {
    "ZAKI_MINUTES_CAPTURE_ENABLED",
    "ZAKI_MINUTES_INVOCATION_V2_ENABLED",
    "ZAKI_MINUTES_READ_ENABLED",
    "ZAKI_MINUTES_AUTO_JOIN_ENABLED",
    "ZAKI_MINUTES_FINALIZED_ENABLED",
    "MINUTES_TTL_ENABLED",
}
minutes_blank = {
    "MINUTES_BROWSER_IMAGE",
    "ZAKI_MINUTES_READ_BASE_URL",
    "ZAKI_READ_TOKEN_MINUTES",
    "ZAKI_MINUTES_HUB_TOKEN",
    "ZAKI_AGENT_ERASURE_SIGNING_KEY_ID",
    "ZAKI_AGENT_ERASURE_SIGNING_SECRET",
    "ZAKI_AGENT_ERASURE_VERIFICATION_KEY_ID",
    "ZAKI_AGENT_ERASURE_VERIFICATION_SECRET",
    "ZAKI_MINUTES_ERASURE_SIGNING_KEY_ID",
    "ZAKI_MINUTES_ERASURE_SIGNING_SECRET",
    "ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_KEY_ID",
    "ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET",
    "ZAKI_MINUTES_FINALIZED_URL",
    "ZAKI_MINUTES_FINALIZED_KEY_ID",
    "ZAKI_MINUTES_FINALIZED_SECRET",
}
for service in (runtime, agent):
    for key in minutes_false:
        assert service.get(key) == "false", f"{key} was not force-disabled"
    for key in minutes_blank:
        assert service.get(key, "") == "", f"{key} leaked through shared env_file"
    assert service.get("ZAKI_MINUTES_MANAGED_ONLY") == "false"
    assert service.get("MINUTES_TTL_INTERVAL_S") == "60"
    assert service.get("MINUTES_TTL_BATCH_SIZE") == "100"
assert runtime["ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID"] == ""
assert runtime["ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET"] == ""
for service in (agent, meeting):
    assert service["ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID"] == "fixture-previous-agent-key"
    assert service["ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET"] == "fixture-previous-agent-secret"
assert not any("ZAKI_AGENT_ERASURE_PREVIOUS_SIGNING" in key for key in agent)
assert agent["VEXA_DEPLOY_MINUTES_CAPTURE_REQUESTED"] == "true"
assert agent["VEXA_DEPLOY_MINUTES_READ_REQUESTED"] == "true"
PY
echo "  OK: shared provider env_file cannot widen runtime or Agent credential authority"

CUSTOM_JSON="$(
  ZAKI_MINUTES_CAPTURE_ENABLED=true \
  ZAKI_MINUTES_INVOCATION_V2_ENABLED=true \
  MINUTES_BROWSER_IMAGE=vexaai/zaki-minutes-bot:v2 \
  ZAKI_MINUTES_READ_ENABLED=true \
  ZAKI_MINUTES_AUTO_JOIN_ENABLED=false \
  ZAKI_MINUTES_MANAGED_ONLY=false \
  ZAKI_MINUTES_READ_BASE_URL=https://minutes.internal \
  ZAKI_READ_TOKEN_MINUTES=operator-supplied-token \
  ZAKI_MINUTES_HUB_TOKEN=operator-hub-secret \
  ZAKI_AGENT_ERASURE_SIGNING_KEY_ID=agent-erasure-2026-07 \
  ZAKI_AGENT_ERASURE_SIGNING_SECRET=agent-shared-hmac \
  ZAKI_AGENT_ERASURE_VERIFICATION_KEY_ID=agent-erasure-2026-07 \
  ZAKI_AGENT_ERASURE_VERIFICATION_SECRET=agent-shared-hmac \
  ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID=agent-erasure-2026-06 \
  ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET=agent-previous-hmac \
  ZAKI_MINUTES_ERASURE_SIGNING_KEY_ID=minutes-erasure-2026-07 \
  ZAKI_MINUTES_ERASURE_SIGNING_SECRET=minutes-independent-hmac \
  ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_KEY_ID=minutes-erasure-2026-06 \
  ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET=minutes-previous-hmac \
  ZAKI_MINUTES_FINALIZED_ENABLED=true \
  ZAKI_MINUTES_FINALIZED_URL=http://hub-api:8080/internal/minutes/finalized \
  ZAKI_MINUTES_FINALIZED_KEY_ID=minutes-platform-2026-07 \
  ZAKI_MINUTES_FINALIZED_SECRET=platform-finalized-hmac \
  MINUTES_TTL_ENABLED=true \
  MINUTES_TTL_INTERVAL_S=15 \
  MINUTES_TTL_BATCH_SIZE=500 \
  RUNTIME_CALLBACK_SECRET=operator-runtime-callback-secret \
  MEETING_TOKEN_SECRET=operator-meeting-token-secret \
  REDIS_PASSWORD=operator-redis-password \
    render
)"
CONFIG_JSON="$CUSTOM_JSON" python3 - <<'PY'
import json
import os

services = json.loads(os.environ["CONFIG_JSON"])["services"]
admin = services["admin-api"]["environment"]
meeting = services["meeting-api"]["environment"]
agent = services["agent-api"]["environment"]
runtime = services["runtime"]["environment"]
agent_command = services["agent-api"]["command"]

assert admin["ZAKI_MINUTES_CAPTURE_ENABLED"] == "false"
assert admin["ZAKI_MINUTES_READ_ENABLED"] == "false"
assert meeting["ZAKI_MINUTES_CAPTURE_ENABLED"] == "false"
assert meeting["ZAKI_MINUTES_INVOCATION_V2_ENABLED"] == "false"
assert runtime["MINUTES_BROWSER_IMAGE"] == ""
assert meeting["ZAKI_MINUTES_READ_ENABLED"] == "false"
assert meeting["ZAKI_MINUTES_AUTO_JOIN_ENABLED"] == "false"
assert meeting["ZAKI_MINUTES_MANAGED_ONLY"] == "false"
assert meeting["ZAKI_READ_TOKEN_MINUTES"] == ""
assert meeting["ZAKI_MINUTES_HUB_TOKEN"] == ""
assert agent["ZAKI_MINUTES_READ_ENABLED"] == "false"
assert agent["ZAKI_MINUTES_CAPTURE_ENABLED"] == "false"
assert agent["ZAKI_MINUTES_READ_BASE_URL"] == ""
assert agent["ZAKI_READ_TOKEN_MINUTES"] == ""
assert agent["ZAKI_MINUTES_HUB_TOKEN"] == ""
assert agent["ZAKI_AGENT_ERASURE_SIGNING_KEY_ID"] == ""
assert agent["ZAKI_AGENT_ERASURE_SIGNING_SECRET"] == ""
assert agent["VEXA_DEPLOY_MINUTES_CAPTURE_REQUESTED"] == "true"
assert agent["VEXA_DEPLOY_MINUTES_READ_REQUESTED"] == "true"
assert agent.get("ZAKI_AGENT_ERASURE_VERIFICATION_SECRET", "") == ""
assert agent["ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID"] == "agent-erasure-2026-06"
assert agent["ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET"] == "agent-previous-hmac"
assert agent.get("ZAKI_MINUTES_ERASURE_SIGNING_SECRET", "") == ""
assert agent.get("ZAKI_MINUTES_FINALIZED_SECRET", "") == ""
assert meeting["ZAKI_AGENT_ERASURE_VERIFICATION_KEY_ID"] == ""
assert meeting["ZAKI_AGENT_ERASURE_VERIFICATION_SECRET"] == ""
assert meeting["ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID"] == "agent-erasure-2026-06"
assert meeting["ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET"] == "agent-previous-hmac"
assert meeting["ZAKI_MINUTES_ERASURE_SIGNING_KEY_ID"] == ""
assert meeting["ZAKI_MINUTES_ERASURE_SIGNING_SECRET"] == ""
assert meeting["ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_KEY_ID"] == ""
assert meeting["ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET"] == ""
assert meeting["ZAKI_MINUTES_FINALIZED_ENABLED"] == "false"
assert meeting["ZAKI_MINUTES_FINALIZED_URL"] == ""
assert meeting["ZAKI_MINUTES_FINALIZED_KEY_ID"] == ""
assert meeting["ZAKI_MINUTES_FINALIZED_SECRET"] == ""
assert meeting.get("ZAKI_AGENT_ERASURE_SIGNING_SECRET", "") == ""
assert meeting["MINUTES_TTL_ENABLED"] == "false"
assert meeting["MINUTES_TTL_INTERVAL_S"] == "60"
assert meeting["MINUTES_TTL_BATCH_SIZE"] == "100"
assert runtime.get("INTERNAL_API_SECRET", "") == ""
assert runtime["RUNTIME_CONTROL_SECRET"]
assert runtime["RUNTIME_CALLBACK_SECRET"] == "operator-runtime-callback-secret"
assert runtime["RUNTIME_CALLBACK_SECRET"] != runtime["RUNTIME_CONTROL_SECRET"]
assert meeting["RUNTIME_CONTROL_SECRET"] == runtime["RUNTIME_CONTROL_SECRET"]
assert meeting["RUNTIME_CALLBACK_SECRET"] == runtime["RUNTIME_CALLBACK_SECRET"]
assert agent["VEXA_RUNTIME_CONTROL_SECRET"] == runtime["RUNTIME_CONTROL_SECRET"]
assert runtime["RUNTIME_CALLBACK_TRUSTED_ORIGINS"] == "http://meeting-api:8080"
assert meeting["MEETING_TOKEN_SECRET"] == "operator-meeting-token-secret"
assert meeting.get("ADMIN_TOKEN", "") == ""
assert services["redis"]["environment"]["REDIS_PASSWORD"] == "operator-redis-password"
expected_redis_url = "redis://:operator-redis-password@redis:6379/0"
assert runtime["REDIS_URL"] == expected_redis_url
assert meeting["REDIS_URL"] == expected_redis_url
assert agent["VEXA_REDIS_URL"] == expected_redis_url
assert services["gateway"]["environment"]["REDIS_URL"] == expected_redis_url
agent_command_text = " ".join(agent_command) if isinstance(agent_command, list) else agent_command
assert "VEXA_DEPLOY_MINUTES_CAPTURE_REQUESTED" in agent_command_text
assert "VEXA_DEPLOY_MINUTES_READ_REQUESTED" in agent_command_text
assert "exit 78" in agent_command_text
for name, service in services.items():
    assert (service.get("environment") or {}).get("ZAKI_READ_TOKEN_MINUTES", "") == ""
    assert (service.get("environment") or {}).get("ZAKI_MINUTES_HUB_TOKEN", "") == ""
    environment = service.get("environment") or {}
    if name not in {"runtime", "meeting-api"}:
        assert environment.get("RUNTIME_CALLBACK_SECRET", "") == ""
    if name != "meeting-api":
        assert environment.get("MEETING_TOKEN_SECRET", "") == ""
    if name != "redis":
        assert environment.get("REDIS_PASSWORD", "") == ""
    if name != "agent-api":
        assert environment.get("ZAKI_AGENT_ERASURE_SIGNING_SECRET", "") == ""
    if name != "meeting-api":
        assert environment.get("ZAKI_AGENT_ERASURE_VERIFICATION_SECRET", "") == ""
        assert environment.get("ZAKI_MINUTES_ERASURE_SIGNING_SECRET", "") == ""
        assert environment.get("ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET", "") == ""
        assert environment.get("ZAKI_MINUTES_FINALIZED_SECRET", "") == ""
    if name not in {"agent-api", "meeting-api"}:
        assert environment.get("ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET", "") == ""
for service_name in ("agent-api", "meeting-api"):
    environment = services[service_name]["environment"]
    assert not any("ZAKI_AGENT_ERASURE_PREVIOUS_SIGNING" in key for key in environment)
PY
echo "  OK: Compose rejects Minutes intent; verifier-only rotation reaches only Agent + meeting"

echo "minutes-config-contract PASS"
