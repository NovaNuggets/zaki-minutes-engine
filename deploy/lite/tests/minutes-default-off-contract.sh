#!/usr/bin/env bash
# Static plus executable Vexa Lite contract: every Minutes setting is rejected, while non-Minutes
# credentials leave the supervisor parent before configuration projection to named consumers.
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
ENTRYPOINT="$HERE/entrypoint.sh"
SUPERVISOR="$HERE/supervisord.conf"
PROVISION_KEY="$HERE/bin/provision-key.sh"
DOCKERFILE="$HERE/Dockerfile.lite"
README="$HERE/README.md"

need() {
  local file="$1" pattern="$2" label="$3"
  if grep -qF -- "$pattern" "$file"; then
    echo "  OK: $label"
  else
    echo "  FAIL: $label" >&2
    exit 1
  fi
}

if grep -Eq 'ZAKI_MINUTES_|ZAKI_READ_TOKEN_MINUTES|ZAKI_AGENT_ERASURE_|MINUTES_(TTL|BOT|BROWSER)' "$SUPERVISOR"; then
  echo "  FAIL: Lite still projects Minutes configuration into supervised services" >&2
  exit 1
else
  echo "  OK: supervised services receive no Minutes configuration"
fi
if grep -Eq '> /run/vexa/(minutes|agent-erasure)' "$ENTRYPOINT"; then
  echo "  FAIL: Lite still materializes Minutes credentials" >&2
  exit 1
else
  echo "  OK: entrypoint materializes no Minutes credentials"
fi

need "$ENTRYPOINT" 'export RUNTIME_CONTROL_SECRET="${RUNTIME_CONTROL_SECRET:-lite-runtime-control-secret}"' 'runtime controller credential is normalized'
need "$ENTRYPOINT" 'export RUNTIME_CALLBACK_SECRET="${RUNTIME_CALLBACK_SECRET:-lite-runtime-callback-secret}"' 'runtime callback credential is normalized independently'
need "$ENTRYPOINT" 'export MEETING_TOKEN_SECRET="${MEETING_TOKEN_SECRET:-lite-meeting-token-secret}"' 'MeetingToken signer is normalized independently'
need "$ENTRYPOINT" 'export GATEWAY_IDENTITY_SECRET="${GATEWAY_IDENTITY_SECRET:-lite-gateway-identity-secret-local-v1}"' 'gateway identity proof is normalized independently'
need "$ENTRYPOINT" 'export GATEWAY_IDENTITY_PREVIOUS_SECRET="${GATEWAY_IDENTITY_PREVIOUS_SECRET:-}"' 'previous gateway verifier is normalized independently'
need "$DOCKERFILE" 'RUNTIME_CONTROL_SECRET=lite-runtime-control-secret RUNTIME_CALLBACK_SECRET=lite-runtime-callback-secret' 'image development defaults keep runtime authorities distinct'
need "$DOCKERFILE" 'MEETING_TOKEN_SECRET=lite-meeting-token-secret' 'image has a dedicated MeetingToken development default'
need "$ENTRYPOINT" 'export BOT_COMMAND="${BOT_COMMAND:-/usr/local/bin/vexa-bot-launch}"' 'ordinary upstream bot launcher remains available'
need "$README" 'rejects every Minutes setting' 'unsupported Minutes material is documented as rejected'
need "$README" 'trusted OS process domain' 'Lite documents that exec projection is not peer-root isolation'
need "$README" 'Agent-only in normal environment projection' 'previous verifier guarantee is scoped honestly'
for key in INTERNAL_API_SECRET RUNTIME_CONTROL_SECRET RUNTIME_CALLBACK_SECRET MEETING_TOKEN_SECRET \
  GATEWAY_IDENTITY_SECRET GATEWAY_IDENTITY_PREVIOUS_SECRET; do
  need "$ENTRYPOINT" "unset $key" "supervisor parent drops $key"
done

if env -i PATH="$PATH" \
  GATEWAY_IDENTITY_PREVIOUS_SECRET=short \
  /bin/bash "$ENTRYPOINT" true >/dev/null 2>&1; then
  echo "  FAIL: Lite accepted an invalid previous gateway verifier" >&2
  exit 1
else
  echo "  OK: Lite validates the optional previous gateway verifier"
fi
if env -i PATH="$PATH" \
  GATEWAY_IDENTITY_SECRET=aliased-gateway-identity-secret-1234567890 \
  GATEWAY_IDENTITY_PREVIOUS_SECRET=aliased-gateway-identity-secret-1234567890 \
  /bin/bash "$ENTRYPOINT" true >/dev/null 2>&1; then
  echo "  FAIL: Lite accepted identical current/previous gateway proof keys" >&2
  exit 1
else
  echo "  OK: Lite keeps current and previous gateway proof keys distinct"
fi
for key in VEXA_INTERNAL_API_SECRET VEXA_RUNTIME_CONTROL_SECRET; do
  need "$ENTRYPOINT" "unset $key" "supervisor parent drops the Agent alias $key"
done
for key in ADMIN_API_TOKEN ADMIN_TOKEN; do
  need "$ENTRYPOINT" "unset $key" "supervisor parent drops $key"
done
need "$ENTRYPOINT" 'printf '\''%s'\'' "$RUNTIME_CALLBACK_SECRET" > /run/vexa/runtime-callback-secret' 'entrypoint materializes the callback credential file'
need "$ENTRYPOINT" 'printf '\''%s'\'' "$MEETING_TOKEN_SECRET" > /run/vexa/meeting-token-secret' 'entrypoint materializes the MeetingToken credential file'
need "$ENTRYPOINT" 'printf '\''%s'\'' "$ADMIN_API_TOKEN" > /run/vexa/admin-api-token' 'entrypoint materializes the admin credential file'
need "$ENTRYPOINT" 'printf '\''%s'\'' "$INTERNAL_API_SECRET" > /run/vexa/internal-api-secret' 'entrypoint materializes the platform-internal credential file'
need "$ENTRYPOINT" 'printf '\''%s'\'' "$RUNTIME_CONTROL_SECRET" > /run/vexa/runtime-control-secret' 'entrypoint materializes the runtime-control credential file'
need "$ENTRYPOINT" 'printf '\''%s'\'' "$GATEWAY_IDENTITY_SECRET" > /run/vexa/gateway-identity-secret' 'entrypoint materializes the gateway identity proof file'
need "$ENTRYPOINT" 'printf '\''%s'\'' "$GATEWAY_IDENTITY_PREVIOUS_SECRET" > /run/vexa/gateway-identity-previous-secret' 'entrypoint materializes the previous verifier file'
need "$ENTRYPOINT" '/run/vexa/admin-api-token' 'entrypoint owns the root-only admin token file'
need "$PROVISION_KEY" '/run/vexa/admin-api-token' 'Terminal provisioner reads the configured admin token file'
need "$PROVISION_KEY" 'chmod 600 /run/vexa/key.env' 'provisioned Terminal tokens stay mode 0600'
need "$PROVISION_KEY" 'restart vexa:terminal' 'Terminal provisioner restarts the sole Lite UI after minting'
if grep -qF 'vexa:dashboard' "$PROVISION_KEY"; then
  echo "  FAIL: Terminal provisioner still targets the absent Lite dashboard program" >&2
  exit 1
fi
need "$SUPERVISOR" '--appendonly yes --appendfsync always --maxmemory %(ENV_REDIS_MAXMEMORY)s --maxmemory-policy noeviction' 'Redis never evicts privacy fences and fsyncs every accepted write'
need "$ENTRYPOINT" 'export REDIS_MAXMEMORY="${REDIS_MAXMEMORY:-512mb}"' 'Redis has an explicit bounded-memory default'
need "$SUPERVISOR" '--maxmemory %(ENV_REDIS_MAXMEMORY)s' 'Redis enforces the configured memory ceiling'
need "$ENTRYPOINT" 'redis_log_url="${REDIS_URL%%://*}://***@${REDIS_URL##*@}"' 'credentialed Redis URLs are redacted through the last userinfo delimiter before logging'
need "$SUPERVISOR" '--log-level "$VEXA_LOG_LEVEL"' 'agent log level is shell-quoted at exec'
if grep -qF 'echo "  - Redis URL:        ${REDIS_URL}"' "$ENTRYPOINT"; then
  echo "  FAIL: Lite logs the complete Redis URL, including possible credentials" >&2
  exit 1
fi
if env -i PATH="$PATH" REDIS_MAXMEMORY=0 /bin/bash "$ENTRYPOINT" true >/dev/null 2>&1; then
  echo "  FAIL: Lite accepted an unbounded Redis memory setting" >&2
  exit 1
else
  echo "  OK: Lite rejects an unbounded Redis memory setting"
fi

# Lite is a local/reference topology, not a supported Minutes topology. Every Minutes-facing
# setting, including false feature flags and historical receipt material, must refuse startup before
# the entrypoint reaches filesystem or database setup.
for key in \
  ZAKI_MINUTES_CAPTURE_ENABLED \
  ZAKI_MINUTES_INVOCATION_V2_ENABLED \
  ZAKI_MINUTES_READ_ENABLED \
  ZAKI_MINUTES_AUTO_JOIN_ENABLED \
  ZAKI_MINUTES_MANAGED_ONLY \
  ZAKI_MINUTES_READ_BASE_URL \
  ZAKI_READ_TOKEN_MINUTES \
  ZAKI_MINUTES_HUB_TOKEN \
  ZAKI_AGENT_ERASURE_SIGNING_KEY_ID \
  ZAKI_AGENT_ERASURE_SIGNING_SECRET \
  ZAKI_AGENT_ERASURE_VERIFICATION_KEY_ID \
  ZAKI_AGENT_ERASURE_VERIFICATION_SECRET \
  ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID \
  ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET \
  ZAKI_MINUTES_ERASURE_SIGNING_KEY_ID \
  ZAKI_MINUTES_ERASURE_SIGNING_SECRET \
  ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_KEY_ID \
  ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET \
  ZAKI_MINUTES_FINALIZED_ENABLED \
  ZAKI_MINUTES_FINALIZED_URL \
  ZAKI_MINUTES_FINALIZED_KEY_ID \
  ZAKI_MINUTES_FINALIZED_SECRET \
  MINUTES_TTL_ENABLED \
  MINUTES_TTL_INTERVAL_S \
  MINUTES_TTL_BATCH_SIZE \
  MINUTES_BROWSER_IMAGE \
  MINUTES_BOT_COMMAND \
  ZAKI_MINUTES_FUTURE_SETTING \
  ZAKI_AGENT_ERASURE_FUTURE_KEY \
  MINUTES_FUTURE_SETTING; do
  output=""
  if output="$(env -i PATH="$PATH" "$key=false" /bin/bash "$ENTRYPOINT" true 2>&1)"; then
    echo "  FAIL: Lite accepted Minutes configuration via $key" >&2
    exit 1
  fi
  if printf '%s\n' "$output" | grep -qF 'Vexa Lite is a local/reference topology' \
    && printf '%s\n' "$output" | grep -qF "$key"; then
    echo "  OK: Lite refuses $key even when its value is false"
  else
    echo "  FAIL: Lite rejection for $key is not actionable" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
done

# Identity-admin, callback, MeetingToken, runtime-control, and platform-internal credentials are separate trust
# domains. Prove every pairwise alias is rejected before supervisord can inherit anything.
secret_names=(ADMIN_API_TOKEN RUNTIME_CALLBACK_SECRET MEETING_TOKEN_SECRET RUNTIME_CONTROL_SECRET INTERNAL_API_SECRET GATEWAY_IDENTITY_SECRET)
for ((i = 0; i < ${#secret_names[@]}; i++)); do
  for ((j = i + 1; j < ${#secret_names[@]}; j++)); do
    left="${secret_names[$i]}"
    right="${secret_names[$j]}"
    output=""
    if output="$(env -i PATH="$PATH" "$left=aliased-secret-1234567890abcdefgh" "$right=aliased-secret-1234567890abcdefgh" \
      /bin/bash "$ENTRYPOINT" true 2>&1)"; then
      echo "  FAIL: Lite accepted aliased $left and $right" >&2
      exit 1
    fi
    if printf '%s\n' "$output" | grep -qF 'must be pairwise distinct'; then
      echo "  OK: Lite rejects aliased $left and $right"
    else
      echo "  FAIL: Lite alias rejection for $left and $right is not actionable" >&2
      printf '%s\n' "$output" >&2
      exit 1
    fi
  done
done

for invalid_gateway_secret in short " leading-space-gateway-identity-secret-1234567890" \
  "$(printf 'x%.0s' {1..513})"; do
  output=""
  if output="$(env -i PATH="$PATH" GATEWAY_IDENTITY_SECRET="$invalid_gateway_secret" \
    /bin/bash "$ENTRYPOINT" true 2>&1)"; then
    echo "  FAIL: Lite accepted an invalid gateway identity proof" >&2
    exit 1
  fi
  if printf '%s\n' "$output" | grep -qF '32..512 unpadded printable-ASCII'; then
    echo "  OK: Lite rejects an invalid gateway identity proof"
  else
    echo "  FAIL: Lite gateway identity rejection is not actionable" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
done

ADMIN_BLOCK="$(sed -n '/^\[program:admin-api\]/,/^\[program:runtime\]/p' "$SUPERVISOR")"
RUNTIME_BLOCK="$(sed -n '/^\[program:runtime\]/,/^\[program:agent-api\]/p' "$SUPERVISOR")"
AGENT_BLOCK="$(sed -n '/^\[program:agent-api\]/,/^\[program:meeting-api\]/p' "$SUPERVISOR")"
MEETING_BLOCK="$(sed -n '/^\[program:meeting-api\]/,/^\[program:gateway\]/p' "$SUPERVISOR")"
GATEWAY_BLOCK="$(sed -n '/^\[program:gateway\]/,/^\[program:terminal\]/p' "$SUPERVISOR")"
TERMINAL_BLOCK="$(sed -n '/^\[program:terminal\]/,/^\[group:vexa\]/p' "$SUPERVISOR")"

if printf '%s\n' "$ADMIN_BLOCK" | grep -qF 'export ADMIN_API_TOKEN="$(cat /run/vexa/admin-api-token)"' \
  && printf '%s\n' "$TERMINAL_BLOCK" | grep -qF 'export VEXA_ADMIN_API_KEY="$(cat /run/vexa/admin-api-token)"'; then
  echo "  OK: admin-api and Terminal receive the admin credential at exec"
else
  echo "  FAIL: admin credential consumers do not reference the configured token" >&2
  exit 1
fi

if printf '%s\n' "$GATEWAY_BLOCK" | grep -qF 'GATEWAY_IDENTITY_SECRET="$(cat /run/vexa/gateway-identity-secret)"' \
  && printf '%s\n' "$AGENT_BLOCK" | grep -qF 'GATEWAY_IDENTITY_SECRET="$(cat /run/vexa/gateway-identity-secret)"' \
  && printf '%s\n' "$AGENT_BLOCK" | grep -qF 'GATEWAY_IDENTITY_PREVIOUS_SECRET="$(cat /run/vexa/gateway-identity-previous-secret)"' \
  && ! printf '%s\n' "$GATEWAY_BLOCK" | grep -qF 'GATEWAY_IDENTITY_PREVIOUS_SECRET'; then
  echo "  OK: only Agent configuration exports the previous verifier; Gateway uses current only"
else
  echo "  FAIL: gateway identity proof is missing from a required owner" >&2
  exit 1
fi
for block in "$ADMIN_BLOCK" "$RUNTIME_BLOCK" "$MEETING_BLOCK" "$GATEWAY_BLOCK" "$TERMINAL_BLOCK"; do
  if printf '%s\n' "$block" | grep -Eq 'GATEWAY_IDENTITY_PREVIOUS_SECRET|gateway-identity-previous-secret'; then
    echo "  FAIL: a non-Agent exec wrapper exports the previous Gateway verifier" >&2
    exit 1
  fi
done
for block in "$ADMIN_BLOCK" "$RUNTIME_BLOCK" "$MEETING_BLOCK" "$TERMINAL_BLOCK"; do
  if printf '%s\n' "$block" | grep -Eq 'GATEWAY_IDENTITY_SECRET|/run/vexa/gateway-identity-secret'; then
    echo "  FAIL: non-owner process receives the gateway identity proof" >&2
    exit 1
  fi
done
for block in "$RUNTIME_BLOCK" "$AGENT_BLOCK" "$MEETING_BLOCK" "$GATEWAY_BLOCK"; do
  if printf '%s\n' "$block" | grep -Eq 'ADMIN_API_TOKEN|ADMIN_TOKEN|/run/vexa/admin-api-token'; then
    echo "  FAIL: non-owner process receives the admin credential" >&2
    exit 1
  fi
done

if printf '%s\n' "$RUNTIME_BLOCK" | grep -qF 'RUNTIME_CALLBACK_SECRET="$(cat /run/vexa/runtime-callback-secret)"' \
  && printf '%s\n' "$RUNTIME_BLOCK" | grep -qF 'RUNTIME_CONTROL_SECRET="$(cat /run/vexa/runtime-control-secret)"' \
  && printf '%s\n' "$RUNTIME_BLOCK" | grep -qF 'RUNTIME_CALLBACK_TRUSTED_ORIGINS="http://localhost:8080"' \
  && ! printf '%s\n' "$RUNTIME_BLOCK" | grep -Eq 'INTERNAL_API_SECRET|/run/vexa/internal-api-secret'; then
  echo "  OK: runtime receives its callback secret at exec and trusts only meeting-api"
else
  echo "  FAIL: runtime callback authentication/trust-boundary wiring is incomplete" >&2
  exit 1
fi

if printf '%s\n' "$AGENT_BLOCK" | grep -qF 'VEXA_RUNTIME_CONTROL_SECRET="$(cat /run/vexa/runtime-control-secret)"' \
  && printf '%s\n' "$MEETING_BLOCK" | grep -qF 'RUNTIME_CONTROL_SECRET="$(cat /run/vexa/runtime-control-secret)"'; then
  echo "  OK: only the Agent and Meeting controllers receive runtime authority"
else
  echo "  FAIL: runtime controller credential is missing from an authorized caller" >&2
  exit 1
fi
for block in "$ADMIN_BLOCK" "$GATEWAY_BLOCK" "$TERMINAL_BLOCK"; do
  if printf '%s\n' "$block" | grep -Eq 'RUNTIME_CONTROL_SECRET|/run/vexa/runtime-control-secret'; then
    echo "  FAIL: non-controller process receives runtime authority" >&2
    exit 1
  fi
done

if printf '%s\n' "$ADMIN_BLOCK" | grep -qF 'INTERNAL_API_SECRET="$(cat /run/vexa/internal-api-secret)"' \
  && printf '%s\n' "$AGENT_BLOCK" | grep -qF 'VEXA_INTERNAL_API_SECRET="$(cat /run/vexa/internal-api-secret)"' \
  && printf '%s\n' "$MEETING_BLOCK" | grep -qF 'INTERNAL_API_SECRET="$(cat /run/vexa/internal-api-secret)"' \
  && printf '%s\n' "$GATEWAY_BLOCK" | grep -qF 'INTERNAL_API_SECRET="$(cat /run/vexa/internal-api-secret)"' \
  && printf '%s\n' "$TERMINAL_BLOCK" | grep -qF 'VEXA_INTERNAL_API_SECRET="$(cat /run/vexa/internal-api-secret)"'; then
  echo "  OK: platform-internal credential is exec-projected only to its service-edge owners"
else
  echo "  FAIL: platform-internal credential projection is incomplete" >&2
  exit 1
fi

for block in "$ADMIN_BLOCK" "$AGENT_BLOCK" "$GATEWAY_BLOCK" "$TERMINAL_BLOCK"; do
  if printf '%s\n' "$block" | grep -Eq 'RUNTIME_CALLBACK_SECRET|/run/vexa/runtime-callback-secret'; then
    echo "  FAIL: non-callback consumer receives the runtime callback credential" >&2
    exit 1
  fi
done
if printf '%s\n' "$MEETING_BLOCK" | grep -qF 'RUNTIME_CALLBACK_SECRET="$(cat /run/vexa/runtime-callback-secret)"'; then
  echo "  OK: meeting-api reads the callback verifier at exec"
else
  echo "  FAIL: meeting-api does not receive the callback verifier" >&2
  exit 1
fi

if printf '%s\n' "$MEETING_BLOCK" | grep -qF 'MEETING_TOKEN_SECRET="$(cat /run/vexa/meeting-token-secret)"' \
  && ! printf '%s\n' "$MEETING_BLOCK" | grep -Eq 'ADMIN_TOKEN|ADMIN_API_TOKEN'; then
  echo "  OK: meeting-api receives only its dedicated MeetingToken signer"
else
  echo "  FAIL: meeting-api MeetingToken/admin configuration projection is incomplete" >&2
  exit 1
fi
for block in "$ADMIN_BLOCK" "$RUNTIME_BLOCK" "$AGENT_BLOCK" "$GATEWAY_BLOCK" "$TERMINAL_BLOCK"; do
  if printf '%s\n' "$block" | grep -Eq 'MEETING_TOKEN_SECRET|/run/vexa/meeting-token-secret'; then
    echo "  FAIL: non-owner process receives the MeetingToken signer" >&2
    exit 1
  fi
done

if printf '%s\n' "$MEETING_BLOCK" | grep -qF 'ADMIN_API_URL="http://localhost:8001"' \
  && printf '%s\n' "$MEETING_BLOCK" | grep -qF 'AGENT_API_URL="http://localhost:8100"'; then
  echo "  OK: meeting-api uses the existing local admin + agent services"
else
  echo "  FAIL: meeting-api local service edges are incomplete" >&2
  exit 1
fi

echo "minutes-default-off-contract PASS"
