#!/bin/sh
# Runs as the final command of entrypoint.sh inside the permission-test container. At this point the
# supervisor parent environment and root-only projection files are exactly what the real image
# would hand off. Root reads below verify values and modes, not isolation from peer root services.
set -eu

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

[ "${BOT_COMMAND:-}" = "/usr/local/bin/vexa-bot-launch" ] || fail "ordinary bot launcher is unavailable"
for key in ADMIN_API_TOKEN ADMIN_TOKEN INTERNAL_API_SECRET RUNTIME_CONTROL_SECRET \
  VEXA_INTERNAL_API_SECRET VEXA_RUNTIME_CONTROL_SECRET \
  RUNTIME_CALLBACK_SECRET MEETING_TOKEN_SECRET GATEWAY_IDENTITY_SECRET; do
  if env | grep -q "^${key}="; then
    fail "supervisor parent retained ${key}"
  fi
done
if env | grep -q '^GATEWAY_IDENTITY_PREVIOUS_SECRET='; then
  fail "supervisor parent retained GATEWAY_IDENTITY_PREVIOUS_SECRET"
fi

[ "$(stat -c '%a' /run/vexa)" = "700" ] || fail "/run/vexa is not mode 0700"
for path in /run/vexa/admin-api-token /run/vexa/internal-api-secret \
  /run/vexa/runtime-control-secret /run/vexa/runtime-callback-secret \
  /run/vexa/meeting-token-secret /run/vexa/gateway-identity-secret; do
  [ "$(stat -c '%a' "$path")" = "600" ] || fail "$path is not mode 0600"
  [ "$(stat -c '%u' "$path")" = "0" ] || fail "$path is not root-owned"
done
[ "$(stat -c '%a' /run/vexa/gateway-identity-previous-secret)" = "600" ] \
  || fail "/run/vexa/gateway-identity-previous-secret is not mode 0600"

[ "$(cat /run/vexa/admin-api-token)" = "operator-admin" ] || fail "admin token file changed value"
[ "$(cat /run/vexa/internal-api-secret)" = "operator-internal" ] \
  || fail "internal secret file changed value"
[ "$(cat /run/vexa/runtime-control-secret)" = "operator-control" ] \
  || fail "runtime-control secret file changed value"
[ "$(cat /run/vexa/runtime-callback-secret)" = "operator-callback" ] \
  || fail "callback secret file changed value"
[ "$(cat /run/vexa/meeting-token-secret)" = "operator-meeting-token" ] \
  || fail "MeetingToken secret file changed value"
[ "$(cat /run/vexa/gateway-identity-secret)" = "operator-gateway-identity-secret-v1" ] \
  || fail "gateway identity secret file changed value"
[ "$(cat /run/vexa/gateway-identity-previous-secret)" = "operator-gateway-identity-secret-v0" ] \
  || fail "previous gateway verifier file changed value"

echo "lite-entrypoint secret projection probe PASS"
