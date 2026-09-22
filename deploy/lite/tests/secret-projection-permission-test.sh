#!/usr/bin/env bash
# Executable success-path proof for the Lite entrypoint: the non-Minutes profile keeps ordinary bot
# support, and credentials leave the ambient supervisor/child-process environment. Lite remains one
# trusted OS process domain; this test intentionally does not claim root-peer file isolation.
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
ENTRYPOINT="$HERE/entrypoint.sh"
PROBE="$HERE/tests/fixtures/lite-entrypoint-probe.sh"
IMAGE="${LITE_BOT_PERMISSION_TEST_IMAGE:-node:24-bookworm-slim}"

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "secret-projection-permission-test SKIP: Docker is unavailable"
  exit 0
fi

# The identity-admin token is a fifth trust domain: a separately named MeetingToken/callback/control
# credential is not dedicated if an operator silently gives it the same value.
for other in RUNTIME_CALLBACK_SECRET MEETING_TOKEN_SECRET RUNTIME_CONTROL_SECRET INTERNAL_API_SECRET \
  GATEWAY_IDENTITY_SECRET; do
  output=""
  if output="$(docker run --rm \
    -e ADMIN_API_TOKEN=aliased-admin-gateway-proof-1234567890 \
    -e "$other=aliased-admin-gateway-proof-1234567890" \
    --mount "type=bind,src=$ENTRYPOINT,dst=/entrypoint.sh,readonly" \
    --entrypoint /bin/bash "$IMAGE" -ec '
      ln -sf /bin/true /usr/local/bin/pg_isready
      exec /entrypoint.sh /bin/true
    ' 2>&1)"; then
    echo "FAIL: Lite accepted aliased ADMIN_API_TOKEN and $other" >&2
    exit 1
  fi
  if ! printf '%s\n' "$output" | grep -qF 'must be pairwise distinct'; then
    echo "FAIL: Lite alias rejection for ADMIN_API_TOKEN and $other is not actionable" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
done

success_output="$(docker run --rm \
  -e ADMIN_API_TOKEN=operator-admin \
  -e ADMIN_TOKEN=legacy-admin \
  -e INTERNAL_API_SECRET=operator-internal \
  -e RUNTIME_CONTROL_SECRET=operator-control \
  -e RUNTIME_CALLBACK_SECRET=operator-callback \
  -e MEETING_TOKEN_SECRET=operator-meeting-token \
  -e GATEWAY_IDENTITY_SECRET=operator-gateway-identity-secret-v1 \
  -e GATEWAY_IDENTITY_PREVIOUS_SECRET=operator-gateway-identity-secret-v0 \
  -e REDIS_URL=redis://redis-user:super@secret@redis.example:6379/0 \
  --mount "type=bind,src=$ENTRYPOINT,dst=/entrypoint.sh,readonly" \
  --mount "type=bind,src=$PROBE,dst=/lite-entrypoint-probe.sh,readonly" \
  --entrypoint /bin/bash "$IMAGE" -ec '
    ln -sf /bin/true /usr/local/bin/pg_isready
    exec /entrypoint.sh /bin/sh /lite-entrypoint-probe.sh
  ')"
printf '%s\n' "$success_output"
if printf '%s\n' "$success_output" | grep -qF 'super@secret'; then
  echo "FAIL: Lite leaked a Redis password suffix through startup logging" >&2
  exit 1
fi
if ! printf '%s\n' "$success_output" | grep -qF 'Redis URL:        redis://***@redis.example:6379/0'; then
  echo "FAIL: Lite did not preserve a useful credential-free Redis endpoint diagnostic" >&2
  exit 1
fi

echo "secret-projection-permission-test PASS"
