#!/usr/bin/env bash
# Executable Linux permission proof for the Lite launcher without building the full all-in-one
# image. The cached/pullable slim Node image supplies only Linux users, setpriv, and Node; this test
# creates the same uid/group/filesystem boundary and then lets the real launcher execute the probe.
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
LAUNCHER="$HERE/bin/vexa-bot-launch"
PROBE="$HERE/tests/fixtures/bot-boundary-probe"
IMAGE="${LITE_BOT_PERMISSION_TEST_IMAGE:-node:24-bookworm-slim}"

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "bot-privilege-permission-test SKIP: Docker is unavailable"
  exit 0
fi

docker run --rm \
  --mount "type=bind,src=$LAUNCHER,dst=/usr/local/bin/vexa-bot-launch,readonly" \
  --mount "type=bind,src=$PROBE,dst=/app/core/meetings/services/bot/dist,readonly" \
  --entrypoint sh "$IMAGE" -ec '
    getent group pulse-access >/dev/null || groupadd --system pulse-access
    groupadd --system vexa-bot
    useradd --system --gid vexa-bot --groups pulse-access \
      --home-dir /var/lib/vexa-bot --shell /usr/sbin/nologin vexa-bot
    install -d -o vexa-bot -g vexa-bot -m 0700 /var/lib/vexa-bot
    install -d -m 0700 /run/vexa
    printf "%s" "callback-only" > /run/vexa/runtime-callback-secret
    printf "%s" "meeting-only" > /run/vexa/meeting-token-secret
    printf "%s" "admin-only" > /run/vexa/admin-api-token
    printf "%s" "internal-only" > /run/vexa/internal-api-secret
    printf "%s" "control-only" > /run/vexa/runtime-control-secret
    chmod 0600 /run/vexa/runtime-callback-secret /run/vexa/meeting-token-secret \
      /run/vexa/admin-api-token /run/vexa/internal-api-secret /run/vexa/runtime-control-secret
    setpriv --reuid=100123 --regid=100123 --clear-groups --no-new-privs \
      --bounding-set=-all --inh-caps=-all --ambient-caps=-all -- \
      sh -ec "! test -x /run/vexa; ! test -r /run/vexa/runtime-callback-secret; \
        ! test -r /run/vexa/meeting-token-secret; ! test -r /run/vexa/admin-api-token; \
        ! test -r /run/vexa/internal-api-secret; ! test -r /run/vexa/runtime-control-secret"
    install -d -o root -g pulse-access -m 0750 /var/run/pulse
    : > /var/run/pulse/native
    chown root:pulse-access /var/run/pulse/native
    chmod 0660 /var/run/pulse/native
    DISPLAY=:99 exec /usr/local/bin/vexa-bot-launch
  '

# Two concurrent bots must have different effective uids, making each other's token-bearing
# environment unreadable through procfs even though they share one Lite container.
docker run --rm \
  --mount "type=bind,src=$LAUNCHER,dst=/usr/local/bin/vexa-bot-launch,readonly" \
  --mount "type=bind,src=$PROBE,dst=/app/core/meetings/services/bot/dist,readonly" \
  --entrypoint sh "$IMAGE" -ec '
    getent group pulse-access >/dev/null || groupadd --system pulse-access
    groupadd --system vexa-bot
    useradd --system --gid vexa-bot --groups pulse-access \
      --home-dir /var/lib/vexa-bot --shell /usr/sbin/nologin vexa-bot
    install -d -o root -g root -m 0711 /var/lib/vexa-bot
    install -d -m 0700 /run/vexa
    printf "%s" "callback-only" > /run/vexa/runtime-callback-secret
    printf "%s" "meeting-only" > /run/vexa/meeting-token-secret
    printf "%s" "admin-only" > /run/vexa/admin-api-token
    printf "%s" "internal-only" > /run/vexa/internal-api-secret
    printf "%s" "control-only" > /run/vexa/runtime-control-secret
    chmod 0600 /run/vexa/runtime-callback-secret /run/vexa/meeting-token-secret \
      /run/vexa/admin-api-token /run/vexa/internal-api-secret /run/vexa/runtime-control-secret
    install -d -o root -g pulse-access -m 0750 /var/run/pulse
    : > /var/run/pulse/native
    chown root:pulse-access /var/run/pulse/native
    chmod 0660 /var/run/pulse/native
    rm -f /tmp/vexa-bot-victim-pid /tmp/vexa-bot-sibling-result
    BOT_BOUNDARY_MODE=victim VEXA_BOT_CONFIG=victim-meeting-token \
      /usr/local/bin/vexa-bot-launch & victim=$!
    BOT_BOUNDARY_MODE=attacker VEXA_BOT_CONFIG=attacker-meeting-token \
      /usr/local/bin/vexa-bot-launch & attacker=$!
    wait "$victim"
    wait "$attacker"
  '

# An arbitrary pre-dropped service identity must be rejected rather than treated as a bot identity.
if docker run --rm --user node \
  --mount "type=bind,src=$LAUNCHER,dst=/usr/local/bin/vexa-bot-launch,readonly" \
  --mount "type=bind,src=$PROBE,dst=/app/core/meetings/services/bot/dist,readonly" \
  --entrypoint /usr/local/bin/vexa-bot-launch "$IMAGE" >/dev/null 2>&1; then
  echo "FAIL: launcher accepted an unexpected non-root user" >&2
  exit 1
fi

echo "bot-privilege-permission-test PASS"
