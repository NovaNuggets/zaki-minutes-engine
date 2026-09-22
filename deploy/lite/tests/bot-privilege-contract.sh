#!/usr/bin/env bash
# Static release contract: the trusted Lite launcher must cross a real kernel identity boundary
# before the meeting-bot's Node process starts. Environment scrubbing alone is insufficient in the
# one-container profile because a root workload could still read operator-only files in /run/vexa.
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
DOCKERFILE="$HERE/Dockerfile.lite"
LAUNCHER="$HERE/bin/vexa-bot-launch"
ENTRYPOINT="$HERE/entrypoint.sh"
SUPERVISOR="$HERE/supervisord.conf"

need() {
  local file="$1" pattern="$2" label="$3"
  if grep -qF -- "$pattern" "$file"; then
    echo "  OK: $label"
  else
    echo "  FAIL: $label" >&2
    exit 1
  fi
}

need "$DOCKERFILE" 'util-linux' 'the image explicitly installs setpriv'
need "$DOCKERFILE" 'groupadd --system vexa-bot' 'the image creates a dedicated bot group'
need "$DOCKERFILE" 'useradd --system --gid vexa-bot --groups pulse-access' 'the bot user receives only its primary and PulseAudio groups'
need "$DOCKERFILE" 'install -d -o root -g root -m 0711 /var/lib/vexa-bot' 'per-meeting homes live below a non-listable root parent'
need "$DOCKERFILE" '/run/vexa/.bot-deny-probe' 'the image build verifies the root-only file boundary as the bot user'

need "$LAUNCHER" '#!/bin/sh' 'the root launcher avoids Bash startup hooks'
need "$LAUNCHER" 'readonly TRUSTED_PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"' 'the root launcher replaces any workload-controlled PATH'
need "$LAUNCHER" 'unset BASH_ENV ENV CDPATH LD_PRELOAD LD_LIBRARY_PATH' 'the root launcher removes shell and loader injection variables before exec'
need "$LAUNCHER" 'bot_uid=$((BOT_UID_BASE + $$))' 'each live meeting receives a distinct uid'
need "$LAUNCHER" 'rm -rf "$bot_home" "$bot_runtime"' 'pid-reuse cannot expose an earlier meeting home'
need "$LAUNCHER" 'setpriv --reuid="$bot_uid" --regid="$BOT_GROUP" --groups=pulse-access' 'the trusted launcher drops into the per-meeting uid before Node starts'
need "$LAUNCHER" '--no-new-privs' 'the bot cannot regain privilege through exec'
need "$LAUNCHER" '--bounding-set=-all' 'the bot capability bounding set is empty'
need "$LAUNCHER" '--inh-caps=-all' 'the bot inheritable capability set is empty'
need "$LAUNCHER" '--ambient-caps=-all' 'the bot ambient capability set is empty'
need "$LAUNCHER" 'id -u)" != "$VEXA_LITE_BOT_UID"' 'the launcher refuses an unexpected non-root identity'
need "$LAUNCHER" '[ -x /run/vexa ] || [ -r /run/vexa ]' 'the dropped process fails closed if operator files become searchable'
need "$LAUNCHER" 'exec node dist/index.js' 'Node starts only after the launcher boundary checks'

need "$ENTRYPOINT" 'install -d -m 700 /run/vexa' 'operator secret files stay below a root-only directory'
need "$ENTRYPOINT" 'install -d -o root -g root -m 0755 /workspaces' 'the workspace root cannot be renamed by a bot'
if grep -qE 'chmod[[:space:]]+777[[:space:]]+/workspaces' "$ENTRYPOINT"; then
  echo "  FAIL: entrypoint reopens the workspace root to untrusted writers" >&2
  exit 1
fi
need "$SUPERVISOR" 'Xvfb :99 -screen 0 1920x1080x24 -ac' 'the unprivileged bot can use the shared X display without Xauthority'

echo "bot-privilege-contract PASS"
