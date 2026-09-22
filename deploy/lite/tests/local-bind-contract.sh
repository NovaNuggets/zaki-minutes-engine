#!/usr/bin/env bash
# Static release contract: zero-login Lite is reachable only through loopback-published ports, and
# the Terminal receives the exact host-side bind so its startup policy can reject unsafe overrides.
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
MAKEFILE="$HERE/Makefile"
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

need "$MAKEFILE" 'HOST_BIND          ?= 127.0.0.1' 'Lite host publication defaults to loopback'
need "$MAKEFILE" '-p $(HOST_BIND):$(HOST_GATEWAY_PORT):8056' 'gateway publication uses HOST_BIND'
need "$MAKEFILE" '-p $(HOST_BIND):$(HOST_TERMINAL_PORT):3001' 'Terminal publication uses HOST_BIND'
need "$MAKEFILE" '-p $(HOST_BIND):$(HOST_AGENT_PORT):8100' 'agent publication uses HOST_BIND'
need "$MAKEFILE" '-e VEXA_TERMINAL_HOST_BIND=$(HOST_BIND)' 'Terminal receives the actual publication bind'
need "$MAKEFILE" '-e TERMINAL_PUBLIC_URL=$(TERMINAL_PUBLIC_URL)' 'Terminal receives the operator-selected public origin'
need "$SUPERVISOR" 'VEXA_REQUIRE_GATEWAY_IDENTITY="1"' 'agent-api requires gateway-proven identity'
need "$SUPERVISOR" 'VEXA_TERMINAL_HOST_BIND="%(ENV_VEXA_TERMINAL_HOST_BIND)s"' 'supervisor preserves bind attestation'

echo "local-bind-contract PASS"
