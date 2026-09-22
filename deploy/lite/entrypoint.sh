#!/bin/bash
# =============================================================================
# Vexa Lite (v0.12) — container entrypoint
# =============================================================================
# 1. Normalizes the runtime env (every var supervisord references via %(ENV_X)s MUST exist,
#    or supervisord refuses to start that program — so we default them all here).
# 2. Derives DATABASE_URL + REDIS_URL from parts (or parses a supplied URL into parts).
# 3. Waits for the (external) PostgreSQL — schema convergence runs in-process on each
#    service's startup (admin-api/meeting-api ensure_schema()).
# 4. Hands off to supervisord, which brings up the whole control plane.
# =============================================================================
set -e

echo "=============================================="
echo "  Vexa Lite (v0.12) — starting container"
echo "=============================================="

# Lite is deliberately outside the Minutes launch topology. Reject every Minutes-facing setting,
# including false-valued feature flags and historical erasure keys: accepting dormant material here
# makes this topology look like a supported partial Minutes deployment.
while IFS= read -r minutes_config_name; do
    case "$minutes_config_name" in
        ZAKI_MINUTES_*|ZAKI_READ_TOKEN_MINUTES*|ZAKI_AGENT_ERASURE_*|MINUTES_*)
            echo "ERROR: Vexa Lite is a local/reference topology and does not accept Minutes configuration ${minutes_config_name}; use the managed Helm profile with bundled Agent/Gateway/Terminal off and external Nullalis." >&2
            exit 64
            ;;
    esac
done < <(compgen -e)
unset minutes_config_name

# ─── Redis (internal by default; an external REDIS_URL is honored) ────────────────────────────────
if [ -z "${REDIS_URL:-}" ]; then
    export REDIS_HOST="${REDIS_HOST:-localhost}"
    export REDIS_PORT="${REDIS_PORT:-6379}"
    export REDIS_URL="redis://${REDIS_HOST}:${REDIS_PORT}/0"
fi
export REDIS_MAXMEMORY="${REDIS_MAXMEMORY:-512mb}"
if [[ ! "$REDIS_MAXMEMORY" =~ ^[1-9][0-9]*(b|kb|mb|gb)$ ]]; then
    echo "ERROR: REDIS_MAXMEMORY must be a positive lowercase b/kb/mb/gb quantity." >&2
    exit 64
fi

# ─── Database — DB_* only. Each service builds its own async URL (postgresql+asyncpg://) from these
#     (admin_api/_database_url, meeting_api/_database_url). We deliberately do NOT export DATABASE_URL:
#     a plain `postgresql://` would force SQLAlchemy onto the psycopg2 (sync) driver, which lite does
#     not install (asyncpg only). For an external managed DB, set DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD.
export DB_HOST="${DB_HOST:-localhost}"
export DB_PORT="${DB_PORT:-5432}"
export DB_NAME="${DB_NAME:-vexa}"
export DB_USER="${DB_USER:-postgres}"
export DB_PASSWORD="${DB_PASSWORD:-postgres}"

# ─── Defaults for every var supervisord interpolates (empty is fine; must be SET) ─────────────────
export LOG_LEVEL="${LOG_LEVEL:-info}"
export DISPLAY="${DISPLAY:-:99}"
export ADMIN_API_TOKEN="${ADMIN_API_TOKEN:-${ADMIN_TOKEN:-changeme}}"
export INTERNAL_API_SECRET="${INTERNAL_API_SECRET:-lite-internal-secret}"
export RUNTIME_CONTROL_SECRET="${RUNTIME_CONTROL_SECRET:-lite-runtime-control-secret}"
export RUNTIME_CALLBACK_SECRET="${RUNTIME_CALLBACK_SECRET:-lite-runtime-callback-secret}"
export MEETING_TOKEN_SECRET="${MEETING_TOKEN_SECRET:-lite-meeting-token-secret}"
export GATEWAY_IDENTITY_SECRET="${GATEWAY_IDENTITY_SECRET:-lite-gateway-identity-secret-local-v1}"
export GATEWAY_IDENTITY_PREVIOUS_SECRET="${GATEWAY_IDENTITY_PREVIOUS_SECRET:-}"
# Agent config.v1 uses the VEXA_* spellings. Normalize them from the canonical Lite operator keys
# for contract discovery, then drop both aliases from the supervisor parent after file projection.
export VEXA_INTERNAL_API_SECRET="$INTERNAL_API_SECRET"
export VEXA_RUNTIME_CONTROL_SECRET="$RUNTIME_CONTROL_SECRET"

# These credentials authenticate five different trust edges. Reject every alias before materializing
# them so a local default cannot hide a deployment that collapses identity admin, callback, token,
# controller, or platform-internal authority into one bearer.
gateway_identity_pattern='^[!-~][ -~]*[!-~]$'
gateway_identity_length="${#GATEWAY_IDENTITY_SECRET}"
if [ "$gateway_identity_length" -lt 32 ] || [ "$gateway_identity_length" -gt 512 ] \
    || ! (export LC_ALL=C; [[ "$GATEWAY_IDENTITY_SECRET" =~ $gateway_identity_pattern ]]); then
    echo "ERROR: GATEWAY_IDENTITY_SECRET must contain 32..512 unpadded printable-ASCII characters." >&2
    exit 64
fi
if [ -n "$GATEWAY_IDENTITY_PREVIOUS_SECRET" ]; then
    gateway_identity_previous_length="${#GATEWAY_IDENTITY_PREVIOUS_SECRET}"
    if [ "$gateway_identity_previous_length" -lt 32 ] \
        || [ "$gateway_identity_previous_length" -gt 512 ] \
        || ! (export LC_ALL=C; [[ "$GATEWAY_IDENTITY_PREVIOUS_SECRET" =~ $gateway_identity_pattern ]]); then
        echo "ERROR: GATEWAY_IDENTITY_PREVIOUS_SECRET must contain 32..512 unpadded printable-ASCII characters when set." >&2
        exit 64
    fi
fi
secret_names=(ADMIN_API_TOKEN RUNTIME_CALLBACK_SECRET MEETING_TOKEN_SECRET RUNTIME_CONTROL_SECRET INTERNAL_API_SECRET GATEWAY_IDENTITY_SECRET)
if [ -n "$GATEWAY_IDENTITY_PREVIOUS_SECRET" ]; then
    secret_names+=(GATEWAY_IDENTITY_PREVIOUS_SECRET)
fi
for ((i = 0; i < ${#secret_names[@]}; i++)); do
    for ((j = i + 1; j < ${#secret_names[@]}; j++)); do
        left="${secret_names[$i]}"
        right="${secret_names[$j]}"
        if [ "${!left}" = "${!right}" ]; then
            echo "ERROR: all current trust-domain credentials and the optional previous Gateway verifier must be pairwise distinct (${left} aliases ${right})." >&2
            exit 64
        fi
    done
done
unset secret_names i j left right gateway_identity_pattern gateway_identity_length gateway_identity_previous_length

# Lite is one container and one trusted OS process domain: these root-only files remove credentials
# from supervisord's ambient environment, but do not isolate mutually hostile root services. Each
# exec wrapper references only its configured credentials; use Compose/Helm for a kernel boundary.
install -d -m 700 /run/vexa
printf '%s' "$ADMIN_API_TOKEN" > /run/vexa/admin-api-token
printf '%s' "$INTERNAL_API_SECRET" > /run/vexa/internal-api-secret
printf '%s' "$RUNTIME_CONTROL_SECRET" > /run/vexa/runtime-control-secret
printf '%s' "$RUNTIME_CALLBACK_SECRET" > /run/vexa/runtime-callback-secret
printf '%s' "$MEETING_TOKEN_SECRET" > /run/vexa/meeting-token-secret
printf '%s' "$GATEWAY_IDENTITY_SECRET" > /run/vexa/gateway-identity-secret
printf '%s' "$GATEWAY_IDENTITY_PREVIOUS_SECRET" > /run/vexa/gateway-identity-previous-secret
chmod 600 /run/vexa/admin-api-token \
    /run/vexa/internal-api-secret \
    /run/vexa/runtime-control-secret \
    /run/vexa/runtime-callback-secret \
    /run/vexa/meeting-token-secret \
    /run/vexa/gateway-identity-secret \
    /run/vexa/gateway-identity-previous-secret
unset ADMIN_API_TOKEN
unset ADMIN_TOKEN
unset INTERNAL_API_SECRET
unset RUNTIME_CONTROL_SECRET
unset VEXA_INTERNAL_API_SECRET
unset VEXA_RUNTIME_CONTROL_SECRET
unset RUNTIME_CALLBACK_SECRET
unset MEETING_TOKEN_SECRET
unset GATEWAY_IDENTITY_SECRET
unset GATEWAY_IDENTITY_PREVIOUS_SECRET

export TRANSCRIPTION_SERVICE_URL="${TRANSCRIPTION_SERVICE_URL:-}"
export TRANSCRIPTION_SERVICE_TOKEN="${TRANSCRIPTION_SERVICE_TOKEN:-}"

export MINIO_ENDPOINT="${MINIO_ENDPOINT:-}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-}"
export MINIO_BUCKET="${MINIO_BUCKET:-vexa}"
export MINIO_SECURE="${MINIO_SECURE:-false}"

# Gateway edge guard (fastapi-guard): ON by default with generous limits (owner ruling).
# Opt out with -e GUARD_ENABLED=false on the container. Other GUARD_* tuning keys
# (GUARD_RATE_LIMIT_RPM, GUARD_TRUSTED_PROXIES, …) flow through container env untouched.
export GUARD_ENABLED="${GUARD_ENABLED:-true}"
export GUARD_WS_ENABLED="${GUARD_WS_ENABLED:-false}"

# Process-backend launchers — DEFAULTS ONLY: an operator-provided BOT_COMMAND /
# AGENT_WORKER_COMMAND on the container env wins. supervisord interpolates these into the
# runtime program via %(ENV_…)s — never hardcode them there (that clobbers operator env).
export BOT_COMMAND="${BOT_COMMAND:-/usr/local/bin/vexa-bot-launch}"
export AGENT_WORKER_COMMAND="${AGENT_WORKER_COMMAND:-/usr/local/bin/vexa-agent-worker}"

# Agent control plane + worker (BYO inference; credentials brokered by the runtime).
export VEXA_AGENT_DEFAULT_SUBJECT="${VEXA_AGENT_DEFAULT_SUBJECT:-}"
export VEXA_DISPATCH_SIGNING_KEY="${VEXA_DISPATCH_SIGNING_KEY:-dev-dispatch-signing-key}"
export VEXA_BOT_API_KEY="${VEXA_BOT_API_KEY:-}"
export VEXA_AGENT_MODEL="${VEXA_AGENT_MODEL:-}"
export VEXA_MEETING_MODEL="${VEXA_MEETING_MODEL:-}"
# HOST_CLAUDE_CREDENTIALS (config.v1 `model_inference`): path of a claude credentials JSON as seen
# INSIDE this lite container (mount it in, e.g. -v ~/.claude/.credentials.json:/claude-creds.json:ro
# and set HOST_CLAUDE_CREDENTIALS=/claude-creds.json). Lite's runtime uses the process backend, so
# the worker reads the file directly; the runtime's config.v1 file probe verifies it on /health.
# Alternative: leave empty and set ANTHROPIC_API_KEY / ANTHROPIC_AUTH_TOKEN instead.
export HOST_CLAUDE_CREDENTIALS="${HOST_CLAUDE_CREDENTIALS:-}"
export CLAUDE_CODE_OAUTH_TOKEN="${CLAUDE_CODE_OAUTH_TOKEN:-}"
export ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}"
export ANTHROPIC_AUTH_TOKEN="${ANTHROPIC_AUTH_TOKEN:-}"
export ANTHROPIC_BASE_URL="${ANTHROPIC_BASE_URL:-}"
export ANTHROPIC_MODEL="${ANTHROPIC_MODEL:-}"
export ANTHROPIC_DEFAULT_OPUS_MODEL="${ANTHROPIC_DEFAULT_OPUS_MODEL:-}"
export ANTHROPIC_DEFAULT_SONNET_MODEL="${ANTHROPIC_DEFAULT_SONNET_MODEL:-}"
export ANTHROPIC_DEFAULT_HAIKU_MODEL="${ANTHROPIC_DEFAULT_HAIKU_MODEL:-}"

# Terminal (Next.js UI)
export VEXA_PUBLIC_API_URL="${VEXA_PUBLIC_API_URL:-http://localhost:8056}"
export VEXA_API_KEY="${VEXA_API_KEY:-}"
# Lite deliberately provisions one local self-host identity so its one-container quick start remains
# zero-login. Set false to require user cookies instead. A public/non-loopback TERMINAL_PUBLIC_URL
# makes the Terminal fail startup rather than exposing this shared identity.
export VEXA_TERMINAL_SHARED_KEY_MODE="${VEXA_TERMINAL_SHARED_KEY_MODE:-true}"
export TERMINAL_PUBLIC_URL="${TERMINAL_PUBLIC_URL:-http://localhost:3001}"
export VEXA_TERMINAL_HOST_BIND="${VEXA_TERMINAL_HOST_BIND:-}"
export NEXTAUTH_SECRET="${NEXTAUTH_SECRET:-vexa-lite-nextauth-secret}"
export JWT_SECRET="${JWT_SECRET:-vexa-lite-jwt-secret}"

# Workspace store for the agent (shared dir; the worker runs in-process, no volume bind). Only the
# root control plane may create/rename tenant roots; workers receive ownership below this boundary.
install -d -o root -g root -m 0755 /workspaces
mkdir -p /var/lib/redis /var/run/redis

# An external Redis URL may contain operator credentials. Keep startup diagnostics useful without
# copying userinfo into container logs, where it would outlive the process environment.
redis_log_url="$REDIS_URL"
case "$REDIS_URL" in
    # Strip through the last raw '@'. Even a malformed URL with extra delimiters must not leave
    # a password suffix in logs; a valid percent-encoded userinfo component is handled unchanged.
    *://*@*) redis_log_url="${REDIS_URL%%://*}://***@${REDIS_URL##*@}" ;;
esac

echo "Configuration:"
echo "  - Redis URL:        ${redis_log_url}"
echo "  - Database:         postgresql+asyncpg://${DB_USER}:***@${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo "  - Transcription:    ${TRANSCRIPTION_SERVICE_URL:-NOT SET (bots capture, no transcript)}"
echo "  - Object storage:   ${MINIO_ENDPOINT:-NOT SET (recordings disabled)}"
echo "  - Log level:        ${LOG_LEVEL}"
echo ""
unset redis_log_url

# ─── Wait for PostgreSQL (external) ───────────────────────────────────────────────────────────────
if [ -n "$DB_HOST" ]; then
    echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."
    for attempt in $(seq 1 30); do
        if pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -q 2>/dev/null; then
            echo "PostgreSQL is ready."
            break
        fi
        [ "$attempt" -eq 30 ] && echo "WARNING: PostgreSQL not reachable after 30 attempts; starting anyway."
        sleep 2
    done
    echo ""
fi

# Background: once admin-api is up, mint a self-host API key and hand it to Terminal (zero-login).
# No-op if VEXA_API_KEY was supplied. Only meaningful for the supervisord CMD (the real bring-up).
case "$*" in
    *supervisord*) /usr/local/bin/provision-key.sh & ;;
esac

echo "Starting services via supervisord..."
exec "$@"
