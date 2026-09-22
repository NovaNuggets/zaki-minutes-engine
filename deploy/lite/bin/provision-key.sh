#!/bin/bash
# =============================================================================
# Vexa Lite — self-host API key provisioner (zero-login)
# =============================================================================
# Runs in the background from the entrypoint. Once admin-api is up, mints scoped API tokens for a
# self-host user and writes them to /run/vexa/key.env, then restarts Terminal so its supervisord
# command sources that file. This is what makes Terminal work without an interactive login on a
# fresh Lite stack.
#
# No-op when VEXA_API_KEY was supplied by the operator (their key wins) — login mode otherwise.
set -u

if [ -n "${VEXA_API_KEY:-}" ]; then
    echo "[provision-key] VEXA_API_KEY provided by operator; skipping self-host mint"
    exit 0
fi

ADMIN="${ADMIN_API_TOKEN:-}"
if [ -z "$ADMIN" ] && [ -r /run/vexa/admin-api-token ]; then
    ADMIN="$(cat /run/vexa/admin-api-token)"
fi
if [ -z "$ADMIN" ]; then
    echo "[provision-key] ERROR: root-only ADMIN_API_TOKEN file is unavailable" >&2
    exit 1
fi
echo "[provision-key] waiting for admin-api..."
for _ in $(seq 1 60); do
    curl -sf -o /dev/null http://localhost:8001/health 2>/dev/null && break
    sleep 2
done

TOKS=$(ADMIN="$ADMIN" python3 - <<'PY'
import os, sys, json, urllib.request, urllib.error
admin = os.environ["ADMIN"]; B = "http://localhost:8001"
H = {"X-Admin-API-Key": admin, "Content-Type": "application/json"}
def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(B + path, data=data, method=method, headers=H)
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception:
        return 0, ""
s, b = call("GET", "/admin/users/email/self-host@vexa.ai")
if s == 200:
    uid = json.loads(b)["id"]
else:
    s, b = call("POST", "/admin/users", {"email": "self-host@vexa.ai", "name": "self host"})
    uid = json.loads(b)["id"]
# Terminal gateway key (bot,tx)
s, b = call("POST", f"/admin/users/{uid}/tokens?scopes=bot,tx")
d = json.loads(b)
sys.stdout.write("VEXA_API_KEY=" + (d.get("token") or d.get("api_token") or "") + "\n")
# bot api key (bot,tx)
s, b = call("POST", f"/admin/users/{uid}/tokens?scopes=bot,tx")
d = json.loads(b)
sys.stdout.write("VEXA_BOT_API_KEY=" + (d.get("token") or d.get("api_token") or "") + "\n")
PY
)

if [ -n "$TOKS" ]; then
    mkdir -p /run/vexa
    printf '%s\n' "$TOKS" > /run/vexa/key.env
    chmod 600 /run/vexa/key.env
    supervisorctl -c /etc/supervisor/conf.d/vexa.conf restart vexa:terminal >/dev/null 2>&1 || true
    echo "[provision-key] self-host keys provisioned; Terminal restarted (zero-login)"
else
    echo "[provision-key] WARN: could not mint keys — Terminal will require an interactive login"
fi
