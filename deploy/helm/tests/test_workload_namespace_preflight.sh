#!/usr/bin/env bash
# The chart deliberately does not own the spawned-workload Namespace. This gate proves the operator
# preflight refuses a namespace without restricted PSA or one that already contains Secrets.
set -euo pipefail

HELM_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PREFLIGHT="$HELM_DIR/bin/verify-workload-namespace.sh"
FAKE_DIR="$(mktemp -d)"
trap 'rm -rf "$FAKE_DIR"' EXIT

cat > "$FAKE_DIR/kubectl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " == *" get namespace "* ]]; then
  if [[ " $* " == *"enforce-version"* ]]; then
    printf '%s' "${FAKE_PSA_VERSION:-}"
  else
    printf '%s' "${FAKE_PSA_ENFORCE:-}"
  fi
elif [[ " $* " == *" get secrets "* ]]; then
  printf '%s' "${FAKE_SECRETS:-}"
else
  echo "unexpected kubectl call: $*" >&2
  exit 97
fi
EOF
chmod +x "$FAKE_DIR/kubectl"

run_preflight() {
  PATH="$FAKE_DIR:$PATH" "$PREFLIGHT" vexa-workloads
}

if FAKE_PSA_ENFORCE=restricted FAKE_PSA_VERSION=latest FAKE_SECRETS= run_preflight >/dev/null; then
  echo "  OK: restricted secret-free workload namespace passes preflight"
else
  echo "  FAIL: valid workload namespace failed preflight" >&2
  exit 1
fi

if FAKE_PSA_ENFORCE=baseline FAKE_PSA_VERSION=latest FAKE_SECRETS= run_preflight >/dev/null 2>&1; then
  echo "  FAIL: baseline PSA passed the restricted namespace preflight" >&2
  exit 1
else
  echo "  OK: workload namespace requires restricted PSA enforcement"
fi

if FAKE_PSA_ENFORCE=restricted FAKE_PSA_VERSION=latest \
  FAKE_SECRETS=secret/unrelated-credential run_preflight >/dev/null 2>&1; then
  echo "  FAIL: workload namespace containing a Secret passed preflight" >&2
  exit 1
else
  echo "  OK: workload namespace must be secret-free"
fi

echo "workload-namespace-preflight PASS"
