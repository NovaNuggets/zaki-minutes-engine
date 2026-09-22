#!/usr/bin/env bash
# Fail-closed operator preflight for the externally owned runtime workload namespace. The chart
# intentionally has no cluster-scoped Namespace/RBAC authority, so Infra runs this before every
# hosted install/upgrade that changes runtime.workloadNamespace.
set -euo pipefail

namespace="${1:-}"
if [[ ! "$namespace" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || [ "${#namespace}" -gt 63 ]; then
  echo "usage: $0 <canonical-workload-namespace>" >&2
  exit 64
fi

kubectl_bin="${KUBECTL_BIN:-kubectl}"
if ! command -v "$kubectl_bin" >/dev/null 2>&1; then
  echo "ERROR: kubectl is required for workload namespace preflight" >&2
  exit 69
fi

kubectl_call() {
  if [ -n "${KUBE_CONTEXT:-}" ]; then
    "$kubectl_bin" --context "$KUBE_CONTEXT" "$@"
  else
    "$kubectl_bin" "$@"
  fi
}

enforce="$(kubectl_call get namespace "$namespace" \
  -o 'jsonpath={.metadata.labels.pod-security\.kubernetes\.io/enforce}')"
enforce_version="$(kubectl_call get namespace "$namespace" \
  -o 'jsonpath={.metadata.labels.pod-security\.kubernetes\.io/enforce-version}')"
if [ "$enforce" != "restricted" ] || [ "$enforce_version" != "latest" ]; then
  echo "ERROR: namespace $namespace must enforce Pod Security restricted at version latest" >&2
  exit 78
fi

secrets="$(kubectl_call -n "$namespace" get secrets -o name)"
if [[ "$secrets" =~ [^[:space:]] ]]; then
  echo "ERROR: namespace $namespace must contain no Secret objects" >&2
  exit 78
fi

echo "workload namespace $namespace: restricted PSA + secret-free"
