# Helm operator helpers

`verify-workload-namespace.sh` is the fail-closed preflight for the externally managed namespace
where the runtime may create short-lived workload Pods. Run it before every hosted install or
upgrade that changes `runtime.workloadNamespace`:

```sh
deploy/helm/bin/verify-workload-namespace.sh <namespace>
```

The check requires Pod Security `restricted` at `latest` and rejects any Secret object in that
namespace. The chart intentionally does not create or mutate the cluster-scoped Namespace itself.
Set `KUBE_CONTEXT` to select a non-current kubectl context.
