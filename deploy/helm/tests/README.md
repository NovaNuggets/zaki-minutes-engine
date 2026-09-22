# helm · tests

Smoke tests for the `vexa` Helm chart: `test_helm_lint.sh` (chart lint) and `test_template.sh`
(render/template validation, including all-mode bundled-Agent rejection, the complete read/capture
erasure boundary, external Nullalis URL validation, retention of the Minutes-only runtime for
rollback withdrawal/teardown, removal of runtime Agent worker profiles,
MinIO credential `secretKeyRef` isolation (including operator-Secret projection and literal-leak
denial),
pre-created Postgres credential-Secret mode (including no chart recreation, all-consumer
projection, and hardened chart-created password validation),
dedicated Gateway/Agent identity-proof projection and non-owner denial, bounded no-eviction Redis,
verifier-only previous Gateway key projection to Agent, isolated workload-namespace RBAC,
platform/workload default-deny NetworkPolicies, immutable image admission, Secret-revision rollouts,
restricted Redis/Postgres/MinIO/Job Pod Security, profile-specific spawned-worker identities,
mandatory hosted migration hooks, enabled-component-only PDBs, and the external staging UI edge,
meeting-only Hub/read/verifier secrets, bidirectional verifier-only rotation, reserved `extraEnv`
guards, and bounded TTL controls). `test_workload_namespace_preflight.sh` exercises the cluster-side
operator guard that requires restricted Pod Security Admission and a secret-free externally owned
workload namespace. Run as
part of the helm deploy checks.
