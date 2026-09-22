"""K8sBackend — runs a workload as a real Kubernetes Pod (the cluster substrate). Uses the kubectl CLI
via subprocess (no client lib), matching the DockerBackend approach. Implements the same Backend port,
so the kernel's runtime.v1 lifecycle is identical to process/docker. A workload is a bare Pod with
restart=Never; the kernel owns restart policy, so the Pod must not resurrect itself."""
from __future__ import annotations

import json
import os
import subprocess
from typing import Optional, TypeVar

from .backend import WorkloadHandle
from .models import Resources
from .mounts import k8s_volume_mounts
from .profiles import Runnable

MANAGED_LABEL = "runtime.managed"
WORKLOAD_ID_LABEL = "runtime.workload_id"
_T = TypeVar("_T")


def _kubectl(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    r = subprocess.run(["kubectl", *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)} failed: {r.stderr.strip()}")
    return r


def _stop_grace_sec() -> int:
    """Graceful-delete window (SIGTERM → SIGKILL). Same env knob as the Docker backend
    (RUNTIME_STOP_GRACE_SEC, default 30) so a live meeting bot can honour SIGTERM — leave the
    meeting, flush, POST its terminal callback (<25s by its own watchdog) — before the kubelet
    SIGKILLs it."""
    try:
        return max(1, int(float(os.getenv("RUNTIME_STOP_GRACE_SEC", "30"))))
    except ValueError:
        return 30


def _json_policy(name: str, expected_type: type[_T], default: _T) -> _T:
    """Read one operator-owned JSON policy from the runtime process environment.

    Workload callers cannot set these values through ``runtime.v1``: ``pod_overrides`` reads
    ``os.environ`` rather than the per-workload ``env`` payload. Invalid policy fails the spawn before
    kubectl creates a partially governed Pod.
    """
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} must contain valid JSON") from exc
    if not isinstance(value, expected_type):
        raise ValueError(f"{name} must contain a JSON {expected_type.__name__}")
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return value


def _positive_int(name: str, default: int, *, allow_zero: bool = False) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 0 if allow_zero else value <= 0:
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{name} must be a {qualifier} integer")
    return value


def _cpu_quantity(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _resource_requirements(resources: Optional[Resources]) -> dict:
    default_cpu = _positive_float("RUNTIME_K8S_DEFAULT_CPU", 1.0)
    default_memory = _positive_int("RUNTIME_K8S_DEFAULT_MEMORY_MB", 1024)
    max_cpu = _positive_float("RUNTIME_K8S_MAX_CPU", 4.0)
    max_memory = _positive_int("RUNTIME_K8S_MAX_MEMORY_MB", 4096)
    max_gpu = _positive_int("RUNTIME_K8S_MAX_GPU", 0, allow_zero=True)
    if default_cpu > max_cpu or default_memory > max_memory:
        raise ValueError("runtime k8s default resources must not exceed operator caps")

    requested_cpu = resources.cpu if resources and resources.cpu is not None else default_cpu
    requested_memory = (
        resources.memoryMb
        if resources and resources.memoryMb is not None
        else default_memory
    )
    requested_gpu = resources.gpu if resources and resources.gpu is not None else 0
    if requested_cpu <= 0:
        raise ValueError("workload resources.cpu must be positive")
    if requested_memory <= 0:
        raise ValueError("workload resources.memoryMb must be positive")
    if requested_gpu < 0:
        raise ValueError("workload resources.gpu must be non-negative")
    if requested_cpu > max_cpu:
        raise ValueError("workload resources.cpu exceeds the operator cap")
    if requested_memory > max_memory:
        raise ValueError("workload resources.memoryMb exceeds the operator cap")
    if requested_gpu > max_gpu:
        raise ValueError("workload resources.gpu exceeds the operator cap")

    requests = {
        "cpu": _cpu_quantity(float(requested_cpu)),
        "memory": f"{requested_memory}Mi",
    }
    limits = {
        "cpu": _cpu_quantity(max_cpu),
        "memory": f"{max_memory}Mi",
    }
    if requested_gpu:
        requests["nvidia.com/gpu"] = str(requested_gpu)
        # Kubernetes extended resources are not overcommittable: a GPU request must equal its
        # limit. ``max_gpu`` remains the admission ceiling, not the allocation placed on the Pod.
        limits["nvidia.com/gpu"] = str(requested_gpu)
    return {"requests": requests, "limits": limits}


def _workload_identity(workload_profile: str) -> tuple[int, int, int]:
    """Resolve the operator-owned identity for one spawned workload class.

    Browser bots and Agent workers execute unrelated image stacks and must not share a Unix
    identity.  The legacy single-UID setting remains only as the bot fallback for non-Helm users;
    Agent workers have an independent default and independent UID/GID/fsGroup knobs.
    """
    if workload_profile == "agent":
        prefix = "RUNTIME_K8S_AGENT"
        fallback_uid = 10007
    else:
        prefix = "RUNTIME_K8S_BOT"
        fallback_uid = _positive_int("RUNTIME_K8S_RUN_AS_USER", 10001)
    uid = _positive_int(f"{prefix}_RUN_AS_USER", fallback_uid)
    gid = _positive_int(f"{prefix}_RUN_AS_GROUP", uid)
    fs_group = _positive_int(f"{prefix}_FS_GROUP", gid)
    return uid, gid, fs_group


def pod_overrides(
    env: dict[str, str],
    *,
    container_name: str,
    image: Optional[str] = None,
    command: Optional[list[str]] = None,
    resources: Optional[Resources] = None,
    workload_profile: str = "meeting-bot",
) -> dict:
    """Build the governed ``kubectl run --overrides`` Pod policy.

    The workspace mount set comes from the signed workload environment. Scheduling, registry,
    security, and resource ceilings come only from the runtime Deployment's operator-owned process
    environment, so a caller cannot weaken them through ``runtime.v1``.
    """
    pvc = env.get("VEXA_WORKSPACE_MOUNT_SOURCE")
    root = env.get("VEXA_WORKSPACE_MOUNT_TARGET")
    volumes, volume_mounts = k8s_volume_mounts(env, pvc_name=pvc or "", store_target=root or "")
    run_as_user, run_as_group, fs_group = _workload_identity(workload_profile)
    container = {
        "name": container_name,
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
            "runAsNonRoot": True,
            "runAsUser": run_as_user,
            "runAsGroup": run_as_group,
        },
        "resources": _resource_requirements(resources),
    }
    if workload_profile == "agent":
        # The worker image contains code and tools only. Its root filesystem stays immutable; the
        # workspace, HOME and /tmp are the complete declared writable surface for a turn.
        container["securityContext"]["readOnlyRootFilesystem"] = True
        container["env"] = [
            {"name": key, "value": value}
            for key, value in {**env, "HOME": "/home/vexa"}.items()
        ]
    # Kubernetes strategic-merge semantics replace the generated ``containers`` list with this
    # override list. Preserve kubectl run's required image explicitly; omitting it creates an
    # invalid Pod even though ``--image`` was also present on the command line.
    if image is not None:
        container["image"] = image
    if command:
        container["command"] = command
    if workload_profile == "agent":
        volume_mounts.extend(
            [
                {"name": "tmp", "mountPath": "/tmp"},
                {"name": "agent-home", "mountPath": "/home/vexa"},
            ]
        )
        volumes.extend(
            [
                {"name": "tmp", "emptyDir": {}},
                {"name": "agent-home", "emptyDir": {}},
            ]
        )
    if volume_mounts:
        container["volumeMounts"] = volume_mounts
    image_pull_policy = os.getenv("RUNTIME_K8S_IMAGE_PULL_POLICY", "").strip()
    if image_pull_policy:
        if image_pull_policy not in {"Always", "IfNotPresent", "Never"}:
            raise ValueError(
                "RUNTIME_K8S_IMAGE_PULL_POLICY must be Always, IfNotPresent, or Never"
            )
        container["imagePullPolicy"] = image_pull_policy

    spec = {
        "automountServiceAccountToken": False,
        "securityContext": {
            "runAsNonRoot": True,
            "runAsUser": run_as_user,
            "runAsGroup": run_as_group,
            "fsGroup": fs_group,
            "fsGroupChangePolicy": "OnRootMismatch",
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "containers": [container],
    }
    if volumes:
        spec["volumes"] = volumes
    policy_fields = (
        ("nodeSelector", "RUNTIME_K8S_NODE_SELECTOR_JSON", dict, {}),
        ("tolerations", "RUNTIME_K8S_TOLERATIONS_JSON", list, []),
        ("affinity", "RUNTIME_K8S_AFFINITY_JSON", dict, {}),
        ("imagePullSecrets", "RUNTIME_K8S_IMAGE_PULL_SECRETS_JSON", list, []),
    )
    for field, name, expected_type, default in policy_fields:
        value = _json_policy(name, expected_type, default)
        if value:
            spec[field] = value
    return {"spec": spec}


class K8sBackend:
    name = "k8s"

    def __init__(self, name_prefix: str = "vexa-", namespace: Optional[str] = None) -> None:
        self._prefix = name_prefix
        self._ns = namespace

    def _pname(self, workload_id: str) -> str:
        return f"{self._prefix}{workload_id}"            # must be DNS-1123 (lowercase alnum + '-')

    def _ns_args(self) -> list[str]:
        return ["-n", self._ns] if self._ns else []

    def start(
        self,
        workload_id: str,
        runnable: Runnable,
        env: dict[str, str],
        *,
        resources: Optional[Resources] = None,
    ) -> WorkloadHandle:
        if not runnable.image:
            raise ValueError("k8s backend requires an image")
        name = self._pname(workload_id)
        args = [
            "run", name, f"--image={runnable.image}", "--restart=Never",
            # Adoption labels (the orphaned-live-bot fix): a recreated runtime re-discovers its
            # still-running Pods by this label pair and re-registers them (see the kernel's adopt()).
            f"--labels={MANAGED_LABEL}=true,{WORKLOAD_ID_LABEL}={workload_id}",
            *self._ns_args(),
        ]
        for k, v in env.items():
            args += [f"--env={k}={v}"]
        # Workspace mount set (WP-A1.1): the store PVC is bound at the store root via --overrides,
        # exposing every active workspace (they live under the root). The container name kubectl uses
        # for a `run` Pod is the Pod name, so the volumeMount targets that container.
        overrides = pod_overrides(
            env,
            container_name=name,
            image=runnable.image,
            command=runnable.command,
            resources=resources,
            workload_profile="agent" if runnable.broker_model_credentials else "meeting-bot",
        )
        args += ["--overrides", json.dumps(overrides, separators=(",", ":"))]
        _kubectl(*args)
        return WorkloadHandle(id=workload_id, impl=name)

    def find(self, workload_id: str) -> Optional[WorkloadHandle]:
        """Re-derive a handle for a workload whose in-process handle was lost (restart): the Pod
        name is deterministic (``prefix + workload_id``); an existing Pod (any phase) is found."""
        name = self._pname(workload_id)
        r = _kubectl("get", "pod", name, "-o", "name", *self._ns_args(), check=False)
        if r.returncode != 0:
            return None
        return WorkloadHandle(id=workload_id, impl=name)

    def list_workload_containers(self) -> list[dict]:
        """Discover the workload Pods THIS backend spawned — for boot re-adoption. Label-selected
        only (``runtime.managed=true``): a name-prefix fallback is unsafe in a shared namespace
        (the chart's own service Pods can share the prefix), so Pods spawned by a pre-label runtime
        are not re-adopted. Never raises."""
        try:
            r = _kubectl(
                "get", "pods", "-l", f"{MANAGED_LABEL}=true", "-o", "json",
                *self._ns_args(), check=False,
            )
            if r.returncode != 0:
                return []
            out = []
            for pod in json.loads(r.stdout).get("items", []):
                meta = pod.get("metadata", {})
                wid = (meta.get("labels") or {}).get(WORKLOAD_ID_LABEL)
                if not wid:
                    continue
                phase = pod.get("status", {}).get("phase")
                running = phase in ("Pending", "Running")
                exit_code: Optional[int] = None
                if not running:
                    exit_code = 0 if phase == "Succeeded" else 1
                    for cs in pod.get("status", {}).get("containerStatuses", []):
                        term = cs.get("state", {}).get("terminated")
                        if term and "exitCode" in term:
                            exit_code = int(term["exitCode"])
                out.append({
                    "workload_id": wid,
                    "name": meta.get("name", self._pname(wid)),
                    "running": running,
                    "exit_code": exit_code,
                })
            return out
        except Exception:  # noqa: BLE001 — discovery is a boot aid; it must never crash the boot
            return []

    def exit_code(self, h: WorkloadHandle) -> Optional[int]:
        r = _kubectl("get", "pod", h._impl, "-o", "json", *self._ns_args(), check=False)  # type: ignore[attr-defined]
        if r.returncode != 0:
            return 0                                     # gone (deleted/never-found) → no longer running
        status = json.loads(r.stdout).get("status", {})
        phase = status.get("phase")
        if phase in ("Pending", "Running"):
            return None                                  # still scheduling / running
        if phase == "Succeeded":
            return 0
        if phase == "Failed":
            for cs in status.get("containerStatuses", []):
                term = cs.get("state", {}).get("terminated")
                if term and "exitCode" in term:
                    return int(term["exitCode"])
            return 1
        return None

    def terminate(self, h: WorkloadHandle) -> None:      # graceful: SIGTERM + grace, then SIGKILL
        _kubectl("delete", "pod", h._impl, f"--grace-period={_stop_grace_sec()}", "--wait=false",
                 *self._ns_args(), check=False)  # type: ignore[attr-defined]

    def kill(self, h: WorkloadHandle) -> None:           # force: immediate SIGKILL + drop the object
        _kubectl("delete", "pod", h._impl, "--grace-period=0", "--force", "--wait=false",
                 *self._ns_args(), check=False)  # type: ignore[attr-defined]

    def cleanup(self, h: WorkloadHandle) -> None:
        """Irreversibly reclaim a workload Pod and prove substrate absence before returning.

        Privacy erasure calls this path synchronously. A fire-and-forget delete would let the
        control plane acknowledge erasure while a terminating Pod could still retain transcript
        material, so wait for Kubernetes deletion and then perform an explicit absence check.
        """
        _kubectl("delete", "pod", h._impl, "--ignore-not-found", "--grace-period=0", "--force",
                 "--wait=true", "--timeout=60s", *self._ns_args())  # type: ignore[attr-defined]
        remaining = _kubectl(
            "get", "pod", h._impl, "-o", "name", *self._ns_args(), check=False  # type: ignore[attr-defined]
        )
        if remaining.returncode == 0:
            raise RuntimeError("kubernetes workload Pod still exists after cleanup")
