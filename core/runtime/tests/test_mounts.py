"""L2: the runtime's workspace MOUNT-SET plumbing (WP-A1.1) — the ONE env-driven mount computation the
docker/k8s/process backends share, proven offline (no docker, no kubectl).

STRICT isolation (default): one bind PER MOUNT — the worker's filesystem contains ONLY the dispatch's
declared workspaces (tenant isolation enforced by the mount table). A named-volume store rides
``volume_subpath`` (docker VolumeOptions.Subpath, engine ≥ v26); a host-path store joins the subpath;
k8s uses native ``subPath`` + ``readOnly``. The whole-store bind is never emitted."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from runtime_kernel.mounts import MountBind, k8s_volume_mounts, mount_set, workspace_binds
from runtime_kernel.docker_backend import DockerBackend  # for the docker bind-string shape
from runtime_kernel.backend import WorkloadHandle
from runtime_kernel.k8s_backend import K8sBackend, pod_overrides
from runtime_kernel.models import Resources
from runtime_kernel.profiles import Runnable


def _env(mounts=None, *, source="agent-workspaces", target="/workspaces", path="/workspaces/u1"):
    e = {"VEXA_WORKSPACE_MOUNT_SOURCE": source, "VEXA_WORKSPACE_MOUNT_TARGET": target,
         "VEXA_WORKSPACE_PATH": path}
    if mounts is not None:
        e["VEXA_MOUNTS"] = json.dumps(mounts)
    return e


# ── one bind per mount, nothing else reachable ───────────────────────────────

def test_strict_emits_one_volume_subpath_bind_per_in_store_mount():
    """The tenant-isolation core: NO store-root bind; each in-store mount becomes its own bind of the
    store volume's subpath, so another tenant's workspace is simply not in the container."""
    mounts = [
        {"slug": "seed", "path": "/workspaces/u1", "role": "private", "write": True, "primary": True},
        {"slug": "x", "path": "/workspaces/.attached/u1/x", "role": "private", "write": True},
        {"slug": "deal-9", "path": "/workspaces/deal-9", "role": "shared", "write": False},
    ]
    binds = workspace_binds(_env(mounts))
    assert binds == [
        MountBind("agent-workspaces", "/workspaces/u1", read_only=False, volume_subpath="u1"),
        MountBind("agent-workspaces", "/workspaces/.attached/u1/x", read_only=False,
                  volume_subpath=".attached/u1/x"),
        MountBind("agent-workspaces", "/workspaces/deal-9", read_only=True, volume_subpath="deal-9"),
    ]
    # the whole-store bind is GONE
    assert not any(b.target == "/workspaces" for b in binds)


def test_strict_read_only_role_is_enforced_by_the_bind():
    """A viewer-role shared mount binds :ro — enforced by the substrate now, not just the commit token."""
    mounts = [{"slug": "deal-9", "path": "/workspaces/deal-9", "role": "shared", "write": False}]
    [b] = workspace_binds(_env(mounts))
    assert b.read_only is True


def test_strict_host_path_store_joins_the_subpath():
    """A host-path store (source starts with /) needs no volume subpath — plain source-join bind, which
    also means NO docker-engine version requirement on such deployments."""
    mounts = [{"slug": "seed", "path": "/workspaces/u1", "role": "private", "write": True, "primary": True}]
    [b] = workspace_binds(_env(mounts, source="/srv/vexa/workspaces"))
    assert b == MountBind("/srv/vexa/workspaces/u1", "/workspaces/u1", read_only=False)
    assert b.volume_subpath is None


def test_strict_global_and_out_of_store_mounts_bind_their_own_source():
    """_global (own host source) and out-of-store mounts bind source→target directly — same as legacy."""
    mounts = [
        {"slug": "_global", "source": "/srv/vexa-global", "path": "/workspaces/_global",
         "role": "global", "write": False},
        {"slug": "shared-z", "path": "/shared-store/team-z", "role": "shared", "write": False},
        {"slug": "seed", "path": "/workspaces/u1", "role": "private", "write": True, "primary": True},
    ]
    binds = workspace_binds(_env(mounts))
    assert MountBind("/srv/vexa-global", "/workspaces/_global", read_only=True) in binds
    assert MountBind("/shared-store/team-z", "/shared-store/team-z", read_only=True) in binds
    assert MountBind("agent-workspaces", "/workspaces/u1", read_only=False, volume_subpath="u1") in binds


def test_strict_never_re_exposes_the_store_root():
    """A (mis)declared mount AT the store root must not silently re-open the whole store."""
    mounts = [{"slug": "evil", "path": "/workspaces", "role": "private", "write": True}]
    assert workspace_binds(_env(mounts)) == []


def test_strict_legacy_dispatch_still_binds_its_baseline():
    """A dispatch predating VEXA_MOUNTS (only VEXA_WORKSPACE_PATH) gets its one private-baseline bind."""
    binds = workspace_binds(_env())
    assert binds == [MountBind("agent-workspaces", "/workspaces/u1", read_only=False, volume_subpath="u1")]


def test_no_store_configured_yields_direct_binds_only():
    assert workspace_binds({"VEXA_MOUNTS": json.dumps([{"slug": "a", "path": "/x", "write": True}])}) == [
        MountBind("/x", "/x", read_only=False)
    ]
    assert workspace_binds({}) == []


def test_mount_set_falls_back_to_the_private_baseline():
    """A dispatch predating VEXA_MOUNTS → the single private baseline from VEXA_WORKSPACE_PATH."""
    assert mount_set(_env()) == [
        {"slug": "u1", "path": "/workspaces/u1", "role": "private", "write": True, "primary": True}
    ]
    got = mount_set(_env([{"slug": "seed", "path": "/workspaces/u1", "primary": True, "write": True},
                          {"slug": "x", "path": "/workspaces/.attached/u1/x", "write": True}]))
    assert [m["slug"] for m in got] == ["seed", "x"]


def test_malformed_vexa_mounts_is_ignored_not_fatal():
    e = _env()
    e["VEXA_MOUNTS"] = "{not json"
    # falls back to the baseline mount (strict: its own subpath bind) — never raises
    assert workspace_binds(e) == [
        MountBind("agent-workspaces", "/workspaces/u1", read_only=False, volume_subpath="u1")
    ]
    assert mount_set(e)[0]["slug"] == "u1"


# ── docker: bind strings + Mounts API entries ─────────────────────────────────

def test_docker_bind_strings_cover_the_non_subpath_binds():
    mounts = [
        {"slug": "seed", "path": "/workspaces/u1", "primary": True, "write": True},
        {"slug": "shared-z", "path": "/shared-store/team-z", "write": False},
    ]
    binds = workspace_binds(_env(mounts))
    strings = [f"{b.source}:{b.target}:ro" if b.read_only else f"{b.source}:{b.target}"
               for b in binds if not b.volume_subpath]
    assert strings == ["/shared-store/team-z:/shared-store/team-z:ro"]
    subpathed = [b for b in binds if b.volume_subpath]
    assert [(b.source, b.target, b.volume_subpath) for b in subpathed] == [
        ("agent-workspaces", "/workspaces/u1", "u1")
    ]
    # sanity: the backend can be constructed (no daemon needed for this pure check)
    assert DockerBackend().name == "docker"


# ── k8s: per-mount subPath volumeMounts (native isolation) ────────────────────

def test_k8s_strict_emits_per_mount_subpath_readonly():
    mounts = [
        {"slug": "seed", "path": "/workspaces/u1", "role": "private", "write": True, "primary": True},
        {"slug": "deal-9", "path": "/workspaces/deal-9", "role": "shared", "write": False},
    ]
    volumes, vmounts = k8s_volume_mounts(_env(mounts), pvc_name="vexa-agent-workspaces",
                                         store_target="/workspaces")
    assert volumes == [{"name": "workspace-store", "persistentVolumeClaim": {"claimName": "vexa-agent-workspaces"}}]
    assert vmounts == [
        {"name": "workspace-store", "mountPath": "/workspaces/u1", "subPath": "u1", "readOnly": False},
        {"name": "workspace-store", "mountPath": "/workspaces/deal-9", "subPath": "deal-9", "readOnly": True},
    ]
    assert not any(vm["mountPath"] == "/workspaces" for vm in vmounts)   # whole store never mounted


def test_k8s_pod_overrides_carry_the_per_mount_spec():
    """The Pod spec the worker gets: per-mount subPath volumeMounts against the ONE store PVC."""
    ov = pod_overrides(_env(source="vexa-agent-workspaces"), container_name="vexa-worker-u1")
    spec = ov["spec"]
    assert spec["volumes"][0]["persistentVolumeClaim"]["claimName"] == "vexa-agent-workspaces"
    c = spec["containers"][0]
    assert c["name"] == "vexa-worker-u1"
    assert c["volumeMounts"] == [
        {"name": "workspace-store", "mountPath": "/workspaces/u1", "subPath": "u1", "readOnly": False}
    ]


def test_k8s_pod_overrides_harden_every_spawn_even_without_a_workspace(monkeypatch):
    monkeypatch.setenv("RUNTIME_K8S_RUN_AS_USER", "10003")
    ov = pod_overrides({}, container_name="x")
    spec = ov["spec"]
    assert spec["automountServiceAccountToken"] is False
    assert spec["securityContext"] == {
        "runAsNonRoot": True,
        "runAsUser": 10003,
        "runAsGroup": 10003,
        "fsGroup": 10003,
        "fsGroupChangePolicy": "OnRootMismatch",
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    [container] = spec["containers"]
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "runAsNonRoot": True,
        "runAsUser": 10003,
        "runAsGroup": 10003,
    }
    assert container["resources"] == {
        "requests": {"cpu": "1", "memory": "1024Mi"},
        "limits": {"cpu": "4", "memory": "4096Mi"},
    }


def test_k8s_pod_overrides_preserve_the_kubectl_run_image():
    ov = pod_overrides(
        {},
        container_name="x",
        image="registry.example/minutes-bot@sha256:abc123",
        command=["sleep", "30"],
    )

    assert ov["spec"]["containers"][0]["image"] == (
        "registry.example/minutes-bot@sha256:abc123"
    )
    assert ov["spec"]["containers"][0]["command"] == ["sleep", "30"]


def test_k8s_pod_overrides_inherit_operator_scheduling_and_pull_policy(monkeypatch):
    monkeypatch.setenv("RUNTIME_K8S_NODE_SELECTOR_JSON", '{"vexa.ai/pool":"temp"}')
    monkeypatch.setenv(
        "RUNTIME_K8S_TOLERATIONS_JSON",
        '[{"key":"vexa.ai/pool","operator":"Equal","value":"temp","effect":"NoSchedule"}]',
    )
    monkeypatch.setenv(
        "RUNTIME_K8S_AFFINITY_JSON",
        '{"nodeAffinity":{"preferredDuringSchedulingIgnoredDuringExecution":[]}}',
    )
    monkeypatch.setenv(
        "RUNTIME_K8S_IMAGE_PULL_SECRETS_JSON", '[{"name":"private-registry"}]'
    )

    spec = pod_overrides({}, container_name="x")["spec"]

    assert spec["nodeSelector"] == {"vexa.ai/pool": "temp"}
    assert spec["tolerations"][0]["effect"] == "NoSchedule"
    assert "nodeAffinity" in spec["affinity"]
    assert spec["imagePullSecrets"] == [{"name": "private-registry"}]


def test_k8s_pod_overrides_honor_bounded_workload_resources(monkeypatch):
    monkeypatch.setenv("RUNTIME_K8S_MAX_CPU", "2")
    monkeypatch.setenv("RUNTIME_K8S_MAX_MEMORY_MB", "2048")
    spec = pod_overrides(
        {},
        container_name="x",
        resources=Resources(cpu=0.5, memoryMb=768),
    )["spec"]
    assert spec["containers"][0]["resources"] == {
        "requests": {"cpu": "0.5", "memory": "768Mi"},
        "limits": {"cpu": "2", "memory": "2048Mi"},
    }


def test_k8s_gpu_request_and_limit_are_identical(monkeypatch):
    """Extended resources cannot be overcommitted: Kubernetes requires GPU request == limit."""
    monkeypatch.setenv("RUNTIME_K8S_MAX_GPU", "4")

    resources = pod_overrides(
        {},
        container_name="x",
        resources=Resources(gpu=2),
    )["spec"]["containers"][0]["resources"]

    assert resources["requests"]["nvidia.com/gpu"] == "2"
    assert resources["limits"]["nvidia.com/gpu"] == "2"


def test_k8s_agent_worker_has_dedicated_identity_and_only_declared_writable_paths(
    monkeypatch,
):
    monkeypatch.setenv("RUNTIME_K8S_AGENT_RUN_AS_USER", "10007")
    monkeypatch.setenv("RUNTIME_K8S_AGENT_RUN_AS_GROUP", "10008")
    monkeypatch.setenv("RUNTIME_K8S_AGENT_FS_GROUP", "10009")

    spec = pod_overrides(
        _env(source="vexa-agent-workspaces"),
        container_name="vexa-worker-u1",
        workload_profile="agent",
    )["spec"]

    assert spec["securityContext"] == {
        "runAsNonRoot": True,
        "runAsUser": 10007,
        "runAsGroup": 10008,
        "fsGroup": 10009,
        "fsGroupChangePolicy": "OnRootMismatch",
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    [container] = spec["containers"]
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
        "runAsNonRoot": True,
        "runAsUser": 10007,
        "runAsGroup": 10008,
    }
    assert {mount["mountPath"] for mount in container["volumeMounts"]} == {
        "/workspaces/u1",
        "/tmp",
        "/home/vexa",
    }
    assert {volume["name"] for volume in spec["volumes"]} == {
        "workspace-store",
        "tmp",
        "agent-home",
    }


def test_k8s_bot_and_agent_profiles_do_not_share_runtime_identity(monkeypatch):
    monkeypatch.setenv("RUNTIME_K8S_BOT_RUN_AS_USER", "10003")
    monkeypatch.setenv("RUNTIME_K8S_BOT_RUN_AS_GROUP", "10003")
    monkeypatch.setenv("RUNTIME_K8S_BOT_FS_GROUP", "10003")
    monkeypatch.setenv("RUNTIME_K8S_AGENT_RUN_AS_USER", "10007")
    monkeypatch.setenv("RUNTIME_K8S_AGENT_RUN_AS_GROUP", "10007")
    monkeypatch.setenv("RUNTIME_K8S_AGENT_FS_GROUP", "10007")

    bot = pod_overrides({}, container_name="bot", workload_profile="meeting-bot")["spec"]
    agent = pod_overrides({}, container_name="worker", workload_profile="agent")["spec"]

    assert bot["securityContext"]["runAsUser"] == 10003
    assert agent["securityContext"]["runAsUser"] == 10007


def test_k8s_pod_overrides_reject_resources_above_operator_caps(monkeypatch):
    monkeypatch.setenv("RUNTIME_K8S_MAX_CPU", "2")
    monkeypatch.setenv("RUNTIME_K8S_MAX_MEMORY_MB", "2048")
    with pytest.raises(ValueError, match="cpu exceeds"):
        pod_overrides({}, container_name="x", resources=Resources(cpu=3))
    with pytest.raises(ValueError, match="memoryMb exceeds"):
        pod_overrides({}, container_name="x", resources=Resources(memoryMb=4096))


def test_k8s_cleanup_waits_for_deletion_and_confirms_the_pod_is_absent(monkeypatch):
    calls: list[tuple[tuple[str, ...], bool]] = []

    def fake_kubectl(*args: str, check: bool = True):
        calls.append((args, check))
        return SimpleNamespace(returncode=1 if args[0] == "get" else 0, stderr="")

    monkeypatch.setattr("runtime_kernel.k8s_backend._kubectl", fake_kubectl)
    K8sBackend(namespace="meetings").cleanup(WorkloadHandle(id="m-1", impl="vexa-m-1"))

    delete_args, delete_check = calls[0]
    assert delete_check is True
    assert delete_args[:3] == ("delete", "pod", "vexa-m-1")
    assert "--wait=true" in delete_args
    assert "--timeout=60s" in delete_args
    assert calls[-1][0][:3] == ("get", "pod", "vexa-m-1")


def test_k8s_cleanup_fails_closed_if_the_pod_still_exists(monkeypatch):
    def fake_kubectl(*args: str, check: bool = True):
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr("runtime_kernel.k8s_backend._kubectl", fake_kubectl)
    with pytest.raises(RuntimeError, match="still exists"):
        K8sBackend().cleanup(WorkloadHandle(id="m-1", impl="vexa-m-1"))


# ── process: shares the host FS — no binds, but N-mount aware (parity) ────────

def test_process_backend_reads_the_mount_set_without_binding(tmp_path):
    """The lite/process backend shares the host FS: it materializes NO binds but still resolves the
    mount set (POSIX isolation is its wall — see test_isolation.py). Non-root test run → the isolation
    plan is unavailable and the spawn degrades loudly to shared-trust, which is exactly this path."""
    from runtime_kernel.process_backend import ProcessBackend
    mounts = [
        {"slug": "seed", "path": str(tmp_path / "u1"), "primary": True, "write": True},
        {"slug": "shared-x", "path": str(tmp_path / "shared"), "write": True},
    ]
    assert [m["slug"] for m in mount_set(_env(mounts))] == ["seed", "shared-x"]
    b = ProcessBackend()
    h = b.start("rt-mnt", Runnable(command=["true"]), _env(mounts))
    try:
        assert h.id == "rt-mnt"
    finally:
        b.cleanup(h)
