"""``RUNTIME_BACKEND`` selects the spawn backend in the production entrypoint (P5.1 helm wiring).

Pure unit test — no Docker daemon and no Kubernetes cluster needed: the backend constructors are
lazy (they don't connect at __init__), so we only assert the env→class mapping and the invariant
that the Docker-only worker-image ensure step is skipped for backends that lack ``ensure_worker_image``."""
from __future__ import annotations

import pytest
import fakeredis

from runtime_kernel.__main__ import _build_backend
from runtime_kernel.docker_backend import DockerBackend
from runtime_kernel.k8s_backend import K8sBackend
from runtime_kernel.process_backend import ProcessBackend
from runtime_kernel.callbacks import RedisPendingStore
from runtime_kernel.store import RedisStore


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, DockerBackend),          # default → docker (compose/desktop)
        ("docker", DockerBackend),
        ("DOCKER", DockerBackend),      # case-insensitive
        ("k8s", K8sBackend),            # helm / real cluster
        ("process", ProcessBackend),    # no-container fallback
    ],
)
def test_runtime_backend_env_selects_backend(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("RUNTIME_BACKEND", raising=False)
    else:
        monkeypatch.setenv("RUNTIME_BACKEND", value)
    assert isinstance(_build_backend(), expected)


def test_k8s_backend_reads_pod_namespace(monkeypatch):
    monkeypatch.setenv("RUNTIME_BACKEND", "k8s")
    monkeypatch.setenv("POD_NAMESPACE", "vexa-prod")
    backend = _build_backend()
    assert isinstance(backend, K8sBackend)
    assert backend._ns == "vexa-prod"


def test_only_docker_backend_ensures_worker_image():
    # build_production_app() guards the AGENT_WORKER_IMAGE presence-ensure (pull-when-absent) on
    # hasattr(backend, "ensure_worker_image"): docker pre-pulls via the socket API (which never
    # implicit-pulls at create); k8s/process pull by full ref themselves at spawn time.
    assert hasattr(DockerBackend, "ensure_worker_image")
    assert not hasattr(K8sBackend, "ensure_worker_image")
    assert not hasattr(ProcessBackend, "ensure_worker_image")


# ── RUNTIME_STOP_GRACE_SEC → kernel stop-poll wiring (review fix #2) ─────────────────────────────
# The backends' terminate() already honoured the env, but the kernel's stop() polled for its own
# grace_sec — stuck at the 5.0 constructor default because build_production_app never set it. On
# k8s (terminate returns immediately, pod phase stays Running during graceful deletion) that meant
# a force-kill at t≈5: the 30s grace was a dead letter. _kernel_grace_sec derives the kernel window
# from the SAME env + a margin so the kernel's force-kill lands strictly AFTER the substrate grace.

def test_kernel_grace_follows_runtime_stop_grace_env(monkeypatch):
    from runtime_kernel.__main__ import _kernel_grace_sec

    monkeypatch.setenv("RUNTIME_STOP_GRACE_SEC", "30")
    assert _kernel_grace_sec() == 35.0                      # backend grace + margin

    monkeypatch.setenv("RUNTIME_STOP_GRACE_SEC", "120")
    assert _kernel_grace_sec() == 125.0

    monkeypatch.delenv("RUNTIME_STOP_GRACE_SEC", raising=False)
    assert _kernel_grace_sec() == 35.0                      # default 30 + margin

    monkeypatch.setenv("RUNTIME_STOP_GRACE_SEC", "not-a-number")
    assert _kernel_grace_sec() == 35.0                      # malformed → default, never a crash


def test_production_runtime_gets_the_env_grace(monkeypatch):
    """The wiring itself: build_production_app constructs Runtime(grace_sec=_kernel_grace_sec()) —
    the kernel's stop-poll window reflects the env, not the 5.0 constructor default."""
    from runtime_kernel.__main__ import build_production_app

    monkeypatch.setenv("RUNTIME_BACKEND", "process")        # no docker daemon needed
    monkeypatch.setenv("RUNTIME_STOP_GRACE_SEC", "42")
    monkeypatch.setenv("RUNTIME_CALLBACK_SECRET", "runtime-callback-secret")
    monkeypatch.setenv("RUNTIME_CONTROL_SECRET", "runtime-control-secret")
    monkeypatch.setenv(
        "RUNTIME_CALLBACK_TRUSTED_ORIGINS",
        "http://meeting-api:8080",
    )
    monkeypatch.delenv("REDIS_URL", raising=False)          # no scheduler ticker
    monkeypatch.delenv("AGENT_IMAGE", raising=False)        # no worker-image ensure
    app = build_production_app()
    assert app.state.runtime.grace_sec == 47.0
    assert app.state.control_auth_enabled is True
    assert app.state.callback_queue.default_headers == {
        "X-Runtime-Callback-Secret": "runtime-callback-secret"
    }


def test_disabled_agent_profile_skips_worker_image_ensure(monkeypatch):
    """Managed Minutes omits the bundled Agent and must not pull or expose its worker image."""
    from runtime_kernel.__main__ import build_production_app

    monkeypatch.setenv("RUNTIME_BACKEND", "docker")
    monkeypatch.setenv("RUNTIME_AGENT_PROFILE_ENABLED", "false")
    monkeypatch.setenv("AGENT_IMAGE", "registry.example.com/agent-api:must-not-pull")
    monkeypatch.setenv("RUNTIME_CALLBACK_SECRET", "runtime-callback-secret")
    monkeypatch.setenv("RUNTIME_CONTROL_SECRET", "runtime-control-secret")
    monkeypatch.setenv("RUNTIME_CALLBACK_TRUSTED_ORIGINS", "http://meeting-api:8080")
    monkeypatch.delenv("REDIS_URL", raising=False)

    def unexpected_ensure(*_args, **_kwargs):
        raise AssertionError("disabled Agent profile attempted to ensure its worker image")

    monkeypatch.setattr(DockerBackend, "ensure_worker_image", unexpected_ensure)

    app = build_production_app()

    assert app.state.runtime.profiles.resolve("agent") is None


def test_production_runtime_rejects_shared_or_multi_origin_callback_credentials(monkeypatch):
    from runtime_kernel.__main__ import build_production_app

    monkeypatch.setenv("RUNTIME_BACKEND", "process")
    monkeypatch.setenv("RUNTIME_CONTROL_SECRET", "same-secret")
    monkeypatch.setenv("RUNTIME_CALLBACK_SECRET", "same-secret")
    monkeypatch.setenv("RUNTIME_CALLBACK_TRUSTED_ORIGINS", "http://meeting-api:8080")
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("AGENT_IMAGE", raising=False)

    with pytest.raises(RuntimeError, match="must be distinct"):
        build_production_app()

    monkeypatch.setenv("RUNTIME_CALLBACK_SECRET", "dedicated-callback-secret")
    monkeypatch.setenv(
        "RUNTIME_CALLBACK_TRUSTED_ORIGINS",
        "http://meeting-api:8080,http://agent-api:8100",
    )
    with pytest.raises(RuntimeError, match="exactly one"):
        build_production_app()


def test_redis_url_builds_one_durable_workload_and_callback_state(monkeypatch):
    from runtime_kernel.__main__ import _build_runtime_stores

    redis_client = fakeredis.FakeStrictRedis(decode_responses=True)
    import redis

    calls = []
    monkeypatch.setattr(
        redis,
        "from_url",
        lambda url, **kwargs: calls.append((url, kwargs)) or redis_client,
    )

    workload_store, callback_store = _build_runtime_stores("redis://runtime-state:6379/0")

    assert isinstance(workload_store, RedisStore)
    assert isinstance(callback_store, RedisPendingStore)
    assert calls == [("redis://runtime-state:6379/0", {"decode_responses": True})]
