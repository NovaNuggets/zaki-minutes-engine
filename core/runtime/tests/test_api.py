"""Stage-2 (API) gate — drive the full runtime.v1 lifecycle OVER HTTP, assert the status responses +
the delivered RuntimeEvents all conform to the frozen contract."""
import json
from pathlib import Path

import jsonschema
import pytest
from fastapi.testclient import TestClient
from referencing import Registry, Resource

from runtime_kernel import Runtime
from runtime_kernel.api import create_app

SCHEMA = json.loads(
    (Path(__file__).resolve().parents[1] / "contracts" / "runtime.v1" / "runtime.schema.json").read_text()
)
_REGISTRY = Registry().with_resource(SCHEMA["$id"], Resource.from_contents(SCHEMA))


def _conforms(obj: dict, shape: str) -> None:
    jsonschema.Draft202012Validator(
        {"$ref": f"{SCHEMA['$id']}#/$defs/{shape}"}, registry=_REGISTRY
    ).validate(obj)


def test_lifecycle_over_http_conforms():
    events = []
    app = create_app(Runtime(profiles={"test": ["sleep", "30"]}, grace_sec=3.0), deliver=events.append)
    client = TestClient(app)

    r = client.post("/workloads", json={"workloadId": "w1", "profile": "test", "env": {}})
    assert r.status_code == 201
    _conforms(r.json(), "WorkloadStatus")

    assert client.get("/workloads/w1").json()["state"] == "running"
    assert any(s["workloadId"] == "w1" for s in client.get("/workloads").json())

    s = client.post("/workloads/w1/stop", json={"reason": "stopped"})
    assert s.status_code == 200 and s.json()["state"] == "stopped"
    _conforms(s.json(), "WorkloadStatus")

    d = client.delete("/workloads/w1")
    assert d.status_code == 200 and d.json()["state"] == "destroyed"

    # the API delivered the full legal lifecycle, every event conforming to runtime.v1
    assert [e.state.value for e in events] == ["starting", "running", "stopping", "stopped", "destroyed"]
    for e in events:
        _conforms(json.loads(e.model_dump_json(exclude_none=True)), "RuntimeEvent")


def test_unknown_profile_is_400_and_unknown_workload_404():
    client = TestClient(create_app(Runtime(profiles={})))
    assert client.post("/workloads", json={"workloadId": "x", "profile": "nope", "env": {}}).status_code == 400
    assert client.get("/workloads/missing").status_code == 404


def test_runtime_control_surface_is_fail_closed_before_body_parsing():
    client = TestClient(
        create_app(Runtime(profiles={}), control_secret="operator-control-secret")
    )

    # Every workload/scheduler operation is a privileged control-plane edge.  A
    # malformed body must not become an authentication oracle: auth runs first.
    denied = (
        client.post(
            "/workloads",
            content=b"{not-json",
            headers={"Content-Type": "application/json"},
        ),
        client.get("/workloads"),
        client.get("/workloads/missing"),
        client.post("/workloads/missing/stop", content=b"{not-json"),
        client.post("/workloads/missing/scrub", content=b"{not-json"),
        client.delete("/workloads/missing"),
        client.post("/schedule", content=b"{not-json"),
        client.get("/schedule"),
        client.delete("/schedule/missing"),
    )
    assert {response.status_code for response in denied} == {403}
    assert all(response.headers["cache-control"] == "no-store" for response in denied)

    # Liveness remains probeable without granting workload authority.
    assert client.get("/health").status_code == 200


def test_runtime_control_surface_accepts_only_the_exact_dedicated_secret():
    client = TestClient(
        create_app(Runtime(profiles={}), control_secret="operator-control-secret")
    )

    assert client.get(
        "/workloads",
        headers={"X-Runtime-Control-Secret": "operator-control-secret-wrong"},
    ).status_code == 403
    allowed = client.get(
        "/workloads",
        headers={"X-Runtime-Control-Secret": "operator-control-secret"},
    )
    assert allowed.status_code == 200
    assert allowed.json() == []


def test_runtime_control_secret_rejects_an_empty_configured_value():
    with pytest.raises(ValueError, match="control secret"):
        create_app(Runtime(profiles={}), control_secret="")


def test_authenticated_scrub_is_idempotent_and_removes_durable_launch_state():
    secret = "operator-control-secret"
    runtime = Runtime(profiles={"test": ["sleep", "30"]}, grace_sec=3.0)
    client = TestClient(create_app(runtime, control_secret=secret))
    headers = {"X-Runtime-Control-Secret": secret}
    sentinel = "meeting-url-passcode-token-and-funded-stt-secret"
    created = client.post(
        "/workloads",
        headers=headers,
        json={
            "workloadId": "mtg-41-private",
            "profile": "test",
            "env": {"VEXA_BOT_CONFIG": sentinel, "BOT_CONFIG": sentinel},
        },
    )
    assert created.status_code == 201
    assert runtime.store.get("mtg-41-private").spec.env == {}

    first = client.post("/workloads/mtg-41-private/scrub", headers=headers)
    second = client.post("/workloads/mtg-41-private/scrub", headers=headers)

    assert first.status_code == second.status_code == 200
    assert first.json() == second.json() == {"scrubbed": True}
    assert runtime.store.get("mtg-41-private") is None


def test_scrub_refuses_to_claim_an_unknown_process_workload_is_absent():
    """ProcessBackend cannot rediscover children after a runtime restart.

    An arbitrary unknown id therefore cannot receive an idempotent success: unlike Docker/K8s,
    this backend has no substrate lookup that proves a credential-bearing process is gone.
    """
    secret = "operator-control-secret"
    client = TestClient(
        create_app(Runtime(profiles={}), control_secret=secret)
    )

    response = client.post(
        "/workloads/untracked-private-bot/scrub",
        headers={"X-Runtime-Control-Secret": secret},
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "unknown workload"}


def test_double_create_over_http_touches_not_respawns():
    """runtime.v1 idempotent create (ADR 0027): a second POST /workloads for a running workloadId
    returns its live status — no second spawn, no duplicate lifecycle events."""
    events = []
    app = create_app(Runtime(profiles={"test": ["sleep", "30"]}, grace_sec=3.0), deliver=events.append)
    client = TestClient(app)
    try:
        first = client.post("/workloads", json={"workloadId": "w1", "profile": "test", "env": {}})
        assert first.status_code == 201 and first.json()["state"] == "running"

        touched = client.post("/workloads", json={"workloadId": "w1", "profile": "test", "env": {}})
        assert touched.status_code == 201 and touched.json()["state"] == "running"
        _conforms(touched.json(), "WorkloadStatus")

        # one spawn's worth of events — the touch emitted nothing new
        assert [e.state.value for e in events] == ["starting", "running"]
    finally:
        client.post("/workloads/w1/stop", json={"reason": "stopped"})
        client.delete("/workloads/w1")
