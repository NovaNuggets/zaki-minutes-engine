"""gate:health — admin-api exposes a conforming liveness /health.

A pure liveness probe (process-up): no DB dependency, so it returns 200 without a live
Postgres. Readiness (DB reachable) is a separate concern covered by the stack evals.
"""
from fastapi.testclient import TestClient
import pytest

from admin_api.app.main import create_app


def test_health_ok():
    client = TestClient(create_app())
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["service"] == "admin-api"


def test_boot_rejects_admin_and_internal_auth_domain_alias(monkeypatch):
    shared = "one-secret-must-not-authorize-both-domains"
    monkeypatch.setenv("ADMIN_API_TOKEN", shared)
    monkeypatch.setenv("INTERNAL_API_SECRET", shared)

    with pytest.raises(RuntimeError, match="must be distinct"):
        create_app()
