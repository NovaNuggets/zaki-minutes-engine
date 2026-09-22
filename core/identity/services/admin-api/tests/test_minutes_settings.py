"""Identity-owned Minutes consent, retention and Agent-read preferences."""
from __future__ import annotations

from datetime import datetime
import time

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from admin_api.app import db as app_db
from admin_api.app import main as main_module
from admin_api.app.main import create_app
from admin_api.schema.models import Base
from admin_api.schema.sync import ensure_schema_sync

from conftest import requires_docker
from test_stack_admin_api import ADMIN_TOKEN, INTERNAL_SECRET, _admin, _dispose_async_engine


pytestmark = requires_docker


@pytest.fixture()
def client(pg_url, pg_async_url, monkeypatch):
    sync_engine = create_engine(pg_url)
    Base.metadata.drop_all(sync_engine)
    ensure_schema_sync(sync_engine, Base)
    sync_engine.dispose()
    monkeypatch.setenv("ADMIN_API_TOKEN", ADMIN_TOKEN)
    monkeypatch.setenv("INTERNAL_API_SECRET", INTERNAL_SECRET)
    monkeypatch.setenv("DEV_MODE", "false")
    monkeypatch.setenv("ZAKI_MINUTES_CAPTURE_ENABLED", "true")
    monkeypatch.setenv("ZAKI_MINUTES_READ_ENABLED", "true")
    app_db.configure(pg_async_url)
    with TestClient(create_app()) as value:
        yield value
    _dispose_async_engine()


def _internal():
    return {"X-Internal-Secret": INTERNAL_SECRET}


def _user(client):
    user_id = client.post(
        "/admin/users", headers=_admin(), json={"email": "minutes@example.com"}
    ).json()["id"]
    token = client.post(
        f"/admin/users/{user_id}/tokens?scopes=browser", headers=_admin()
    ).json()["token"]
    return user_id, {"X-API-Key": token}


@pytest.mark.parametrize(("scope", "contract_version"), [
    ("bot", "identity.v1"),
    ("tx", "identity.v1"),
    ("agent", "identity.v2"),
])
def test_minutes_user_preferences_require_an_interactive_browser_token(
    client, scope, contract_version
):
    user_id = client.post(
        "/admin/users", headers=_admin(), json={"email": f"{scope}-minutes@example.com"}
    ).json()["id"]
    token = client.post(
        f"/admin/users/{user_id}/tokens",
        headers=_admin(),
        params={"scopes": scope, "contract_version": contract_version},
    ).json()["token"]
    headers = {"X-API-Key": token}

    assert client.get("/user/minutes", headers=headers).status_code == 403
    assert client.put(
        "/user/minutes", headers=headers, json={"agent_read_enabled": True}
    ).status_code == 403


def test_minutes_defaults_keep_both_user_permissions_off(client):
    user_id, headers = _user(client)
    response = client.get("/user/minutes", headers=headers)
    assert response.status_code == 200
    assert response.json() == {
        "operator_enabled": True,
        "read_operator_enabled": True,
        "capture_enabled": False,
        "agent_read_enabled": False,
        "capture_requested": False,
        "agent_read_requested": False,
        "retention_days": {"audio": 7, "transcript": 30, "summary": 30},
        "policy_version": "minutes-capture.v1",
        "attested_at": None,
        "capture_repair": None,
    }
    assert client.get(
        f"/internal/users/{user_id}/minutes", headers=_internal()
    ).json() == response.json()


def test_enabling_capture_stamps_server_attestation_and_ignores_authority_fields(client):
    user_id, headers = _user(client)
    before = datetime.now().astimezone()
    response = client.put("/user/minutes", headers=headers, json={
        "capture_enabled": True,
        "retention_days": {"audio": 3, "transcript": 60, "summary": 60},
        "operator_enabled": False,
        "policy_version": "attacker-policy",
        "attested_at": "2099-01-01T00:00:00Z",
    })
    after = datetime.now().astimezone()
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["capture_enabled"] is True
    assert body["operator_enabled"] is True
    assert body["policy_version"] == "minutes-capture.v1"
    attested = datetime.fromisoformat(body["attested_at"])
    assert before <= attested <= after
    assert body["retention_days"] == {"audio": 3, "transcript": 60, "summary": 60}
    assert client.get(
        f"/internal/users/{user_id}/minutes", headers=_internal()
    ).json() == body


@pytest.mark.parametrize("retention", [
    {"audio": 0, "transcript": 30, "summary": 90},
    {"audio": True, "transcript": 30, "summary": 90},
    {"audio": 7, "transcript": "30", "summary": 90},
    {"audio": 7, "transcript": 30},
    {"audio": 7, "transcript": 30, "summary": 3651},
    {"audio": 31, "transcript": 30, "summary": 30},
    {"audio": 7, "transcript": 30, "summary": 31},
])
def test_retention_policy_is_complete_and_bounded(client, retention):
    _user_id, headers = _user(client)
    assert client.put(
        "/user/minutes", headers=headers, json={"retention_days": retention}
    ).status_code == 422


def test_operator_flags_cannot_be_bypassed_by_user_preferences(client, monkeypatch):
    _user_id, headers = _user(client)
    monkeypatch.setenv("ZAKI_MINUTES_CAPTURE_ENABLED", "false")
    capture = client.put(
        "/user/minutes", headers=headers, json={"capture_enabled": True}
    )
    assert capture.status_code == 403
    assert client.get("/user/minutes", headers=headers).json()["operator_enabled"] is False

    monkeypatch.setenv("ZAKI_MINUTES_READ_ENABLED", "false")
    read = client.put(
        "/user/minutes", headers=headers, json={"agent_read_enabled": True}
    )
    assert read.status_code == 403
    body = client.get("/user/minutes", headers=headers).json()
    assert body["read_operator_enabled"] is False
    assert body["agent_read_enabled"] is False


def test_disabling_and_reenabling_capture_requires_a_fresh_attestation(client):
    _user_id, headers = _user(client)
    first = client.put(
        "/user/minutes", headers=headers, json={"capture_enabled": True}
    ).json()["attested_at"]
    disabled = client.put(
        "/user/minutes", headers=headers, json={"capture_enabled": False}
    ).json()
    assert disabled["capture_enabled"] is False
    assert disabled["attested_at"] is None
    second = client.put(
        "/user/minutes", headers=headers, json={"capture_enabled": True}
    ).json()["attested_at"]
    assert second is not None and second >= first


def test_retention_change_during_active_capture_refreshes_the_attestation(client):
    _user_id, headers = _user(client)
    first = client.put(
        "/user/minutes", headers=headers, json={"capture_enabled": True}
    ).json()["attested_at"]
    time.sleep(0.01)

    updated = client.put(
        "/user/minutes",
        headers=headers,
        json={"retention_days": {"audio": 3, "transcript": 14, "summary": 14}},
    ).json()

    assert updated["capture_enabled"] is True
    assert updated["attested_at"] > first


def test_policy_change_invalidates_old_capture_attestation_until_user_reconsents(client, monkeypatch):
    _user_id, headers = _user(client)
    initial = client.put(
        "/user/minutes", headers=headers, json={"capture_enabled": True}
    ).json()
    assert initial["capture_enabled"] is True
    assert initial["policy_version"] == "minutes-capture.v1"

    monkeypatch.setattr(main_module, "_MINUTES_POLICY_VERSION", "minutes-capture.v2")
    stale = client.get("/user/minutes", headers=headers).json()
    assert stale["policy_version"] == "minutes-capture.v2"
    assert stale["capture_enabled"] is False
    assert stale["attested_at"] is None
    assert stale["capture_repair"] == "reconsent_required"

    renewed = client.put(
        "/user/minutes", headers=headers, json={"capture_enabled": True}
    ).json()
    assert renewed["capture_enabled"] is True
    assert renewed["policy_version"] == "minutes-capture.v2"
    assert renewed["attested_at"] is not None


def test_internal_minutes_authority_is_secret_protected_and_unknown_is_404(client):
    user_id, _headers = _user(client)
    assert client.get(f"/internal/users/{user_id}/minutes").status_code == 403
    assert client.get(
        f"/internal/users/{user_id}/minutes", headers={"X-Internal-Secret": "wrong"}
    ).status_code == 403
    assert client.get(
        "/internal/users/999999/minutes", headers=_internal()
    ).status_code == 404


def test_expired_user_token_cannot_read_or_change_minutes_preferences(client):
    user_id = client.post(
        "/admin/users", headers=_admin(), json={"email": "expired-minutes@example.com"}
    ).json()["id"]
    token = client.post(
        f"/admin/users/{user_id}/tokens?scopes=bot&expires_in=1", headers=_admin()
    ).json()["token"]
    headers = {"X-API-Key": token}

    time.sleep(1.5)

    read = client.get("/user/minutes", headers=headers)
    write = client.put("/user/minutes", headers=headers, json={"agent_read_enabled": True})
    assert read.status_code == 401
    assert write.status_code == 401
    assert "expired" in read.json()["detail"].lower()
    assert "expired" in write.json()["detail"].lower()
