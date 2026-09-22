"""Public gateway coverage for the managed Minutes control-plane edges."""

import json

from fastapi.testclient import TestClient

from conftest import VALID_KEY, FakeAuthorizer, FakeDownstream, FakeRedis
from gateway import create_app


AUTH = {"x-api-key": VALID_KEY}


def _client(downstream=None):
    downstream = downstream or FakeDownstream(status_code=200, body={"ok": True})
    app = create_app(FakeAuthorizer(), downstream, FakeRedis())
    return TestClient(app), downstream


def test_user_minutes_config_get_and_put_forward_to_admin_with_user_authority():
    client, downstream = _client()

    get_response = client.get("/user/minutes", headers=AUTH)
    assert get_response.status_code == 200
    assert downstream.last["method"] == "GET"
    assert downstream.last["url"] == "http://admin-api/user/minutes"
    assert downstream.last["headers"]["x-user-id"] == "7"
    assert downstream.last["headers"]["x-api-key"] == VALID_KEY
    assert get_response.headers["cache-control"] == "no-store"

    put_response = client.put(
        "/user/minutes",
        headers={**AUTH, "x-user-id": "999"},
        json={"capture_enabled": True, "retention_days": 30},
    )
    assert put_response.status_code == 200
    assert downstream.last["method"] == "PUT"
    assert downstream.last["url"] == "http://admin-api/user/minutes"
    assert downstream.last["headers"]["x-user-id"] == "7"
    assert json.loads(downstream.last["content"]) == {
        "capture_enabled": True,
        "retention_days": 30,
    }


def test_minutes_capture_post_forwards_to_meeting_api_and_enforces_body_cap(monkeypatch):
    import gateway.app as gateway_app

    monkeypatch.setattr(gateway_app, "MAX_PROXY_BODY_BYTES", 16)
    client, downstream = _client()

    accepted = client.post("/minutes/captures", headers=AUTH, json={})
    assert accepted.status_code == 200
    assert accepted.headers["cache-control"] == "no-store"
    assert downstream.last["method"] == "POST"
    assert downstream.last["url"] == "http://meeting-api/minutes/captures"
    assert downstream.last["headers"]["x-user-id"] == "7"
    assert downstream.last["headers"]["x-user-limits"] == "3"
    accepted_forward = downstream.last

    rejected = client.post(
        "/minutes/captures",
        headers=AUTH,
        content=iter((b"1234567890", b"abcdefghij")),
    )
    assert rejected.status_code == 413
    assert rejected.json()["detail"] == "request body too large"
    assert downstream.last is accepted_forward


def test_minutes_withdrawal_edges_forward_to_meeting_api_with_user_authority():
    client, downstream = _client()

    for public_path, downstream_path in (
        (
            "/minutes/captures/google_meet/abc-defg-hij",
            "/minutes/captures/google_meet/abc-defg-hij",
        ),
        ("/minutes/meetings/42", "/minutes/meetings/42"),
    ):
        response = client.delete(public_path, headers=AUTH)
        assert response.status_code == 200
        assert response.headers["cache-control"] == "no-store"
        assert downstream.last["method"] == "DELETE"
        assert downstream.last["url"] == f"http://meeting-api{downstream_path}"
        assert downstream.last["headers"]["x-user-id"] == "7"


def test_minutes_status_forwards_the_canonical_row_to_meeting_api():
    client, downstream = _client()

    response = client.get("/minutes/meetings/42/status", headers=AUTH)

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert downstream.last["method"] == "GET"
    assert downstream.last["url"] == "http://meeting-api/minutes/meetings/42/status"
    assert downstream.last["headers"]["x-user-id"] == "7"
