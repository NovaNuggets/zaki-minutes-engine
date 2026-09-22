"""Stage 2 (P20) — the gateway fronts the AGENT control plane (agent-api) under /agent/*.

Proves, in isolation from agent-api (injected FakeDownstream), the load-bearing guarantees:
  * fail-closed auth — a keyless agent call is 401 BEFORE any downstream hop;
  * the request is forwarded to agent-api's matching /api/<path> (path + method verbatim);
  * X-User-Id is injected from the RESOLVED key — never trusted from the client (anti-spoof),
    so agent-api (Stage 1) scopes `subject` to the authenticated user;
  * chat (SSE) is STREAMED, not buffered, and carries the same injected identity.
This is the seam every agent surface (sessions · routines · workspace · chat) rides for per-user scope.
"""
import hashlib

import pytest
from fastapi.testclient import TestClient

from gateway import create_app
from conftest import VALID_KEY, FakeAuthorizer, FakeDownstream, FakeRedis

AUTH = {"x-api-key": VALID_KEY}
GATEWAY_PROOF = "test-gateway-proof-0123456789abcdef"
AGENT_USER = {
    "user_id": 7,
    "scopes": ["agent"],
    "max_concurrent": 3,
    "email": "u@example.com",
}


def _client(downstream=None, *, authorizer=None):
    downstream = downstream or FakeDownstream(status_code=200, body={"sessions": []})
    app = create_app(
        authorizer or FakeAuthorizer(user=AGENT_USER),
        downstream,
        FakeRedis(),
        agent_api_url="http://agent-api",
        gateway_identity_secret=GATEWAY_PROOF,
    )
    return TestClient(app), downstream


@pytest.mark.parametrize(("method", "path"), [
    ("GET", "/agent/sessions"),
    ("POST", "/agent/chat"),
    ("GET", "/agent/meeting/stream"),
])
def test_bot_only_token_is_rejected_by_buffered_and_streaming_agent_routes(method, path):
    bot_only = FakeAuthorizer(user={"user_id": 7, "scopes": ["bot"], "max_concurrent": 3})
    client, downstream = _client(authorizer=bot_only)

    response = client.request(method, path, headers=AUTH, json={} if method == "POST" else None)

    assert response.status_code == 403
    assert response.json()["detail"] == "Insufficient scope for this endpoint"
    assert downstream.last is None


def test_keyless_agent_call_is_401_before_downstream():
    client, downstream = _client()
    r = client.get("/agent/sessions")
    assert r.status_code == 401
    assert r.json()["detail"] == "Missing API key"
    assert downstream.last is None, "must reject before any downstream hop"


def test_invalid_key_is_401():
    client, _ = _client()
    r = client.get("/agent/sessions", headers={"x-api-key": "nope"})
    assert r.status_code == 401
    assert r.json()["detail"] == "Invalid API key"


def test_agent_scoped_route_forwards_to_agent_api_with_injected_user():
    """An agent-only token reaches /api/sessions with its resolved user authority."""
    client, downstream = _client(FakeDownstream(status_code=200, body={"sessions": [{"id": "s1"}]}))
    r = client.get("/agent/sessions", headers=AUTH)
    assert r.status_code == 200
    assert r.json() == {"sessions": [{"id": "s1"}]}
    assert downstream.last["url"] == "http://agent-api/api/sessions"
    assert downstream.last["headers"]["x-user-id"] == "7"  # the resolved user, not the client's
    assert downstream.last["headers"]["x-user-email"] == "u@example.com"
    assert "x-api-key" not in downstream.last["headers"]
    for unused in (
        "x-user-scopes", "x-user-limits", "x-user-workspaces",
        "x-user-webhook-url", "x-user-webhook-secret", "x-user-webhook-events",
    ):
        assert unused not in downstream.last["headers"]
    assert "x-gateway-verified" not in downstream.last["headers"]
    assert downstream.last["headers"]["x-gateway-key-id"]
    assert downstream.last["headers"]["x-gateway-timestamp"]
    assert downstream.last["headers"]["x-gateway-nonce"]
    assert downstream.last["headers"]["x-gateway-content-sha256"] == hashlib.sha256(b"").hexdigest()
    assert len(downstream.last["headers"]["x-gateway-signature"]) == 64
    assert GATEWAY_PROOF not in downstream.last["headers"].values()


def test_buffered_agent_route_preserves_retryable_replay_store_outage():
    client, downstream = _client(FakeDownstream(
        status_code=503,
        body={"detail": "gateway identity boundary is unavailable"},
        response_headers={"retry-after": "1"},
    ))

    response = client.get("/agent/sessions", headers=AUTH)

    assert downstream.last is not None
    assert response.status_code == 503
    assert response.json() == {"detail": "gateway identity boundary is unavailable"}
    assert response.headers["retry-after"] == "1"
    assert response.headers["cache-control"] == "no-store"


def test_nested_agent_path_and_query_carry_through():
    """Workspace tree is a nested path with a query — both forward verbatim under /api/."""
    client, downstream = _client()
    client.get("/agent/workspace/tree?hidden=1", headers=AUTH)
    assert downstream.last["url"] == "http://agent-api/api/workspace/tree"
    assert downstream.last["params"] == "hidden=1"
    assert downstream.last["headers"]["x-user-id"] == "7"


def test_agent_query_is_normalized_once_and_preserves_duplicate_value_order():
    client, downstream = _client()

    response = client.get(
        "/agent/sessions?name=hello%20world&tag=b&tag=a",
        headers=AUTH,
    )

    assert response.status_code == 200
    assert downstream.last["params"] == "name=hello+world&tag=b&tag=a"


def test_client_supplied_user_id_is_stripped_then_reinjected():
    """A spoofed X-User-Id is dropped; the gateway re-injects the RESOLVED user (anti-spoof)."""
    client, downstream = _client()
    client.post("/agent/routines", headers={**AUTH, "x-user-id": "999"}, json={"name": "x"})
    assert downstream.last["headers"]["x-user-id"] == "7"


def test_client_supplied_gateway_proof_headers_are_stripped_then_replaced():
    client, downstream = _client()
    client.get("/agent/sessions", headers={
        **AUTH,
        "x-gateway-verified": "attacker-proof",
        "x-gateway-key-id": "attacker-key",
        "x-gateway-timestamp": "0",
        "x-gateway-nonce": "attacker-nonce-0123456789",
        "x-gateway-content-sha256": "b" * 64,
        "x-gateway-signature": "a" * 64,
    })
    headers = downstream.last["headers"]
    assert "x-gateway-verified" not in headers
    assert headers["x-gateway-key-id"] != "attacker-key"
    assert headers["x-gateway-timestamp"] != "0"
    assert headers["x-gateway-nonce"] != "attacker-nonce-0123456789"
    assert headers["x-gateway-content-sha256"] != "b" * 64
    assert headers["x-gateway-signature"] != "a" * 64


def test_agent_proxy_fails_closed_without_gateway_proof_configuration():
    downstream = FakeDownstream(status_code=200, body={"sessions": []})
    app = create_app(
        FakeAuthorizer(user=AGENT_USER), downstream, FakeRedis(), agent_api_url="http://agent-api",
        gateway_identity_secret="",
    )
    response = TestClient(app).get("/agent/sessions", headers=AUTH)
    assert response.status_code == 503
    assert downstream.last is None


def test_agent_write_methods_forward():
    """POST/PUT/DELETE reach agent-api too (routine enable, session reset, workspace write)."""
    client, downstream = _client()
    client.put("/agent/routines/daily/enabled", headers=AUTH, json={"enabled": True})
    assert downstream.last["method"] == "PUT"
    assert downstream.last["url"] == "http://agent-api/api/routines/daily/enabled"


def test_agent_patch_method_forwards():
    """PATCH reaches agent-api too — the Routines surface toggles enable/disable via PATCH
    (routinesApi.setRoutineEnabled), and agent-api defines @app.patch(.../enabled). Regression:
    PATCH was absent from the proxy's methods list, so the toggle 405'd before any downstream hop."""
    client, downstream = _client()
    client.patch("/agent/routines/daily/enabled", headers=AUTH, json={"enabled": False})
    assert downstream.last["method"] == "PATCH"
    assert downstream.last["url"] == "http://agent-api/api/routines/daily/enabled"
    assert downstream.last["headers"]["x-user-id"] == "7"  # resolved user, injected downstream


def test_unicode_agent_path_gets_a_valid_request_proof_instead_of_503():
    client, downstream = _client()

    response = client.patch(
        "/agent/routines/caf%C3%A9/enabled",
        headers=AUTH,
        json={"enabled": False},
    )

    assert response.status_code == 200
    assert downstream.last["url"] == "http://agent-api/api/routines/café/enabled"
    assert downstream.last["headers"]["x-gateway-signature"]


def test_agent_proof_is_minted_only_after_the_request_body_is_drained(monkeypatch):
    import gateway.app as gateway_app

    body_drained = False
    real_signer = gateway_app.GatewayIdentitySigner

    class OrderingSigner(real_signer):
        def headers(self, **kwargs):
            assert body_drained, "proof was minted before the request body was drained"
            return super().headers(**kwargs)

    monkeypatch.setattr(gateway_app, "GatewayIdentitySigner", OrderingSigner)
    client, downstream = _client()

    def chunks():
        nonlocal body_drained
        yield b'{"enabled":'
        body_drained = True
        yield b"false}"

    response = client.patch(
        "/agent/routines/daily/enabled",
        headers=AUTH,
        content=chunks(),
    )

    assert response.status_code == 200
    assert downstream.last["content"] == b'{"enabled":false}'


def test_chat_is_streamed_not_buffered_with_injected_user():
    """POST /api/chat returns an SSE stream (text/event-stream), relays the downstream chunks, and
    carries the injected X-User-Id (so the streamed turn is scoped to the authenticated user)."""
    client, downstream = _client(FakeDownstream(stream_chunks=[
        b'data: {"type":"token","text":"he"}\n\n',
        b'data: {"type":"token","text":"llo"}\n\n',
        b'data: {"type":"done"}\n\n',
    ]))
    r = client.post("/agent/chat", headers=AUTH, json={"prompt": "hi", "session": "s1"})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")
    assert r.headers["cache-control"] == "no-store"
    body = r.text
    assert '"text":"he"' in body and '"text":"llo"' in body and '"type":"done"' in body
    assert downstream.last["url"] == "http://agent-api/api/chat"
    assert downstream.last["headers"]["x-user-id"] == "7"


def test_chat_keyless_is_401():
    client, downstream = _client()
    r = client.post("/agent/chat", json={"prompt": "hi"})
    assert r.status_code == 401
    assert downstream.last is None


@pytest.mark.parametrize(("status", "retry_after"), [(401, None), (503, "1")])
def test_chat_preserves_agent_auth_and_replay_boundary_failures(status, retry_after):
    downstream = FakeDownstream(
        status_code=status,
        stream_chunks=[b'{"detail":"gateway identity boundary failed"}'],
        stream_headers={
            "content-type": "application/json",
            **({"retry-after": retry_after} if retry_after else {}),
        },
    )
    client, _ = _client(downstream)

    response = client.post("/agent/chat", headers=AUTH, json={"prompt": "hello"})

    assert response.status_code == status
    assert response.json() == {"detail": "gateway identity boundary failed"}
    assert response.headers["content-type"].startswith("application/json")
    assert response.headers["cache-control"] == "no-store"
    if retry_after:
        assert response.headers["retry-after"] == retry_after
    else:
        assert "retry-after" not in response.headers
    assert downstream.stream_response is not None
    assert downstream.stream_response.closed is True


def test_chat_rejects_chunked_body_above_gateway_cap(monkeypatch):
    """The SSE route shares the buffered request-body budget with ordinary proxy routes."""
    import gateway.app as gateway_app

    monkeypatch.setattr(gateway_app, "MAX_PROXY_BODY_BYTES", 8)
    client, downstream = _client()

    response = client.post(
        "/agent/chat",
        headers=AUTH,
        content=iter((b"12345", b"67890")),
    )

    assert response.status_code == 413
    assert response.json()["detail"] == "request body too large"
    assert downstream.last is None


def test_meeting_stream_is_streamed_with_injected_user():
    """GET /api/meeting/stream (the live transcript+copilot SSE) is streamed (not buffered by the
    catch-all) and carries the injected X-User-Id, with its query (meeting_id/session_uid) forwarded."""
    client, downstream = _client(FakeDownstream(stream_chunks=[
        b'data: {"type":"transcript","text":"hello"}\n\n',
        b'data: {"type":"copilot","text":"note"}\n\n',
    ]))
    r = client.get("/agent/meeting/stream?meeting_id=41&session_uid=41", headers=AUTH)
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")
    assert '"type":"transcript"' in r.text and '"type":"copilot"' in r.text
    assert downstream.last["url"] == "http://agent-api/api/meeting/stream"
    assert downstream.last["params"] == "meeting_id=41&session_uid=41"
    assert downstream.last["headers"]["x-user-id"] == "7"
    assert downstream.last["headers"]["x-gateway-signature"]


def test_verified_email_injected_and_spoof_stripped():
    """Lane M / AMENDMENT 5: the gateway injects the RESOLVED verified email as x-user-email (never the
    client's) — agent-api's restricted-invite redeem checks it."""
    client, downstream = _client()
    client.post("/agent/workspace/invites/accept",
                headers={**AUTH, "x-user-email": "attacker@evil.com"}, json={"token": "t"})
    assert downstream.last["headers"]["x-user-email"] == "u@example.com"  # resolved, not the spoof
