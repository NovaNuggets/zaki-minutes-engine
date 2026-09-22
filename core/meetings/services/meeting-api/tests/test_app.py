"""The unified meeting-api app — ``create_app`` composes the front-doored modules onto ONE app.

Proves the modular-monolith assembly (P2): the single ``create_app`` mounts lifecycle + bot_spawn +
collector + recordings onto one FastAPI app, answers the shared ``/health``, and each module's core
route is reachable on that one app (driven over the default in-memory stack — no DB / redis / MinIO /
runtime kernel).
"""
from __future__ import annotations

from fastapi.testclient import TestClient

from meeting_api import create_app
from meeting_api.bot_spawn.fakes import InMemoryMeetingRepo
from meeting_api.collector.fakes import InMemoryTranscriptStore

USER = 7
HEADERS = {"x-user-id": str(USER)}


def test_create_app_health():
    client = TestClient(create_app())
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["service"] == "meeting-api"


def test_unified_app_mounts_every_module_route():
    """Every module's core route is reachable on the ONE app (the routing table is composed)."""
    store = InMemoryTranscriptStore()
    store.seed_meeting(user_id=USER, platform="google_meet", native_meeting_id="abc-defg-hij")
    client = TestClient(create_app(transcript_store=store))

    # Each module's core route is MOUNTED (resolves to a handler — never 404). One app, one router
    # table: lifecycle + bot_spawn + collector + recordings.
    mounted = [
        ("POST", "/bots", {}),                                 # bot_spawn
        ("POST", "/bots/internal/callback/lifecycle", {"foo": "bar"}),  # lifecycle
        ("GET", "/transcripts/google_meet/abc-defg-hij", None),  # collector
        ("GET", "/meetings", None),                            # collector
        ("POST", "/ws/authorize-subscribe", {}),               # collector
        ("GET", "/recordings", None),                          # recordings
    ]
    for method, path, body in mounted:
        r = client.request(method, path, json=body)
        assert r.status_code != 404, f"{method} {path} not mounted"
    # The recordings upload route is multipart — assert it is mounted (415/422, not 404).
    assert client.post("/internal/recordings/upload").status_code != 404

    # And a collector route actually serves data through the unified app.
    r = client.get("/meetings", headers=HEADERS)
    assert r.status_code == 200
    assert any(m["native_meeting_id"] == "abc-defg-hij" for m in r.json()["meetings"])


def test_internal_row_owner_lookup_is_secret_protected_and_minimal(monkeypatch):
    """The agent watcher may learn only the authoritative owner of one exact numeric row."""
    monkeypatch.setenv("INTERNAL_API_SECRET", "owner-edge-secret")
    store = InMemoryTranscriptStore()
    store.seed_meeting(
        meeting_id=42,
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        data={"private": "must not cross the owner edge"},
    )
    client = TestClient(create_app(transcript_store=store))

    denied = client.get("/internal/meetings/42/owner")
    assert denied.status_code == 403
    assert client.get(
        "/internal/meetings/42/owner",
        headers={"X-Internal-Secret": "wrong"},
    ).status_code == 403

    found = client.get(
        "/internal/meetings/42/owner",
        headers={"X-Internal-Secret": "owner-edge-secret"},
    )
    assert found.status_code == 200
    assert found.json() == {"meeting_id": "42", "user_id": str(USER)}
    assert client.get(
        "/internal/meetings/99/owner",
        headers={"X-Internal-Secret": "owner-edge-secret"},
    ).status_code == 404


def test_internal_row_owner_lookup_fails_closed_without_server_secret(monkeypatch):
    monkeypatch.delenv("INTERNAL_API_SECRET", raising=False)
    store = InMemoryTranscriptStore()
    store.seed_meeting(
        meeting_id=42,
        user_id=USER,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
    )

    response = TestClient(create_app(transcript_store=store)).get(
        "/internal/meetings/42/owner",
        headers={"X-Internal-Secret": "anything"},
    )

    assert response.status_code == 503


def test_internal_row_owner_lookup_roundtrips_adjacent_large_ids_as_decimal_text(monkeypatch):
    monkeypatch.setenv("INTERNAL_API_SECRET", "owner-edge-secret")
    meeting_id = 9_007_199_254_740_993
    user_id = 9_007_199_254_740_992
    store = InMemoryTranscriptStore()
    store.seed_meeting(
        meeting_id=meeting_id,
        user_id=user_id,
        platform="google_meet",
        native_meeting_id="large-row",
    )

    response = TestClient(create_app(transcript_store=store)).get(
        f"/internal/meetings/{meeting_id}/owner",
        headers={"X-Internal-Secret": "owner-edge-secret"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "meeting_id": "9007199254740993",
        "user_id": "9007199254740992",
    }


def test_internal_meeting_doc_link_targets_exact_row_across_tenants_and_reused_links(monkeypatch):
    """A watcher callback names only the numeric row; meeting-api derives its owner/native doc."""
    monkeypatch.setenv("INTERNAL_API_SECRET", "doc-edge-secret")
    store = InMemoryTranscriptStore()
    shared_native = "same-link-aaa"
    tenant_a = store.seed_meeting(
        meeting_id=41,
        user_id=7,
        platform="google_meet",
        native_meeting_id=shared_native,
        created_at="2026-07-15T09:00:00Z",
    )
    tenant_b_older = store.seed_meeting(
        meeting_id=42,
        user_id=8,
        platform="google_meet",
        native_meeting_id=shared_native,
        created_at="2026-07-15T09:01:00Z",
    )
    tenant_b_newer = store.seed_meeting(
        meeting_id=43,
        user_id=8,
        platform="google_meet",
        native_meeting_id=shared_native,
        created_at="2026-07-15T09:02:00Z",
    )
    client = TestClient(create_app(transcript_store=store))

    response = client.post(
        f"/internal/meetings/{tenant_b_older}/docs",
        headers={"X-Internal-Secret": "doc-edge-secret"},
    )

    assert response.status_code == 200, response.text
    expected_doc = {
        "workspace": "8",
        "path": f"kg/entities/meeting/{tenant_b_older}.md",
        "title": f"Meeting {tenant_b_older}",
        "kind": "meeting",
    }
    assert response.json() == {"meeting_id": str(tenant_b_older), "doc": expected_doc}
    assert client.get(f"/meetings/{tenant_b_older}", headers={"X-User-Id": "8"}).json()["data"]["docs"] == [expected_doc]
    assert client.get(f"/meetings/{tenant_b_newer}", headers={"X-User-Id": "8"}).json()["data"].get("docs", []) == []
    assert client.get(f"/meetings/{tenant_a}", headers={"X-User-Id": "7"}).json()["data"].get("docs", []) == []


def test_internal_meeting_doc_link_fails_closed_after_summary_authority_is_withdrawn(monkeypatch):
    monkeypatch.setenv("INTERNAL_API_SECRET", "doc-edge-secret")
    store = InMemoryTranscriptStore()
    store.seed_meeting(
        meeting_id=42,
        user_id=7,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        data={"zaki_capture": {"state": "withdrawn"}},
    )
    client = TestClient(create_app(transcript_store=store))

    response = client.post(
        "/internal/meetings/42/docs",
        headers={"X-Internal-Secret": "doc-edge-secret"},
    )

    assert response.status_code == 404
    assert store._meetings[42]["data"].get("docs", []) == []


def test_post_bots_on_unified_app(monkeypatch):
    monkeypatch.setenv("MEETING_TOKEN_SECRET", "test-admin-token")
    client = TestClient(create_app())
    r = client.post("/bots", headers=HEADERS,
                    json={"platform": "google_meet", "native_meeting_id": "abc-defg-hij"})
    assert r.status_code == 201, r.text
    assert r.json()["status"] == "requested"


def test_lifecycle_callback_on_unified_app():
    """The lifecycle receiver's callback advances the FSM on the shared app + store."""
    import json
    from pathlib import Path

    # Load a lifecycle.v1 golden by path (the seam).
    for parent in Path(__file__).resolve().parents:
        gdir = parent / "meetings" / "contracts" / "lifecycle.v1" / "golden"
        if gdir.is_dir():
            break
    events = sorted(gdir.glob("LifecycleEvent.*.json"))
    assert events, "expected lifecycle.v1 goldens"
    event = json.loads(events[0].read_text())

    client = TestClient(create_app())
    r = client.post("/bots/internal/callback/lifecycle", json=event)
    assert r.status_code in (200, 409), r.text  # accepted, or a legal-transition rejection


def test_unified_callbacks_use_separate_spawn_and_runtime_credentials():
    import asyncio

    from meeting_api.bot_spawn import mint_meeting_token

    repo = InMemoryMeetingRepo()
    meeting = asyncio.run(
        repo.create_meeting(
            user_id=1,
            platform="google_meet",
            native_meeting_id="authenticated",
            data={},
        )
    )
    asyncio.run(
        repo.create_session(meeting_id=meeting["id"], session_uid="authenticated")
    )
    client = TestClient(
        create_app(
            meeting_repo=repo,
            token_secret="meeting-token-secret",
            runtime_callback_secret="runtime-callback-secret",
        )
    )
    lifecycle_token = mint_meeting_token(
        meeting["id"],
        1,
        "google_meet",
        "authenticated",
        session_uid="authenticated",
        secret="meeting-token-secret",
    )

    missing = client.post("/bots/internal/callback/lifecycle", content=b"not-json")
    wrong = client.post(
        "/bots/internal/callback/lifecycle",
        headers={"X-Internal-Secret": "wrong"},
        content=b"not-json",
    )
    accepted = client.post(
        "/bots/internal/callback/lifecycle",
        headers={"Authorization": f"Bearer {lifecycle_token}"},
        json={"connection_id": "authenticated", "status": "joining"},
    )

    assert missing.status_code == 403
    assert wrong.status_code == 403
    assert accepted.status_code == 200

    runtime_missing = client.post("/runtime/callback", content=b"not-json")
    runtime_wrong = client.post(
        "/runtime/callback",
        headers={"X-Runtime-Callback-Secret": "wrong"},
        content=b"not-json",
    )
    runtime_internal = client.post(
        "/runtime/callback",
        headers={"X-Internal-Secret": "platform-internal-secret"},
        json={
            "workloadId": "unknown",
            "state": "destroyed",
            "at": "2026-07-15T12:00:00Z",
        },
    )
    runtime_accepted = client.post(
        "/runtime/callback",
        headers={"X-Runtime-Callback-Secret": "runtime-callback-secret"},
        json={
            "workloadId": "unknown",
            "state": "destroyed",
            "at": "2026-07-15T12:00:00Z",
        },
    )

    assert runtime_missing.status_code == 403
    assert runtime_wrong.status_code == 403
    assert runtime_internal.status_code == 403
    assert runtime_accepted.status_code == 200


def test_runtime_callback_rejects_malformed_or_off_contract_events_before_ack():
    client = TestClient(create_app(runtime_callback_secret="runtime-callback-secret"))
    headers = {
        "X-Runtime-Callback-Secret": "runtime-callback-secret",
        "Content-Type": "application/json",
    }

    malformed = client.post(
        "/runtime/callback",
        headers=headers,
        content=b'{"workloadId":"wl-1",',
    )
    missing_required = client.post(
        "/runtime/callback",
        headers=headers,
        json={"workloadId": "wl-1", "state": "destroyed"},
    )
    invalid_state = client.post(
        "/runtime/callback",
        headers=headers,
        json={
            "workloadId": "wl-1",
            "state": "gone",
            "at": "2026-07-15T12:00:00Z",
        },
    )
    invalid_timestamp = client.post(
        "/runtime/callback",
        headers=headers,
        json={"workloadId": "wl-1", "state": "destroyed", "at": "not-a-date"},
    )

    assert malformed.status_code == 400
    assert missing_required.status_code == 400
    assert invalid_state.status_code == 400
    assert invalid_timestamp.status_code == 400


def test_unified_lifecycle_meeting_token_is_bound_to_session_and_authoritative_meeting():
    import asyncio

    from meeting_api.bot_spawn import mint_meeting_token

    repo = InMemoryMeetingRepo()
    first = asyncio.run(
        repo.create_meeting(user_id=1, platform="google_meet", native_meeting_id="first", data={})
    )
    second = asyncio.run(
        repo.create_meeting(user_id=2, platform="google_meet", native_meeting_id="second", data={})
    )
    asyncio.run(repo.create_session(meeting_id=first["id"], session_uid="session-first"))
    asyncio.run(repo.create_session(meeting_id=first["id"], session_uid="session-sibling"))
    asyncio.run(repo.create_session(meeting_id=second["id"], session_uid="session-second"))
    app = create_app(
        meeting_repo=repo,
        token_secret="meeting-token-secret",
        runtime_callback_secret="runtime-callback-secret",
    )
    client = TestClient(app)
    valid = mint_meeting_token(
        first["id"],
        1,
        "google_meet",
        "first",
        session_uid="session-first",
        secret="meeting-token-secret",
    )

    accepted = client.post(
        "/bots/internal/callback/lifecycle",
        headers={"Authorization": f"Bearer {valid}"},
        json={"connection_id": "session-first", "status": "joining"},
    )
    cross_session = client.post(
        "/bots/internal/callback/lifecycle",
        headers={"Authorization": f"Bearer {valid}"},
        json={"connection_id": "session-sibling", "status": "joining"},
    )
    cross_meeting = mint_meeting_token(
        first["id"],
        1,
        "google_meet",
        "first",
        session_uid="session-second",
        secret="meeting-token-secret",
    )
    rejected_meeting = client.post(
        "/bots/internal/callback/lifecycle",
        headers={"Authorization": f"Bearer {cross_meeting}"},
        json={"connection_id": "session-second", "status": "joining"},
    )

    assert accepted.status_code == 200, accepted.text
    assert cross_session.status_code == 403
    assert rejected_meeting.status_code == 403
    assert repo._meetings[first["id"]]["status"] == "joining"
    assert repo._meetings[second["id"]]["status"] == "requested"


def test_unified_authenticated_malformed_lifecycle_json_is_a_bounded_400():
    from meeting_api.bot_spawn import mint_meeting_token

    token = mint_meeting_token(
        1,
        1,
        "google_meet",
        "malformed",
        session_uid="sess-uid",
        secret="meeting-token-secret",
    )
    client = TestClient(create_app(token_secret="meeting-token-secret"))

    response = client.post(
        "/bots/internal/callback/lifecycle",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        content=b'{"connection_id":"sess-uid",',
    )

    assert response.status_code == 400
    assert response.json() == {"status": "error", "detail": "malformed JSON body"}
