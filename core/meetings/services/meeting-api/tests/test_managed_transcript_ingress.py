"""Managed invocation.v2 bot ingress never gives an ephemeral bot Redis authority."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from meeting_api import create_app
from meeting_api.bot_spawn.fakes import InMemoryMeetingRepo
from meeting_api.bot_spawn.invocation import mint_meeting_token
from meeting_api.collector.fakes import InMemoryTranscriptStore


SECRET = "managed-transcript-test-secret"
ENDPOINT = "/bots/internal/transcripts/ingest"
FENCE_ENDPOINT = "/bots/internal/transcripts/fence"


class _Bus:
    def __init__(self) -> None:
        self.entries: list[tuple[str, list[dict]]] = []
        self.published: list[tuple[str, str]] = []

    async def xadd_many(self, stream: str, payloads: list[dict]) -> list[str]:
        self.entries.append((stream, payloads))
        return [f"{index}-0" for index, _ in enumerate(payloads, 1)]

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1


class _Fencer:
    def __init__(self) -> None:
        self.calls: list[tuple[int, bool, bool]] = []

    async def __call__(self, meeting_id: int, *, raw: bool, processed: bool) -> None:
        self.calls.append((meeting_id, raw, processed))


def _seed(repo: InMemoryMeetingRepo, store: InMemoryTranscriptStore, *, user_id: int, native: str, session: str) -> int:
    async def seed() -> int:
        expiry = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        row = await repo.create_meeting(
            user_id=user_id,
            platform="google_meet",
            native_meeting_id=native,
            data={
                "zaki_capture": {"state": "authorized"},
                "zaki_retention": {
                    "state": "open",
                    "expired_scopes": [],
                    "scope_expiries": {
                        "audio": expiry,
                        "transcript": expiry,
                        "summary": expiry,
                    },
                },
            },
        )
        await repo.create_session(meeting_id=row["id"], session_uid=session)
        store.seed_meeting(
            meeting_id=row["id"],
            user_id=user_id,
            platform="google_meet",
            native_meeting_id=native,
            data=row["data"],
        )
        return row["id"]

    return asyncio.run(seed())


def _segment(segment_id: str = "sess-a:speaker:1000") -> dict:
    return {
        "segment_id": segment_id,
        "speaker": "Ada",
        "text": "Launch review",
        "start": 1.0,
        "end": 2.0,
        "completed": True,
        "language": "en",
    }


def _client():
    repo = InMemoryMeetingRepo()
    store = InMemoryTranscriptStore()
    bus = _Bus()
    fencer = _Fencer()
    meeting_a = _seed(repo, store, user_id=7, native="aaa-bbbb-ccc", session="sess-a")
    meeting_b = _seed(repo, store, user_id=8, native="ddd-eeee-fff", session="sess-b")
    app = create_app(
        transcript_store=store,
        redis=bus,
        meeting_repo=repo,
        token_secret=SECRET,
        minutes_capture_enabled=True,
        minutes_invocation_v2_enabled=True,
        minutes_settings=lambda _user_id: None,
        minutes_capture_fencer=fencer,
        minutes_hub_token="minutes-hub-service-token-0123456789",
    )
    return TestClient(app), repo, store, bus, fencer, meeting_a, meeting_b


def test_ingress_routes_are_physically_absent_while_minutes_capture_is_off():
    client = TestClient(create_app(token_secret=SECRET))

    assert client.post(ENDPOINT, json={}).status_code == 404
    assert client.post(FENCE_ENDPOINT, json={}).status_code == 404


def _token(meeting_id: int, *, user_id: int, native: str, session: str) -> str:
    return mint_meeting_token(
        meeting_id,
        user_id,
        "google_meet",
        native,
        session_uid=session,
        secret=SECRET,
    )


def test_token_bound_segment_is_ingested_through_the_existing_retention_lease():
    client, _repo, store, bus, _fencer, meeting_a, _meeting_b = _client()
    token = _token(meeting_a, user_id=7, native="aaa-bbbb-ccc", session="sess-a")

    response = client.post(
        ENDPOINT,
        headers={"Authorization": f"Bearer {token}"},
        json={"connection_id": "sess-a", "segment": _segment()},
    )

    assert response.status_code == 202, response.text
    assert response.json() == {"accepted": True}
    assert store._meetings[meeting_a]["segments"]["sess-a:speaker:1000"]["text"] == "Launch review"
    assert bus.entries[0][0] == f"tc:meeting:{meeting_a}"


def test_stolen_token_cannot_select_another_session_or_meeting():
    client, _repo, store, _bus, _fencer, meeting_a, meeting_b = _client()
    token = _token(meeting_a, user_id=7, native="aaa-bbbb-ccc", session="sess-a")

    response = client.post(
        ENDPOINT,
        headers={"Authorization": f"Bearer {token}"},
        json={"connection_id": "sess-b", "segment": _segment("stolen")},
    )

    assert response.status_code == 403
    assert "stolen" not in store._meetings[meeting_a]["segments"]
    assert "stolen" not in store._meetings[meeting_b]["segments"]


def test_signed_token_claiming_the_wrong_meeting_cannot_rebind_an_authoritative_session():
    client, _repo, store, _bus, _fencer, meeting_a, meeting_b = _client()
    token = _token(meeting_b, user_id=8, native="ddd-eeee-fff", session="sess-a")

    response = client.post(
        ENDPOINT,
        headers={"Authorization": f"Bearer {token}"},
        json={"connection_id": "sess-a", "segment": _segment("wrong-meeting")},
    )

    assert response.status_code == 403
    assert "wrong-meeting" not in store._meetings[meeting_a]["segments"]
    assert "wrong-meeting" not in store._meetings[meeting_b]["segments"]


def test_expired_meeting_token_is_rejected_before_any_write(monkeypatch):
    from meeting_api.bot_spawn import invocation

    client, _repo, store, _bus, _fencer, meeting_a, _meeting_b = _client()
    token = _token(meeting_a, user_id=7, native="aaa-bbbb-ccc", session="sess-a")
    real_datetime = datetime

    class FutureDatetime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return real_datetime.now(tz) + timedelta(days=1)

    monkeypatch.setattr(invocation, "datetime", FutureDatetime)
    response = client.post(
        ENDPOINT,
        headers={"Authorization": f"Bearer {token}"},
        json={"connection_id": "sess-a", "segment": _segment("expired")},
    )

    assert response.status_code == 401
    assert "expired" not in store._meetings[meeting_a]["segments"]


def test_ingress_rejects_missing_auth_unknown_fields_and_oversized_bodies_before_writes():
    client, _repo, store, _bus, _fencer, meeting_a, _meeting_b = _client()
    token = _token(meeting_a, user_id=7, native="aaa-bbbb-ccc", session="sess-a")

    missing = client.post(ENDPOINT, json={"connection_id": "sess-a", "segment": _segment()})
    unknown = client.post(
        ENDPOINT,
        headers={"Authorization": f"Bearer {token}"},
        json={"connection_id": "sess-a", "segment": _segment(), "meeting_id": meeting_a},
    )
    oversized = client.post(
        ENDPOINT,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        content=b"{" + b"x" * (64 * 1024) + b"}",
    )
    unauthenticated_oversized = client.post(
        ENDPOINT,
        headers={"Content-Type": "application/json"},
        content=b"{" + b"x" * (64 * 1024) + b"}",
    )

    assert missing.status_code == 401
    assert unknown.status_code == 422
    assert oversized.status_code == 413
    assert unauthenticated_oversized.status_code == 401
    assert store._meetings[meeting_a]["segments"] == {}


def test_ingress_is_idempotent_by_segment_identity_and_retention_withdrawal_wins():
    client, _repo, store, _bus, _fencer, meeting_a, _meeting_b = _client()
    token = _token(meeting_a, user_id=7, native="aaa-bbbb-ccc", session="sess-a")
    request = {
        "headers": {"Authorization": f"Bearer {token}"},
        "json": {"connection_id": "sess-a", "segment": _segment("stable-segment")},
    }

    assert client.post(ENDPOINT, **request).status_code == 202
    assert client.post(ENDPOINT, **request).status_code == 202
    assert list(store._meetings[meeting_a]["segments"]) == ["stable-segment"]

    store._meetings[meeting_a]["data"]["zaki_capture"]["state"] = "withdrawn"
    refused = client.post(
        ENDPOINT,
        headers=request["headers"],
        json={"connection_id": "sess-a", "segment": _segment("too-late")},
    )
    assert refused.status_code == 409
    assert "too-late" not in store._meetings[meeting_a]["segments"]


def test_retention_fence_is_token_bound_and_never_accepts_body_selected_ids():
    client, _repo, _store, _bus, fencer, meeting_a, _meeting_b = _client()
    token = _token(meeting_a, user_id=7, native="aaa-bbbb-ccc", session="sess-a")

    response = client.post(
        FENCE_ENDPOINT,
        headers={"Authorization": f"Bearer {token}"},
        json={"connection_id": "sess-a", "raw": True, "processed": True},
    )

    assert response.status_code == 200, response.text
    assert response.json() == {"fenced": True}
    assert fencer.calls == [(meeting_a, True, True)]

    selected = client.post(
        FENCE_ENDPOINT,
        headers={"Authorization": f"Bearer {token}"},
        json={
            "connection_id": "sess-a",
            "meeting_id": meeting_a,
            "raw": True,
            "processed": True,
        },
    )
    assert selected.status_code == 422
    assert fencer.calls == [(meeting_a, True, True)]
