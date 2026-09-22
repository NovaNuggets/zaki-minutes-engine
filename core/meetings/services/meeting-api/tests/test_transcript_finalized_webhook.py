"""Durable finalization is independent of the operator-only platform event."""
from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

from meeting_api import create_app
from meeting_api.bot_spawn.fakes import InMemoryMeetingRepo
from meeting_api.webhooks import DeliveryResult


class _Finalizer:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls: list[int] = []

    async def __call__(self, meeting_id: int):
        self.calls.append(meeting_id)
        if self.fail:
            raise RuntimeError("durable transcript unavailable")


class _Sink:
    def __init__(self, finalizer: _Finalizer):
        self.finalizer = finalizer
        self.events: list[dict] = []

    async def deliver(self, _url, envelope, _secret=None, **_kwargs):
        self.events.append(envelope)
        return DeliveryResult(status="delivered", status_code=200)


def _stack(*, fail: bool = False):
    repo = InMemoryMeetingRepo()
    meeting = asyncio.run(repo.create_meeting(
        user_id=7,
        platform="google_meet",
        native_meeting_id="minutes-finalized",
        data={
            "webhook_url": "https://hooks.example.test/minutes",
            "webhook_events": {
                "meeting.status_change": True,
                "meeting.started": True,
                "meeting.completed": True,
                "transcript.finalized": True,
            },
        },
    ))
    asyncio.run(repo.create_session(meeting_id=meeting["id"], session_uid="finalized-session"))
    finalizer = _Finalizer(fail=fail)
    sink = _Sink(finalizer)
    client = TestClient(create_app(
        meeting_repo=repo,
        transcript_finalizer=finalizer,
        webhook_sink=sink,
    ))
    return client, meeting["id"], finalizer, sink


def _complete(client: TestClient):
    for event in (
        {"connection_id": "finalized-session", "status": "joining"},
        {"connection_id": "finalized-session", "status": "active"},
        {
            "connection_id": "finalized-session",
            "status": "completed",
            "completion_reason": "stopped",
        },
    ):
        response = client.post("/bots/internal/callback/lifecycle", json=event)
        assert response.status_code == 200, response.text


def test_durable_finalization_does_not_publish_an_operator_event_without_composition():
    client, meeting_id, finalizer, sink = _stack()

    _complete(client)

    assert finalizer.calls == [meeting_id]
    assert client.app.state.transcript_finalized_webhooks == []
    assert [item["event_type"] for item in sink.events][-2:] == [
        "meeting.status_change",
        "meeting.completed",
    ]

    # A retry of the terminal callback is an acknowledged no-op, not a second finalize/event.
    replay = client.post(
        "/bots/internal/callback/lifecycle",
        json={
            "connection_id": "finalized-session",
            "status": "completed",
            "completion_reason": "stopped",
        },
    )
    assert replay.status_code == 200
    assert finalizer.calls == [meeting_id]
    assert client.app.state.transcript_finalized_webhooks == []


def test_finalized_event_is_suppressed_when_durable_finalization_fails():
    client, meeting_id, finalizer, sink = _stack(fail=True)

    _complete(client)

    assert finalizer.calls == [meeting_id]
    assert client.app.state.transcript_finalized_webhooks == []
    assert "transcript.finalized" not in [event["event_type"] for event in sink.events]
