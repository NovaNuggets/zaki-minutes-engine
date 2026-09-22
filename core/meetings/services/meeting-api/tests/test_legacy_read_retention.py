"""Request-time privacy gates for the ordinary Vexa collector read plane.

Managed Minutes rows share the legacy collector tables, but consent withdrawal and retention
remain authoritative on every read.  Untagged upstream Vexa rows keep their historical behavior.
"""
from __future__ import annotations

import sys
from types import SimpleNamespace

from fastapi.testclient import TestClient

from meeting_api.collector import create_app
from meeting_api.collector.adapters import SqlAlchemyTranscriptStore
from meeting_api.collector.fakes import InMemoryTranscriptStore
from meeting_api.meeting_writes import (
    MAX_FINAL_TRANSCRIPT_CONTENT_BYTES,
    MAX_FINAL_TRANSCRIPT_SEGMENTS,
)


OWNER = 7
SHARED_USER = 8
PLATFORM = "google_meet"


def _managed_data(*, capture_state: str = "authorized", retention_state: str = "open") -> dict:
    return {
        "zaki_capture": {"state": capture_state},
        "zaki_retention": {
            "state": retention_state,
            "scope_expiries": {
                "audio": "2099-01-01T00:00:00+00:00",
                "transcript": "2099-01-01T00:00:00+00:00",
                "summary": "2099-01-01T00:00:00+00:00",
            },
            "expired_scopes": [],
        },
    }


def test_withdrawn_managed_owner_cannot_read_legacy_transcript() -> None:
    store = InMemoryTranscriptStore()
    data = _managed_data(capture_state="withdrawn")
    meeting_id = store.seed_meeting(
        user_id=OWNER,
        platform=PLATFORM,
        native_meeting_id="withdrawn-owner",
        data=data,
        segments=[{"segment_id": "private", "text": "private words"}],
    )
    client = TestClient(create_app(store, redis=None))

    native = client.get(
        f"/transcripts/{PLATFORM}/withdrawn-owner",
        headers={"x-user-id": str(OWNER)},
    )
    exact = client.get(
        f"/transcripts/by-id/{meeting_id}",
        headers={"x-user-id": str(OWNER)},
    )

    assert native.status_code == 404
    assert exact.status_code == 404


def test_erasing_managed_share_recipient_loses_detail_and_live_subscription() -> None:
    store = InMemoryTranscriptStore()
    data = _managed_data(retention_state="erasing")
    data["transcript_viewers"] = [SHARED_USER]
    meeting_id = store.seed_meeting(
        user_id=OWNER,
        platform=PLATFORM,
        native_meeting_id="erasing-share",
        data=data,
        segments=[{"segment_id": "private", "text": "private words"}],
    )
    client = TestClient(create_app(store, redis=None))
    headers = {"x-user-id": str(SHARED_USER)}

    detail = client.get(f"/transcripts/by-id/{meeting_id}", headers=headers)
    subscribe = client.post(
        "/ws/authorize-subscribe",
        headers=headers,
        json={
            "meetings": [
                {
                    "platform": PLATFORM,
                    "native_meeting_id": "erasing-share",
                }
            ]
        },
    )

    assert detail.status_code == 404
    assert subscribe.json()["authorized"] == []


def test_expired_managed_workspace_cannot_read_or_subscribe_and_skips_live_hash() -> None:
    class LiveRedis:
        def __init__(self) -> None:
            self.reads = 0

        async def hgetall(self, _key: str) -> dict[str, str]:
            self.reads += 1
            return {
                "live-private": (
                    '{"segment_id":"live-private","start":1,"end":2,'
                    '"text":"live private words"}'
                )
            }

    redis = LiveRedis()
    store = InMemoryTranscriptStore(redis_client=redis)
    data = _managed_data()
    data["workspace_id"] = "private-workspace"
    data["zaki_retention"]["scope_expiries"]["transcript"] = (
        "2020-01-01T00:00:00+00:00"
    )
    meeting_id = store.seed_meeting(
        user_id=OWNER,
        platform=PLATFORM,
        native_meeting_id="expired-workspace",
        data=data,
        segments=[{"segment_id": "durable-private", "text": "durable private words"}],
    )
    client = TestClient(create_app(store, redis=None))
    headers = {
        "x-user-id": str(SHARED_USER),
        "x-user-workspaces": "private-workspace",
    }

    detail = client.get(f"/transcripts/by-id/{meeting_id}", headers=headers)
    subscribe = client.post(
        "/ws/authorize-subscribe",
        headers=headers,
        json={
            "meetings": [
                {
                    "platform": PLATFORM,
                    "native_meeting_id": "expired-workspace",
                }
            ]
        },
    )

    assert detail.status_code == 404
    assert subscribe.json()["authorized"] == []
    assert redis.reads == 0


def test_legacy_list_omits_unreadable_managed_rows_but_preserves_ordinary_vexa() -> None:
    store = InMemoryTranscriptStore()
    store.seed_meeting(
        user_id=OWNER,
        platform=PLATFORM,
        native_meeting_id="ordinary-vexa",
        data={"ordinary": "preserved"},
        created_at="2026-07-16T00:00:00Z",
    )
    withdrawn = _managed_data(capture_state="withdrawn")
    store.seed_meeting(
        user_id=OWNER,
        platform=PLATFORM,
        native_meeting_id="withdrawn-list",
        data=withdrawn,
        created_at="2026-07-16T01:00:00Z",
    )
    erasing = _managed_data(retention_state="erasing")
    store.seed_meeting(
        user_id=OWNER,
        platform=PLATFORM,
        native_meeting_id="erasing-list",
        data=erasing,
        created_at="2026-07-16T02:00:00Z",
    )
    expired = _managed_data()
    expired["zaki_retention"]["scope_expiries"]["transcript"] = (
        "2020-01-01T00:00:00+00:00"
    )
    store.seed_meeting(
        user_id=OWNER,
        platform=PLATFORM,
        native_meeting_id="expired-list",
        data=expired,
        created_at="2026-07-16T03:00:00Z",
    )
    client = TestClient(create_app(store, redis=None))

    response = client.get("/meetings", headers={"x-user-id": str(OWNER)})

    assert response.status_code == 200
    assert [row["native_meeting_id"] for row in response.json()["meetings"]] == [
        "ordinary-vexa"
    ]
    assert response.json()["meetings"][0]["data"] == {"ordinary": "preserved"}


def test_legacy_reads_redact_expired_audio_and_summary_scopes_independently() -> None:
    store = InMemoryTranscriptStore()
    data = _managed_data()
    data.update(
        {
            "recordings": [{"id": "private-audio"}],
            "zaki_recording_prefixes": ["recordings/7/91/private-session/"],
            "summary": "private summary",
            "notes": "private legacy notes",
            "docs": [{"path": "kg/entities/meeting/1.md"}],
            "processed": {"views": [{"doc": {"notes": ["private derived note"]}}]},
        }
    )
    data["zaki_retention"]["scope_expiries"].update(
        {
            "audio": "2020-01-01T00:00:00+00:00",
            "summary": "2020-01-01T00:00:00+00:00",
        }
    )
    meeting_id = store.seed_meeting(
        user_id=OWNER,
        platform=PLATFORM,
        native_meeting_id="partially-expired",
        data=data,
        segments=[{"segment_id": "allowed", "text": "transcript still retained"}],
    )
    client = TestClient(create_app(store, redis=None))
    headers = {"x-user-id": str(OWNER)}

    transcript = client.get(f"/transcripts/by-id/{meeting_id}", headers=headers)
    listed = client.get("/meetings", headers=headers)

    assert transcript.status_code == 200
    assert [segment["text"] for segment in transcript.json()["segments"]] == [
        "transcript still retained"
    ]
    assert transcript.json()["recordings"] == []
    assert transcript.json()["notes"] is None
    assert transcript.json()["data"]["recordings"] == []
    assert "zaki_recording_prefixes" not in transcript.json()["data"]
    assert "summary" not in transcript.json()["data"]
    assert "notes" not in transcript.json()["data"]
    assert "docs" not in transcript.json()["data"]
    assert "processed" not in transcript.json()["data"]
    assert listed.json()["meetings"][0]["data"] == transcript.json()["data"]


def test_managed_legacy_transcript_rejects_oversized_segment_census() -> None:
    store = InMemoryTranscriptStore()
    meeting_id = store.seed_meeting(
        user_id=OWNER,
        platform=PLATFORM,
        native_meeting_id="too-many-segments",
        data=_managed_data(),
        segments=[
            {"segment_id": f"s-{index}", "text": "bounded"}
            for index in range(MAX_FINAL_TRANSCRIPT_SEGMENTS + 1)
        ],
    )
    client = TestClient(create_app(store, redis=None))

    response = client.get(
        f"/transcripts/by-id/{meeting_id}",
        headers={"x-user-id": str(OWNER)},
    )

    assert response.status_code == 404


def test_managed_legacy_transcript_rejects_oversized_aggregate_content() -> None:
    store = InMemoryTranscriptStore()
    meeting_id = store.seed_meeting(
        user_id=OWNER,
        platform=PLATFORM,
        native_meeting_id="too-many-bytes",
        data=_managed_data(),
        segments=[
            {
                "segment_id": "oversized",
                "text": "x" * (MAX_FINAL_TRANSCRIPT_CONTENT_BYTES + 1),
            }
        ],
    )
    client = TestClient(create_app(store, redis=None))

    response = client.get(
        f"/transcripts/by-id/{meeting_id}",
        headers={"x-user-id": str(OWNER)},
    )

    assert response.status_code == 404


class _AdapterResult:
    def __init__(self, *, row=None, rows=None) -> None:
        self._row = row
        self._rows = list(rows or [])

    def mappings(self):
        return self

    def first(self):
        return self._row

    def scalars(self):
        return self

    def all(self):
        return self._rows


class _BoundedAdapterSession:
    def __init__(self, census: dict) -> None:
        self.census = census
        self.events: list[str] = []

    async def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        if sql.startswith("SELECT COUNT(*) AS segment_count"):
            self.events.append("census")
            assert params == {"meeting_id": 41}
            return _AdapterResult(row=self.census)
        self.events.append("rows")
        return _AdapterResult(rows=[])


class _FakeField:
    def __eq__(self, _other):
        return self


class _FakeSelect:
    def where(self, *_args):
        return self

    def limit(self, _limit):
        return self

    def execution_options(self, **_options):
        return self


def _install_fake_sqlalchemy(monkeypatch) -> None:
    monkeypatch.setitem(
        sys.modules,
        "sqlalchemy",
        SimpleNamespace(select=lambda _model: _FakeSelect()),
    )
    monkeypatch.setitem(
        sys.modules,
        "meeting_api.collector.models",
        SimpleNamespace(
            Meeting=SimpleNamespace(id=_FakeField()),
            Transcription=SimpleNamespace(meeting_id=_FakeField()),
        ),
    )


async def test_postgres_managed_legacy_read_stops_at_oversized_segment_census(
    monkeypatch,
) -> None:
    _install_fake_sqlalchemy(monkeypatch)
    session = _BoundedAdapterSession(
        {
            "segment_count": MAX_FINAL_TRANSCRIPT_SEGMENTS + 1,
            "content_bytes": 1,
        }
    )
    store = SqlAlchemyTranscriptStore(
        lambda: session,
        statement_factory=lambda sql: sql,
    )
    meeting = SimpleNamespace(id=41, data=_managed_data())

    document = await store._transcript_doc(session, meeting, data=_managed_data())

    assert document is None
    assert session.events == ["census"]


async def test_postgres_managed_legacy_read_stops_at_oversized_byte_census(
    monkeypatch,
) -> None:
    _install_fake_sqlalchemy(monkeypatch)
    session = _BoundedAdapterSession(
        {
            "segment_count": 1,
            "content_bytes": MAX_FINAL_TRANSCRIPT_CONTENT_BYTES + 1,
        }
    )
    store = SqlAlchemyTranscriptStore(
        lambda: session,
        statement_factory=lambda sql: sql,
    )
    meeting = SimpleNamespace(id=41, data=_managed_data())

    document = await store._transcript_doc(session, meeting, data=_managed_data())

    assert document is None
    assert session.events == ["census"]


async def test_postgres_managed_legacy_read_never_hgetalls_oversized_live_hash(
    monkeypatch,
) -> None:
    _install_fake_sqlalchemy(monkeypatch)
    session = _BoundedAdapterSession(
        {"segment_count": 0, "content_bytes": 0}
    )

    class Redis:
        async def hlen(self, _key):
            return MAX_FINAL_TRANSCRIPT_SEGMENTS + 1

        async def hscan(self, *_args, **_kwargs):
            raise AssertionError("oversized hash was scanned")

        async def hgetall(self, _key):
            raise AssertionError("managed legacy reads must not use HGETALL")

    store = SqlAlchemyTranscriptStore(
        lambda: session,
        Redis(),
        statement_factory=lambda sql: sql,
    )
    meeting = SimpleNamespace(id=41, data=_managed_data())

    document = await store._transcript_doc(session, meeting, data=_managed_data())

    assert document is None
    assert session.events == ["census", "rows"]


async def test_postgres_legacy_detail_checks_withdrawal_under_shared_barrier(
    monkeypatch,
) -> None:
    _install_fake_sqlalchemy(monkeypatch)
    events: list[str] = []
    meeting = SimpleNamespace(
        id=41,
        user_id=OWNER,
        data=_managed_data(capture_state="withdrawn"),
    )

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def execute(self, _statement, _params=None):
            events.append("row")
            return _AdapterResult(row=meeting)

        async def commit(self):
            events.append("commit")

    class Store(SqlAlchemyTranscriptStore):
        async def _acquire_legacy_read_barrier(self, _db, meeting_id):
            assert meeting_id == 41
            events.append("barrier")

    session = Session()
    store = Store(lambda: session, statement_factory=lambda sql: sql)

    document = await store.get_transcript_by_id(OWNER, 41)

    assert document is None
    assert events == ["barrier", "row", "commit"]
