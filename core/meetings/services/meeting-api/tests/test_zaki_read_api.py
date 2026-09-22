"""Production Minutes ``zaki-read.v1`` reference surface.

These tests intentionally drive the mounted FastAPI app.  The fixture store stands in for the
same PostgreSQL/Redis transcript adapter used in production; auth, opt-in, retention and response
bounds are therefore exercised at the public read-plane boundary.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker
import pytest
from referencing import Registry, Resource

from meeting_api.app import create_app
from meeting_api.collector.fakes import InMemoryTranscriptStore
from meeting_api.collector.adapters import SqlAlchemyTranscriptStore
from meeting_api.meeting_writes import build_transcript_finalization_marker
from meeting_api.zaki_read import _speaker_turns


NOW = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
TOKEN = "minutes-read-test-token-0123456789abcdef"


def _conforms(payload: dict, shape: str) -> None:
    schema_path = Path(__file__).parents[3] / "contracts" / "zaki-read.v1" / "zaki-read.schema.json"
    schema = json.loads(schema_path.read_text())
    registry = Registry().with_resource(schema["$id"], Resource.from_contents(schema))
    validator = Draft202012Validator(
        {"$ref": f"{schema['$id']}#/$defs/{shape}"},
        registry=registry,
        format_checker=FormatChecker(),
    )
    validator.validate(payload)


def _capture_data(*, expires_at: str = "2026-08-15T12:00:00+00:00", summary: str | None = None):
    data = {
        "title": "Launch review",
        "attendees": ["Participant A", "Participant B"],
        "zaki_capture": {
            "state": "authorized",
            "bot_name": "ZAKI Notetaker",
            "tenant_attested": True,
            "tenant_policy_version": "minutes-capture.v1",
            "tenant_attested_at": "2026-07-15T08:55:00+00:00",
        },
        "zaki_retention": {
            "state": "open",
            "scope_expiries": {
                "audio": expires_at,
                "transcript": expires_at,
                "summary": expires_at,
            },
            "expired_scopes": [],
        },
        "zaki_transcript_finalization": {
            "state": "finalized",
            "revision": "sha256:" + ("a" * 64),
            "finalized_at": "2026-07-15T10:01:00+00:00",
            "segment_count": 1,
        },
    }
    if summary is not None:
        data["summary"] = summary
    return data


def _seed(store: InMemoryTranscriptStore, *, owner: int = 7, meeting_id: int = 41,
          expires_at: str = "2026-08-15T12:00:00+00:00", summary: str | None = None,
          text: str = "We agreed to launch Minutes behind the tenant flag.") -> int:
    segments = [{
        "segment_id": f"segment-{meeting_id}",
        "start": 1.0,
        "end": 4.0,
        "speaker": "Participant A",
        "language": "en",
        "text": text,
    }]
    data = _capture_data(expires_at=expires_at, summary=summary)
    data["zaki_transcript_finalization"] = build_transcript_finalization_marker(
        segments,
        finalized_at=datetime(2026, 7, 15, 10, 1, tzinfo=timezone.utc),
    )
    return store.seed_meeting(
        user_id=owner,
        platform="google_meet",
        native_meeting_id=f"native-secret-{meeting_id}",
        meeting_id=meeting_id,
        status="completed",
        start_time="2026-07-15T09:00:00+00:00",
        end_time="2026-07-15T10:00:00+00:00",
        created_at="2026-07-15T08:59:00+00:00",
        updated_at="2026-07-15T10:01:00+00:00",
        data=data,
        segments=segments,
    )


def _client(store: InMemoryTranscriptStore, *, enabled: bool = True,
            opted_in: bool = True) -> TestClient:
    async def read_scope(user_id: int):
        return {"agent_read_enabled": opted_in} if user_id in {7, 8} else None

    return TestClient(create_app(
        transcript_store=store,
        zaki_read_enabled=enabled,
        zaki_read_token=TOKEN if enabled else None,
        zaki_read_scope=read_scope,
        zaki_read_now=lambda: NOW,
    ))


def _headers(*, token: str = TOKEN, user: str = "7"):
    return {
        "X-Zaki-Read-Token": token,
        "X-Zaki-User-Id": user,
        "X-Request-Id": "request-1",
    }


def test_read_plane_is_absent_when_operator_flag_is_off():
    response = _client(InMemoryTranscriptStore(), enabled=False).get(
        "/api/zaki/read/v1/7/index", headers=_headers()
    )
    assert response.status_code == 404


def test_sql_read_projection_marks_naive_postgres_instants_as_utc():
    row = SimpleNamespace(
        id=41,
        user_id=7,
        platform="google_meet",
        status="completed",
        start_time=datetime(2026, 7, 15, 9, 0),
        end_time=datetime(2026, 7, 15, 10, 0),
        created_at=datetime(2026, 7, 15, 8, 59),
        updated_at=datetime(2026, 7, 15, 10, 1),
        data={},
    )
    projected = SqlAlchemyTranscriptStore._zaki_read_row(row)
    assert projected["start_time"] == "2026-07-15T09:00:00+00:00"
    assert projected["updated_at"] == "2026-07-15T10:01:00+00:00"


def test_sql_zaki_read_streams_bounded_durable_rows_and_never_reads_live_redis():
    class StreamRows:
        def __init__(self):
            self.visited = 0

        def mappings(self):
            return self

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.visited >= 10_000:
                raise StopAsyncIteration
            self.visited += 1
            return {
                "segment_id": f"segment-{self.visited}",
                "start": self.visited,
                "end": self.visited + 1,
                "text": "x" * 65_536,
                "speaker": "Participant",
                "language": "en",
            }

    class Session:
        def __init__(self):
            self.rows = StreamRows()
            self.statement = None
            self.params = None
            self.execution_options = None

        async def stream(self, statement, params, execution_options):
            self.statement = statement
            self.params = params
            self.execution_options = execution_options
            return self.rows

    class ForbiddenRedis:
        async def hgetall(self, *_args, **_kwargs):
            raise AssertionError("finalized zaki-read touched the live Redis hash")

    session = Session()
    meeting = SimpleNamespace(
        id=41,
        platform="google_meet",
        status="completed",
        start_time=datetime(2026, 7, 15, 9, 0),
        end_time=datetime(2026, 7, 15, 10, 0),
        data={
            "zaki_transcript_finalization": build_transcript_finalization_marker(
                [{
                    "segment_id": "expected-segment",
                    "start": 1,
                    "end": 2,
                    "text": "expected",
                    "speaker": "Participant",
                    "language": "en",
                }],
                finalized_at=datetime(2026, 7, 15, 10, 1, tzinfo=timezone.utc),
            ),
        },
    )
    store = SqlAlchemyTranscriptStore(
        lambda: None,
        redis_client=ForbiddenRedis(),
        statement_factory=lambda sql: sql,
    )

    document = asyncio.run(store._zaki_read_transcript_doc(session, meeting))

    assert document == {"_zaki_read_invalid": "content_too_large", "segments": []}
    assert 0 < session.rows.visited <= 8
    assert "LEFT(text, 65537)" in session.statement
    assert "LEFT(segment_id, 257)" in session.statement
    assert "ORDER BY start_time ASC, segment_id ASC, id ASC" in session.statement
    assert session.params == {"meeting_id": 41}
    assert session.execution_options == {"yield_per": 8}


def test_sql_zaki_read_matches_the_finalizer_census_and_rejects_altered_rows():
    rows = [{
        "segment_id": "segment-41",
        "start": 1.0,
        "end": 4.0,
        "text": "durable transcript",
        "speaker": "Participant A",
        "language": "en",
    }]

    class StreamRows:
        def __init__(self, values):
            self._values = iter(values)

        def mappings(self):
            return self

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self._values)
            except StopIteration:
                raise StopAsyncIteration from None

    class Session:
        def __init__(self, values):
            self._values = values

        async def stream(self, *_args, **_kwargs):
            return StreamRows(self._values)

    meeting = SimpleNamespace(
        id=41,
        platform="google_meet",
        status="completed",
        start_time=datetime(2026, 7, 15, 9, 0),
        end_time=datetime(2026, 7, 15, 10, 0),
        data={
            "zaki_transcript_finalization": build_transcript_finalization_marker(
                rows,
                finalized_at=datetime(2026, 7, 15, 10, 1, tzinfo=timezone.utc),
            ),
        },
    )
    store = SqlAlchemyTranscriptStore(lambda: None, statement_factory=lambda sql: sql)

    exact = asyncio.run(store._zaki_read_transcript_doc(Session(rows), meeting))
    altered = asyncio.run(store._zaki_read_transcript_doc(Session([
        {**rows[0], "text": "altered after finalization"},
    ]), meeting))

    assert exact["segments"][0]["segment_id"] == "segment-41"
    assert altered == {"_zaki_read_invalid": "revision_mismatch", "segments": []}


def test_enabled_read_plane_requires_a_dedicated_token_at_composition():
    with pytest.raises(ValueError, match="dedicated token"):
        create_app(zaki_read_enabled=True, zaki_read_token=None)


@pytest.mark.parametrize(
    "weak_token",
    (
        "too-short",
        " " + TOKEN,
        TOKEN + " ",
        "a" * 16 + "\n" + "b" * 16,
        "a" * 16 + "\x00" + "b" * 16,
        "a" * 16 + "\x7f" + "b" * 16,
        "é" * 32,
        "a" * 513,
    ),
)
def test_enabled_read_plane_rejects_noncanonical_token_material(weak_token):
    with pytest.raises(
        ValueError, match="unpadded printable ASCII between 32 and 512",
    ):
        create_app(
            transcript_store=InMemoryTranscriptStore(),
            zaki_read_enabled=True,
            zaki_read_token=weak_token,
            zaki_read_scope=lambda _user_id: {"agent_read_enabled": True},
        )


@pytest.mark.parametrize("boundary_token", ("a" * 32, "z" * 512))
def test_enabled_read_plane_accepts_canonical_token_length_boundaries(boundary_token):
    create_app(
        transcript_store=InMemoryTranscriptStore(),
        zaki_read_enabled=True,
        zaki_read_token=boundary_token,
        zaki_read_scope=lambda _user_id: {"agent_read_enabled": True},
    )


def test_index_and_item_are_owner_scoped_bounded_and_non_cacheable():
    store = InMemoryTranscriptStore()
    _seed(store, summary="The team approved the pilot.")
    response = _client(store).get(
        "/api/zaki/read/v1/7/index?limit=200", headers=_headers()
    )
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-request-id"] == "request-1"
    body = response.json()
    _conforms(body, "IndexResponse")
    assert [item["id"] for item in body["items"]] == [
        "meeting:41", "transcript:41", "summary:41",
    ]
    assert all(item["sensitivity"] == "sensitive_pii" for item in body["items"])
    assert "content" not in json.dumps(body["items"])
    assert "native-secret" not in response.text
    assert body["truncated"] is False
    assert "next_cursor" not in body
    exact_page = _client(store).get(
        "/api/zaki/read/v1/7/index?limit=3", headers=_headers()
    ).json()
    assert exact_page["truncated"] is False
    assert "next_cursor" not in exact_page

    item = _client(store).get(
        "/api/zaki/read/v1/7/item/transcript:41", headers=_headers()
    )
    assert item.status_code == 200
    assert item.headers["cache-control"] == "no-store"
    payload = item.json()["item"]
    _conforms(item.json(), "ItemResponse")
    assert payload["meeting_id"] == "meeting:41"
    assert payload["capture_notice"]["bot_visible"] is True
    assert payload["content"]["format"] == "speaker_turns"
    assert payload["content"]["turns"][0]["started_at"] == "2026-07-15T09:00:01+00:00"


def test_full_transcript_refuses_missing_or_altered_durable_rows_but_summary_remains():
    for mutation in ("missing", "altered"):
        store = InMemoryTranscriptStore()
        _seed(store, summary="Bounded independent summary")
        if mutation == "missing":
            store._meetings[41]["segments"].clear()
        else:
            store._meetings[41]["segments"]["segment-41"]["text"] = (
                "altered after finalization"
            )
        client = _client(store)

        full = client.get(
            "/api/zaki/read/v1/7/item/transcript:41", headers=_headers()
        )
        summary = client.get(
            "/api/zaki/read/v1/7/item/transcript:41?variant=summary",
            headers=_headers(),
        )

        assert full.status_code == 503
        assert full.json()["error"]["code"] == "item_unavailable"
        assert "altered after finalization" not in full.text
        assert summary.status_code == 200
        assert summary.json()["item"]["content"] == {
            "format": "summary",
            "text": "Bounded independent summary",
        }


def test_bad_token_path_header_mismatch_and_foreign_item_fail_closed():
    store = InMemoryTranscriptStore()
    _seed(store)
    client = _client(store)
    assert client.get(
        "/api/zaki/read/v1/7/index", headers=_headers(token="wrong")
    ).status_code == 401
    mismatch = client.get(
        "/api/zaki/read/v1/7/index", headers=_headers(user="8")
    )
    assert mismatch.status_code == 404
    foreign = client.get(
        "/api/zaki/read/v1/8/item/transcript:41", headers=_headers(user="8")
    )
    unknown = client.get(
        "/api/zaki/read/v1/8/item/transcript:999", headers=_headers(user="8")
    )
    assert foreign.status_code == unknown.status_code == 404
    assert foreign.json() == unknown.json()
    assert "native-secret" not in foreign.text


def test_tenant_opt_in_and_retention_are_checked_before_pagination():
    store = InMemoryTranscriptStore()
    _seed(store, meeting_id=41)
    _seed(store, meeting_id=42, expires_at="2026-07-15T11:59:59+00:00")
    disabled = _client(store, opted_in=False).get(
        "/api/zaki/read/v1/7/index", headers=_headers()
    )
    assert disabled.status_code == 403
    page = _client(store).get(
        "/api/zaki/read/v1/7/index?limit=3", headers=_headers()
    ).json()
    assert [item["id"] for item in page["items"]] == [
        "meeting:41", "transcript:41",
    ]
    assert page == {"items": page["items"], "truncated": False}


def test_unknown_retention_state_fails_closed_for_index_and_item():
    store = InMemoryTranscriptStore()
    _seed(store, summary="Must remain private.")
    store._meetings[41]["data"]["zaki_retention"]["state"] = "unexpected"
    client = _client(store)

    index = client.get("/api/zaki/read/v1/7/index", headers=_headers())
    item = client.get(
        "/api/zaki/read/v1/7/item/transcript:41", headers=_headers()
    )

    assert index.status_code == 200
    assert index.json() == {"items": [], "truncated": False}
    assert item.status_code == 404
    assert item.json()["error"]["code"] == "unknown_item"


def test_terminal_row_is_hidden_until_a_valid_durable_finalization_marker_exists():
    store = InMemoryTranscriptStore()
    _seed(store)
    client = _client(store)

    store._meetings[41]["data"].pop("zaki_transcript_finalization")
    assert client.get(
        "/api/zaki/read/v1/7/index", headers=_headers()
    ).json() == {"items": [], "truncated": False}
    assert client.get(
        "/api/zaki/read/v1/7/item/transcript:41", headers=_headers()
    ).status_code == 404

    store._meetings[41]["data"]["zaki_transcript_finalization"] = {
        "state": "finalized",
        "revision": "not-a-content-revision",
        "finalized_at": "2026-07-15T10:01:00+00:00",
        "segment_count": 1,
    }
    assert client.get(
        "/api/zaki/read/v1/7/index", headers=_headers()
    ).json() == {"items": [], "truncated": False}


def test_expired_and_unknown_items_take_the_same_no_content_lookup_path():
    class ForbiddenCarrier:
        def items(self):
            raise AssertionError("expired item reached transcript storage")

    class CountingStore(InMemoryTranscriptStore):
        def __init__(self):
            super().__init__()
            self.transcript_reads = []

        async def get_transcript_by_id(self, user_id, meeting_id, member_workspaces=None):
            self.transcript_reads.append((user_id, meeting_id))
            return await super().get_transcript_by_id(
                user_id, meeting_id, member_workspaces,
            )

    store = CountingStore()
    _seed(store, expires_at="2026-07-15T11:59:59+00:00")
    store._meetings[41]["segments"] = ForbiddenCarrier()
    client = _client(store)

    expired = client.get(
        "/api/zaki/read/v1/7/item/transcript:41", headers=_headers()
    )
    unknown = client.get(
        "/api/zaki/read/v1/7/item/transcript:999", headers=_headers()
    )

    assert expired.status_code == unknown.status_code == 404
    assert expired.json() == unknown.json()
    assert store.transcript_reads == []


def test_index_is_metadata_only_and_never_loads_transcript_bodies():
    class MetadataOnlyStore(InMemoryTranscriptStore):
        async def get_transcript_by_id(self, *_args, **_kwargs):
            raise AssertionError("index attempted to materialize a transcript body")

    store = MetadataOnlyStore()
    _seed(store, summary="A bounded summary")

    response = _client(store).get(
        "/api/zaki/read/v1/7/index", headers=_headers()
    )

    assert response.status_code == 200
    assert [item["id"] for item in response.json()["items"]] == [
        "meeting:41", "transcript:41", "summary:41",
    ]


def test_item_uses_one_atomic_owner_retention_and_content_snapshot():
    class AtomicSnapshotStore(InMemoryTranscriptStore):
        async def get_zaki_read_meeting(self, *_args, **_kwargs):
            raise AssertionError("item performed the stale metadata half-read")

        async def get_transcript_by_id(self, *_args, **_kwargs):
            raise AssertionError("item performed the racy content half-read")

    store = AtomicSnapshotStore()
    _seed(store)

    response = _client(store).get(
        "/api/zaki/read/v1/7/item/transcript:41", headers=_headers()
    )

    assert response.status_code == 200
    assert response.json()["item"]["content"]["turns"][0]["text"].startswith("We agreed")


def test_malformed_finalized_segment_fails_closed_instead_of_serving_a_partial_transcript():
    store = InMemoryTranscriptStore()
    _seed(store, summary="Bounded fallback.")
    store._meetings[41]["segments"]["corrupt"] = {
        "segment_id": "corrupt",
        "start": 4,
        "end": 3,
        "speaker": "Participant B",
        "text": "This turn must not silently disappear.",
    }
    client = _client(store)

    full = client.get(
        "/api/zaki/read/v1/7/item/transcript:41", headers=_headers()
    )
    fallback = client.get(
        "/api/zaki/read/v1/7/item/transcript:41?variant=summary",
        headers=_headers(),
    )

    assert full.status_code == 503
    assert full.json()["error"]["code"] == "item_unavailable"
    assert fallback.status_code == 200


def test_turn_builder_stops_at_the_serialized_content_cap():
    document = {
        "start_time": "2026-07-15T09:00:00+00:00",
        "segments": [
            {
                "start": index,
                "end": index + 1,
                "speaker": "Participant",
                "text": "x" * 60_000,
            }
            for index in range(8)
        ],
    }

    assert _speaker_turns(document) == {"invalid": "content_too_large"}


def test_more_than_scan_cap_expired_rows_do_not_hide_or_cursor_visible_items():
    store = InMemoryTranscriptStore()
    _seed(store, meeting_id=41, text="the visible meeting")
    for meeting_id in range(100, 1_101):
        _seed(
            store,
            meeting_id=meeting_id,
            expires_at="2026-07-15T11:59:59+00:00",
            text="expired",
        )

    response = _client(store).get(
        "/api/zaki/read/v1/7/index?limit=2", headers=_headers()
    )

    assert response.status_code == 200
    assert response.json() == {
        "items": response.json()["items"],
        "truncated": False,
    }
    assert [item["id"] for item in response.json()["items"]] == [
        "meeting:41", "transcript:41",
    ]


def test_more_than_scan_cap_unfinalized_rows_do_not_hide_finalized_history():
    store = InMemoryTranscriptStore()
    _seed(store, meeting_id=41, text="the durable visible meeting")
    for meeting_id in range(100, 1_101):
        _seed(store, meeting_id=meeting_id, text="not durably finalized")
        store._meetings[meeting_id]["data"].pop("zaki_transcript_finalization")

    response = _client(store).get(
        "/api/zaki/read/v1/7/index?limit=2", headers=_headers()
    )

    assert response.status_code == 200
    assert [item["id"] for item in response.json()["items"]] == [
        "meeting:41", "transcript:41",
    ]
    assert response.json()["truncated"] is False


def test_item_rejects_an_impossible_finalization_count_before_reading_the_carrier():
    class BoundedCarrier:
        def __init__(self):
            self.visited = 0

        def items(self):
            for index in range(10_000):
                self.visited += 1
                yield str(index), {
                    "segment_id": str(index),
                    "start": index,
                    "end": index + 1,
                    "speaker": "Participant",
                    "text": "bounded",
                }

    store = InMemoryTranscriptStore()
    _seed(store, summary="Bounded fallback")
    carrier = BoundedCarrier()
    store._meetings[41]["segments"] = carrier
    store._meetings[41]["data"]["zaki_transcript_finalization"]["segment_count"] = 10_000

    response = _client(store).get(
        "/api/zaki/read/v1/7/item/transcript:41", headers=_headers()
    )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "unknown_item"
    assert carrier.visited == 0


def test_search_filters_foreign_content_before_counts_and_cursoring():
    store = InMemoryTranscriptStore()
    _seed(store, owner=7, meeting_id=41, text="launch decision")
    _seed(store, owner=8, meeting_id=81, text="launch decision secret foreign")
    response = _client(store).get(
        "/api/zaki/read/v1/7/search?q=launch&limit=50", headers=_headers()
    )
    assert response.status_code == 404
    body = response.json()
    assert body["error"]["code"] == "search_disabled"
    assert "foreign" not in response.text


def test_query_validation_runs_only_after_read_token_auth_and_never_echoes_input():
    store = InMemoryTranscriptStore()
    _seed(store)
    client = _client(store)
    marker = "private-query-marker-" + ("x" * 600)

    denied = client.get(
        "/api/zaki/read/v1/7/search?q=" + marker,
        headers=_headers(token="wrong"),
    )
    malformed = client.get(
        "/api/zaki/read/v1/7/search?q=" + marker,
        headers=_headers(),
    )
    bad_limit = client.get(
        "/api/zaki/read/v1/7/index?limit=not-a-number",
        headers=_headers(token="wrong"),
    )

    assert denied.status_code == bad_limit.status_code == 401
    assert denied.json()["error"]["code"] == "bad_token"
    assert malformed.status_code == 400
    assert malformed.json()["error"]["code"] == "bad_query"
    assert marker not in denied.text
    assert marker not in malformed.text


def test_index_orders_last_meeting_by_occurrence_not_late_reprocessing_time():
    store = InMemoryTranscriptStore()
    _seed(store, meeting_id=41, text="older meeting reprocessed later")
    store._meetings[41]["start_time"] = "2026-07-15T08:00:00+00:00"
    store._meetings[41]["end_time"] = "2026-07-15T08:30:00+00:00"
    store._meetings[41]["updated_at"] = "2026-07-15T11:30:00+00:00"
    _seed(store, meeting_id=42, text="actual latest meeting")
    store._meetings[42]["start_time"] = "2026-07-15T10:00:00+00:00"
    store._meetings[42]["end_time"] = "2026-07-15T10:30:00+00:00"
    store._meetings[42]["updated_at"] = "2026-07-15T10:31:00+00:00"

    first_page = _client(store).get(
        "/api/zaki/read/v1/7/index?limit=2", headers=_headers()
    ).json()

    assert [item["id"] for item in first_page["items"]] == [
        "meeting:42", "transcript:42",
    ]
    assert first_page["truncated"] is True
    second_page = _client(store).get(
        "/api/zaki/read/v1/7/index?limit=2&cursor=" + first_page["next_cursor"],
        headers=_headers(),
    ).json()
    assert [item["id"] for item in second_page["items"]] == [
        "meeting:41", "transcript:41",
    ]


def test_index_cursor_is_bound_to_the_since_filter():
    store = InMemoryTranscriptStore()
    _seed(store)
    client = _client(store)
    first = client.get(
        "/api/zaki/read/v1/7/index?limit=1&since=2026-07-15T00:00:00Z",
        headers=_headers(),
    ).json()
    assert first["truncated"] is True

    changed_filter = client.get(
        "/api/zaki/read/v1/7/index?limit=1&cursor=" + first["next_cursor"],
        headers=_headers(),
    )

    assert changed_filter.status_code == 400
    assert changed_filter.json()["error"]["code"] == "bad_cursor"


def test_oversized_transcript_is_413_while_summary_variant_stays_available():
    store = InMemoryTranscriptStore()
    _seed(store, summary="Bounded fallback.")
    store._meetings[41]["segments"]["segment-41"]["text"] = "x" * (256 * 1024)
    client = _client(store)
    full = client.get(
        "/api/zaki/read/v1/7/item/transcript:41", headers=_headers()
    )
    assert full.status_code == 413
    assert full.json()["error"]["code"] == "item_too_large"
    summary = client.get(
        "/api/zaki/read/v1/7/item/transcript:41?variant=summary", headers=_headers()
    )
    assert summary.status_code == 200
    assert summary.json()["item"]["content"] == {
        "format": "summary", "text": "Bounded fallback."
    }


def test_transcript_summary_variant_fails_closed_after_summary_retention_expires():
    store = InMemoryTranscriptStore()
    private_summary = "This summary must expire independently."
    _seed(store, summary=private_summary)
    store._meetings[41]["data"]["zaki_retention"]["scope_expiries"]["summary"] = (
        "2026-07-15T11:59:59+00:00"
    )
    client = _client(store)

    full = client.get(
        "/api/zaki/read/v1/7/item/transcript:41", headers=_headers()
    )
    variant = client.get(
        "/api/zaki/read/v1/7/item/transcript:41?variant=summary",
        headers=_headers(),
    )
    summary = client.get(
        "/api/zaki/read/v1/7/item/summary:41", headers=_headers()
    )

    assert full.status_code == 200
    assert variant.status_code == summary.status_code == 404
    assert variant.json()["error"]["code"] == "unknown_item"
    assert summary.json()["error"]["code"] == "unknown_item"
    assert private_summary not in variant.text
    assert private_summary not in summary.text


def test_transcript_summary_variant_advertises_its_shorter_content_expiry():
    store = InMemoryTranscriptStore()
    _seed(store, summary="Short-lived bounded fallback.")
    summary_expiry = "2026-07-16T12:00:00+00:00"
    store._meetings[41]["data"]["zaki_retention"]["scope_expiries"]["summary"] = (
        summary_expiry
    )

    response = _client(store).get(
        "/api/zaki/read/v1/7/item/transcript:41?variant=summary",
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json()["item"]["retention"] == {
        "scope": "minutes.transcript",
        "expires_at": summary_expiry,
    }


def test_transcript_segment_bound_fails_closed_instead_of_silently_dropping_turns():
    store = InMemoryTranscriptStore()
    _seed(store, summary="Bounded fallback.")
    segments = {
        "first": {
            "segment_id": "first", "start": 1, "end": 2,
            "speaker": "Participant A", "text": "first turn",
        },
        **{
            f"empty-{index}": {
                "segment_id": f"empty-{index}", "start": 2 + index / 10_000,
                "end": 2 + index / 10_000, "speaker": "Participant A", "text": "",
            }
            for index in range(4095)
        },
        "last": {
            "segment_id": "last", "start": 3, "end": 4,
            "speaker": "Participant B", "text": "must not disappear",
        },
    }
    store._meetings[41]["segments"] = segments

    client = _client(store)
    full = client.get(
        "/api/zaki/read/v1/7/item/transcript:41", headers=_headers()
    )
    summary = client.get(
        "/api/zaki/read/v1/7/item/transcript:41?variant=summary", headers=_headers()
    )

    assert full.status_code == 413
    assert full.json()["error"]["code"] == "item_too_large"
    assert summary.status_code == 200


def test_summary_accepts_the_existing_structured_meeting_data_shape():
    store = InMemoryTranscriptStore()
    _seed(store, summary={"text": "Structured summary."})

    response = _client(store).get(
        "/api/zaki/read/v1/7/item/summary:41", headers=_headers()
    )

    assert response.status_code == 200
    assert response.json()["item"]["content"] == {
        "format": "summary", "text": "Structured summary."
    }


def test_malformed_item_and_cursor_never_echo_sensitive_identifiers():
    store = InMemoryTranscriptStore()
    _seed(store)
    client = _client(store)
    item = client.get(
        "/api/zaki/read/v1/7/item/%2E%2E%2Fsecret", headers=_headers()
    )
    cursor = client.get(
        "/api/zaki/read/v1/7/index?cursor=not-a-cursor", headers=_headers()
    )
    oversized_row = client.get(
        "/api/zaki/read/v1/7/item/transcript:9999999999999999999", headers=_headers()
    )
    oversized_user = client.get(
        "/api/zaki/read/v1/9999999999999999999/index",
        headers=_headers(user="9999999999999999999"),
    )
    assert item.status_code == 404
    assert cursor.status_code == 400
    assert oversized_row.status_code == 404
    assert oversized_user.status_code == 404
    assert "secret" not in item.text
    assert "not-a-cursor" not in cursor.text


def test_read_rejects_user_ids_outside_postgres_bigint_before_authority_lookup():
    async def exploding_scope(_user_id: int):
        raise AssertionError("out-of-range user reached Identity adapter")

    client = TestClient(create_app(
        transcript_store=InMemoryTranscriptStore(),
        zaki_read_enabled=True,
        zaki_read_token=TOKEN,
        zaki_read_scope=exploding_scope,
        zaki_read_now=lambda: NOW,
    ))

    response = client.get(
        "/api/zaki/read/v1/9999999999999999999/index",
        headers=_headers(user="9999999999999999999"),
    )

    assert response.status_code == 404
