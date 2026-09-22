"""Production-style capture withdrawal adapter tests."""
from __future__ import annotations

import asyncio
import json
import sys
from types import MappingProxyType, SimpleNamespace

import pytest

from meeting_api.bot_spawn.adapters import SqlAlchemyMeetingRepo
from meeting_api.bot_spawn.ports import CaptureGrantConsumed
from meeting_api.collector.adapters import SqlAlchemyTranscriptStore
from meeting_api.collector.ports import TranscriptWriteRefused


class _OwnerLookupSession:
    def __init__(self, owner):
        self.owner = owner
        self.statement = None
        self.params = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement, params=None):
        self.statement = statement
        self.params = params
        return _Result(self.owner)


async def test_postgres_owner_lookup_selects_only_the_exact_row_owner():
    session = _OwnerLookupSession(7)
    store = SqlAlchemyTranscriptStore(lambda: session, statement_factory=lambda sql: sql)

    assert await store.owner_for(42) == 7
    sql = str(session.statement)
    assert sql == "SELECT user_id FROM meetings WHERE id = :meeting_id"
    assert session.params == {"meeting_id": 42}


async def test_postgres_owner_lookup_rejects_noncanonical_identity():
    called = False

    def session_factory():
        nonlocal called
        called = True
        return _OwnerLookupSession(7)

    store = SqlAlchemyTranscriptStore(session_factory)

    assert await store.owner_for("not-a-row") is None
    assert await store.owner_for(0) is None
    assert called is False


class _MeetingDocSession:
    def __init__(self, row):
        self.row = row
        self.statements: list[tuple[str, dict]] = []
        self.committed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        params = params or {}
        self.statements.append((sql, params))
        if sql.startswith("SELECT user_id, data FROM meetings"):
            return _Result(self.row)
        if sql.startswith("UPDATE meetings SET data"):
            self.row["data"] = json.loads(params["data"])
            return _Result()
        raise AssertionError(f"unexpected SQL: {sql}")

    async def commit(self):
        self.committed = True


async def test_postgres_internal_doc_link_derives_owner_and_canonical_row_path_from_locked_exact_row():
    row = {
        "user_id": 8,
        "platform_specific_id": "same-link-aaa",
        "data": {"docs": [{"workspace": "8", "path": "existing.md"}]},
    }
    session = _MeetingDocSession(row)
    store = SqlAlchemyTranscriptStore(lambda: session, statement_factory=lambda sql: sql)

    doc = await store.connect_meeting_doc_by_id(42)

    assert doc == {
        "workspace": "8",
        "path": "kg/entities/meeting/42.md",
        "title": "Meeting 42",
        "kind": "meeting",
    }
    assert session.statements[0] == (
        "SELECT user_id, data FROM meetings WHERE id = :meeting_id FOR UPDATE",
        {"meeting_id": 42},
    )
    assert row["data"]["docs"] == [
        {"workspace": "8", "path": "existing.md"},
        doc,
    ]
    assert session.committed is True


class _Result:
    def __init__(self, row=None):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row

    def scalar(self):
        return self._row

    def scalars(self):
        return self


class _Session:
    def __init__(self, meeting):
        self.meeting = meeting
        self.events: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def commit(self):
        self.events.append("commit")

    async def execute(self, statement, params=None):
        sql = " ".join(statement.split())
        params = params or {}
        if "pg_advisory_xact_lock(:uid)" in sql:
            self.events.append("user_lock")
            return _Result()
        if sql.startswith("SELECT id FROM meetings"):
            assert "data->'zaki_capture'->>'tenant_id' = :tenant_id" in sql
            assert params["tenant_id"] == "tenant-a"
            self.events.append("lookup")
            return _Result({"id": self.meeting["id"]})
        if "pg_advisory_xact_lock" in sql:
            self.events.append("lock")
            return _Result()
        if sql.startswith("SELECT id, user_id, platform"):
            self.events.append("row")
            return _Result(dict(self.meeting))
        if sql.startswith("UPDATE meetings SET status = CASE"):
            self.events.append("confirm_teardown")
            assert params["teardown_state"] == json.dumps("confirmed")
            assert params["completion_reason"] == json.dumps("stopped")
            if self.meeting["status"] not in ("completed", "failed"):
                self.meeting["status"] = "completed"
                self.meeting["data"]["completion_reason"] = "stopped"
            self.meeting["end_time"] = self.meeting.get("end_time") or "2026-07-15T08:41:01Z"
            self.meeting["data"]["zaki_capture"]["teardown_state"] = "confirmed"
            return _Result({"id": self.meeting["id"]})
        if sql.startswith("UPDATE meetings SET status"):
            self.events.append("update")
            self.meeting["status"] = params["status"]
            self.meeting["data"] = json.loads(params["data"])
            return _Result()
        raise AssertionError(f"unexpected SQL: {sql}")


async def test_postgres_withdrawal_serializes_with_spawn_before_selecting_the_capture():
    meeting = {
        "id": 41,
        "user_id": 7,
        "platform": "google_meet",
        "platform_specific_id": "abc-defg-hij",
        "status": "active",
        "bot_container_id": "mtg-41",
        "start_time": None,
        "end_time": None,
        "data": {
            "zaki_capture": {
                "tenant_id": "tenant-a",
                "state": "authorized",
            }
        },
        "created_at": None,
        "updated_at": None,
    }
    session = _Session(meeting)
    repo = SqlAlchemyMeetingRepo(
        lambda: session,
        statement_factory=lambda sql: sql,
    )

    result = await repo.withdraw_capture(
        tenant_id="tenant-a",
        user_id=7,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at="2026-07-15T08:41:00+00:00",
    )

    assert session.events == ["user_lock", "lookup", "lock", "row", "update", "commit"]
    assert result["changed"] is True
    assert result["should_stop"] is True
    assert meeting["status"] == "stopping"
    assert meeting["data"]["zaki_capture"] == {
        "tenant_id": "tenant-a",
        "state": "withdrawn",
        "withdrawal_reason": "consent_withdrawn",
        "withdrawn_at": "2026-07-15T08:41:00+00:00",
        "teardown_state": "pending",
    }
    assert meeting["data"]["stop_requested"] is True


@pytest.mark.parametrize("terminal_status", ["completed", "failed"])
async def test_postgres_terminal_withdrawal_confirms_without_changing_terminal_outcome(
    terminal_status
):
    meeting = {
        "id": 41,
        "user_id": 7,
        "platform": "google_meet",
        "platform_specific_id": "abc-defg-hij",
        "status": terminal_status,
        "bot_container_id": "mtg-41",
        "start_time": None,
        "end_time": "2026-07-15T08:40:00Z",
        "data": {
            "zaki_capture": {
                "tenant_id": "tenant-a",
                "state": "authorized",
            },
            "terminal_evidence": "preserve-me",
        },
        "created_at": None,
        "updated_at": None,
    }
    session = _Session(meeting)
    repo = SqlAlchemyMeetingRepo(lambda: session, statement_factory=lambda sql: sql)

    result = await repo.withdraw_capture(
        tenant_id="tenant-a",
        user_id=7,
        platform="google_meet",
        native_meeting_id="abc-defg-hij",
        withdrawn_at="2026-07-15T08:41:00+00:00",
    )

    assert result["changed"] is True
    assert result["should_stop"] is False
    assert meeting["status"] == terminal_status
    assert meeting["end_time"] == "2026-07-15T08:40:00Z"
    assert meeting["data"]["terminal_evidence"] == "preserve-me"
    assert meeting["data"]["zaki_capture"]["teardown_state"] == "confirmed"


async def test_postgres_teardown_confirmation_is_narrow_terminal_and_preserves_concurrent_fields():
    meeting = {
        "id": 41,
        "status": "stopping",
        "end_time": None,
        "data": {
            "zaki_capture": {
                "tenant_id": "tenant-a",
                "state": "withdrawn",
                "teardown_state": "pending",
                "concurrent_retry_evidence": "preserve-me",
            },
            "unrelated": {"also": "preserve-me"},
        },
    }
    session = _Session(meeting)
    repo = SqlAlchemyMeetingRepo(
        lambda: session,
        statement_factory=lambda sql: sql,
    )

    confirmed = await repo.confirm_capture_teardown(meeting_id=41)

    assert confirmed is True
    assert session.events == ["confirm_teardown", "commit"]
    assert meeting["status"] == "completed"
    assert meeting["end_time"] is not None
    assert meeting["data"] == {
        "zaki_capture": {
            "tenant_id": "tenant-a",
            "state": "withdrawn",
            "teardown_state": "confirmed",
            "concurrent_retry_evidence": "preserve-me",
        },
        "completion_reason": "stopped",
        "unrelated": {"also": "preserve-me"},
    }


@pytest.mark.parametrize("terminal_status", ["completed", "failed"])
async def test_postgres_teardown_confirmation_preserves_existing_terminal_status(terminal_status):
    meeting = {
        "id": 42,
        "status": terminal_status,
        "end_time": "2026-07-15T08:40:00Z",
        "data": {
            "zaki_capture": {
                "state": "withdrawn",
                "teardown_state": "pending",
            },
            "completion_reason": "left_alone",
        },
    }
    session = _Session(meeting)
    repo = SqlAlchemyMeetingRepo(lambda: session, statement_factory=lambda sql: sql)

    confirmed = await repo.confirm_capture_teardown(meeting_id=42)

    assert confirmed is True
    assert meeting["status"] == terminal_status
    assert meeting["end_time"] == "2026-07-15T08:40:00Z"
    assert meeting["data"]["completion_reason"] == "left_alone"
    assert meeting["data"]["zaki_capture"]["teardown_state"] == "confirmed"


class _GuardedSpawnSession:
    def __init__(self):
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement, params=None):
        self.calls += 1
        if self.calls == 1:
            return _Result()
        if self.calls == 2:
            sql = " ".join(statement.split())
            assert "data->'zaki_capture'->>'tenant_id' = :tenant_id" in sql
            assert params["tenant_id"] == "tenant-a"
            return _Result(
                MappingProxyType({
                    "zaki_capture": {
                        "state": "withdrawn",
                        "withdrawn_at": "2026-07-15T08:32:00+00:00",
                    }
                })
            )
        raise AssertionError(f"unexpected guarded-spawn query: {statement}")


async def test_postgres_spawn_rejects_authority_that_predates_scope_withdrawal():
    repo = SqlAlchemyMeetingRepo(
        lambda: _GuardedSpawnSession(),
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(CaptureGrantConsumed, match="predates"):
        await repo.create_meeting_guarded(
            user_id=7,
            platform="google_meet",
            native_meeting_id="abc-defg-hij",
            data={
                "zaki_capture": {
                    "tenant_id": "tenant-a",
                    "state": "authorized",
                    "authorized_at": "2026-07-15T08:31:00+00:00",
                    "grant_id_sha256": "a" * 64,
                }
            },
        )


class _TranscriptSession:
    def __init__(self, data):
        self.data = data
        self.events: list[str] = []
        self.transaction_lock = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement, params=None):
        sql = " ".join(statement.split())
        if "pg_advisory_xact_lock_shared" in sql:
            self.events.append("lock")
            self.transaction_lock = True
            return _Result()
        if sql.startswith("SELECT data FROM meetings"):
            self.events.append("state")
            return _Result({"data": self.data})
        raise AssertionError(f"unexpected SQL: {sql}")

    async def commit(self):
        self.events.append("commit")
        self.transaction_lock = False

    async def rollback(self):
        self.events.append("rollback")
        self.transaction_lock = False

    async def invalidate(self):
        self.events.append("invalidate")
        self.transaction_lock = False


class _NoRedisWrite:
    def pipeline(self, **kwargs):
        raise AssertionError("withdrawn transcript reached Redis")


class _TerminalRedis:
    def __init__(self, meeting_id):
        self.meeting_id = meeting_id
        self.deleted = []
        self.removed = []

    async def hgetall(self, key):
        raise AssertionError("terminal finalization must not materialize the Redis hash")

    async def hscan(self, key, *, cursor, count):
        assert count <= 100
        if int(cursor):
            return 0, {}
        return 0, {
            b"s1": json.dumps({
                "segment_id": "s1", "start": 1.0, "end": 2.0,
                "text": "durable", "speaker": "Alice", "language": "en",
            }).encode()
        }

    async def delete(self, key):
        self.deleted.append(key)

    async def srem(self, key, member):
        self.removed.append((key, member))


class _RowsResult(_Result):
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        raise AssertionError("terminal finalization must stream the durable census")

    def __aiter__(self):
        async def rows():
            for row in self.rows:
                yield row
        return rows()


class _TerminalSession:
    def __init__(self, data):
        self.data = data
        self.rows = []
        self.events = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        params = params or {}
        if sql == "SELECT pg_advisory_xact_lock(:meeting_lock_key)":
            assert params == {"meeting_lock_key": -2_147_483_648}
            self.events.append("lock")
            return _Result()
        if sql.startswith("SELECT data FROM meetings"):
            self.events.append("state")
            return _Result({"data": self.data})
        if sql.startswith("INSERT INTO transcriptions"):
            self.events.append("write")
            self.rows.append({
                "segment_id": params["segid"], "start": params["start"],
                "end": params["end"], "text": params["text"],
                "speaker": params["speaker"], "language": params["lang"],
            })
            return _Result()
        if sql.startswith("UPDATE meetings SET data"):
            self.events.append("marker")
            self.data = json.loads(params["data"])
            return _Result()
        raise AssertionError(f"unexpected SQL: {sql}")

    async def stream(self, statement, params=None, **kwargs):
        sql = " ".join(str(statement).split())
        assert sql.startswith("SELECT LEFT(segment_id, 257) AS segment_id")
        assert kwargs == {"execution_options": {"yield_per": 100}}
        self.events.append("census")
        return _RowsResult(self.rows)

    async def commit(self):
        self.events.append("commit")

    async def rollback(self):
        self.events.append("rollback")


async def test_postgres_terminal_finalization_seals_bigint_meeting_under_one_barrier():
    data = {
        "zaki_capture": {"state": "authorized"},
        "zaki_retention": {
            "state": "open", "expired_scopes": [],
            "scope_expiries": {
                "audio": "2099-01-01T00:00:00+00:00",
                "transcript": "2099-01-01T00:00:00+00:00",
                "summary": "2099-01-01T00:00:00+00:00",
            },
        },
    }
    session = _TerminalSession(data)
    redis = _TerminalRedis(2_147_483_648)
    store = SqlAlchemyTranscriptStore(
        lambda: session, redis, statement_factory=lambda sql: sql,
    )

    outcome = await store.finalize_transcript(redis, 2_147_483_648)

    assert outcome.state == "finalized"
    assert outcome.marker["segment_count"] == 1
    assert session.events == ["lock", "state", "write", "census", "marker", "commit"]
    assert session.data["zaki_transcript_finalization"] == outcome.marker
    assert redis.deleted == ["meeting:2147483648:segments"]


class _CancellationPool:
    def __init__(self):
        self.transaction_lock_held = False


class _CancellationSession(_TerminalSession):
    def __init__(self, data, pool):
        super().__init__(data)
        self.pool = pool

    async def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        if sql == "SELECT pg_advisory_xact_lock(:meeting_lock_key)":
            self.pool.transaction_lock_held = True
        return await super().execute(statement, params)

    async def commit(self):
        await super().commit()
        self.pool.transaction_lock_held = False

    async def rollback(self):
        await super().rollback()
        self.pool.transaction_lock_held = False


class _CancelledTerminalRedis(_TerminalRedis):
    def __init__(self, meeting_id, entered):
        super().__init__(meeting_id)
        self.entered = entered

    async def hscan(self, key, *, cursor, count):
        self.entered.set()
        await asyncio.Future()


async def test_postgres_terminal_finalization_cancellation_releases_transaction_lock():
    pool = _CancellationPool()
    session = _CancellationSession({
        "zaki_capture": {"state": "authorized"},
        "zaki_retention": {
            "state": "open", "expired_scopes": [],
            "scope_expiries": {
                "audio": "2099-01-01T00:00:00+00:00",
                "transcript": "2099-01-01T00:00:00+00:00",
                "summary": "2099-01-01T00:00:00+00:00",
            },
        },
    }, pool)
    entered = asyncio.Event()
    redis = _CancelledTerminalRedis(2_147_483_648, entered)
    store = SqlAlchemyTranscriptStore(
        lambda: session, redis, statement_factory=lambda sql: sql,
    )

    task = asyncio.create_task(store.finalize_transcript(redis, 2_147_483_648))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert pool.transaction_lock_held is False
    assert session.events == ["lock", "state", "rollback"]
    assert redis.deleted == []


class _OversizedTerminalRedis(_TerminalRedis):
    async def hscan(self, key, *, cursor, count):
        return 0, {b"oversized": b"x" * (512 * 1024 + 1)}


async def test_postgres_terminal_finalization_fails_closed_before_oversized_carrier_load():
    session = _TerminalSession({
        "zaki_capture": {"state": "authorized"},
        "zaki_retention": {
            "state": "open", "expired_scopes": [],
            "scope_expiries": {
                "audio": "2099-01-01T00:00:00+00:00",
                "transcript": "2099-01-01T00:00:00+00:00",
                "summary": "2099-01-01T00:00:00+00:00",
            },
        },
    })
    redis = _OversizedTerminalRedis(2_147_483_648)
    store = SqlAlchemyTranscriptStore(
        lambda: session, redis, statement_factory=lambda sql: sql,
    )

    with pytest.raises(ValueError, match="safe bounds"):
        await store.finalize_transcript(redis, 2_147_483_648)

    assert session.events == ["lock", "state", "rollback"]
    assert "zaki_transcript_finalization" not in session.data
    assert redis.deleted == []


class _CleanupOnlyRedis(_TerminalRedis):
    async def hscan(self, key, *, cursor, count):
        raise AssertionError("sealed retry rebuilt the transcript carrier")


async def test_postgres_terminal_finalization_retry_uses_marker_then_only_cleans_carrier():
    marker = {
        "state": "finalized",
        "revision": "sha256:" + "a" * 64,
        "finalized_at": "2026-07-16T12:00:00Z",
        "segment_count": 1,
    }
    session = _TerminalSession({
        "zaki_capture": {"state": "authorized"},
        "zaki_retention": {
            "state": "open", "expired_scopes": [],
            "scope_expiries": {
                "audio": "2099-01-01T00:00:00+00:00",
                "transcript": "2099-01-01T00:00:00+00:00",
                "summary": "2099-01-01T00:00:00+00:00",
            },
        },
        "zaki_transcript_finalization": marker,
    })
    redis = _CleanupOnlyRedis(2_147_483_648)
    store = SqlAlchemyTranscriptStore(
        lambda: session, redis, statement_factory=lambda sql: sql,
    )

    outcome = await store.finalize_transcript(redis, 2_147_483_648)

    assert outcome.marker == marker
    assert session.events == ["lock", "state", "commit"]
    assert redis.deleted == ["meeting:2147483648:segments"]


async def test_postgres_transcript_writer_refuses_after_withdrawal_under_shared_barrier():
    session = _TranscriptSession({"zaki_capture": {"state": "withdrawn"}})
    store = SqlAlchemyTranscriptStore(
        lambda: session,
        _NoRedisWrite(),
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(TranscriptWriteRefused, match="not writable"):
        await store.append_segment(41, {"segment_id": "late"})

    assert session.events == ["lock", "state", "rollback"]


async def test_postgres_transcript_writer_fails_closed_on_malformed_retention_state():
    session = _TranscriptSession(
        {"zaki_retention": {"state": "open", "expired_scopes": "transcript"}}
    )
    store = SqlAlchemyTranscriptStore(
        lambda: session,
        _NoRedisWrite(),
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(TranscriptWriteRefused, match="not writable"):
        await store.append_segment(41, {"segment_id": "late"})

    assert session.events == ["lock", "state", "rollback"]


async def test_postgres_transcript_writer_refuses_after_finalization_marker():
    session = _TranscriptSession({
        "zaki_transcript_finalization": {
            "state": "finalized",
            "revision": "sha256:" + "a" * 64,
            "finalized_at": "2026-07-16T12:00:00Z",
            "segment_count": 1,
        }
    })
    store = SqlAlchemyTranscriptStore(
        lambda: session,
        _NoRedisWrite(),
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(TranscriptWriteRefused, match="not writable"):
        await store.append_segment(41, {"segment_id": "late"})

    assert session.events == ["lock", "state", "rollback"]


async def test_postgres_transcript_write_lease_cancellation_releases_transaction_lock():
    session = _TranscriptSession({})
    store = SqlAlchemyTranscriptStore(
        lambda: session,
        redis_client=None,
        statement_factory=lambda sql: sql,
    )
    entered = asyncio.Event()
    blocked = asyncio.Event()

    async def writer():
        async with store.transcript_write_lease(41):
            entered.set()
            await blocked.wait()

    task = asyncio.create_task(writer())
    await entered.wait()
    assert session.transaction_lock is True
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert session.transaction_lock is False
    assert session.events == ["lock", "state", "rollback"]


async def test_postgres_transcript_write_lease_invalidates_when_rollback_is_uncertain():
    class UncertainSession(_TranscriptSession):
        async def rollback(self):
            self.events.append("rollback")
            raise RuntimeError("connection lost during rollback")

    session = UncertainSession({})
    store = SqlAlchemyTranscriptStore(
        lambda: session,
        redis_client=None,
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(ValueError, match="body failed"):
        async with store.transcript_write_lease(41):
            raise ValueError("body failed")

    assert session.transaction_lock is False
    assert session.events == ["lock", "state", "rollback", "invalidate"]


class _DurableTranscriptSession:
    def __init__(self, data):
        self.data = data
        self.events: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def commit(self):
        self.events.append("commit")

    async def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        if "pg_advisory_xact_lock_shared" in sql:
            self.events.append("lock")
            return _Result()
        if sql.startswith("SELECT data FROM meetings"):
            self.events.append("state")
            return _Result({"data": self.data})
        if sql.startswith("INSERT INTO transcriptions"):
            self.events.append("write")
            return _Result()
        raise AssertionError(f"unexpected SQL: {sql}")


async def test_postgres_durable_transcript_flush_refuses_after_withdrawal(monkeypatch):
    monkeypatch.setitem(sys.modules, "sqlalchemy", SimpleNamespace(text=lambda sql: sql))
    session = _DurableTranscriptSession({"zaki_capture": {"state": "withdrawn"}})
    store = SqlAlchemyTranscriptStore(
        lambda: session,
        None,
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(TranscriptWriteRefused, match="not writable"):
        await store.upsert_segments(
            41,
            [{"segment_id": "late", "start": 0, "end": 1, "text": "sensitive"}],
        )

    assert session.events == ["lock", "state"]


async def test_postgres_durable_transcript_flush_refuses_after_transcript_expiry(monkeypatch):
    monkeypatch.setitem(sys.modules, "sqlalchemy", SimpleNamespace(text=lambda sql: sql))
    session = _DurableTranscriptSession(
        {"zaki_retention": {"state": "open", "expired_scopes": ["transcript"]}}
    )
    store = SqlAlchemyTranscriptStore(
        lambda: session,
        None,
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(TranscriptWriteRefused, match="not writable"):
        await store.upsert_segments(
            41,
            [{"segment_id": "late", "start": 0, "end": 1, "text": "sensitive"}],
        )

    assert session.events == ["lock", "state"]


class _ProcessedViewSession:
    def __init__(self, data=None):
        self.data = data or {"zaki_capture": {"state": "withdrawn"}}
        self.events: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def commit(self):
        self.events.append("commit")

    async def execute(self, statement, params=None):
        sql = " ".join(statement.split())
        if "pg_advisory_xact_lock_shared" in sql:
            self.events.append("lock")
            return _Result()
        if sql.startswith("SELECT data FROM meetings"):
            self.events.append("row")
            return _Result({"data": self.data})
        raise AssertionError(f"unexpected SQL: {sql}")


async def test_postgres_processed_view_refuses_after_withdrawal_under_shared_barrier():
    session = _ProcessedViewSession()
    store = SqlAlchemyTranscriptStore(
        lambda: session,
        None,
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(TranscriptWriteRefused, match="not writable"):
        await store.merge_processed_view(
            41,
            view_id="meeting-copilot:cleaned-transcript:v1",
            kind="cleaned_transcript",
            notes=[{"id": "s1", "text": "sensitive"}],
            source_cursor="1-0",
        )

    assert session.events == ["lock", "row"]


async def test_postgres_processed_view_refuses_after_summary_expiry_under_shared_barrier():
    session = _ProcessedViewSession(
        {
            "zaki_retention": {
                "state": "open",
                "scope_expiries": {
                    "transcript": "2099-01-01T00:00:00+00:00",
                    "summary": "2099-01-01T00:00:00+00:00",
                },
                "expired_scopes": ["summary"],
            }
        }
    )
    store = SqlAlchemyTranscriptStore(
        lambda: session,
        None,
        statement_factory=lambda sql: sql,
    )

    with pytest.raises(TranscriptWriteRefused, match="not writable"):
        await store.merge_processed_view(
            41,
            view_id="meeting-copilot:cleaned-transcript:v1",
            kind="cleaned_transcript",
            notes=[{"id": "s1", "text": "sensitive"}],
            source_cursor="1-0",
        )

    assert session.events == ["lock", "row"]
