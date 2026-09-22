"""Production-adapter proofs for lifecycle compare-and-set durability."""
from __future__ import annotations

import sys
from types import SimpleNamespace

from meeting_api.bot_spawn.adapters import SqlAlchemyMeetingRepo


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalars(self):
        return self

    def first(self):
        return self.value


class _ConflictSession:
    def __init__(self):
        self.meeting = SimpleNamespace(
            id=1,
            user_id=7,
            platform="google_meet",
            platform_specific_id="race",
            status="completed",
            bot_container_id="wl-1",
            start_time=None,
            end_time=None,
            data={"completion_reason": "left_alone"},
            created_at=None,
            updated_at=None,
        )
        self.statements: list[str] = []
        self.committed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement):
        sql = str(statement)
        self.statements.append(sql)
        if len(self.statements) == 1:
            return _ScalarResult(SimpleNamespace(meeting_id=1))
        return _ScalarResult(self.meeting)

    async def commit(self):
        self.committed = True


async def test_sql_status_write_locks_then_rejects_a_stale_expected_status(monkeypatch):
    """The CAS check happens against the row selected FOR UPDATE in the same transaction."""
    class _Field:
        def __eq__(self, other):
            return (self, other)

    class _Statement:
        def __init__(self):
            self.for_update = False

        def where(self, *predicates):
            return self

        def with_for_update(self):
            self.for_update = True
            return self

        def __str__(self):
            return "SELECT fake FOR UPDATE" if self.for_update else "SELECT fake"

    fake_models = SimpleNamespace(
        Meeting=SimpleNamespace(id=_Field()),
        MeetingSession=SimpleNamespace(session_uid=_Field()),
    )
    monkeypatch.setitem(sys.modules, "sqlalchemy", SimpleNamespace(select=lambda *args: _Statement()))
    monkeypatch.setitem(sys.modules, "sqlalchemy.orm", SimpleNamespace())
    monkeypatch.setitem(
        sys.modules,
        "sqlalchemy.orm.attributes",
        SimpleNamespace(flag_modified=lambda *args: None),
    )
    monkeypatch.setitem(sys.modules, "meeting_api.sessions.models", fake_models)

    session = _ConflictSession()
    repo = SqlAlchemyMeetingRepo(lambda: session)

    result = await repo.update_meeting_status(
        session_uid="race-session",
        status="active",
        expected_status="joining",
    )

    assert "FOR UPDATE" in session.statements[1]
    assert result.disposition == "conflict"
    assert result.row["status"] == "completed"
    assert session.meeting.status == "completed"
    assert session.committed is False
