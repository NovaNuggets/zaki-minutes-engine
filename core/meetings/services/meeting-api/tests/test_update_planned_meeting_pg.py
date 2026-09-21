"""Real-Postgres conformance for the PATCH guard and the rename clock-pin (L-0188, gate-1 bounce).

The offline suite drives the guard through ``InMemoryTranscriptStore`` — a stand-in that never
executes the SQL, so mutants inside ``SqlAlchemyTranscriptStore.update_planned_meeting`` survived
the whole offline run (gate 1 on #67: E1–E3 each passed 1199 tests). These tests run the
PRODUCTION adapters — ``SqlAlchemyTranscriptStore`` for the PATCH, ``SqlAlchemyMeetingRepo`` for
the two staleness sweeps — against a REAL Postgres: the same ``pg_advisory_xact_lock`` +
``SELECT … FOR UPDATE`` + ``onupdate=func.now()`` the row actually gets. Gated on
``MEETING_API_TEST_DATABASE_URL`` (set by the ``python`` leg of ``gates.yml``, which also layers
SQLAlchemy/asyncpg in via ``uv run --with``); skips clean in the offline venv, like
``test_single_flight.py::test_pg_advisory_lock_runs_on_real_postgres``.

What is pinned here — each bullet names the mutant it kills:

- the lock matrix on the REAL adapter: a title-only PATCH lands on every FSM status — kills
  E1 (the guard restored to the blanket ``status not in ("idle", "scheduled")`` lock);
- every other key of a started meeting — alone or smuggled beside a title — returns
  ``{"error": "conflict"}`` and changes nothing — kills E2 (all fields unlocked, ``and False``)
  and E3 (``workspace_id`` / ``auto_join`` also exempted);
- a rename on a NON-TERMINAL FSM row does not move ``meetings.updated_at`` — the FSM's
  staleness clock — so a row past its grace stays in ``list_stale_stopping`` /
  ``list_stale_nonterminal``. Parametrized over ALL SIX live statuses: the first pin seeded
  only `stopping` + `active`, so single-site mutants returning `needs_help` / `joining` /
  `requested` / `awaiting_admission` to the clock-moving side passed the full suite (gate 1's
  N7–N10 on the ESCALATE). That is the defect gate 1 reproduced on the unfixed head: a
  `stopping` row quiet 30 min went ``[178] → []`` after one title-only PATCH.
- the boundary the pin stops at: a rename DOES move ``updated_at`` on ``completed`` /
  ``failed`` rows (the CoS's Q2 ruling on the ESCALATE — the read index sorts and
  ``since``-filters on it, so a renamed finished meeting floats to the top) and on the intent
  statuses ``idle`` / ``scheduled`` (not FSM-owned; the base behaviour). Kills N3 (pin the
  terminal rows too), N11 (pin ``failed`` only) and N4 (pin the intent rows too).
"""
from __future__ import annotations

import os
import secrets
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip(
    "sqlalchemy",
    reason="the production adapters need SQLAlchemy; the offline gate venv lacks it by design",
)

pytestmark = pytest.mark.skipif(
    not os.getenv("MEETING_API_TEST_DATABASE_URL"),
    reason="real-Postgres conformance for the PATCH guard; set MEETING_API_TEST_DATABASE_URL to run",
)

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from meeting_api.bot_spawn.adapters import SqlAlchemyMeetingRepo
from meeting_api.collector.adapters import SqlAlchemyTranscriptStore
from meeting_api.sessions.models import Base, Meeting, MeetingSession

# All eight FSM-owned statuses (gate 1's matrix) — every one must accept a rename.
FSM_OWNED = [
    "requested", "joining", "awaiting_admission", "active",
    "needs_help", "stopping", "completed", "failed",
]

# The six LIVE FSM-owned statuses — the domain of the staleness-clock pin (the ruling's
# "non-terminal FSM rows"). Mirrors ``list_stale_nonterminal``'s ``non_terminal`` list.
LIVE_FSM = [
    "requested", "joining", "awaiting_admission", "active", "needs_help", "stopping",
]
# Terminal FSM rows: a rename DOES move ``updated_at`` (the CoS's Q2 ruling — the archive
# index sorts and ``since``-filters on it).
TERMINAL_FSM = ["completed", "failed"]
# Intent rows are not FSM-owned; a PATCH there keeps the base behaviour and moves the clock.
INTENT = ["idle", "scheduled"]


async def _schema(engine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def _seed(sf, *, user_id, status, title=None, updated_at=None) -> int:
    """Insert one owned ``meetings`` row + one ``meeting_sessions`` row. ``updated_at`` is set by
    an explicit UPDATE value — an explicit SET overrides the column's ``onupdate=func.now()``
    (the same mechanism the rename pin relies on, used here to make the row stale on demand)."""
    tag = secrets.token_hex(4)
    async with sf() as db:
        m = Meeting(
            user_id=user_id, platform="google_meet",
            platform_specific_id=f"pg-{status}-{tag}", status=status,
            data={"title": title} if title else {},
        )
        db.add(m)
        await db.flush()
        db.add(MeetingSession(
            meeting_id=m.id, session_uid=f"sess-{m.id}-{tag}",
            session_start_time=datetime.now(timezone.utc),
        ))
        if updated_at is not None:
            await db.execute(
                update(Meeting).where(Meeting.id == m.id).values(updated_at=updated_at)
            )
        await db.commit()
        return m.id


async def _cleanup(engine, meeting_ids) -> None:
    """Drop only the rows this file inserted (the CI database is throwaway; a hand-run DB is
    not, so the fixture leaves nothing behind)."""
    if not meeting_ids:
        return
    async with engine.begin() as conn:
        await conn.execute(
            delete(MeetingSession).where(MeetingSession.meeting_id.in_(meeting_ids))
        )
        await conn.execute(delete(Meeting).where(Meeting.id.in_(meeting_ids)))


def _engine():
    return create_async_engine(os.environ["MEETING_API_TEST_DATABASE_URL"])


async def test_pg_adapter_lock_matrix_every_fsm_status():
    """The production guard, per status: a title-only PATCH lands on all eight FSM statuses;
    every other key — alone or smuggled beside a title — returns ``{"error": "conflict"}`` and
    leaves the row byte-identical."""
    engine = _engine()
    try:
        await _schema(engine)
        sf = async_sessionmaker(engine, expire_on_commit=False)
        store = SqlAlchemyTranscriptStore(sf)
        uid = secrets.randbelow(600_000_000) + 1_500_000_000
        seeded: list[int] = []
        locked_bodies = [
            {"scheduled_at": "2026-10-01T15:00:00Z"},
            {"constructed_meeting_url": "https://meet.google.com/aaa-bbbb-ccc"},
            {"workspace_id": "ws-gate"},
            {"auto_join": False},
            {"native_meeting_id": "other-native-id"},
            {"platform": "teams", "native_meeting_id": "other"},
            {"attendees": ["someone@example.com"]},
            {"calendar_uid": "uid-gate"},
            {"title": "smuggled", "auto_join": False},
            {"title": "smuggled", "workspace_id": "ws-gate"},
            {"status": "completed"},  # not an updates key at all — the adapter still refuses
        ]
        try:
            for status in FSM_OWNED:
                mid = await _seed(sf, user_id=uid, status=status)
                seeded.append(mid)
                res = await store.update_planned_meeting(
                    uid, mid, {"title": f"renamed-{status}"}
                )
                # Kills E1 (blanket lock): the mutant returns {"error": "conflict"} here.
                assert res.get("data", {}).get("title") == f"renamed-{status}", (status, res)
                assert res["status"] == status, "a rename never touches the FSM state"
                for body in locked_bodies:
                    res = await store.update_planned_meeting(uid, mid, body)
                    # Kills E2 (every field unlocked) and E3 (workspace_id/auto_join exempted).
                    assert res == {"error": "conflict"}, (status, body, res)
                # No breach: a refused patch changed nothing — not even via a smuggled title.
                async with sf() as db:
                    st, data = (
                        await db.execute(
                            select(Meeting.status, Meeting.data).where(Meeting.id == mid)
                        )
                    ).one()
                assert st == status
                assert data == {"title": f"renamed-{status}"}, (status, data)

            # Intent rows are not FSM-owned: a full PATCH still applies there.
            mid_idle = await _seed(sf, user_id=uid, status="idle")
            seeded.append(mid_idle)
            res = await store.update_planned_meeting(
                uid, mid_idle,
                {"title": "planned", "scheduled_at": "2026-10-01T15:00:00Z",
                 "auto_join": False},
            )
            assert res["status"] == "scheduled"
            assert res["data"]["scheduled_at"] == "2026-10-01T15:00:00Z"
            assert res["data"]["auto_join"] is False
        finally:
            await _cleanup(engine, seeded)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("status", LIVE_FSM)
async def test_pg_rename_keeps_the_fsm_staleness_clock(status):
    """A title-only rename on a LIVE FSM row must not move ``meetings.updated_at``: that
    column is the staleness clock ``list_stale_stopping`` (stop backstop) and
    ``list_stale_nonterminal`` (general reap) read. Gate 1's repro on the unfixed head: one
    rename pushed the clock one window forward and the sweeps lost the rows. Parametrized
    over all six live statuses — seeding only `stopping` + `active` let the N7–N10 single-site
    mutants (one of `needs_help`/`joining`/`requested`/`awaiting_admission` back on the
    clock-moving side) pass the full suite."""
    engine = _engine()
    try:
        await _schema(engine)
        sf = async_sessionmaker(engine, expire_on_commit=False)
        store = SqlAlchemyTranscriptStore(sf)
        repo = SqlAlchemyMeetingRepo(sf)
        uid = secrets.randbelow(600_000_000) + 1_500_000_000
        seeded: list[int] = []
        try:
            stale = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=3600)
            mid = await _seed(sf, user_id=uid, status=status, updated_at=stale)
            seeded.append(mid)

            # Past grace before the rename — gate 1's `before` state ([178], [178,179]).
            if status == "stopping":
                assert mid in [
                    t[0] for t in await repo.list_stale_stopping(older_than_seconds=45)
                ]
            nt_before = [
                t[0] for t in await repo.list_stale_nonterminal(stop_grace=45, active_grace=300)
            ]
            assert mid in nt_before

            res = await store.update_planned_meeting(
                uid, mid, {"title": f"renamed while {status}"}
            )
            assert res.get("data", {}).get("title") == f"renamed while {status}", res

            # The sweeps still see the row — the rename did not buy it another window.
            # Kills N7–N10: under a mutant the status returns to the clock-moving side, the
            # pinned instant is overwritten by ``onupdate``, and the row drops out here.
            if status == "stopping":
                assert mid in [
                    t[0] for t in await repo.list_stale_stopping(older_than_seconds=45)
                ], "the stop backstop lost a stuck `stopping` row to a rename"
            nt_after = [
                t[0] for t in await repo.list_stale_nonterminal(stop_grace=45, active_grace=300)
            ]
            assert mid in nt_after, (
                f"the general reap lost a stale `{status}` row to a rename"
            )

            # And the column itself is untouched — the pinned instant survives verbatim.
            async with sf() as db:
                upd = (
                    await db.execute(
                        select(Meeting.updated_at).where(Meeting.id == mid)
                    )
                ).scalar_one()
            assert upd == stale, f"updated_at moved on a rename: {stale} -> {upd}"
        finally:
            await _cleanup(engine, seeded)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("status", TERMINAL_FSM)
async def test_pg_rename_moves_the_clock_on_terminal_rows(status):
    """The pin stops AT the terminal boundary (the CoS's Q2 ruling on gate 1's ESCALATE): a
    rename on a ``completed``/``failed`` row DOES move ``meetings.updated_at`` — the zaki-read
    index sorts on it and ``since``-filters on it, so a renamed finished meeting floats to the
    top and ``since`` readers see the new title. Kills N3 (``flag_modified`` extended to the
    terminal rows) and N11 (extended to ``failed`` only): under either mutant the pinned
    instant survives and the ``upd > stale`` assertion goes red."""
    engine = _engine()
    try:
        await _schema(engine)
        sf = async_sessionmaker(engine, expire_on_commit=False)
        store = SqlAlchemyTranscriptStore(sf)
        uid = secrets.randbelow(600_000_000) + 1_500_000_000
        seeded: list[int] = []
        try:
            stale = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=3600)
            mid = await _seed(sf, user_id=uid, status=status, updated_at=stale)
            seeded.append(mid)

            res = await store.update_planned_meeting(
                uid, mid, {"title": f"renamed after {status}"}
            )
            assert res.get("data", {}).get("title") == f"renamed after {status}", res

            async with sf() as db:
                upd = (
                    await db.execute(
                        select(Meeting.updated_at).where(Meeting.id == mid)
                    )
                ).scalar_one()
            assert upd > stale, (
                f"a rename on a `{status}` row must move updated_at "
                f"(archive order + `since` readers): {stale} -> {upd}"
            )
        finally:
            await _cleanup(engine, seeded)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("status", INTENT)
async def test_pg_rename_moves_the_clock_on_intent_rows(status):
    """``idle``/``scheduled`` rows are not FSM-owned — ``update_planned_meeting`` is their
    normal full-PATCH path — so a rename there keeps the base behaviour and moves
    ``updated_at`` (the head's reading of the ruling's "non-terminal FSM rows": the pin's
    domain is exactly the six live statuses). Kills N4 (``flag_modified`` extended to the
    intent rows)."""
    engine = _engine()
    try:
        await _schema(engine)
        sf = async_sessionmaker(engine, expire_on_commit=False)
        store = SqlAlchemyTranscriptStore(sf)
        uid = secrets.randbelow(600_000_000) + 1_500_000_000
        seeded: list[int] = []
        try:
            stale = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=3600)
            mid = await _seed(sf, user_id=uid, status=status, updated_at=stale)
            seeded.append(mid)

            res = await store.update_planned_meeting(
                uid, mid, {"title": f"renamed while {status}"}
            )
            assert res.get("data", {}).get("title") == f"renamed while {status}", res

            async with sf() as db:
                upd = (
                    await db.execute(
                        select(Meeting.updated_at).where(Meeting.id == mid)
                    )
                ).scalar_one()
            assert upd > stale, (
                f"a rename on an `{status}` (intent) row must move updated_at "
                f"(base behaviour, unchanged): {stale} -> {upd}"
            )
        finally:
            await _cleanup(engine, seeded)
    finally:
        await engine.dispose()
