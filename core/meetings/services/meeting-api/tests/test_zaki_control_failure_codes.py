"""A failed capture must record WHY it failed, not the generic `internal_failure`.

Measured on 2026-08-14 from the live control-plane DB: 9 failed captures on staging + 2 on prod,
a ~28% capture failure rate in two independent environments, and EVERY one of them carrying
`failure_code = 'internal_failure'`.  The per-meeting bot pods are deleted after each run and
neither cluster aggregates logs, so those failures were undiagnosable — the row was the only
surviving evidence and it said nothing.

The cause was a dead translation, not a missing one.  The engine derives real attribution
server-side and persists it in `meetings.data` (`completion_reason`, `failure_stage`,
`spawn_failure_reason`), but the control layer read only `failure_stage` — whose vocabulary
(`joining`, `awaiting_admission`, `active`, `runtime_spawn`) shares NO value with the sealed
`FailureCode` enum.  So `_emit_capture_status`'s "not a known code" floor caught 100% of failures
and stamped `internal_failure` on all of them.

These tests drive the production `ControlCallbackDispatcher` over the in-memory store and assert
the specific code for each failure class the issue asks to distinguish.
"""
from __future__ import annotations

import logging

from meeting_api.zaki_control.callbacks import (
    ControlCallbackDispatcher,
    failure_code_from_meeting_data,
)
from meeting_api.zaki_control.fakes import InMemoryControlStore
from meeting_api.zaki_control.ports import Capture, Subject


def _dispatcher(state: str = "awaiting_admission"):
    store = InMemoryControlStore()
    capture = Capture(
        capture_id="cap-1", subject=Subject(tenant_id="tenant-1", user_id="42"),
        operation_id="op-1", reservation_id="reserve-1", platform="google_meet",
        native_meeting_id="abc-defg-hij", meeting_id="7", state=state,
    )
    store.captures[capture.capture_id] = capture
    return store, ControlCallbackDispatcher(
        store,
        callback_url="https://hub.example/api/minutes/callback/v1",
        hmac_key="hub-callback-hmac-key-0123456789abcdef",
    )


def _meeting(data: dict, *, status: str = "failed") -> dict:
    return {"id": 7, "status": status, "data": data, "end_time": None}


async def _failed_via_lifecycle(data: dict, *, from_state: str = "awaiting_admission") -> str:
    """Drive the real bot-callback feeder (`app.py` → `record_lifecycle`) to a failed capture."""
    store, dispatcher = _dispatcher(from_state)
    await dispatcher.record_lifecycle(_meeting(data), state="failed")
    capture = store.captures["cap-1"]
    assert capture.state == "failed"
    return capture.failure_code


# ── the classes the issue asks to distinguish ────────────────────────────────────────────────────


async def test_admission_rejected_records_join_denied():
    """A host who refuses the bot is a JOIN denial, not an internal fault."""
    code = await _failed_via_lifecycle(
        {"completion_reason": "awaiting_admission_rejected", "failure_stage": "awaiting_admission"}
    )
    assert code == "join_denied"


async def test_admission_timeout_says_nobody_admitted_it_not_that_time_ran_out():
    """The waiting-room timer firing is a JOIN denial, NOT `capture_timeout`.

    The dominant live class: 4 of the 10 failed captures (3 staging + prod meeting 9).  `join_denied`
    is what the Hub renders as "Nobody admitted the notetaker, so it left the waiting room"
    (`MinutesControls.tsx:374`), which is exactly what happened.  `capture_timeout` is spoken for by
    the plan LIFETIME cap and renders as "The capture reached its maximum length and was closed"
    (:387) — asserting a full-length capture about a bot that captured nothing.
    """
    code = await _failed_via_lifecycle(
        {"completion_reason": "awaiting_admission_timeout", "failure_stage": "awaiting_admission"}
    )
    assert code == "join_denied"


async def test_the_lifetime_cap_is_the_only_capture_timeout():
    """`capture_timeout` belongs to the one reason that really is a time limit."""
    assert await _failed_via_lifecycle(
        {"completion_reason": "max_bot_time_exceeded", "failure_stage": "active"},
        from_state="active",
    ) == "capture_timeout"


async def test_a_capture_abandoned_in_the_lobby_did_not_end_a_meeting():
    """`stopped` — the most common `completion_reason` in live data — is stage-dependent.

    All 5 live `stopped` failures are PRE-ACTIVE with `start_time` NULL (4 at `awaiting_admission`,
    1 at `joining`): the user cancelled while the bot was still outside the door.
    `meeting_ended_early` renders as "The meeting ended before the capture could start", which
    asserts an ending for a meeting that never began — wrong in the most misleading direction.
    Only a `stopped` that reached `active` is a real early end.
    """
    for stage, from_state in (("awaiting_admission", "awaiting_admission"), ("joining", "joining")):
        assert await _failed_via_lifecycle(
            {"completion_reason": "stopped", "failure_stage": stage}, from_state=from_state
        ) == "join_denied", stage
    assert await _failed_via_lifecycle(
        {"completion_reason": "stopped", "failure_stage": "active"}, from_state="active"
    ) == "meeting_ended_early"


def test_every_live_failure_row_translates_to_a_specific_code():
    """Replay of the exact 10 failed captures in staging + prod, re-queried 2026-08-17.

    This is the acceptance assertion for the whole PR: not one of them may land on
    `internal_failure`, and the two causes that share a stage must not be smeared into a code that
    blames the wrong party.
    """
    live = [
        ("staging", "awaiting_admission_timeout", "awaiting_admission", "join_denied"),
        ("staging", "awaiting_admission_timeout", "awaiting_admission", "join_denied"),
        ("staging", "awaiting_admission_timeout", "awaiting_admission", "join_denied"),
        ("staging", "stopped", "awaiting_admission", "join_denied"),
        ("staging", "stopped", "awaiting_admission", "join_denied"),
        ("staging", "stopped", "awaiting_admission", "join_denied"),
        ("staging", "stopped", "awaiting_admission", "join_denied"),
        ("staging", "stopped", "joining", "join_denied"),
        ("prod", "awaiting_admission_timeout", "awaiting_admission", "join_denied"),
        ("prod", "left_alone", "requested", "upstream_unavailable"),
    ]
    for env, reason, stage, expected in live:
        code = failure_code_from_meeting_data(
            {"completion_reason": reason, "failure_stage": stage}
        )
        assert code == expected, f"{env} {reason}@{stage} -> {code}"
        assert code != "internal_failure"


async def test_join_failure_records_upstream_unavailable():
    """The bot could not drive the join at all — our fault, and it must read as ours."""
    code = await _failed_via_lifecycle(
        {"completion_reason": "join_failure", "failure_stage": "joining"},
        from_state="joining",
    )
    assert code == "upstream_unavailable"


async def test_validation_error_records_invalid_meeting():
    code = await _failed_via_lifecycle(
        {"completion_reason": "validation_error", "failure_stage": "requested"},
        from_state="requested",
    )
    assert code == "invalid_meeting"


async def test_evicted_records_kicked():
    code = await _failed_via_lifecycle(
        {"completion_reason": "evicted", "failure_stage": "active"}, from_state="active"
    )
    assert code == "kicked"


async def test_spawn_rejection_records_the_runtime_refusal():
    """`mark_spawn_rejected` is the ONE class no bot can report — no bot ever ran.

    It reaches the control plane through the reconcile sweep, so the spawn reason has to survive
    that path too.  Both spawn verdicts must be separable: a quota refusal is the operator's
    problem, a runtime spawn failure is ours.
    """
    for spawn_reason, expected in (
        ("quota_exhausted", "quota_exhausted"),
        ("runtime_spawn_failed", "upstream_unavailable"),
    ):
        store, dispatcher = _dispatcher("requested")
        store.reconcile_meetings = [
            _meeting({"failure_stage": "runtime_spawn", "spawn_failure_reason": spawn_reason})
        ]
        assert await dispatcher.reconcile_once() == 1
        assert store.captures["cap-1"].state == "failed"
        assert store.captures["cap-1"].failure_code == expected


async def test_lost_workload_pre_active_is_not_a_short_meeting():
    """The stale/untracked sweeps reuse `left_alone` for a bot that never got in.

    Pre-active that is a lost workload (`upstream_unavailable`); at `active` the same reason really
    does mean everyone left (`meeting_ended_early`).  Collapsing both would hide every reaped bot
    behind a benign-looking code.
    """
    assert await _failed_via_lifecycle(
        {"completion_reason": "left_alone", "failure_stage": "joining"}, from_state="joining"
    ) == "upstream_unavailable"
    assert await _failed_via_lifecycle(
        {"completion_reason": "left_alone", "failure_stage": "active"}, from_state="active"
    ) == "meeting_ended_early"


async def test_true_unknown_still_records_internal_failure_but_says_so(caplog):
    """The contract floor stays — but it is never taken silently again.

    A cause the map cannot translate is a real unknown; it keeps `internal_failure` (the schema
    permits nothing else) and names the untranslated value in the log, so the next vocabulary gap
    is visible instead of dying with the deleted bot pod.
    """
    with caplog.at_level(logging.WARNING, logger="meeting_api.zaki_control.callbacks"):
        code = await _failed_via_lifecycle({"completion_reason": "something_new"})
    assert code == "internal_failure"
    assert any(
        "untranslated cause" in record.message and record.levelno == logging.WARNING
        for record in caplog.records
    )


# ── the pure translation, including what it must REFUSE to do ────────────────────────────────────


def test_a_bare_failure_stage_is_not_a_cause():
    """The regression itself: `failure_stage` alone is not a FailureCode and never was.

    Reading it as one is what produced 9/9 `internal_failure`.  It must translate to "unknown"
    (None) rather than being smuggled through as a code or guessed into a specific one — a
    fabricated cause is worse than a named unknown.
    """
    assert failure_code_from_meeting_data({"failure_stage": "awaiting_admission"}) is None
    assert failure_code_from_meeting_data({"failure_stage": "active"}) is None
    assert failure_code_from_meeting_data({}) is None
    assert failure_code_from_meeting_data(None) is None


def test_a_bot_asking_for_help_is_not_a_failed_capture():
    """`meetings.status` is a SUPERSET of the control graph, and the mis-read would be permanent.

    `needs_help` is a live bot escalating while it waits to be admitted.  The read path has no name
    for it, falls into the unknown branch, and reports the capture as `failed` / `internal_failure`
    — which the settlement path then writes back onto the row, making the generic code permanent for
    a capture that was never even finished.

    LATENT, NOT OBSERVED: `needs_help` has never occurred in prod or staging, so it contributed
    nothing to the measured failure rate.  Guarded because it is reachable and the damage is
    irreversible, not because it has happened.
    """
    from meeting_api.zaki_control.adapters import SqlAlchemyControlStore

    row = {
        "capture_id": "cap-1", "tenant_id": "tenant-1", "user_id": 42,
        "operation_id": "op-1", "reservation_id": "reserve-1", "platform": "google_meet",
        "native_meeting_id": "abc-defg-hij", "meeting_id": 7,
        "state": "awaiting_admission", "failure_code": None,
        "meeting_state": "needs_help", "start_time": None, "end_time": None,
        "captured_seconds_total": 0, "max_capture_seconds": 3600,
    }

    capture = SqlAlchemyControlStore._capture_from_row(row)

    assert capture.state == "awaiting_admission"
    assert capture.failure_code is None


def test_an_explicit_sealed_code_is_honoured():
    assert failure_code_from_meeting_data({"failure_code": "kicked"}) == "kicked"
    # ...but a free-form value in that key is not a code, and must not be laundered into one.
    assert failure_code_from_meeting_data({"failure_code": "boom"}) is None


# ── L-0165: the engine records a lobby cancellation `completed(stopped)`; the Hub's graph cannot ──


def _lobby_cancellation_row() -> dict:
    """The row L-0165 writes for a capture the user stopped before admission: not a failure in the
    engine (`completed`, reason `stopped`), with the stage it reached kept so it is never mistaken
    for a run that delivered a meeting."""
    return _meeting(
        {"completion_reason": "stopped", "failure_stage": "awaiting_admission", "stop_requested": True},
        status="completed",
    )


async def test_a_lobby_cancellation_still_reaches_the_hub_as_the_same_failed_join_denied():
    """`zaki-control.v1` reaches `completed` only through `active` — its graph has no non-failure
    terminal before admission (`_ADJACENCY`, pinned by the conformance suite). So the engine's
    `completed(stopped)` for a lobby cancellation must not be forwarded as `completed`: the
    adjacency guard would drop it and the Hub's capture would sit at `awaiting_admission` forever,
    never settling. The Hub is told what it was told before L-0165 — `failed` / `join_denied` —
    until the contract gains the edge."""
    store, dispatcher = _dispatcher("awaiting_admission")
    await dispatcher.record_lifecycle(_lobby_cancellation_row(), state="completed")
    capture = store.captures["cap-1"]
    assert capture.state == "failed", "the terminal was dropped by the adjacency guard — Hub stranded"
    assert capture.failure_code == "join_denied"


async def test_a_lobby_cancellation_replayed_by_recovery_takes_the_same_path():
    """Crash recovery replays a row's terminal from the Hub's `requested`. A `completed` row walks
    `joining → active → completed` and would assert an `active` that never happened; the lobby
    cancellation replays as the same `failed` / `join_denied` the live path reports."""
    store, dispatcher = _dispatcher("requested")
    store.reconcile_meetings = [_lobby_cancellation_row()]
    assert await dispatcher.reconcile_once() == 1
    capture = store.captures["cap-1"]
    assert capture.state == "failed"
    assert capture.failure_code == "join_denied"


async def test_a_completion_that_went_active_is_still_forwarded_as_completed():
    """No-regression: a run that reached `active` and was then stopped by the user is a real
    completion (a delivered, possibly partial, meeting) and reaches the Hub as `completed`."""
    store, dispatcher = _dispatcher("active")
    await dispatcher.record_lifecycle(
        _meeting({"completion_reason": "stopped", "stop_requested": True}, status="completed"),
        state="completed",
    )
    assert store.captures["cap-1"].state == "completed"
    assert store.captures["cap-1"].failure_code is None
