"""Agent-owned account tombstone and aggregate Minutes derivative erasure."""
from __future__ import annotations

import threading
import traceback
from concurrent.futures import ThreadPoolExecutor

import fakeredis
import pytest

from control_plane.minutes_account_erasure import (
    AgentMinutesAccountErasure,
    MinutesAccountErasureFenced,
    MinutesAccountErasurePending,
    RedisMinutesAccountErasureState,
)
from control_plane.minutes_erasure import AgentMinutesErasure, RedisMinutesErasureState
from control_plane.minutes_ownership import (
    MinutesOwnershipDenied,
    RedisMinutesOwnershipRegistry,
)


class _Runtime:
    def __init__(self):
        self.stopped: list[str] = []

    def stop(self, workload_id: str) -> str:
        self.stopped.append(workload_id)
        return "stopped"


class _Brain:
    def __init__(self, *, meeting_count: int = 0, account_count: int = 0):
        self.meeting_calls: list[dict] = []
        self.account_calls: list[dict] = []
        self.meeting_count = meeting_count
        self.account_count = account_count

    def erase_meeting(self, **query) -> int:
        self.meeting_calls.append(query)
        return self.meeting_count

    def erase_account(self, **query) -> int:
        self.account_calls.append(query)
        return self.account_count


class _WorkspaceResidue:
    def __init__(self, *, count: int = 0):
        self.calls: list[dict] = []
        self.count = count

    def erase_account(self, **query) -> int:
        self.calls.append(query)
        return self.count


def test_account_erasure_purges_only_the_selected_tenants_indexed_meetings(tmp_path):
    redis = fakeredis.FakeRedis(decode_responses=True)
    state = RedisMinutesAccountErasureState(redis)
    state.register(user_id=7, meeting_id="41")
    state.register(user_id=8, meeting_id="42")
    redis.xadd("unit:agent-meet-41:out", {"event": "owner seven"})
    redis.xadd("unit:agent-meet-42:out", {"event": "owner eight"})
    runtime = _Runtime()
    brain = _Brain()
    meeting_eraser = AgentMinutesErasure(
        state=RedisMinutesErasureState(redis),
        workspaces_root=tmp_path,
        stop_workload=runtime.stop,
        brain_eraser=brain,
    )
    workspace = _WorkspaceResidue()
    eraser = AgentMinutesAccountErasure(
        state=state,
        meeting_eraser=meeting_eraser,
        workspace_eraser=workspace,
        brain_eraser=brain,
    )

    receipt = eraser.erase(user_id=7)

    assert receipt == {
        "user_id": "7",
        "tombstoned": True,
        "deleted": {
            "unit_streams": 1,
            "workspace_documents": 0,
            "brain_records": 0,
        },
    }
    assert runtime.stopped == ["agent-meet-41"]
    assert redis.exists("unit:agent-meet-41:out") == 0
    assert redis.exists("unit:agent-meet-42:out") == 1
    assert redis.smembers("zaki:agent:minutes-owner-index:8") == {"42"}
    assert redis.exists("zaki:agent:minutes-account-erasure:8") == 0


def test_invalid_owner_index_installs_a_permanent_fail_closed_tombstone():
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.sadd("zaki:agent:minutes-owner-index:7", "not-a-row")
    state = RedisMinutesAccountErasureState(redis)

    with pytest.raises(MinutesAccountErasurePending):
        state.begin(user_id=7)

    assert redis.hgetall("zaki:agent:minutes-account-erasure:7") == {
        "user_id": "7",
        "state": "pending",
        "error": "index_invalid",
    }
    assert redis.ttl("zaki:agent:minutes-account-erasure:7") == -1
    assert redis.smembers("zaki:agent:minutes-owner-index:7") == {"not-a-row"}
    with pytest.raises(MinutesAccountErasureFenced):
        state.register(user_id=7, meeting_id="41")


def test_begin_snapshots_registration_and_every_later_registration_is_fenced():
    redis = fakeredis.FakeRedis(decode_responses=True)
    state = RedisMinutesAccountErasureState(redis)
    state.register(user_id=7, meeting_id="41")

    first = state.begin(user_id=7)
    with pytest.raises(MinutesAccountErasureFenced):
        state.register(user_id=7, meeting_id="42")
    retried = state.begin(user_id=7)

    assert first == retried
    assert first.meeting_ids == ("41",)
    assert redis.hgetall("zaki:agent:minutes-account-erasure:7") == {
        "user_id": "7",
        "state": "pending",
        "snapshot": '["41"]',
    }
    assert redis.smembers("zaki:agent:minutes-owner-index:7") == set()
    assert redis.hgetall("zaki:agent:minutes-erasure:42") == {}


def test_account_tombstone_cancels_and_drains_even_a_pre_read_processing_claim():
    redis = fakeredis.FakeRedis(decode_responses=True)
    claim = RedisMinutesOwnershipRegistry(redis).claim_processing(7)
    state = RedisMinutesAccountErasureState(redis)

    begun = state.begin(user_id=7)

    assert begun.meeting_ids == ()
    assert redis.hget("zaki:agent:minutes-account-erasure:7", "state") == "pending"
    assert redis.hget("zaki:agent:minutes-processing:7", "state") == "cancelled"
    with pytest.raises(MinutesOwnershipDenied):
        claim.checkpoint()

    started = threading.Event()

    def drain():
        started.set()
        state.drain_processing(user_id=7)

    with ThreadPoolExecutor(max_workers=1) as pool:
        draining = pool.submit(drain)
        assert started.wait(timeout=5)
        assert draining.done() is False
        claim.release()
        draining.result(timeout=5)

    receipt = state.complete(
        user_id=7,
        meeting_ids=(),
        counts={"unit_streams": 0, "workspace_documents": 0, "brain_records": 0},
    )
    assert receipt["tombstoned"] is True


def test_over_bound_census_is_tombstoned_and_preserved_for_operator_repair():
    redis = fakeredis.FakeRedis(decode_responses=True)
    state = RedisMinutesAccountErasureState(redis, max_meetings=1)
    state.register(user_id=7, meeting_id="41")
    state.register(user_id=7, meeting_id="42")

    with pytest.raises(MinutesAccountErasurePending, match="configured bound"):
        state.begin(user_id=7)

    assert redis.hgetall("zaki:agent:minutes-account-erasure:7") == {
        "user_id": "7",
        "state": "pending",
        "error": "census_too_large",
    }
    assert redis.smembers("zaki:agent:minutes-owner-index:7") == {"41", "42"}
    with pytest.raises(MinutesAccountErasureFenced):
        state.register(user_id=7, meeting_id="43")


def test_begin_checks_cardinality_before_any_owner_index_materialization():
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.sadd("zaki:agent:minutes-owner-index:7", "41", "42")
    calls = {"smembers": 0, "sscan": 0}

    class GuardedPipeline:
        def __init__(self, inner):
            self._inner = inner

        def __enter__(self):
            self._inner.__enter__()
            return self

        def __exit__(self, *args):
            return self._inner.__exit__(*args)

        def smembers(self, *_args, **_kwargs):
            calls["smembers"] += 1
            raise AssertionError("unbounded SMEMBERS must never be used")

        def sscan(self, *args, **kwargs):
            calls["sscan"] += 1
            return self._inner.sscan(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    class GuardedRedis:
        def pipeline(self, **kwargs):
            return GuardedPipeline(redis.pipeline(**kwargs))

    state = RedisMinutesAccountErasureState(GuardedRedis(), max_meetings=1)

    with pytest.raises(MinutesAccountErasurePending, match="configured bound"):
        state.begin(user_id=7)

    assert calls == {"smembers": 0, "sscan": 0}
    assert redis.hget("zaki:agent:minutes-account-erasure:7", "error") == "census_too_large"


def test_begin_fails_closed_when_bounded_scan_disagrees_with_watched_cardinality():
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.sadd("zaki:agent:minutes-owner-index:7", "41", "42")

    class IncompleteScanPipeline:
        def __init__(self, inner):
            self._inner = inner

        def __enter__(self):
            self._inner.__enter__()
            return self

        def __exit__(self, *args):
            return self._inner.__exit__(*args)

        def sscan(self, *_args, **_kwargs):
            return 0, ["41"]

        def __getattr__(self, name):
            return getattr(self._inner, name)

    class IncompleteScanRedis:
        def pipeline(self, **kwargs):
            return IncompleteScanPipeline(redis.pipeline(**kwargs))

    state = RedisMinutesAccountErasureState(IncompleteScanRedis(), max_meetings=2)

    with pytest.raises(MinutesAccountErasurePending, match="changed during snapshot"):
        state.begin(user_id=7)

    assert redis.hgetall("zaki:agent:minutes-account-erasure:7") == {
        "user_id": "7",
        "state": "pending",
        "error": "index_changed",
    }
    assert redis.smembers("zaki:agent:minutes-owner-index:7") == {"41", "42"}


def test_begin_sanitizes_pipeline_lifecycle_failure():
    marker = "redis-owner-index-private-native-id"

    class UnavailableRedis:
        def pipeline(self, **_kwargs):
            raise OSError(marker)

    with pytest.raises(
        MinutesAccountErasurePending, match="state is unavailable"
    ) as raised:
        RedisMinutesAccountErasureState(UnavailableRedis()).begin(user_id=7)

    assert marker not in "".join(traceback.format_exception(raised.value))
    assert raised.value.__cause__ is None


def test_account_orchestrator_sanitizes_state_adapter_failure():
    marker = "account-state-storage-key-private-native-id"

    class UnavailableState:
        def begin(self, *, user_id):
            raise RuntimeError(marker)

    class Unused:
        def erase(self, **_kwargs):
            pytest.fail("destructive dependencies must not run")

        def erase_account(self, **_kwargs):
            pytest.fail("destructive dependencies must not run")

    eraser = AgentMinutesAccountErasure(
        state=UnavailableState(),
        meeting_eraser=Unused(),
        workspace_eraser=Unused(),
        brain_eraser=Unused(),
    )

    with pytest.raises(
        MinutesAccountErasurePending, match="requires retry"
    ) as raised:
        eraser.erase(user_id=7)

    assert marker not in "".join(traceback.format_exception(raised.value))
    assert raised.value.__cause__ is None


def test_index_reappearance_after_snapshot_fails_closed_without_hiding_the_row():
    redis = fakeredis.FakeRedis(decode_responses=True)
    state = RedisMinutesAccountErasureState(redis)
    state.register(user_id=7, meeting_id="41")
    state.begin(user_id=7)
    redis.sadd("zaki:agent:minutes-owner-index:7", "42")

    with pytest.raises(MinutesAccountErasurePending, match="index changed"):
        state.begin(user_id=7)

    assert redis.smembers("zaki:agent:minutes-owner-index:7") == {"42"}


def test_completed_retry_replays_byte_stable_receipt_and_counts_without_side_effects(tmp_path):
    redis = fakeredis.FakeRedis(decode_responses=True)
    state = RedisMinutesAccountErasureState(redis)
    state.register(user_id=7, meeting_id="41")
    redis.xadd("unit:agent-meet-41:out", {"event": "one"})
    runtime = _Runtime()
    brain = _Brain(meeting_count=1, account_count=3)
    workspace = _WorkspaceResidue(count=2)
    eraser = AgentMinutesAccountErasure(
        state=state,
        meeting_eraser=AgentMinutesErasure(
            state=RedisMinutesErasureState(redis),
            workspaces_root=tmp_path,
            stop_workload=runtime.stop,
            brain_eraser=brain,
        ),
        workspace_eraser=workspace,
        brain_eraser=brain,
    )

    first = eraser.erase(user_id=7)
    encoded_first = redis.hget("zaki:agent:minutes-account-erasure:7", "receipt")
    second = eraser.erase(user_id=7)
    encoded_second = redis.hget("zaki:agent:minutes-account-erasure:7", "receipt")

    assert first == second == {
        "user_id": "7",
        "tombstoned": True,
        "deleted": {
            "unit_streams": 1,
            "workspace_documents": 2,
            "brain_records": 4,
        },
    }
    assert encoded_first == encoded_second
    assert runtime.stopped == ["agent-meet-41"]
    assert len(brain.meeting_calls) == 1
    assert len(brain.account_calls) == 1
    assert len(workspace.calls) == 1


def test_partial_meeting_failure_leaves_snapshot_pending_and_skips_account_residue():
    redis = fakeredis.FakeRedis(decode_responses=True)
    state = RedisMinutesAccountErasureState(redis)
    state.register(user_id=7, meeting_id="41")
    state.register(user_id=7, meeting_id="42")

    class PartialMeetings:
        def __init__(self):
            self.calls: list[str] = []

        def erase(self, *, user_id, meeting_id):
            self.calls.append(meeting_id)
            if meeting_id == "42":
                raise RuntimeError("store unavailable")
            return {
                "meeting_id": meeting_id,
                "tombstoned": True,
                "deleted": {
                    "unit_streams": 1,
                    "workspace_documents": 2,
                    "brain_records": 3,
                },
            }

    meetings = PartialMeetings()
    workspace = _WorkspaceResidue()
    brain = _Brain()
    eraser = AgentMinutesAccountErasure(
        state=state,
        meeting_eraser=meetings,
        workspace_eraser=workspace,
        brain_eraser=brain,
    )

    with pytest.raises(MinutesAccountErasurePending, match="requires retry"):
        eraser.erase(user_id=7)

    assert meetings.calls == ["41", "42"]
    assert workspace.calls == []
    assert brain.account_calls == []
    assert redis.hgetall("zaki:agent:minutes-account-erasure:7") == {
        "user_id": "7",
        "state": "pending",
        "snapshot": '["41","42"]',
    }


def test_empty_account_still_purges_orphan_provenance_and_completes():
    redis = fakeredis.FakeRedis(decode_responses=True)
    workspace = _WorkspaceResidue(count=2)
    brain = _Brain(account_count=3)

    class NoMeetings:
        def erase(self, **_query):
            raise AssertionError("empty account must not invoke a meeting eraser")

    receipt = AgentMinutesAccountErasure(
        state=RedisMinutesAccountErasureState(redis),
        meeting_eraser=NoMeetings(),
        workspace_eraser=workspace,
        brain_eraser=brain,
    ).erase(user_id=7)

    assert receipt["deleted"] == {
        "unit_streams": 0,
        "workspace_documents": 2,
        "brain_records": 3,
    }
    assert workspace.calls == [{
        "user_id": 7,
        "write_origin": "meeting_ingest",
        "source_spoke": "minutes",
        "idempotency_key": "minutes-erasure:v1:account:7:workspace",
    }]
    assert brain.account_calls == [{
        "user_id": 7,
        "write_origin": "meeting_ingest",
        "source_spoke": "minutes",
        "idempotency_key": "minutes-erasure:v1:account:7:brain",
    }]


def test_partial_completed_meeting_receipt_blocks_account_completion():
    redis = fakeredis.FakeRedis(decode_responses=True)
    state = RedisMinutesAccountErasureState(redis)
    state.register(user_id=7, meeting_id="41")
    begun = state.begin(user_id=7)
    redis.hset("zaki:agent:minutes-erasure:41", mapping={"state": "complete"})

    with pytest.raises(MinutesAccountErasurePending, match="receipt is incomplete"):
        state.complete(
            user_id=7,
            meeting_ids=begun.meeting_ids,
            counts={
                "unit_streams": 0,
                "workspace_documents": 0,
                "brain_records": 0,
            },
        )


def test_completed_meeting_without_processed_tombstone_blocks_account_completion():
    redis = fakeredis.FakeRedis(decode_responses=True)
    state = RedisMinutesAccountErasureState(redis)
    state.register(user_id=7, meeting_id="41")
    begun = state.begin(user_id=7)
    redis.hset("zaki:agent:minutes-erasure:41", mapping={
        "state": "complete",
        "unit_streams": "0",
        "workspace_documents": "0",
        "brain_records": "0",
    })

    with pytest.raises(MinutesAccountErasurePending, match="tombstone is incomplete"):
        state.complete(
            user_id=7,
            meeting_ids=begun.meeting_ids,
            counts={
                "unit_streams": 0,
                "workspace_documents": 0,
                "brain_records": 0,
            },
        )


def test_completion_turns_redis_uncertainty_into_a_retryable_fail_closed_result():
    marker = "redis-storage-key-and-private-native-id"

    class UnavailableRedis:
        def scard(self, _key):
            raise OSError(marker)

    state = RedisMinutesAccountErasureState(UnavailableRedis())

    with pytest.raises(
        MinutesAccountErasurePending, match="verification is unavailable"
    ) as raised:
        state.complete(
            user_id=7,
            meeting_ids=(),
            counts={
                "unit_streams": 0,
                "workspace_documents": 0,
                "brain_records": 0,
            },
        )

    assert marker not in "".join(traceback.format_exception(raised.value))
    assert raised.value.__cause__ is None


def test_invalid_account_state_and_meeting_owner_mismatch_both_fail_closed(tmp_path):
    invalid = fakeredis.FakeRedis(decode_responses=True)
    invalid.hset(
        "zaki:agent:minutes-account-erasure:7",
        mapping={"user_id": "7", "state": "complete"},
    )
    with pytest.raises(MinutesAccountErasurePending, match="state is invalid"):
        RedisMinutesAccountErasureState(invalid).begin(user_id=7)

    redis = fakeredis.FakeRedis(decode_responses=True)
    state = RedisMinutesAccountErasureState(redis)
    state.register(user_id=7, meeting_id="41")
    redis.hset("zaki:agent:minutes-erasure:41", "user_id", "8")
    runtime = _Runtime()
    brain = _Brain()
    workspace = _WorkspaceResidue()
    eraser = AgentMinutesAccountErasure(
        state=state,
        meeting_eraser=AgentMinutesErasure(
            state=RedisMinutesErasureState(redis),
            workspaces_root=tmp_path,
            stop_workload=runtime.stop,
            brain_eraser=brain,
        ),
        workspace_eraser=workspace,
        brain_eraser=brain,
    )

    with pytest.raises(MinutesAccountErasurePending, match="requires retry"):
        eraser.erase(user_id=7)

    assert redis.hget("zaki:agent:minutes-erasure:41", "user_id") == "8"
    assert redis.hget("zaki:agent:minutes-account-erasure:7", "state") == "pending"
    assert runtime.stopped == []
    assert brain.meeting_calls == []
    assert brain.account_calls == []
    assert workspace.calls == []
