"""Redis-backed owner index and account tombstone gate for Minutes ingestion."""
from __future__ import annotations

import traceback

import fakeredis
import pytest

from control_plane.minutes_ownership import (
    MinutesOwnershipDenied,
    RedisMinutesOwnershipRegistry,
    _decoded_hash,
)


def test_register_binds_meeting_owner_and_persists_the_owner_index_atomically():
    redis = fakeredis.FakeRedis(decode_responses=True)
    registry = RedisMinutesOwnershipRegistry(redis)

    registry.assert_read_allowed(7)
    registry.register_before_write(user_id=7, meeting_id="meeting:41")

    assert redis.hget("zaki:agent:minutes-erasure:41", "user_id") == "7"
    assert redis.smembers("zaki:agent:minutes-owner-index:7") == {"41"}
    assert redis.ttl("zaki:agent:minutes-erasure:41") == -1
    assert redis.ttl("zaki:agent:minutes-owner-index:7") == -1


def test_processing_claim_is_exclusive_bound_and_cancellation_checked_until_release():
    redis = fakeredis.FakeRedis(decode_responses=True)
    registry = RedisMinutesOwnershipRegistry(redis)

    claim = registry.claim_processing(7)

    key = "zaki:agent:minutes-processing:7"
    stored = redis.hgetall(key)
    assert stored["user_id"] == "7"
    assert stored["state"] == "active"
    assert stored["meeting_id"] == ""
    assert redis.ttl(key) == -1
    with pytest.raises(MinutesOwnershipDenied):
        registry.claim_processing(7)

    claim.bind_meeting("meeting:41")
    assert redis.hget(key, "meeting_id") == "41"
    claim.checkpoint()

    redis.hset(key, "state", "cancelled")
    with pytest.raises(MinutesOwnershipDenied):
        claim.checkpoint()

    claim.release()
    assert redis.exists(key) == 0


def test_register_refuses_a_meeting_already_bound_to_another_owner():
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.hset("zaki:agent:minutes-erasure:41", mapping={"user_id": "8"})
    registry = RedisMinutesOwnershipRegistry(redis)

    with pytest.raises(MinutesOwnershipDenied):
        registry.register_before_write(user_id=7, meeting_id="meeting:41")

    assert redis.smembers("zaki:agent:minutes-owner-index:7") == set()
    assert redis.hget("zaki:agent:minutes-erasure:41", "user_id") == "8"


@pytest.mark.parametrize("state", ["pending", "complete", "invalid-state"])
def test_any_account_tombstone_state_refuses_reads_and_registration(state):
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.hset("zaki:agent:minutes-account-erasure:7", mapping={"state": state})
    registry = RedisMinutesOwnershipRegistry(redis)

    with pytest.raises(MinutesOwnershipDenied):
        registry.assert_read_allowed(7)
    with pytest.raises(MinutesOwnershipDenied):
        registry.register_before_write(user_id=7, meeting_id="meeting:41")

    assert redis.hget("zaki:agent:minutes-erasure:41", "user_id") is None
    assert redis.smembers("zaki:agent:minutes-owner-index:7") == set()


def _assert_content_free_denial(caught, marker: str) -> None:
    assert marker not in str(caught.value)
    assert marker not in "".join(traceback.format_exception(caught.value))
    assert caught.value.__cause__ is None


def test_decoded_hash_failure_does_not_chain_sensitive_redis_state():
    marker = "REDIS-CREDENTIAL-MARKER-73f4"

    class ExplodingHash(dict):
        def items(self):
            raise RuntimeError(marker)

    with pytest.raises(MinutesOwnershipDenied) as caught:
        _decoded_hash(ExplodingHash())

    _assert_content_free_denial(caught, marker)


def test_read_gate_failure_does_not_chain_sensitive_redis_state():
    marker = "REDIS-CREDENTIAL-MARKER-73f4"

    class ExplodingRedis:
        def exists(self, _key):
            raise RuntimeError(marker)

    with pytest.raises(MinutesOwnershipDenied) as caught:
        RedisMinutesOwnershipRegistry(ExplodingRedis()).assert_read_allowed(7)

    _assert_content_free_denial(caught, marker)


@pytest.mark.parametrize("stage", ["create", "enter", "watch", "close"])
def test_registration_failure_does_not_chain_sensitive_redis_state(stage):
    marker = "REDIS-CREDENTIAL-MARKER-73f4"

    class ExplodingPipeline:
        def __enter__(self):
            if stage == "enter":
                raise RuntimeError(marker)
            return self

        def __exit__(self, *_args):
            if stage == "close":
                raise RuntimeError(marker)
            return False

        def watch(self, *_keys):
            if stage == "watch":
                raise RuntimeError(marker)

        def exists(self, _key):
            return False

        def hgetall(self, _key):
            return {}

        def unwatch(self):
            pass

        def multi(self):
            pass

        def hset(self, *_args, **_kwargs):
            pass

        def persist(self, _key):
            pass

        def sadd(self, *_args):
            pass

        def execute(self):
            return []

    class ExplodingRedis:
        def pipeline(self, **_kwargs):
            if stage == "create":
                raise RuntimeError(marker)
            return ExplodingPipeline()

    with pytest.raises(MinutesOwnershipDenied) as caught:
        RedisMinutesOwnershipRegistry(ExplodingRedis()).register_before_write(
            user_id=7,
            meeting_id="meeting:41",
        )

    _assert_content_free_denial(caught, marker)
