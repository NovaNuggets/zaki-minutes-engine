"""O-RT-2 retry-callback eval — durable RuntimeEvent delivery. A receiver that 500s twice then 200s is
retried (across sweeps) until it acks. Replaces the old fire-once POST.

Two layers:
  • CallbackQueue directly — a fake poster returns 500, 500, 200; the event stays queued until acked.
  • over the API — a FastAPI TestClient receiver that 500s twice then 200s receives the lifecycle
    event after enough sweeps, and the queue drains.
"""
import asyncio
import threading

import fakeredis
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from runtime_kernel import CallbackQueue, InMemoryPendingStore, RedisPendingStore, Runtime
from runtime_kernel.api import _callback_sweep_loop, create_app


def test_queue_retries_until_ack():
    codes = iter([500, 500, 200])
    posted = []

    def poster(url, payload, headers):
        posted.append((url, payload))
        return next(codes)

    q = CallbackQueue(poster=poster)
    q.enqueue("http://receiver/cb", {"workloadId": "w1", "state": "stopped"})

    # First attempt (in enqueue) got 500 → still pending.
    assert q.pending_count() == 1
    # Sweep → 500 again → still pending.
    assert q.sweep() == 1
    # Sweep → 200 → drained.
    assert q.sweep() == 0
    assert len(posted) == 3
    assert q.pending_count() == 0


def test_queue_gives_up_at_max_attempts():
    def always_500(url, payload, headers):
        return 500

    q = CallbackQueue(poster=always_500, max_attempts=2)
    q.enqueue("http://receiver/cb", {"workloadId": "w1"})  # attempt 1 → 500
    assert q.pending_count() == 1
    assert q.sweep() == 0  # attempt 2 → 500 → cap reached → dropped
    assert q.pending_count() == 0


def test_redirect_is_not_an_acknowledgement_and_remains_pending():
    queue = CallbackQueue(poster=lambda _url, _payload, _headers: 302)

    queue.enqueue("http://receiver/cb", {"workloadId": "w1"})

    assert queue.pending_count() == 1


def test_redis_pending_callbacks_do_not_expire_before_acknowledgement():
    redis = fakeredis.FakeStrictRedis(decode_responses=True)
    store = RedisPendingStore(redis)

    store.put(
        "durable",
        {
            "url": "http://receiver/cb",
            "headers": {},
            "event": {"workloadId": "w1"},
            "attempts": 0,
        },
    )

    assert redis.ttl(f"{store.PREFIX}durable") == -1


def test_queue_restart_cannot_overwrite_an_existing_pending_callback():
    store = InMemoryPendingStore()

    def unavailable(_url, _payload, _headers):
        return 503

    first = CallbackQueue(poster=unavailable, store=store)
    first_key = first.enqueue("http://receiver/cb", {"workloadId": "first"})
    restarted = CallbackQueue(poster=unavailable, store=store)
    second_key = restarted.enqueue("http://receiver/cb", {"workloadId": "second"})

    assert first_key != second_key
    assert restarted.pending_count() == 2
    assert {record["event"]["workloadId"] for record in store.get_all().values()} == {
        "first",
        "second",
    }


def test_corrupt_redis_callback_cannot_starve_valid_delivery():
    redis = fakeredis.FakeStrictRedis(decode_responses=True)
    store = RedisPendingStore(redis)
    store.put(
        "valid",
        {
            "url": "http://receiver/cb",
            "headers": {},
            "event": {"workloadId": "valid"},
            "attempts": 0,
        },
    )
    redis.set(f"{store.PREFIX}corrupt", "{not-json")
    posted = []
    queue = CallbackQueue(
        poster=lambda url, payload, headers: posted.append(payload) or 200,
        store=store,
    )

    assert queue.sweep() == 0
    assert posted == [{"workloadId": "valid"}]
    assert redis.get(f"{store.PREFIX}corrupt") is None


def test_callback_sweep_loop_retries_and_survives_one_failed_tick():
    class Queue:
        def __init__(self):
            self.calls = 0

        def sweep(self):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("redis temporarily unavailable")

    queue = Queue()
    sleeps = []

    async def run_sync(fn):
        return fn()

    async def sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 3:
            raise asyncio.CancelledError

    async def scenario():
        with pytest.raises(asyncio.CancelledError):
            await _callback_sweep_loop(
                queue,
                interval_seconds=0.25,
                sleep=sleep,
                run_sync=run_sync,
            )

    import pytest

    asyncio.run(scenario())
    assert queue.calls == 3
    assert sleeps == [0.25, 0.25, 0.25]


def test_app_lifespan_starts_the_callback_retry_sweep():
    swept = threading.Event()

    class Queue:
        def sweep(self):
            swept.set()

        def enqueue(self, *_args, **_kwargs):
            return "unused"

    app = create_app(
        Runtime(profiles={}),
        callback_queue=Queue(),
        callback_sweep_interval_seconds=0.01,
    )

    with TestClient(app):
        assert swept.wait(timeout=1.0)


def test_queue_injects_operator_callback_auth_without_persisting_or_allowing_override():
    posted = []
    store = InMemoryPendingStore()

    def poster(url, payload, headers):
        posted.append((url, payload, headers))
        return 500

    q = CallbackQueue(
        poster=poster,
        store=store,
        default_headers={"X-Runtime-Callback-Secret": "runtime-only-secret"},
        trusted_origins={"http://receiver"},
    )
    q.enqueue(
        "http://receiver/cb",
        {"workloadId": "w1"},
        headers={
            "X-Trace-Id": "trace-1",
            "X-Runtime-Callback-Secret": "untrusted-override",
        },
    )

    assert posted[0][2] == {
        "X-Trace-Id": "trace-1",
        "X-Runtime-Callback-Secret": "runtime-only-secret",
    }
    pending = next(iter(store.get_all().values()))
    assert pending["headers"] == {
        "X-Trace-Id": "trace-1",
        "X-Runtime-Callback-Secret": "untrusted-override",
    }
    assert "runtime-only-secret" not in str(pending)

    q.enqueue("https://untrusted.example/cb", {"workloadId": "w2"})
    assert "X-Runtime-Callback-Secret" not in posted[-1][2]


def test_callback_transport_failure_never_logs_the_operator_credential(caplog):
    secret = "runtime-only-secret"

    def leaked_request(_url, _payload, headers):
        raise RuntimeError(
            f"failed callback carried X-Runtime-Callback-Secret: "
            f"{headers['X-Runtime-Callback-Secret']}"
        )

    queue = CallbackQueue(
        poster=leaked_request,
        default_headers={"X-Runtime-Callback-Secret": secret},
        trusted_origins={"http://receiver"},
    )

    with caplog.at_level("WARNING", logger="runtime_kernel.callbacks"):
        queue.enqueue("http://receiver/cb", {"workloadId": "w1"})

    assert secret not in caplog.text
    assert "RuntimeError" in caplog.text


def test_api_delivers_lifecycle_event_durably():
    # A receiver app that 500s its first two calls, then 200s.
    receiver = FastAPI()
    state = {"calls": 0, "received": []}

    @receiver.post("/runtime/callback")
    async def cb(req: Request):
        state["calls"] += 1
        body = await req.json()
        if state["calls"] <= 2:
            from fastapi.responses import JSONResponse
            return JSONResponse({"ok": False}, status_code=500)
        state["received"].append(body)
        return {"ok": True}

    receiver_client = TestClient(receiver)

    # Poster routes through the in-process receiver TestClient.
    def poster(url, payload, headers):
        return receiver_client.post("/runtime/callback", json=payload).status_code

    queue = CallbackQueue(poster=poster)
    rt = Runtime(profiles={"test": ["sleep", "30"]}, grace_sec=2.0)
    app = create_app(rt, callback_queue=queue)
    client = TestClient(app)

    # Create with a callbackUrl → emits starting+running events; deliveries 500 (attempts 1,2).
    r = client.post(
        "/workloads",
        json={"workloadId": "w1", "profile": "test", "env": {}, "callbackUrl": "http://receiver/runtime/callback"},
    )
    assert r.status_code == 201

    # At least one event is still pending (the 500s).
    assert queue.pending_count() >= 1
    assert state["received"] == []  # nothing acked yet

    # Sweep until the receiver starts acking (3rd call onward → 200) and the queue drains.
    for _ in range(10):
        if queue.sweep() == 0:
            break
    assert queue.pending_count() == 0
    assert len(state["received"]) >= 1  # the lifecycle event was durably delivered

    client.post("/workloads/w1/stop")  # cleanup child process
