"""Cross-spoke meeting erasure: Agent tombstone first, then Minutes carriers, with retry receipt."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from fastapi.testclient import TestClient as _TestClient
import pytest

from meeting_api.agent_erasure import AgentErasureReceipt
from meeting_api.app import create_app as _create_app
from meeting_api.erasure_receipts import sign_erasure_receipt, verify_erasure_receipt
from meeting_api.retention import erase_meeting
from meeting_api.retention.fakes import InMemoryRetentionRepo, InMemoryRetentionStorage


NOW = datetime(2026, 7, 15, 12, 30, tzinfo=timezone.utc)
AGENT_KEY_ID = "agent-erasure-2026-07"
AGENT_SECRET = "agent-erasure-signing-secret-012345"
MINUTES_KEY_ID = "minutes-erasure-2026-07"
MINUTES_SECRET = "minutes-erasure-signing-secret-0123"
MINUTES_NONCE = "01J2M3N4P5Q6R7S8T9V0WXYZM1"
HUB_TOKEN = "minutes-hub-service-token-0123456789"


async def _historical_settings(_user_id: int):
    return {
        "operator_enabled": False,
        "capture_enabled": False,
        "agent_read_enabled": False,
        "policy_version": "minutes-capture.v1",
        "attested_at": "2026-07-15T11:00:00+00:00",
        "retention_days": {"audio": 7, "transcript": 30, "summary": 30},
    }


async def _historical_fencer(_meeting_id, *, raw, processed):
    del raw, processed


def create_app(**kwargs):
    kwargs.setdefault("minutes_hub_token", HUB_TOKEN)
    kwargs.setdefault("minutes_settings", _historical_settings)
    kwargs.setdefault("minutes_capture_fencer", _historical_fencer)
    return _create_app(**kwargs)


def TestClient(app, **kwargs):
    headers = dict(kwargs.pop("headers", {}))
    headers.setdefault("X-Zaki-Minutes-Token", HUB_TOKEN)
    return _TestClient(app, headers=headers, **kwargs)


def _agent_receipt(user_id: int, meeting_id: int) -> dict:
    return sign_erasure_receipt(
        owner="agent",
        scope="meeting",
        user_id=user_id,
        meeting_id=meeting_id,
        counts={
            "agent_unit_streams": 1,
            "agent_workspace_documents": 2,
            "agent_brain_records": 3,
        },
        issued_at=NOW,
        key_id=AGENT_KEY_ID,
        nonce="01J2M3N4P5Q6R7S8T9V0WXYZA1",
        secret=AGENT_SECRET,
    )


class AgentEraser:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls = []

    async def __call__(self, *, user_id: int, meeting_id: int):
        self.calls.append((user_id, meeting_id))
        if self.fail:
            raise RuntimeError("private Agent failure detail")
        return AgentErasureReceipt(receipt=_agent_receipt(user_id, meeting_id))

    def verify_durable_receipt(self, receipt, *, user_id: int, meeting_id: int):
        return verify_erasure_receipt(
            receipt,
            AGENT_SECRET,
            expected_owner="agent",
            expected_scope="meeting",
            expected_user_id=user_id,
            expected_meeting_id=meeting_id,
            now=lambda: NOW,
        ) and set(receipt.get("counts", {})) == {
            "agent_unit_streams",
            "agent_workspace_documents",
            "agent_brain_records",
        }


def _stack(*, agent=None):
    repo = InMemoryRetentionRepo()
    storage = InMemoryRetentionStorage()
    agent = agent or AgentEraser()
    client = TestClient(create_app(
        minutes_retention_repo=repo,
        minutes_retention_storage=storage,
        minutes_agent_eraser=agent,
        minutes_now=lambda: NOW,
        minutes_erasure_signing_key_id=MINUTES_KEY_ID,
        minutes_erasure_signing_secret=MINUTES_SECRET,
        minutes_erasure_verification_keys={MINUTES_KEY_ID: MINUTES_SECRET},
        minutes_erasure_nonce_factory=lambda: MINUTES_NONCE,
    ))
    return client, repo, storage, agent


def _seed(
    repo,
    storage,
    *,
    user="7",
    meeting="41",
    prefix=None,
    workload_id=None,
):
    prefix = prefix or f"recordings/{user}/recording_{meeting}/session_{meeting}/"
    repo.seed_meeting(
        user_id=user,
        meeting_id=meeting,
        transcript_rows=["private transcript"],
        summaries=["private summary"],
        recording_prefixes=[prefix],
        recording_objects=1,
        workload_id=workload_id,
    )
    storage.seed(f"{prefix}audio/master.wav", b"private audio")


def test_erasure_requires_hub_auth_before_identity_or_repository_work():
    client, repo, _storage, agent = _stack()

    async def must_not_run(*_args, **_kwargs):
        raise AssertionError("unauthenticated request reached the erasure repository")

    repo.completed_erasure = must_not_run
    uncredentialed = _TestClient(client.app)
    for headers in ({}, {"X-Zaki-Minutes-Token": "x" * 32}):
        response = uncredentialed.delete("/minutes/meetings/not-a-row", headers=headers)
        assert response.status_code == 401
        assert response.json() == {"detail": "Unauthorized"}
    assert agent.calls == []


def test_erasure_route_is_independent_of_capture_flag_and_idempotent():
    client, repo, storage, agent = _stack()
    _seed(repo, storage)

    first = client.delete("/minutes/meetings/41", headers={"X-User-Id": "7"})
    second = client.delete("/minutes/meetings/41", headers={"X-User-Id": "7"})

    assert first.status_code == second.status_code == 200
    assert first.content == second.content
    assert first.json() == second.json() == {
        "version": "erasure.v1",
        "owner": "minutes",
        "scope": "meeting",
        "subject": {"user_id": "7", "meeting_id": "41"},
        "counts": {
            "meeting_rows": 1,
            "transcript_rows": 1,
            "summary_documents": 1,
            "recording_objects": 1,
            "agent_unit_streams": 1,
            "agent_workspace_documents": 2,
            "agent_brain_records": 3,
        },
        "issued_at": "2026-07-15T12:30:00Z",
        "key_id": MINUTES_KEY_ID,
        "nonce": MINUTES_NONCE,
        "digest": first.json()["digest"],
        "signature": first.json()["signature"],
    }
    assert verify_erasure_receipt(
        first.json(),
        MINUTES_SECRET,
        expected_owner="minutes",
        expected_scope="meeting",
        expected_user_id="7",
        expected_meeting_id="41",
        now=lambda: NOW,
    )
    assert agent.calls == [(7, 41)]
    assert repo.snapshot("41") is None
    assert storage.snapshot("recordings/7/") == {}
    assert first.headers["cache-control"] == "no-store"
    assert "private" not in first.text


def test_erasure_scrubs_runtime_before_agent_and_minutes_content():
    events = []

    class RuntimeScrubber:
        async def scrub_workload(self, workload_id):
            events.append(("runtime", workload_id))

    class OrderedAgentEraser(AgentEraser):
        async def __call__(self, *, user_id, meeting_id):
            events.append(("agent", meeting_id))
            return await super().__call__(user_id=user_id, meeting_id=meeting_id)

    repo = InMemoryRetentionRepo()
    storage = InMemoryRetentionStorage()
    agent = OrderedAgentEraser()
    client = TestClient(create_app(
        runtime=RuntimeScrubber(),
        minutes_retention_repo=repo,
        minutes_retention_storage=storage,
        minutes_agent_eraser=agent,
        minutes_now=lambda: NOW,
        minutes_erasure_signing_key_id=MINUTES_KEY_ID,
        minutes_erasure_signing_secret=MINUTES_SECRET,
        minutes_erasure_verification_keys={MINUTES_KEY_ID: MINUTES_SECRET},
        minutes_erasure_nonce_factory=lambda: MINUTES_NONCE,
    ))
    _seed(repo, storage, workload_id="mtg-41-private")

    response = client.delete("/minutes/meetings/41", headers={"X-User-Id": "7"})

    assert response.status_code == 200
    assert events[:2] == [("runtime", "mtg-41-private"), ("agent", 41)]
    assert repo.snapshot("41") is None
    assert storage.snapshot("recordings/7/") == {}


def test_runtime_scrub_failure_blocks_agent_and_all_minutes_deletion():
    class FailingRuntimeScrubber:
        def __init__(self):
            self.calls = []

        async def scrub_workload(self, workload_id):
            self.calls.append(workload_id)
            raise RuntimeError("private runtime cleanup detail")

    repo = InMemoryRetentionRepo()
    storage = InMemoryRetentionStorage()
    agent = AgentEraser()
    runtime = FailingRuntimeScrubber()
    client = TestClient(create_app(
        runtime=runtime,
        minutes_retention_repo=repo,
        minutes_retention_storage=storage,
        minutes_agent_eraser=agent,
        minutes_now=lambda: NOW,
        minutes_erasure_signing_key_id=MINUTES_KEY_ID,
        minutes_erasure_signing_secret=MINUTES_SECRET,
        minutes_erasure_verification_keys={MINUTES_KEY_ID: MINUTES_SECRET},
        minutes_erasure_nonce_factory=lambda: MINUTES_NONCE,
    ))
    _seed(repo, storage, workload_id="mtg-41-private")

    response = client.delete("/minutes/meetings/41", headers={"X-User-Id": "7"})

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "erasure_pending"}}
    assert runtime.calls == ["mtg-41-private"]
    assert agent.calls == []
    assert repo.snapshot("41") is not None
    assert storage.snapshot("recordings/7/") != {}
    assert "private runtime" not in response.text


def test_minutes_receipt_is_persisted_before_delete_and_retry_is_byte_stable():
    repo = InMemoryRetentionRepo()

    class FailOnceStorage(InMemoryRetentionStorage):
        def __init__(self):
            super().__init__()
            self.calls = 0

        async def delete_prefix(self, prefix):
            self.calls += 1
            draft = repo.snapshot("41")["minutes_erasure_receipt"]
            assert draft["owner"] == "minutes"
            if self.calls == 1:
                raise RuntimeError("private object-store failure")
            return await super().delete_prefix(prefix)

    storage = FailOnceStorage()
    agent = AgentEraser()
    nonces = iter((MINUTES_NONCE, "01J2M3N4P5Q6R7S8T9V0WXYZM2"))
    client = TestClient(create_app(
        minutes_retention_repo=repo,
        minutes_retention_storage=storage,
        minutes_agent_eraser=agent,
        minutes_now=lambda: NOW,
        minutes_erasure_signing_key_id=MINUTES_KEY_ID,
        minutes_erasure_signing_secret=MINUTES_SECRET,
        minutes_erasure_verification_keys={MINUTES_KEY_ID: MINUTES_SECRET},
        minutes_erasure_nonce_factory=lambda: next(nonces),
    ))
    _seed(repo, storage)

    failed = client.delete("/minutes/meetings/41", headers={"X-User-Id": "7"})
    fenced = repo.snapshot("41")
    stable = fenced["minutes_erasure_receipt"]
    assert fenced["agent_erasure"] == _agent_receipt(7, 41)
    retried = client.delete("/minutes/meetings/41", headers={"X-User-Id": "7"})
    replay = client.delete("/minutes/meetings/41", headers={"X-User-Id": "7"})

    assert failed.status_code == 503
    assert retried.status_code == replay.status_code == 200
    assert retried.content == replay.content
    assert retried.json() == stable
    assert retried.json()["nonce"] == MINUTES_NONCE
    assert agent.calls == [(7, 41)]


async def test_concurrent_receipt_candidates_converge_on_first_durable_bytes():
    repo = InMemoryRetentionRepo()
    repo.seed_meeting(
        user_id="7",
        meeting_id="41",
        transcript_rows=[],
        summaries=[],
        recording_prefixes=[],
        recording_objects=0,
    )
    plan = await repo.begin_erasure("7", "41")
    assert plan is not None
    plan = await repo.record_agent_erasure(plan, _agent_receipt(7, 41))

    def candidate(nonce):
        return sign_erasure_receipt(
            owner="minutes",
            scope="meeting",
            user_id="7",
            meeting_id="41",
            counts={
                "meeting_rows": 1,
                "transcript_rows": 0,
                "summary_documents": 0,
                "recording_objects": 0,
                "agent_unit_streams": 1,
                "agent_workspace_documents": 2,
                "agent_brain_records": 3,
            },
            issued_at=NOW,
            key_id=MINUTES_KEY_ID,
            nonce=nonce,
            secret=MINUTES_SECRET,
        )

    first, second = await asyncio.gather(
        repo.record_erasure_receipt(plan, candidate(MINUTES_NONCE)),
        repo.record_erasure_receipt(
            plan, candidate("01J2M3N4P5Q6R7S8T9V0WXYZM2")
        ),
    )

    assert first == second
    assert first["nonce"] == MINUTES_NONCE


async def test_concurrent_erasure_callers_return_the_same_committed_receipt():
    repo = InMemoryRetentionRepo()

    class BarrierStorage(InMemoryRetentionStorage):
        def __init__(self):
            super().__init__()
            self.arrivals = 0
            self.ready = asyncio.Event()

        async def delete_prefix(self, prefix):
            self.arrivals += 1
            if self.arrivals == 2:
                self.ready.set()
            await self.ready.wait()
            return await super().delete_prefix(prefix)

    storage = BarrierStorage()
    _seed(repo, storage)
    plan = await repo.begin_erasure("7", "41")
    assert plan is not None
    await repo.record_agent_erasure(plan, _agent_receipt(7, 41))
    nonces = iter((MINUTES_NONCE, "01J2M3N4P5Q6R7S8T9V0WXYZM2"))

    def signer(stable_plan):
        return sign_erasure_receipt(
            owner="minutes",
            scope="meeting",
            user_id=stable_plan.user_id,
            meeting_id=stable_plan.meeting_id,
            counts={
                "meeting_rows": 1,
                "transcript_rows": stable_plan.transcript_rows,
                "summary_documents": stable_plan.summary_documents,
                "recording_objects": stable_plan.recording_objects,
                "agent_unit_streams": stable_plan.agent_unit_streams,
                "agent_workspace_documents": stable_plan.agent_workspace_documents,
                "agent_brain_records": stable_plan.agent_brain_records,
            },
            issued_at=NOW,
            key_id=MINUTES_KEY_ID,
            nonce=next(nonces),
            secret=MINUTES_SECRET,
        )

    def verifier(receipt, stable_plan):
        return verify_erasure_receipt(
            receipt,
            MINUTES_SECRET,
            expected_owner="minutes",
            expected_scope="meeting",
            expected_user_id=stable_plan.user_id,
            expected_meeting_id=stable_plan.meeting_id,
            now=lambda: NOW,
        )

    async def erase():
        return await erase_meeting(
            repo,
            storage,
            user_id="7",
            meeting_id="41",
            erased_at=NOW,
            policy_version="minutes-erasure.v1",
            receipt_factory=signer,
            receipt_verifier=verifier,
        )

    first, second = await asyncio.gather(erase(), erase())

    assert first is not None and second is not None
    assert first.as_dict() == second.as_dict()
    assert first.as_dict()["nonce"] == MINUTES_NONCE


def test_tampered_durable_minutes_receipt_fails_closed():
    client, repo, storage, _agent = _stack()
    _seed(repo, storage)
    assert client.delete(
        "/minutes/meetings/41", headers={"X-User-Id": "7"}
    ).status_code == 200
    repo._receipts[("7", "41")]["counts"]["meeting_rows"] = 0

    response = client.delete("/minutes/meetings/41", headers={"X-User-Id": "7"})

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "erasure_pending"}}


def test_tampered_precommit_receipt_is_rejected_before_destructive_retry():
    class FailOnceStorage(InMemoryRetentionStorage):
        def __init__(self):
            super().__init__()
            self.failed = False

        async def delete_prefix(self, prefix):
            if not self.failed:
                self.failed = True
                raise RuntimeError("object storage unavailable")
            return await super().delete_prefix(prefix)

    repo = InMemoryRetentionRepo()
    storage = FailOnceStorage()
    client = TestClient(create_app(
        minutes_retention_repo=repo,
        minutes_retention_storage=storage,
        minutes_agent_eraser=AgentEraser(),
        minutes_now=lambda: NOW,
        minutes_erasure_signing_key_id=MINUTES_KEY_ID,
        minutes_erasure_signing_secret=MINUTES_SECRET,
        minutes_erasure_verification_keys={MINUTES_KEY_ID: MINUTES_SECRET},
        minutes_erasure_nonce_factory=lambda: MINUTES_NONCE,
    ))
    _seed(repo, storage)
    assert client.delete(
        "/minutes/meetings/41", headers={"X-User-Id": "7"}
    ).status_code == 503
    repo._meetings["41"]["minutes_erasure_receipt"]["signature"] = "sha256=" + "0" * 64

    retry = client.delete("/minutes/meetings/41", headers={"X-User-Id": "7"})

    assert retry.status_code == 503
    assert repo.snapshot("41") is not None
    assert storage.snapshot("recordings/7/") != {}


def test_tampered_persisted_agent_receipt_is_rejected_before_destructive_retry():
    class FailOnceStorage(InMemoryRetentionStorage):
        def __init__(self):
            super().__init__()
            self.failed = False

        async def delete_prefix(self, prefix):
            if not self.failed:
                self.failed = True
                raise RuntimeError("object storage unavailable")
            return await super().delete_prefix(prefix)

    repo = InMemoryRetentionRepo()
    storage = FailOnceStorage()
    client = TestClient(create_app(
        minutes_retention_repo=repo,
        minutes_retention_storage=storage,
        minutes_agent_eraser=AgentEraser(),
        minutes_now=lambda: NOW,
        minutes_erasure_signing_key_id=MINUTES_KEY_ID,
        minutes_erasure_signing_secret=MINUTES_SECRET,
        minutes_erasure_verification_keys={MINUTES_KEY_ID: MINUTES_SECRET},
        minutes_erasure_nonce_factory=lambda: MINUTES_NONCE,
    ))
    _seed(repo, storage)
    assert client.delete(
        "/minutes/meetings/41", headers={"X-User-Id": "7"}
    ).status_code == 503
    repo._meetings["41"]["agent_erasure"]["signature"] = "sha256=" + "0" * 64

    retry = client.delete("/minutes/meetings/41", headers={"X-User-Id": "7"})

    assert retry.status_code == 503
    assert repo.snapshot("41") is not None
    assert storage.snapshot("recordings/7/") != {}


def test_old_minutes_receipt_finishes_and_replays_during_key_rotation():
    old_key = "minutes-erasure-2026-06"
    old_secret = "minutes-erasure-old-secret-01234567"

    class FailOnceStorage(InMemoryRetentionStorage):
        def __init__(self):
            super().__init__()
            self.failed = False

        async def delete_prefix(self, prefix):
            if not self.failed:
                self.failed = True
                raise RuntimeError("object storage unavailable")
            return await super().delete_prefix(prefix)

    repo = InMemoryRetentionRepo()
    storage = FailOnceStorage()
    common = {
        "minutes_retention_repo": repo,
        "minutes_retention_storage": storage,
        "minutes_agent_eraser": AgentEraser(),
        "minutes_now": lambda: NOW,
        "minutes_erasure_nonce_factory": lambda: MINUTES_NONCE,
    }
    old_client = TestClient(create_app(
        **common,
        minutes_erasure_signing_key_id=old_key,
        minutes_erasure_signing_secret=old_secret,
        minutes_erasure_verification_keys={old_key: old_secret},
    ))
    _seed(repo, storage)
    assert old_client.delete(
        "/minutes/meetings/41", headers={"X-User-Id": "7"}
    ).status_code == 503
    old_draft = repo.snapshot("41")["minutes_erasure_receipt"]

    rotated = TestClient(create_app(
        **common,
        minutes_erasure_signing_key_id=MINUTES_KEY_ID,
        minutes_erasure_signing_secret=MINUTES_SECRET,
        minutes_erasure_verification_keys={
            old_key: old_secret,
            MINUTES_KEY_ID: MINUTES_SECRET,
        },
    ))
    completed = rotated.delete(
        "/minutes/meetings/41", headers={"X-User-Id": "7"}
    )
    replay = rotated.delete(
        "/minutes/meetings/41", headers={"X-User-Id": "7"}
    )

    assert completed.status_code == replay.status_code == 200
    assert completed.json() == replay.json() == old_draft
    assert completed.json()["key_id"] == old_key


@pytest.mark.parametrize("kind", ["tamper", "scope", "user", "key", "counts"])
def test_concurrent_completion_race_verifies_receipt_before_return(kind):
    receipt_values = {
        "owner": "minutes",
        "scope": "meeting",
        "user_id": "7",
        "meeting_id": "41",
        "counts": {
            "meeting_rows": 1,
            "transcript_rows": 0,
            "summary_documents": 0,
            "recording_objects": 0,
            "agent_unit_streams": 0,
            "agent_workspace_documents": 0,
            "agent_brain_records": 0,
        },
        "issued_at": NOW,
        "key_id": MINUTES_KEY_ID,
        "nonce": MINUTES_NONCE,
        "secret": MINUTES_SECRET,
    }
    if kind == "scope":
        receipt_values.update(scope="account", meeting_id=None)
    elif kind == "user":
        receipt_values["user_id"] = "8"
    elif kind == "key":
        receipt_values["key_id"] = "minutes-erasure-untrusted"
    receipt = sign_erasure_receipt(**receipt_values)
    if kind == "tamper":
        receipt["counts"]["meeting_rows"] = 0
    elif kind == "counts":
        # The production signer refuses incomplete manifests; emulate a corrupt persisted
        # candidate after signing so the route's replay verifier still proves fail-closed.
        receipt["counts"].pop("agent_brain_records")

    class RaceRepo(InMemoryRetentionRepo):
        def __init__(self):
            super().__init__()
            self.completed_calls = 0

        async def completed_erasure(self, user_id, meeting_id):
            self.completed_calls += 1
            return None if self.completed_calls == 1 else receipt

        async def begin_erasure(self, user_id, meeting_id):
            return None

    client = TestClient(create_app(
        minutes_retention_repo=RaceRepo(),
        minutes_retention_storage=InMemoryRetentionStorage(),
        minutes_agent_eraser=AgentEraser(),
        minutes_now=lambda: NOW,
        minutes_erasure_signing_key_id=MINUTES_KEY_ID,
        minutes_erasure_signing_secret=MINUTES_SECRET,
        minutes_erasure_verification_keys={MINUTES_KEY_ID: MINUTES_SECRET},
        minutes_erasure_nonce_factory=lambda: MINUTES_NONCE,
    ))

    response = client.delete("/minutes/meetings/41", headers={"X-User-Id": "7"})

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "erasure_pending"}}


def test_foreign_and_unknown_rows_are_indistinguishable_and_preserve_other_tenant():
    client, repo, storage, agent = _stack()
    _seed(repo, storage, user="8", meeting="81")
    before = (repo.snapshot("81"), storage.snapshot("recordings/8/"))

    foreign = client.delete("/minutes/meetings/81", headers={"X-User-Id": "7"})
    unknown = client.delete("/minutes/meetings/999", headers={"X-User-Id": "7"})

    assert foreign.status_code == unknown.status_code == 404
    assert foreign.json() == unknown.json()
    assert agent.calls == []
    assert (repo.snapshot("81"), storage.snapshot("recordings/8/")) == before


def test_agent_failure_leaves_durable_minutes_fence_and_requires_retry():
    client, repo, storage, agent = _stack(agent=AgentEraser(fail=True))
    _seed(repo, storage)

    response = client.delete("/minutes/meetings/41", headers={"X-User-Id": "7"})

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "erasure_pending"}}
    assert repo.snapshot("41")["state"] == "erasing"
    assert storage.snapshot("recordings/7/") != {}
    assert agent.calls == [(7, 41)]
    assert "private" not in response.text


def test_erasure_rejects_missing_identity_invalid_row_and_partial_composition():
    client, _repo, _storage, agent = _stack()
    assert client.delete("/minutes/meetings/41").status_code == 401
    assert client.delete(
        "/minutes/meetings/not-a-row", headers={"X-User-Id": "7"}
    ).status_code == 404
    assert client.delete(
        "/minutes/meetings/9999999999999999999", headers={"X-User-Id": "7"}
    ).status_code == 404
    assert agent.calls == []

    try:
        create_app(minutes_retention_repo=InMemoryRetentionRepo())
    except ValueError as error:
        assert "erasure" in str(error)
    else:  # pragma: no cover
        raise AssertionError("partial Minutes erasure composition was accepted")

    for overrides in (
        {"minutes_erasure_signing_key_id": ""},
        {"minutes_erasure_signing_secret": ""},
        {"minutes_erasure_nonce_factory": "not-callable"},
    ):
        dependencies = {
            "minutes_retention_repo": InMemoryRetentionRepo(),
            "minutes_retention_storage": InMemoryRetentionStorage(),
            "minutes_agent_eraser": AgentEraser(),
            "minutes_erasure_signing_key_id": MINUTES_KEY_ID,
            "minutes_erasure_signing_secret": MINUTES_SECRET,
            "minutes_erasure_verification_keys": {MINUTES_KEY_ID: MINUTES_SECRET},
            "minutes_erasure_nonce_factory": lambda: MINUTES_NONCE,
            **overrides,
        }
        with pytest.raises(ValueError, match="signing"):
            create_app(**dependencies)


@pytest.mark.parametrize(
    ("signing_secret", "verification_keys"),
    [
        ("short", {MINUTES_KEY_ID: "short"}),
        (MINUTES_SECRET + " ", {MINUTES_KEY_ID: MINUTES_SECRET + " "}),
        (MINUTES_SECRET + "\x00", {MINUTES_KEY_ID: MINUTES_SECRET + "\x00"}),
        (MINUTES_SECRET + "é", {MINUTES_KEY_ID: MINUTES_SECRET + "é"}),
        ("x" * 513, {MINUTES_KEY_ID: "x" * 513}),
        (
            MINUTES_SECRET,
            {MINUTES_KEY_ID: MINUTES_SECRET, "minutes-erasure-2026-06": "short"},
        ),
        (
            MINUTES_SECRET,
            {MINUTES_KEY_ID: MINUTES_SECRET, "minutes-erasure-2026-06": MINUTES_SECRET},
        ),
        (
            MINUTES_SECRET,
            {
                MINUTES_KEY_ID: MINUTES_SECRET,
                "minutes-erasure-2026-06": "minutes-erasure-old-secret-01234567",
                "minutes-erasure-2026-05": "minutes-erasure-older-secret-012345",
            },
        ),
    ],
)
def test_erasure_composition_rejects_unbounded_or_aliased_verification_secrets(
    signing_secret, verification_keys,
):
    with pytest.raises(ValueError, match="signing boundary"):
        create_app(
            minutes_retention_repo=InMemoryRetentionRepo(),
            minutes_retention_storage=InMemoryRetentionStorage(),
            minutes_agent_eraser=AgentEraser(),
            minutes_erasure_signing_key_id=MINUTES_KEY_ID,
            minutes_erasure_signing_secret=signing_secret,
            minutes_erasure_verification_keys=verification_keys,
            minutes_erasure_nonce_factory=lambda: MINUTES_NONCE,
        )


def test_erasure_rejects_rows_outside_postgres_bigint_before_touching_adapters():
    class ExplodingRepo(InMemoryRetentionRepo):
        async def completed_erasure(self, user_id, meeting_id):
            raise AssertionError("out-of-range row reached PostgreSQL adapter")

    repo = ExplodingRepo()
    client = TestClient(create_app(
        minutes_retention_repo=repo,
        minutes_retention_storage=InMemoryRetentionStorage(),
        minutes_agent_eraser=AgentEraser(),
        minutes_now=lambda: NOW,
        minutes_erasure_signing_key_id=MINUTES_KEY_ID,
        minutes_erasure_signing_secret=MINUTES_SECRET,
        minutes_erasure_verification_keys={MINUTES_KEY_ID: MINUTES_SECRET},
        minutes_erasure_nonce_factory=lambda: MINUTES_NONCE,
    ))

    response = client.delete(
        "/minutes/meetings/9999999999999999999", headers={"X-User-Id": "7"}
    )

    assert response.status_code == 404


def test_erasure_openapi_exposes_only_exact_signed_receipt():
    client, *_ = _stack()

    document = client.get("/openapi.json").json()
    response = document["paths"]["/minutes/meetings/{meeting_id}"]["delete"][
        "responses"
    ]["200"]["content"]["application/json"]["schema"]
    assert response == {
        "$ref": "#/components/schemas/MinutesErasureResponse"
    }
    model = document["components"]["schemas"]["MinutesErasureResponse"]
    assert model["additionalProperties"] is False
    assert set(model["required"]) == {
        "version",
        "owner",
        "scope",
        "subject",
        "counts",
        "issued_at",
        "key_id",
        "nonce",
        "digest",
        "signature",
    }
    assert not ({"deleted", "policy_version", "agent_tombstoned"} & set(model["properties"]))
