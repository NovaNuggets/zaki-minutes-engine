"""Agent-owned half of Minutes meeting erasure, exercised through the internal HTTP edge."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import subprocess
import traceback

import fakeredis
import pytest
from fastapi.testclient import TestClient

from control_plane.api import (
    _LiveMeetings,
    _require_minutes_erasure_backend,
    _validate_agent_erasure_signing_config,
    _validate_gateway_identity_config,
    _wrap_signed_minutes_eraser,
    create_app,
)
from control_plane.dispatch import Dispatcher
from control_plane.minutes_erasure import (
    AgentMinutesErasure,
    ErasureBegin,
    ErasurePending,
    RedisMinutesErasureState,
)
from control_plane.minutes_ownership import RedisMinutesOwnershipRegistry
from control_plane.agent_erasure_receipts import AgentErasureV1Signer, SignedAgentMinutesErasure
import control_plane.minutes_erasure as minutes_erasure_mod
from shared.config import load_settings


class _Runtime:
    def __init__(self):
        self.stopped: list[str] = []

    def spawn(self, workload_id, profile, env):
        return workload_id

    def await_done(self, workload_id, timeout_sec=0.0):
        return "completed"

    def stop(self, workload_id):
        self.stopped.append(workload_id)
        return "stopped"


def test_gateway_identity_config_rejects_malformed_or_aliased_material():
    with pytest.raises(RuntimeError, match="printable ASCII"):
        _validate_gateway_identity_config(
            required=True,
            secret="too-short",
            internal_secret="internal-service-secret-0123456789abcdef",
        )

    shared = "shared-cluster-credential-0123456789abcdef"
    with pytest.raises(RuntimeError, match="distinct"):
        _validate_gateway_identity_config(
            required=True,
            secret=shared,
            internal_secret=shared,
        )

    with pytest.raises(RuntimeError, match="rotation secrets must be distinct"):
        _validate_gateway_identity_config(
            required=True,
            secret=shared,
            previous_secret=shared,
            internal_secret="internal-service-secret-0123456789abcdef",
        )

    with pytest.raises(RuntimeError, match="requires GATEWAY_IDENTITY_SECRET"):
        _validate_gateway_identity_config(
            required=False,
            secret="",
            previous_secret="gateway-previous-secret-0123456789abcdef",
        )


def test_gateway_identity_config_rejects_alias_with_any_agent_credential():
    shared = "shared-runtime-credential-0123456789abcdef"
    with pytest.raises(RuntimeError, match="distinct"):
        _validate_gateway_identity_config(
            required=True,
            secret=shared,
            internal_secret="internal-service-secret-0123456789abcdef",
            credential_environment={
                "GATEWAY_IDENTITY_SECRET": shared,
                "VEXA_RUNTIME_CONTROL_SECRET": shared,
            },
        )


def test_workload_stop_failure_does_not_retain_control_credentials(tmp_path):
    secret = "runtime-control-secret"

    def leaked_stop(_workload_id):
        raise RuntimeError(f"failed X-Runtime-Control-Secret: {secret}")

    eraser = AgentMinutesErasure(
        state=_State(),
        workspaces_root=tmp_path,
        stop_workload=leaked_stop,
        brain_eraser=_Brain(),
    )

    with pytest.raises(ErasurePending, match="stop is unconfirmed") as raised:
        eraser.erase(user_id=7, meeting_id="41")

    assert secret not in str(raised.value)
    assert raised.value.__cause__ is None


class _Identity:
    def mint(self, subject, launcher, workspaces, tools):
        return "token"


class _Eraser:
    def __init__(self):
        self.calls: list[tuple[int, str]] = []

    def erase(self, *, user_id: int, meeting_id: str) -> dict:
        self.calls.append((user_id, meeting_id))
        return {
            "meeting_id": meeting_id,
            "tombstoned": True,
            "deleted": {
                "unit_streams": 1,
                "workspace_documents": 2,
                "brain_records": 3,
            },
        }


_TEST_SIGNER = AgentErasureV1Signer(
    key_id="agent-erasure-test",
    secret="agent-erasure-test-secret-0123456789",
    clock=lambda: datetime(2026, 7, 15, 12, 30, tzinfo=timezone.utc),
    nonce=lambda: "test-agent-erasure-nonce-0001",
)


def test_redis_erasure_pipeline_lifecycle_failure_is_sanitized():
    marker = "redis-state-key-private-native-id"

    class UnavailableRedis:
        def pipeline(self, **_kwargs):
            raise OSError(marker)

    with pytest.raises(ErasurePending, match="state is unavailable") as raised:
        RedisMinutesErasureState(UnavailableRedis()).begin(user_id=7, meeting_id="41")

    assert marker not in "".join(traceback.format_exception(raised.value))
    assert raised.value.__cause__ is None


def test_meeting_tombstone_cancels_an_unbound_pre_read_claim_before_drain():
    redis = fakeredis.FakeRedis(decode_responses=True)
    claim = RedisMinutesOwnershipRegistry(redis).claim_processing(7)
    state = RedisMinutesErasureState(redis)

    begun = state.begin(user_id=7, meeting_id="41")

    assert begun.owner_matches is True
    assert redis.hget("zaki:agent:minutes-erasure:41", "state") == "pending"
    assert redis.hget("zaki:agent:minutes-processing:7", "state") == "cancelled"
    claim.release()
    state.drain_processing(user_id=7, meeting_id="41")


class _SigningReceiptStore:
    """Route-test seam; durable/replay semantics are covered by the real Redis store tests."""

    def persist_meeting(self, *, user_id, meeting_id, receipt):
        return _TEST_SIGNER.sign_meeting(
            user_id=user_id, meeting_id=meeting_id, receipt=receipt,
        )


def _signed(eraser):
    if isinstance(eraser, SignedAgentMinutesErasure):
        return eraser
    return SignedAgentMinutesErasure(eraser=eraser, receipts=_SigningReceiptStore())


def _client(eraser: _Eraser) -> TestClient:
    settings = load_settings(internal_api_secret="internal-secret")
    dispatcher = Dispatcher(settings, _Runtime(), _Identity())
    return TestClient(create_app(dispatcher, minutes_eraser=_signed(eraser)))


class _State:
    """Redis erasure adapter fake: durable owner/count state plus row-scoped carriers."""

    def __init__(self):
        self.rows: dict[str, dict] = {}
        self.keys = {
            "unit:agent-meet-41:out",
            "unit:agent-meet-41:in",
            "proc:meeting:41",
            "proc:meeting:41:on",
            "proc:meeting:41:cursor",
        }
        self.active = {"41"}
        self.fenced: set[str] = set()

    def begin(self, *, user_id: int, meeting_id: str) -> ErasureBegin:
        row = self.rows.setdefault(meeting_id, {"user_id": user_id, "counts": {}})
        if row["user_id"] != user_id:
            return ErasureBegin(owner_matches=False, completed=None, unit_streams=0)
        self.fenced.add(meeting_id)
        unit_key = f"unit:agent-meet-{meeting_id}:out"
        row["counts"].setdefault("unit_streams", int(unit_key in self.keys))
        self.purge_carriers(meeting_id=meeting_id)
        return ErasureBegin(
            owner_matches=True,
            completed=row.get("receipt"),
            unit_streams=row["counts"]["unit_streams"],
        )

    def remember_count(self, *, meeting_id: str, user_id: int, field: str, observed: int) -> int:
        row = self.rows[meeting_id]
        assert row["user_id"] == user_id
        row["counts"].setdefault(field, observed)
        return row["counts"][field]

    def drain_processing(self, *, user_id: int, meeting_id: str) -> None:
        assert user_id == self.rows[meeting_id]["user_id"]

    def purge_carriers(self, *, meeting_id: str) -> None:
        for key in (
            f"unit:agent-meet-{meeting_id}:out",
            f"unit:agent-meet-{meeting_id}:in",
            f"proc:meeting:{meeting_id}",
            f"proc:meeting:{meeting_id}:on",
            f"proc:meeting:{meeting_id}:cursor",
        ):
            self.keys.discard(key)
        self.active.discard(meeting_id)

    def complete(self, *, meeting_id: str, user_id: int, counts: dict) -> dict:
        row = self.rows[meeting_id]
        assert row["user_id"] == user_id
        assert meeting_id in self.fenced
        assert not any(meeting_id in key for key in self.keys)
        row["receipt"] = {
            "meeting_id": meeting_id,
            "tombstoned": True,
            "deleted": dict(counts),
        }
        return row["receipt"]


class _Brain:
    def __init__(self):
        self.records = {
            (7, "meeting:41", "transcript:41"): 2,
            (7, "meeting:41", "summary:41"): 1,
            (7, "meeting:42", "transcript:42"): 4,
            (8, "meeting:41", "transcript:41"): 5,
        }
        self.calls: list[dict] = []
        self.receipts: dict[str, int] = {}

    def erase_meeting(self, **query) -> int:
        self.calls.append(query)
        key = query["idempotency_key"]
        if key in self.receipts:
            return self.receipts[key]
        matched = [
            key for key in self.records
            if key[0] == query["user_id"]
            and key[1] == query["meeting_id"]
            and key[2] in query["source_item_ids"]
        ]
        count = sum(self.records.pop(key) for key in matched)
        self.receipts[key] = count
        return count


class _Live:
    def __init__(self):
        self.rows = {"41": {"native_id": "private"}}

    def erase(self, meeting_id: str) -> None:
        self.rows.pop(meeting_id, None)


def test_production_capture_refuses_to_boot_without_a_brain_erasure_backend():
    with pytest.raises(RuntimeError, match="Brain provenance eraser"):
        _require_minutes_erasure_backend(capture_enabled="true", eraser=None)


def test_production_capture_refuses_missing_agent_receipt_signer_but_off_requires_nothing():
    current_secret = "agent-erasure-current-secret-01234567890"
    previous_secret = "agent-erasure-previous-secret-0123456789"
    _validate_agent_erasure_signing_config(
        capture_enabled="false", key_id=None, secret=None,
    )
    with pytest.raises(RuntimeError, match="ZAKI_AGENT_ERASURE_SIGNING_KEY_ID"):
        _validate_agent_erasure_signing_config(
            capture_enabled="true", key_id=None, secret=current_secret,
        )
    with pytest.raises(RuntimeError, match="ZAKI_AGENT_ERASURE_SIGNING_SECRET"):
        _validate_agent_erasure_signing_config(
            capture_enabled="true", key_id="agent-erasure-2026-07", secret=None,
        )
    _validate_agent_erasure_signing_config(
        capture_enabled="true",
        key_id="agent-erasure-2026-07",
        secret=current_secret,
        previous_key_id="agent-erasure-2026-06",
        previous_secret=previous_secret,
    )
    with pytest.raises(RuntimeError, match="distinct from every agent-api credential"):
        _validate_agent_erasure_signing_config(
            capture_enabled="true",
            key_id="agent-erasure-2026-07",
            secret="shared-erasure-secret-0123456789012345",
            internal_secret="shared-erasure-secret-0123456789012345",
        )


@pytest.mark.parametrize(
    ("previous_key_id", "previous_secret"),
    [
        ("agent-erasure-2026-06", None),
        (None, "agent-erasure-previous-secret-0123456789"),
        ("agent-erasure-2026-06", "agent-erasure-previous-secret-0123456789"),
        (" ", None),
        (None, " "),
    ],
)
def test_capture_off_refuses_any_configured_previous_agent_verifier(
    previous_key_id, previous_secret,
):
    with pytest.raises(RuntimeError, match="requires ZAKI_MINUTES_CAPTURE_ENABLED=true"):
        _validate_agent_erasure_signing_config(
            capture_enabled="false",
            key_id="agent-erasure-2026-07",
            secret="agent-erasure-current-secret-01234567890",
            previous_key_id=previous_key_id,
            previous_secret=previous_secret,
        )


def test_inert_erasure_wrapper_still_refuses_a_previous_verifier_without_an_active_signer():
    assert _wrap_signed_minutes_eraser(
        None,
        redis_client=None,
        key_id="",
        secret="",
    ) is None

    with pytest.raises(RuntimeError, match="requires ZAKI_MINUTES_CAPTURE_ENABLED=true"):
        _wrap_signed_minutes_eraser(
            None,
            redis_client=None,
            key_id="agent-erasure-2026-07",
            secret="agent-erasure-current-secret-01234567890",
            previous_key_id="agent-erasure-2026-06",
            previous_secret="agent-erasure-previous-secret-0123456789",
        )


@pytest.mark.parametrize(
    ("previous_key_id", "previous_secret"),
    [
        ("agent-erasure-2026-06", None),
        (None, "agent-erasure-previous-secret-0123456789"),
    ],
)
def test_production_capture_refuses_partial_previous_agent_receipt_key(
    previous_key_id, previous_secret,
):
    with pytest.raises(RuntimeError, match="previous verification"):
        _validate_agent_erasure_signing_config(
            capture_enabled="true",
            key_id="agent-erasure-2026-07",
            secret="agent-erasure-current-secret-01234567890",
            previous_key_id=previous_key_id,
            previous_secret=previous_secret,
        )


@pytest.mark.parametrize(
    ("previous_key_id", "previous_secret"),
    [
        ("agent-erasure-2026-07", "agent-erasure-previous-secret-0123456789"),
        ("agent-erasure-2026-06", "agent-erasure-current-secret-01234567890"),
        ("agent-erasure-2026-06", "short"),
    ],
)
def test_production_capture_refuses_alias_or_short_previous_agent_receipt_key(
    previous_key_id, previous_secret,
):
    with pytest.raises(RuntimeError, match="previous verification"):
        _validate_agent_erasure_signing_config(
            capture_enabled="true",
            key_id="agent-erasure-2026-07",
            secret="agent-erasure-current-secret-01234567890",
            previous_key_id=previous_key_id,
            previous_secret=previous_secret,
        )


def test_production_capture_refuses_a_short_current_agent_receipt_secret():
    with pytest.raises(RuntimeError, match="unpadded printable ASCII between 32 and 512"):
        _validate_agent_erasure_signing_config(
            capture_enabled="true",
            key_id="agent-erasure-2026-07",
            secret="short",
        )


@pytest.mark.parametrize(
    "invalid_secret",
    [
        " " + "a" * 32,
        "a" * 32 + " ",
        "a" * 16 + "\n" + "b" * 16,
        "a" * 16 + "\x7f" + "b" * 16,
        "é" * 32,
        "a" * 513,
    ],
)
def test_production_capture_refuses_noncanonical_current_or_previous_agent_secret(
    invalid_secret,
):
    with pytest.raises(RuntimeError, match="unpadded printable ASCII between 32 and 512"):
        _validate_agent_erasure_signing_config(
            capture_enabled="true",
            key_id="agent-erasure-2026-07",
            secret=invalid_secret,
        )
    with pytest.raises(RuntimeError, match="previous verification key is invalid"):
        _validate_agent_erasure_signing_config(
            capture_enabled="true",
            key_id="agent-erasure-2026-07",
            secret="agent-erasure-current-secret-01234567890",
            previous_key_id="agent-erasure-2026-06",
            previous_secret=invalid_secret,
        )


@pytest.mark.parametrize("boundary_secret", ["a" * 32, "z" * 512])
def test_production_capture_accepts_canonical_agent_secret_length_boundaries(
    boundary_secret,
):
    _validate_agent_erasure_signing_config(
        capture_enabled="true",
        key_id="agent-erasure-2026-07",
        secret=boundary_secret,
    )


_AGENT_CREDENTIAL_ENV_NAMES = (
    "VEXA_RUNTIME_CONTROL_SECRET",
    "RUNTIME_CONTROL_SECRET",
    "VEXA_AGENT_IDENTITY_TOKEN",
    "VEXA_DISPATCH_SIGNING_KEY",
    "VEXA_BOT_API_KEY",
    "TRANSCRIPTION_SERVICE_TOKEN",
    "ZAKI_READ_TOKEN_MINUTES",
    "VEXA_INTERNAL_API_SECRET",
    "INTERNAL_API_SECRET",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "VEXA_LLM_API_KEY",
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "GITLAB_TOKEN",
    "GL_TOKEN",
    "BITBUCKET_TOKEN",
    "GIT_TOKEN",
    "VEXA_GIT_TOKEN",
)


@pytest.mark.parametrize("credential_name", _AGENT_CREDENTIAL_ENV_NAMES)
@pytest.mark.parametrize("colliding_key", ["current", "previous"])
def test_agent_erasure_keys_are_distinct_from_every_direct_agent_api_credential(
    credential_name, colliding_key,
):
    collision = "credential-boundary-secret-01234567890"
    current = (
        collision
        if colliding_key == "current"
        else "agent-current-distinct-secret-01234567890"
    )
    previous = collision if colliding_key == "previous" else None
    kwargs = {
        "capture_enabled": "true",
        "key_id": "agent-erasure-2026-07",
        "secret": current,
        "credential_environment": {credential_name: collision},
    }
    if previous is not None:
        kwargs.update({
            "previous_key_id": "agent-erasure-2026-06",
            "previous_secret": previous,
        })

    with pytest.raises(
        RuntimeError, match="distinct from every agent-api credential",
    ) as raised:
        _validate_agent_erasure_signing_config(**kwargs)

    assert collision not in str(raised.value)


@pytest.mark.parametrize(
    "credential_environment",
    [
        {"VEXA_REDIS_URL": "redis://:credential-boundary-secret-01234567890@redis:6379/0"},
        {"VEXA_REDIS_URL": "redis://credential-boundary-secret-01234567890:other@redis:6379/0"},
        {"ANTHROPIC_BASE_URL": "https://credential-boundary-secret-01234567890:other@model.invalid/v1"},
        {"VEXA_LLM_BASE_URL": "https://user:credential-boundary-secret-01234567890@model.invalid/v1"},
    ],
)
@pytest.mark.parametrize("colliding_key", ["current", "previous"])
def test_agent_erasure_keys_are_distinct_from_url_embedded_agent_api_credentials(
    credential_environment, colliding_key,
):
    collision = "credential-boundary-secret-01234567890"
    kwargs = {
        "capture_enabled": "true",
        "key_id": "agent-erasure-2026-07",
        "secret": (
            collision
            if colliding_key == "current"
            else "agent-current-distinct-secret-01234567890"
        ),
        "credential_environment": credential_environment,
    }
    if colliding_key == "previous":
        kwargs.update({
            "previous_key_id": "agent-erasure-2026-06",
            "previous_secret": collision,
        })

    with pytest.raises(RuntimeError, match="distinct from every agent-api credential"):
        _validate_agent_erasure_signing_config(**kwargs)


def test_agent_erasure_credential_inventory_handles_non_ascii_environment_material_safely():
    _validate_agent_erasure_signing_config(
        capture_enabled="true",
        key_id="agent-erasure-2026-07",
        secret="agent-erasure-current-secret-01234567890",
        credential_environment={"GITHUB_TOKEN": "\udcff" * 32},
    )


def test_production_boot_checks_the_full_agent_api_credential_environment(monkeypatch):
    from control_plane.api import _build_production_app

    collision = "credential-boundary-secret-01234567890"
    monkeypatch.setenv("ZAKI_MINUTES_CAPTURE_ENABLED", "true")
    monkeypatch.setenv(
        "ZAKI_AGENT_ERASURE_SIGNING_KEY_ID", "agent-erasure-2026-07"
    )
    monkeypatch.setenv("ZAKI_AGENT_ERASURE_SIGNING_SECRET", collision)
    monkeypatch.delenv(
        "ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID", raising=False
    )
    monkeypatch.delenv(
        "ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET", raising=False
    )
    monkeypatch.setenv("VEXA_RUNTIME_CONTROL_SECRET", collision)

    with pytest.raises(
        RuntimeError, match="distinct from every agent-api credential",
    ) as raised:
        _build_production_app()

    assert collision not in str(raised.value)


def test_internal_minutes_erasure_rejects_a_legacy_unsigned_receipt():
    eraser = _Eraser()

    settings = load_settings(internal_api_secret="internal-secret")
    response = TestClient(create_app(
        Dispatcher(settings, _Runtime(), _Identity()), minutes_eraser=eraser,
    )).post(
        "/internal/minutes/meetings/41/erase",
        headers={"X-Internal-Secret": "internal-secret"},
        json={"user_id": "7"},
    )

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "erasure_pending"}}
    assert response.headers["cache-control"] == "no-store"
    assert eraser.calls == [(7, "41")]


def test_internal_minutes_erasure_returns_the_injected_signed_receipt_verbatim_and_replays_it():
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.hset("zaki:agent:minutes-erasure:41", mapping={
        "user_id": "7",
        "state": "complete",
        "unit_streams": "1",
        "workspace_documents": "2",
        "brain_records": "3",
    })
    raw = _Eraser()
    wrapped = _wrap_signed_minutes_eraser(
        raw,
        redis_client=redis,
        key_id="agent-erasure-2026-07",
        secret="agent-erasure-signing-secret-0123456789",
        clock=lambda: datetime(2026, 7, 15, 12, 30, tzinfo=timezone.utc),
        nonce=lambda: "01J2M3N4P5Q6R7S8T9V0WXYZAB",
    )
    client = _client(wrapped)
    request = {
        "headers": {"X-Internal-Secret": "internal-secret"},
        "json": {"user_id": "7"},
    }

    first = client.post("/internal/minutes/meetings/41/erase", **request)
    second = client.post("/internal/minutes/meetings/41/erase", **request)

    persisted = json.loads(
        redis.hget("zaki:agent:minutes-erasure:41", "signed_receipt")
    )
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json() == persisted
    assert set(first.json()) == {
        "version", "owner", "scope", "subject", "counts", "issued_at",
        "key_id", "nonce", "digest", "signature",
    }
    assert first.json()["owner"] == "agent"
    assert first.json()["subject"] == {"user_id": "7", "meeting_id": "41"}
    assert first.headers["Cache-Control"] == "no-store"


def test_internal_minutes_erasure_rejects_bad_auth_row_and_body_before_mutation():
    eraser = _Eraser()
    client = _client(eraser)

    missing_secret = client.post(
        "/internal/minutes/meetings/41/erase", content=b"not-json"
    )
    wrong_secret = client.post(
        "/internal/minutes/meetings/41/erase",
        headers={"X-Internal-Secret": "wrong"},
        content=b"x" * 4096,
    )
    assert missing_secret.status_code == wrong_secret.status_code == 403
    assert missing_secret.json() == wrong_secret.json() == {"error": {"code": "forbidden"}}

    headers = {"X-Internal-Secret": "internal-secret"}
    for row in (
        "0", "01", "not-a-row", "10000000000000000000", "9223372036854775808",
    ):
        response = client.post(
            f"/internal/minutes/meetings/{row}/erase", headers=headers, json={"user_id": "7"}
        )
        assert response.status_code == 404
        assert response.json() == {"error": {"code": "not_found"}}

    invalid_bodies = (
        {},
        {"user_id": True},
        {"user_id": 7},
        {"user_id": "07"},
        {"user_id": "9223372036854775808"},
        {"user_id": 0},
        {"user_id": -1},
        {"user_id": "7", "extra": "not allowed"},
        [7],
    )
    for body in invalid_bodies:
        response = client.post(
            "/internal/minutes/meetings/41/erase", headers=headers, json=body
        )
        assert response.status_code == 400
        assert response.json() == {"error": {"code": "invalid_request"}}

    invalid_json = client.post(
        "/internal/minutes/meetings/41/erase", headers=headers, content=b"not-json"
    )
    too_large = client.post(
        "/internal/minutes/meetings/41/erase",
        headers={**headers, "Content-Type": "application/json"},
        content=b'{"user_id":"7","padding":"' + b"x" * 2048 + b'"}',
    )
    assert invalid_json.status_code == 400
    assert too_large.status_code == 413
    assert invalid_json.json() == {"error": {"code": "invalid_request"}}
    assert too_large.json() == {"error": {"code": "request_too_large"}}
    assert eraser.calls == []


def test_internal_minutes_erasure_accepts_the_signed_bigint_boundary_only():
    eraser = _Eraser()
    client = _client(eraser)
    headers = {"X-Internal-Secret": "internal-secret"}

    accepted = client.post(
        "/internal/minutes/meetings/9223372036854775807/erase",
        headers=headers,
        json={"user_id": "9223372036854775807"},
    )
    rejected_user = client.post(
        "/internal/minutes/meetings/41/erase",
        headers=headers,
        json={"user_id": "9223372036854775808"},
    )

    assert accepted.status_code == 200
    assert rejected_user.status_code == 400
    assert eraser.calls == [
        (9_223_372_036_854_775_807, "9223372036854775807"),
    ]


def test_erasure_tombstones_stops_and_purges_only_the_owner_row(tmp_path):
    owner_dir = tmp_path / "7" / "kg" / "entities" / "meeting"
    foreign_dir = tmp_path / "8" / "kg" / "entities" / "meeting"
    owner_dir.mkdir(parents=True)
    foreign_dir.mkdir(parents=True)
    (owner_dir / "41.md").write_text("owner transcript")
    (owner_dir / "41.envelope.json").write_text('{"owner":7}')
    (foreign_dir / "41.md").write_text("foreign transcript")
    (foreign_dir / "41.envelope.json").write_text('{"owner":8}')

    state = _State()
    brain = _Brain()
    live = _Live()
    runtime = _Runtime()
    eraser = AgentMinutesErasure(
        state=state,
        workspaces_root=tmp_path,
        stop_workload=runtime.stop,
        brain_eraser=brain,
        live_registry=live,
    )
    settings = load_settings(internal_api_secret="internal-secret")
    client = TestClient(create_app(
        Dispatcher(settings, runtime, _Identity()), minutes_eraser=_signed(eraser),
    ))

    response = client.post(
        "/internal/minutes/meetings/41/erase",
        headers={"X-Internal-Secret": "internal-secret"},
        json={"user_id": "7"},
    )

    assert response.status_code == 200
    assert response.json()["counts"] == {
        "agent_unit_streams": 1,
        "agent_workspace_documents": 2,
        "agent_brain_records": 3,
    }
    assert state.fenced == {"41"}
    assert state.keys == set()
    assert state.active == set()
    assert runtime.stopped == ["agent-meet-41"]
    assert live.rows == {}
    assert list(owner_dir.iterdir()) == []
    assert (foreign_dir / "41.md").read_text() == "foreign transcript"
    assert (foreign_dir / "41.envelope.json").read_text() == '{"owner":8}'
    assert brain.records == {
        (7, "meeting:42", "transcript:42"): 4,
        (8, "meeting:41", "transcript:41"): 5,
    }
    assert brain.calls == [{
        "user_id": 7,
        "meeting_id": "meeting:41",
        "write_origin": "meeting_ingest",
        "source_spoke": "minutes",
        "source_item_ids": ("transcript:41", "summary:41"),
        "idempotency_key": "minutes-erasure:v1:7:41",
    }]


def test_completed_retry_reasserts_the_redis_fence_without_reopening_destructive_stores(tmp_path):
    meeting_dir = tmp_path / "7" / "kg" / "entities" / "meeting"
    meeting_dir.mkdir(parents=True)
    (meeting_dir / "41.md").write_text("first")
    (meeting_dir / "41.envelope.json").write_text("first envelope")
    state = _State()
    brain = _Brain()
    runtime = _Runtime()
    eraser = AgentMinutesErasure(
        state=state,
        workspaces_root=tmp_path,
        stop_workload=runtime.stop,
        brain_eraser=brain,
    )
    settings = load_settings(internal_api_secret="internal-secret")
    client = TestClient(create_app(
        Dispatcher(settings, runtime, _Identity()), minutes_eraser=_signed(eraser),
    ))
    request = {
        "headers": {"X-Internal-Secret": "internal-secret"},
        "json": {"user_id": "7"},
    }

    first = client.post("/internal/minutes/meetings/41/erase", **request)
    # begin() always reasserts the Redis fence and carrier purge, even for a completed receipt. The
    # receipt then returns immediately: completed retries must not reopen workspace or Brain stores.
    state.keys.add("unit:agent-meet-41:out")
    second = client.post("/internal/minutes/meetings/41/erase", **request)

    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    assert second.json()["counts"] == {
        "agent_unit_streams": 1,
        "agent_workspace_documents": 2,
        "agent_brain_records": 3,
    }
    assert list(meeting_dir.iterdir()) == []
    assert "unit:agent-meet-41:out" not in state.keys
    assert runtime.stopped == ["agent-meet-41"]
    assert len(brain.calls) == 1


def test_erasure_purges_all_owner_workspace_artifacts_with_canonical_minutes_provenance(tmp_path):
    owner = tmp_path / "7"
    parked = tmp_path / ".attached" / "7" / "project"
    foreign = tmp_path / "8"
    for root in (owner, parked, foreign):
        (root / "kg" / "entities" / "decision").mkdir(parents=True)
        (root / "kg" / "entities" / "action").mkdir(parents=True)
    transcript_artifact = owner / "kg" / "entities" / "decision" / "launch.md"
    summary_artifact = parked / "kg" / "entities" / "action" / "follow-up.json"
    other_meeting = owner / "kg" / "entities" / "decision" / "other.md"
    other_tenant = foreign / "kg" / "entities" / "decision" / "launch.md"
    transcript_artifact.write_text("""---
write_origin: meeting_ingest
source_spoke: minutes
meeting_id: meeting:41
source_item_id: transcript:41
---
derived decision
""")
    summary_artifact.write_text("""{
  "write_origin": "meeting_ingest",
  "source_spoke": "minutes",
  "meeting_id": "meeting:41",
  "source_item_id": "summary:41",
  "content": "derived action"
}
""")
    other_meeting.write_text(transcript_artifact.read_text().replace("meeting:41", "meeting:42").replace("transcript:41", "transcript:42"))
    other_tenant.write_text(transcript_artifact.read_text())

    state = _State()
    brain = _Brain()
    runtime = _Runtime()
    eraser = AgentMinutesErasure(
        state=state,
        workspaces_root=tmp_path,
        stop_workload=runtime.stop,
        brain_eraser=brain,
    )
    settings = load_settings(internal_api_secret="internal-secret")
    client = TestClient(create_app(
        Dispatcher(settings, runtime, _Identity()), minutes_eraser=_signed(eraser),
    ))

    response = client.post(
        "/internal/minutes/meetings/41/erase",
        headers={"X-Internal-Secret": "internal-secret"},
        json={"user_id": "7"},
    )

    assert response.status_code == 200
    assert response.json()["counts"]["agent_workspace_documents"] == 2
    assert not transcript_artifact.exists()
    assert not summary_artifact.exists()
    assert other_meeting.exists()
    assert other_tenant.exists()


def test_erasure_rejects_an_owner_workspace_symlink_without_touching_the_target_tenant(tmp_path):
    foreign_dir = tmp_path / "8" / "kg" / "entities" / "meeting"
    foreign_dir.mkdir(parents=True)
    foreign_document = foreign_dir / "41.md"
    foreign_document.write_text("tenant-eight private transcript")
    (tmp_path / "7").symlink_to(tmp_path / "8", target_is_directory=True)

    state = _State()
    brain = _Brain()
    runtime = _Runtime()
    eraser = AgentMinutesErasure(
        state=state,
        workspaces_root=tmp_path,
        stop_workload=runtime.stop,
        brain_eraser=brain,
    )
    settings = load_settings(internal_api_secret="internal-secret")
    client = TestClient(create_app(
        Dispatcher(settings, runtime, _Identity()), minutes_eraser=_signed(eraser),
    ))

    response = client.post(
        "/internal/minutes/meetings/41/erase",
        headers={"X-Internal-Secret": "internal-secret"},
        json={"user_id": "7"},
    )

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "erasure_pending"}}
    assert foreign_document.read_text() == "tenant-eight private transcript"
    assert brain.calls == []


def test_workspace_parent_symlink_swap_cannot_delete_a_sibling_tenant_file(tmp_path, monkeypatch):
    owner_meeting = tmp_path / "7" / "kg" / "entities" / "meeting"
    foreign_meeting = tmp_path / "8" / "kg" / "entities" / "meeting"
    owner_meeting.mkdir(parents=True)
    foreign_meeting.mkdir(parents=True)
    owner_target = owner_meeting / "41.md"
    foreign_target = foreign_meeting / "41.md"
    owner_target.write_text("owner transcript")
    foreign_target.write_text("foreign transcript")
    original_proof = minutes_erasure_mod._prove_targets_absent_from_git

    def swap_parent_after_census(workspace, targets):
        original_proof(workspace, targets)
        owner_target.unlink()
        owner_meeting.rmdir()
        owner_meeting.symlink_to(foreign_meeting, target_is_directory=True)

    monkeypatch.setattr(
        minutes_erasure_mod, "_prove_targets_absent_from_git", swap_parent_after_census,
    )
    runtime = _Runtime()
    eraser = AgentMinutesErasure(
        state=_State(),
        workspaces_root=tmp_path,
        stop_workload=runtime.stop,
        brain_eraser=_Brain(),
    )
    client = TestClient(create_app(
        Dispatcher(load_settings(internal_api_secret="internal-secret"), runtime, _Identity()),
        minutes_eraser=_signed(eraser),
    ))

    response = client.post(
        "/internal/minutes/meetings/41/erase",
        headers={"X-Internal-Secret": "internal-secret"},
        json={"user_id": "7"},
    )

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "erasure_pending"}}
    assert foreign_target.read_text() == "foreign transcript"


def test_provenance_reader_never_follows_a_symlink_to_foreign_content(tmp_path):
    foreign = tmp_path / "foreign.md"
    foreign.write_text("""---
write_origin: meeting_ingest
source_spoke: minutes
meeting_id: meeting:41
source_item_id: transcript:41
---
foreign content
""")
    candidate = tmp_path / "candidate.md"
    candidate.symlink_to(foreign)

    with pytest.raises(minutes_erasure_mod.ErasurePending, match="provenance"):
        minutes_erasure_mod._provenance_metadata(candidate, meeting_id="41")


def test_completed_erasure_returns_its_receipt_without_reopening_destructive_dependencies(tmp_path):
    state = _State()
    state.rows["41"] = {
        "user_id": 7,
        "counts": {"unit_streams": 1, "workspace_documents": 2, "brain_records": 3},
        "receipt": {
            "meeting_id": "41",
            "tombstoned": True,
            "deleted": {"unit_streams": 1, "workspace_documents": 2, "brain_records": 3},
        },
    }

    class OfflineRuntime(_Runtime):
        def stop(self, workload_id):
            raise RuntimeError("runtime is now offline")

    class OfflineBrain:
        def erase_meeting(self, **_query):
            raise AssertionError("completed retry reopened Brain")

    runtime = OfflineRuntime()
    live = _LiveMeetings()
    live.add({"session_uid": "41", "meeting_id": "41", "title": "private title"})
    eraser = AgentMinutesErasure(
        state=state,
        workspaces_root=tmp_path,
        stop_workload=runtime.stop,
        brain_eraser=OfflineBrain(),
        live_registry=live,
    )
    client = TestClient(create_app(
        Dispatcher(load_settings(internal_api_secret="internal-secret"), runtime, _Identity()),
        minutes_eraser=_signed(eraser),
    ))

    response = client.post(
        "/internal/minutes/meetings/41/erase",
        headers={"X-Internal-Secret": "internal-secret"},
        json={"user_id": "7"},
    )

    assert response.status_code == 200
    assert response.json()["subject"] == {"user_id": "7", "meeting_id": "41"}
    assert response.json()["counts"] == {
        "agent_unit_streams": 1,
        "agent_workspace_documents": 2,
        "agent_brain_records": 3,
    }
    assert live.list() == []


def test_live_registry_row_is_removed_on_first_successful_erasure(tmp_path):
    live = _LiveMeetings()
    live.add({
        "session_uid": "41",
        "meeting_id": "41",
        "native_id": "private-native-id",
        "title": "private meeting title",
    })
    runtime = _Runtime()
    eraser = AgentMinutesErasure(
        state=_State(),
        workspaces_root=tmp_path,
        stop_workload=runtime.stop,
        brain_eraser=_Brain(),
        live_registry=live,
    )

    eraser.erase(user_id=7, meeting_id="41")

    assert live.list() == []


def test_app_binds_its_live_registry_to_the_erasure_composition(tmp_path):
    runtime = _Runtime()
    eraser = AgentMinutesErasure(
        state=_State(),
        workspaces_root=tmp_path,
        stop_workload=runtime.stop,
        brain_eraser=_Brain(),
    )
    app = create_app(
        Dispatcher(load_settings(internal_api_secret="internal-secret"), runtime, _Identity()),
        minutes_eraser=_signed(eraser),
    )
    app.state.live_meetings.add({
        "session_uid": "41", "meeting_id": "41", "title": "private title",
    })

    response = TestClient(app).post(
        "/internal/minutes/meetings/41/erase",
        headers={"X-Internal-Secret": "internal-secret"},
        json={"user_id": "7"},
    )

    assert response.status_code == 200
    assert app.state.live_meetings.list() == []


def test_brain_erasure_retry_uses_one_durable_idempotency_key_and_preserves_the_first_count(tmp_path):
    class FailsAfterBrainOnce(_State):
        def __init__(self):
            super().__init__()
            self.failed = False

        def remember_count(self, *, meeting_id, user_id, field, observed):
            if field == "brain_records" and not self.failed:
                self.failed = True
                raise RuntimeError("Redis failed after the Brain transaction committed")
            return super().remember_count(
                meeting_id=meeting_id, user_id=user_id, field=field, observed=observed,
            )

    state = FailsAfterBrainOnce()
    brain = _Brain()
    runtime = _Runtime()
    eraser = AgentMinutesErasure(
        state=state,
        workspaces_root=tmp_path,
        stop_workload=runtime.stop,
        brain_eraser=brain,
    )
    client = TestClient(create_app(
        Dispatcher(load_settings(internal_api_secret="internal-secret"), runtime, _Identity()),
        minutes_eraser=_signed(eraser),
    ), raise_server_exceptions=False)
    request = {
        "headers": {"X-Internal-Secret": "internal-secret"},
        "json": {"user_id": "7"},
    }

    first = client.post("/internal/minutes/meetings/41/erase", **request)
    second = client.post("/internal/minutes/meetings/41/erase", **request)

    assert first.status_code == 503
    assert second.status_code == 200
    assert second.json()["counts"]["agent_brain_records"] == 3
    assert [call["idempotency_key"] for call in brain.calls] == [
        "minutes-erasure:v1:7:41",
        "minutes-erasure:v1:7:41",
    ]


def _git(cwd, *args):
    subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
    )


def test_workspace_erasure_fails_closed_when_a_target_is_reachable_in_git_history(tmp_path):
    owner = tmp_path / "7"
    target = owner / "kg" / "entities" / "meeting" / "41.md"
    target.parent.mkdir(parents=True)
    target.write_text("private transcript in history")
    _git(owner, "init", "-q")
    _git(owner, "config", "user.name", "Test")
    _git(owner, "config", "user.email", "test@example.invalid")
    _git(owner, "add", "kg/entities/meeting/41.md")
    _git(owner, "commit", "-q", "-m", "meeting")
    before_head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=owner, check=True, capture_output=True, text=True,
    ).stdout.strip()
    eraser = AgentMinutesErasure(
        state=_State(),
        workspaces_root=tmp_path,
        stop_workload=_Runtime().stop,
        brain_eraser=_Brain(),
    )
    client = TestClient(create_app(
        Dispatcher(load_settings(internal_api_secret="internal-secret"), _Runtime(), _Identity()),
        minutes_eraser=_signed(eraser),
    ))

    response = client.post(
        "/internal/minutes/meetings/41/erase",
        headers={"X-Internal-Secret": "internal-secret"},
        json={"user_id": "7"},
    )

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "erasure_pending"}}
    assert target.read_text() == "private transcript in history"
    after_head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=owner, check=True, capture_output=True, text=True,
    ).stdout.strip()
    assert after_head == before_head


def test_workspace_erasure_fails_closed_when_a_remote_cannot_be_proved_purged(tmp_path):
    owner = tmp_path / "7"
    target = owner / "kg" / "entities" / "meeting" / "41.md"
    target.parent.mkdir(parents=True)
    target.write_text("untracked private transcript")
    _git(owner, "init", "-q")
    _git(owner, "remote", "add", "origin", "https://example.invalid/private.git")
    eraser = AgentMinutesErasure(
        state=_State(),
        workspaces_root=tmp_path,
        stop_workload=_Runtime().stop,
        brain_eraser=_Brain(),
    )
    client = TestClient(create_app(
        Dispatcher(load_settings(internal_api_secret="internal-secret"), _Runtime(), _Identity()),
        minutes_eraser=_signed(eraser),
    ))

    response = client.post(
        "/internal/minutes/meetings/41/erase",
        headers={"X-Internal-Secret": "internal-secret"},
        json={"user_id": "7"},
    )

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "erasure_pending"}}
    assert target.read_text() == "untracked private transcript"


def test_erasure_row_owner_binding_is_non_enumerating_and_never_touches_other_user(tmp_path):
    foreign_dir = tmp_path / "8" / "kg" / "entities" / "meeting"
    foreign_dir.mkdir(parents=True)
    (foreign_dir / "41.md").write_text("foreign")
    state = _State()
    state.rows["41"] = {
        "user_id": 7,
        "counts": {"unit_streams": 0, "workspace_documents": 0, "brain_records": 0},
    }
    brain = _Brain()
    runtime = _Runtime()
    eraser = AgentMinutesErasure(
        state=state,
        workspaces_root=tmp_path,
        stop_workload=runtime.stop,
        brain_eraser=brain,
    )
    settings = load_settings(internal_api_secret="internal-secret")
    client = TestClient(create_app(
        Dispatcher(settings, runtime, _Identity()), minutes_eraser=_signed(eraser),
    ), raise_server_exceptions=False)

    response = client.post(
        "/internal/minutes/meetings/41/erase",
        headers={"X-Internal-Secret": "internal-secret"},
        json={"user_id": "8"},
    )

    assert response.status_code == 404
    assert response.json() == {"error": {"code": "not_found"}}
    assert (foreign_dir / "41.md").read_text() == "foreign"
    assert runtime.stopped == []
    assert brain.calls == []


def test_redis_erasure_state_atomically_persists_fence_carrier_purge_and_stable_receipt():
    import fakeredis

    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.xadd("unit:agent-meet-41:out", {"event": "private"})
    redis.xadd("unit:agent-meet-41:in", {"turn": "private"})
    redis.xadd("proc:meeting:41", {"note": "private"})
    redis.set("proc:meeting:41:on", "generation")
    redis.set("proc:meeting:41:cursor", "9-0")
    redis.sadd("active_meetings", "41", "81")
    state = RedisMinutesErasureState(redis)

    begun = state.begin(user_id=7, meeting_id="41")

    assert begun == ErasureBegin(owner_matches=True, completed=None, unit_streams=1)
    assert redis.hget("zaki:retention:meeting:41:fence", "processed") == "1"
    assert redis.ttl("zaki:retention:meeting:41:fence") == -1
    assert redis.ttl("zaki:agent:minutes-erasure:41") == -1
    assert redis.exists(
        "unit:agent-meet-41:out", "unit:agent-meet-41:in", "proc:meeting:41",
        "proc:meeting:41:on", "proc:meeting:41:cursor",
    ) == 0
    assert redis.smembers("active_meetings") == {"81"}

    assert state.remember_count(
        meeting_id="41", user_id=7, field="workspace_documents", observed=2,
    ) == 2
    assert state.remember_count(
        meeting_id="41", user_id=7, field="brain_records", observed=3,
    ) == 3
    receipt = state.complete(
        meeting_id="41",
        user_id=7,
        counts={"unit_streams": 1, "workspace_documents": 2, "brain_records": 3},
    )
    assert receipt["deleted"] == {
        "unit_streams": 1, "workspace_documents": 2, "brain_records": 3,
    }

    # A retry reasserts the permanent fence and cleans late carriers, but preserves the first census.
    redis.xadd("unit:agent-meet-41:out", {"event": "late"})
    redis.set("proc:meeting:41:on", "late-generation")
    retried = state.begin(user_id=7, meeting_id="41")
    assert retried.completed == receipt
    assert retried.unit_streams == 1
    assert redis.exists("unit:agent-meet-41:out", "proc:meeting:41:on") == 0

    foreign = state.begin(user_id=8, meeting_id="41")
    assert foreign.owner_matches is False
    assert redis.hget("zaki:agent:minutes-erasure:41", "user_id") == "7"
