"""Production composition tests for the default-off Minutes retention loop."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import importlib

import pytest


entry = importlib.import_module("meeting_api.__main__")
HUB_TOKEN = "minutes-hub-service-token-0123456789"
READ_SECRET = "minutes-read-secret-012345678901234"
AGENT_SECRET = "agent-hmac-secret-01234567890123456"
MINUTES_SECRET = "minutes-hmac-secret-01234567890123"
PREVIOUS_MINUTES_SECRET = "minutes-previous-secret-0123456789012"
FINALIZED_SECRET = "platform-hmac-secret-012345678901"
INTERNAL_SECRET = "internal-auth-secret-012345678901234"


def test_database_url_encodes_raw_operator_secret_components(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "db.example.internal")
    monkeypatch.setenv("DB_PORT", "5433")
    monkeypatch.setenv("DB_NAME", "minutes/prod")
    monkeypatch.setenv("DB_USER", "user@tenant")
    monkeypatch.setenv("DB_PASSWORD", "p/a%s@s:word")

    assert entry._database_url() == (
        "postgresql+asyncpg://user%40tenant:p%2Fa%25s%40s%3Aword@"
        "db.example.internal:5433/minutes%2Fprod"
    )


def test_database_url_preserves_an_explicit_operator_url(monkeypatch):
    explicit = "postgresql+asyncpg://brokered:opaque@db.example.internal/minutes"
    monkeypatch.setenv("DATABASE_URL", explicit)
    assert entry._database_url() == explicit


def test_database_url_applies_the_operator_tls_mode(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_SSL_MODE", "require")

    assert entry._database_url().endswith("/vexa?ssl=require")


def test_database_url_rejects_an_unknown_tls_mode(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_SSL_MODE", "allow")

    with pytest.raises(RuntimeError, match="DB_SSL_MODE"):
        entry._database_url()


def test_minutes_ttl_runtime_defaults_off_and_validates_bounds():
    assert entry._minutes_ttl_config({}) == (False, 60.0, 100)
    assert entry._minutes_ttl_config({
        "MINUTES_TTL_ENABLED": "true",
        "MINUTES_TTL_INTERVAL_S": "15",
        "MINUTES_TTL_BATCH_SIZE": "500",
    }) == (True, 15.0, 500)
    with pytest.raises(RuntimeError, match="MINUTES_TTL_ENABLED"):
        entry._minutes_ttl_config({"MINUTES_TTL_ENABLED": "sometimes"})
    with pytest.raises(RuntimeError, match="MINUTES_TTL_INTERVAL_S"):
        entry._minutes_ttl_config({"MINUTES_TTL_INTERVAL_S": "0"})
    with pytest.raises(RuntimeError, match="MINUTES_TTL_BATCH_SIZE"):
        entry._minutes_ttl_config({"MINUTES_TTL_BATCH_SIZE": "501"})


def test_minutes_calendar_auto_join_defaults_off_and_refuses_the_legacy_spawn_path():
    assert entry._minutes_auto_join_config({}) is False
    assert entry._minutes_auto_join_config({
        "ZAKI_MINUTES_AUTO_JOIN_ENABLED": "false",
    }) is False
    with pytest.raises(RuntimeError, match="managed consent and retention"):
        entry._minutes_auto_join_config({"ZAKI_MINUTES_AUTO_JOIN_ENABLED": "true"})
    with pytest.raises(RuntimeError, match="explicit boolean"):
        entry._minutes_auto_join_config({"ZAKI_MINUTES_AUTO_JOIN_ENABLED": "sometimes"})


def test_managed_invocation_v2_defaults_off_and_capture_requires_explicit_enablement():
    assert entry._minutes_invocation_v2_config({}, capture_enabled=False) is False
    assert entry._minutes_invocation_v2_config({
        "ZAKI_MINUTES_INVOCATION_V2_ENABLED": "true",
    }, capture_enabled=True) is True
    with pytest.raises(RuntimeError, match="ZAKI_MINUTES_INVOCATION_V2_ENABLED"):
        entry._minutes_invocation_v2_config({}, capture_enabled=True)
    with pytest.raises(RuntimeError, match="explicit boolean"):
        entry._minutes_invocation_v2_config({
            "ZAKI_MINUTES_INVOCATION_V2_ENABLED": "sometimes",
        }, capture_enabled=False)


def test_minutes_activation_refuses_read_without_token_and_capture_without_ttl():
    with pytest.raises(RuntimeError, match="ZAKI_READ_TOKEN_MINUTES"):
        entry._validate_minutes_activation(
            capture_enabled=False, read_enabled=True, ttl_enabled=False, read_token=None,
        )
    with pytest.raises(RuntimeError, match="MINUTES_TTL_ENABLED"):
        entry._validate_minutes_activation(
            capture_enabled=True, read_enabled=False, ttl_enabled=False, read_token=None,
        )
    entry._validate_minutes_activation(
        capture_enabled=False, read_enabled=True, ttl_enabled=False, read_token=READ_SECRET,
    )


@pytest.mark.parametrize(
    "read_token",
    ["short", " " + READ_SECRET, READ_SECRET + " ", READ_SECRET + "\x00", READ_SECRET + "é"],
)
def test_minutes_read_activation_requires_strong_unpadded_ascii_token(read_token):
    with pytest.raises(RuntimeError, match="ZAKI_READ_TOKEN_MINUTES"):
        entry._validate_minutes_activation(
            capture_enabled=False,
            read_enabled=True,
            ttl_enabled=False,
            read_token=read_token,
        )


@pytest.mark.parametrize(
    ("missing", "expected"),
    [
        ("agent_verification_key_id", "ZAKI_AGENT_ERASURE_VERIFICATION_KEY_ID"),
        ("agent_verification_secret", "ZAKI_AGENT_ERASURE_VERIFICATION_SECRET"),
        ("minutes_signing_key_id", "ZAKI_MINUTES_ERASURE_SIGNING_KEY_ID"),
        ("minutes_signing_secret", "ZAKI_MINUTES_ERASURE_SIGNING_SECRET"),
    ],
)
def test_minutes_capture_requires_both_receipt_key_projections(missing, expected):
    values = {
        "hub_token": HUB_TOKEN,
        "agent_verification_key_id": "agent-erasure-2026-07",
        "agent_verification_secret": AGENT_SECRET,
        "minutes_signing_key_id": "minutes-erasure-2026-07",
        "minutes_signing_secret": MINUTES_SECRET,
    }
    values[missing] = None

    with pytest.raises(RuntimeError, match=expected):
        entry._validate_minutes_activation(
            capture_enabled=True,
            read_enabled=False,
            ttl_enabled=True,
            read_token=None,
            **values,
        )


def test_minutes_capture_accepts_complete_receipt_key_projections():
    entry._validate_minutes_activation(
        capture_enabled=True,
        read_enabled=True,
        ttl_enabled=True,
        read_token=READ_SECRET,
        hub_token=HUB_TOKEN,
        agent_verification_key_id="agent-erasure-2026-07",
        agent_verification_secret=AGENT_SECRET,
        minutes_signing_key_id="minutes-erasure-2026-07",
        minutes_signing_secret=MINUTES_SECRET,
    )


@pytest.mark.parametrize(
    "hub_token",
    [None, "short", " " + HUB_TOKEN, HUB_TOKEN + " ", HUB_TOKEN + "\x00", HUB_TOKEN + "é"],
)
def test_managed_minutes_activation_requires_strong_unpadded_ascii_hub_token(hub_token):
    with pytest.raises(RuntimeError, match="ZAKI_MINUTES_HUB_TOKEN"):
        entry._validate_minutes_activation(
            capture_enabled=True,
            read_enabled=False,
            ttl_enabled=True,
            read_token=None,
            hub_token=hub_token,
            agent_verification_key_id="agent-erasure-2026-07",
            agent_verification_secret=AGENT_SECRET,
            minutes_signing_key_id="minutes-erasure-2026-07",
            minutes_signing_secret=MINUTES_SECRET,
        )


def test_partial_erasure_key_projection_is_refused_even_when_capture_is_off():
    with pytest.raises(RuntimeError, match="ZAKI_AGENT_ERASURE_VERIFICATION_SECRET"):
        entry._validate_minutes_activation(
            capture_enabled=False,
            read_enabled=False,
            ttl_enabled=True,
            read_token=None,
            agent_verification_key_id="agent-erasure-2026-07",
        )


def test_capture_off_with_erasure_keys_still_requires_pii_ttl_worker():
    with pytest.raises(RuntimeError, match="MINUTES_TTL_ENABLED"):
        entry._validate_minutes_activation(
            capture_enabled=False,
            read_enabled=False,
            ttl_enabled=False,
            read_token=None,
            agent_verification_key_id="agent-erasure-2026-07",
            agent_verification_secret=AGENT_SECRET,
            minutes_signing_key_id="minutes-erasure-2026-07",
            minutes_signing_secret=MINUTES_SECRET,
            internal_secret="internal-auth-secret",
        )


def test_erasure_signing_keys_must_be_separate_from_auth_and_each_other():
    base = {
        "capture_enabled": True,
        "read_enabled": False,
        "ttl_enabled": True,
        "read_token": None,
        "hub_token": HUB_TOKEN,
        "agent_verification_key_id": "agent-erasure-2026-07",
        "agent_verification_secret": AGENT_SECRET,
        "minutes_signing_key_id": "minutes-erasure-2026-07",
        "minutes_signing_secret": MINUTES_SECRET,
        "internal_secret": INTERNAL_SECRET,
    }
    entry._validate_minutes_activation(**base)
    with pytest.raises(RuntimeError, match="distinct from INTERNAL_API_SECRET"):
        entry._validate_minutes_activation(
            **{**base, "agent_verification_secret": INTERNAL_SECRET}
        )
    with pytest.raises(RuntimeError, match="independent"):
        entry._validate_minutes_activation(
            **{**base, "minutes_signing_secret": AGENT_SECRET}
        )
    with pytest.raises(RuntimeError, match="distinct from INTERNAL_API_SECRET"):
        entry._validate_minutes_activation(
            **{**base, "minutes_signing_secret": INTERNAL_SECRET}
        )


def test_minutes_receipt_verification_keyring_supports_one_previous_rotation_key():
    assert entry._minutes_erasure_verification_keyring(
        current_key_id="minutes-erasure-2026-07",
        current_secret=MINUTES_SECRET,
        previous_key_id=None,
        previous_secret=None,
    ) == {"minutes-erasure-2026-07": MINUTES_SECRET}
    assert entry._minutes_erasure_verification_keyring(
        current_key_id="minutes-erasure-2026-07",
        current_secret=MINUTES_SECRET,
        previous_key_id="minutes-erasure-2026-06",
        previous_secret=PREVIOUS_MINUTES_SECRET,
    ) == {
        "minutes-erasure-2026-07": MINUTES_SECRET,
        "minutes-erasure-2026-06": PREVIOUS_MINUTES_SECRET,
    }


def test_agent_receipt_verification_keyring_supports_one_previous_rotation_key():
    current = "agent-current-verification-secret-012345678"
    previous = "agent-previous-verification-secret-01234567"
    assert entry._agent_erasure_verification_keyring(
        current_key_id="agent-erasure-2026-07",
        current_secret=current,
        previous_key_id=None,
        previous_secret=None,
    ) == {"agent-erasure-2026-07": current}
    assert entry._agent_erasure_verification_keyring(
        current_key_id="agent-erasure-2026-07",
        current_secret=current,
        previous_key_id="agent-erasure-2026-06",
        previous_secret=previous,
    ) == {
        "agent-erasure-2026-07": current,
        "agent-erasure-2026-06": previous,
    }


@pytest.mark.parametrize(
    ("current_secret", "previous_key_id", "previous_secret"),
    [
        ("short", None, None),
        ("agent-current-verification-secret-012345678", "agent-erasure-2026-06", None),
        ("agent-current-verification-secret-012345678", None, "agent-previous-verification-secret-01234567"),
        ("agent-current-verification-secret-012345678", "agent-erasure-2026-07", "agent-previous-verification-secret-01234567"),
        ("agent-current-verification-secret-012345678", "agent-erasure-2026-06", "short"),
    ],
)
def test_agent_receipt_verification_keyring_refuses_weak_partial_or_alias_keys(
    current_secret, previous_key_id, previous_secret,
):
    with pytest.raises(RuntimeError, match="Agent erasure"):
        entry._agent_erasure_verification_keyring(
            current_key_id="agent-erasure-2026-07",
            current_secret=current_secret,
            previous_key_id=previous_key_id,
            previous_secret=previous_secret,
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
def test_agent_receipt_verification_keyring_refuses_noncanonical_current_or_previous_secret(
    invalid_secret,
):
    with pytest.raises(RuntimeError, match="current verification key is invalid"):
        entry._agent_erasure_verification_keyring(
            current_key_id="agent-erasure-2026-07",
            current_secret=invalid_secret,
            previous_key_id=None,
            previous_secret=None,
        )
    with pytest.raises(RuntimeError, match="previous verification key is invalid"):
        entry._agent_erasure_verification_keyring(
            current_key_id="agent-erasure-2026-07",
            current_secret="agent-current-verification-secret-012345678",
            previous_key_id="agent-erasure-2026-06",
            previous_secret=invalid_secret,
        )


@pytest.mark.parametrize("boundary_secret", ["a" * 32, "z" * 512])
def test_agent_receipt_verification_keyring_accepts_canonical_secret_length_boundaries(
    boundary_secret,
):
    assert entry._agent_erasure_verification_keyring(
        current_key_id="agent-erasure-2026-07",
        current_secret=boundary_secret,
        previous_key_id=None,
        previous_secret=None,
    ) == {"agent-erasure-2026-07": boundary_secret}


@pytest.mark.parametrize(
    ("previous_key_id", "previous_secret"),
    [("minutes-erasure-2026-06", None), (None, PREVIOUS_MINUTES_SECRET)],
)
def test_minutes_receipt_verification_keyring_refuses_partial_previous_key(
    previous_key_id, previous_secret,
):
    with pytest.raises(RuntimeError, match="previous verification"):
        entry._minutes_erasure_verification_keyring(
            current_key_id="minutes-erasure-2026-07",
            current_secret=MINUTES_SECRET,
            previous_key_id=previous_key_id,
            previous_secret=previous_secret,
        )


@pytest.mark.parametrize(
    ("current_secret", "previous_secret"),
    [
        ("short", None),
        (" " + MINUTES_SECRET, None),
        (MINUTES_SECRET + " ", None),
        (MINUTES_SECRET + "\x00", None),
        (MINUTES_SECRET + "é", None),
        (MINUTES_SECRET, "short"),
        (MINUTES_SECRET, " " + PREVIOUS_MINUTES_SECRET),
        (MINUTES_SECRET, PREVIOUS_MINUTES_SECRET + "\x00"),
    ],
)
def test_minutes_receipt_keyring_refuses_weak_padded_or_non_ascii_secrets(
    current_secret, previous_secret,
):
    previous_key_id = "minutes-erasure-2026-06" if previous_secret is not None else None
    with pytest.raises(RuntimeError, match="(?:ERASURE|erasure)"):
        entry._minutes_erasure_verification_keyring(
            current_key_id="minutes-erasure-2026-07",
            current_secret=current_secret,
            previous_key_id=previous_key_id,
            previous_secret=previous_secret,
        )


@pytest.mark.parametrize("collision", [
    INTERNAL_SECRET,
    AGENT_SECRET,
    MINUTES_SECRET,
])
def test_cross_spoke_read_token_must_be_dedicated(collision):
    with pytest.raises(RuntimeError, match="ZAKI_READ_TOKEN_MINUTES must be distinct"):
        entry._validate_minutes_activation(
            capture_enabled=True,
            read_enabled=True,
            ttl_enabled=True,
            read_token=collision,
            hub_token=HUB_TOKEN,
            agent_verification_key_id="agent-erasure-2026-07",
            agent_verification_secret=AGENT_SECRET,
            minutes_signing_key_id="minutes-erasure-2026-07",
            minutes_signing_secret=MINUTES_SECRET,
            internal_secret=INTERNAL_SECRET,
        )


@pytest.mark.parametrize(
    "second_name",
    [
        "RUNTIME_CALLBACK_SECRET",
        "MEETING_TOKEN_SECRET",
        "INTERNAL_API_SECRET",
        "ZAKI_READ_TOKEN_MINUTES",
        "ZAKI_MINUTES_HUB_TOKEN",
        "ZAKI_AGENT_ERASURE_VERIFICATION_SECRET",
        "ZAKI_MINUTES_ERASURE_SIGNING_SECRET",
        "ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET",
        "ZAKI_MINUTES_FINALIZED_SECRET",
    ],
)
def test_runtime_control_credential_cannot_alias_any_other_service_secret(second_name):
    with pytest.raises(RuntimeError, match=rf"{second_name} must be distinct"):
        entry._validate_service_secret_isolation(**{
            "RUNTIME_CONTROL_SECRET": "shared-secret",
            second_name: "shared-secret",
        })


def test_service_secret_isolation_accepts_unique_values_and_ignores_unset_optionals():
    entry._validate_service_secret_isolation(
        RUNTIME_CONTROL_SECRET="runtime-control",
        RUNTIME_CALLBACK_SECRET="runtime-callback",
        MEETING_TOKEN_SECRET="meeting-token",
        INTERNAL_API_SECRET="platform-internal",
        ZAKI_READ_TOKEN_MINUTES=None,
    )


def test_meeting_api_secret_inventory_covers_storage_database_and_transcription_credentials():
    values = entry._meeting_api_secret_values({
        "DB_PASSWORD": "database-password",
        "DATABASE_URL": "postgresql://user:url-password@postgres/db",
        "REDIS_URL": "redis://:redis-password@redis:6379/0",
        "TRANSCRIPTION_SERVICE_TOKEN": "transcription-token",
        "MINIO_ACCESS_KEY": "minio-access",
        "MINIO_SECRET_KEY": "minio-secret",
        "S3_ACCESS_KEY": "s3-access",
        "S3_SECRET_KEY": "s3-secret",
    })

    assert values["DB_PASSWORD"] == "database-password"
    assert values["DATABASE_URL_PASSWORD"] == "url-password"
    assert values["REDIS_URL_PASSWORD"] == "redis-password"
    assert values["TRANSCRIPTION_SERVICE_TOKEN"] == "transcription-token"
    assert values["MINIO_ACCESS_KEY"] == "minio-access"
    assert values["MINIO_SECRET_KEY"] == "minio-secret"
    assert values["S3_ACCESS_KEY"] == "s3-access"
    assert values["S3_SECRET_KEY"] == "s3-secret"


def test_storage_database_or_url_credentials_cannot_alias_other_boundaries():
    for env in (
        {
            "DB_PASSWORD": "shared-credential",
            "TRANSCRIPTION_SERVICE_TOKEN": "shared-credential",
        },
        {
            "DATABASE_URL": "postgresql://user:shared-credential@postgres/db",
            "ZAKI_MINUTES_HUB_TOKEN": "shared-credential",
        },
        {
            "MINIO_SECRET_KEY": "shared-credential",
            "ZAKI_MINUTES_FINALIZED_SECRET": "shared-credential",
        },
    ):
        with pytest.raises(RuntimeError, match="must be distinct"):
            entry._validate_service_secret_isolation(
                **entry._meeting_api_secret_values(env)
            )


@pytest.mark.parametrize(
    "env",
    [
        {
            "DATABASE_URL": "postgresql://user:shared%40credential@postgres/db",
            "ZAKI_MINUTES_FINALIZED_SECRET": "shared@credential",
        },
        {
            "REDIS_URL": "redis://:shared%2Fcredential@redis:6379/0",
            "ZAKI_READ_TOKEN_MINUTES": "shared/credential",
        },
    ],
)
def test_percent_encoded_url_passwords_are_decoded_before_isolation(env):
    with pytest.raises(RuntimeError, match="must be distinct"):
        entry._validate_service_secret_isolation(
            **entry._meeting_api_secret_values(env)
        )


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://user:broken%encoding@postgres/db",
        "postgresql://user:broken%FFencoding@postgres/db",
        "postgresql://user:password@[invalid/db",
    ],
)
def test_malformed_url_userinfo_fails_secret_inventory_closed(url):
    with pytest.raises(RuntimeError, match="credential URL userinfo is invalid"):
        entry._meeting_api_secret_values({"DATABASE_URL": url})


def test_platform_finalized_config_defaults_off_without_requiring_operator_secrets():
    assert entry._minutes_finalized_config({}) == (False, None, None, None)
    assert entry._minutes_finalized_config({
        "ZAKI_MINUTES_FINALIZED_ENABLED": "false",
        "ZAKI_MINUTES_FINALIZED_SECRET": "ignored-while-off",
    }) == (False, None, None, None)


@pytest.mark.parametrize(
    ("missing", "expected"),
    [
        ("ZAKI_MINUTES_FINALIZED_URL", "ZAKI_MINUTES_FINALIZED_URL"),
        ("ZAKI_MINUTES_FINALIZED_KEY_ID", "ZAKI_MINUTES_FINALIZED_KEY_ID"),
        ("ZAKI_MINUTES_FINALIZED_SECRET", "ZAKI_MINUTES_FINALIZED_SECRET"),
    ],
)
def test_platform_finalized_config_refuses_enabled_missing_values(missing, expected):
    env = {
        "ZAKI_MINUTES_FINALIZED_ENABLED": "true",
        "ZAKI_MINUTES_FINALIZED_URL": "http://hub-api:8080/internal/minutes/finalized",
        "ZAKI_MINUTES_FINALIZED_KEY_ID": "minutes-platform-2026-07",
        "ZAKI_MINUTES_FINALIZED_SECRET": FINALIZED_SECRET,
    }
    env.pop(missing)

    with pytest.raises(RuntimeError, match=expected):
        entry._minutes_finalized_config(env)


@pytest.mark.parametrize("url", [
    "file:///tmp/hook",
    "http://user:password@hub-api/internal/minutes/finalized",
    "http://hub-api/internal/minutes/finalized?secret=bad",
    "http://hub-api/internal/minutes/finalized#fragment",
])
def test_platform_finalized_config_refuses_unsafe_operator_urls(url):
    with pytest.raises(RuntimeError, match="ZAKI_MINUTES_FINALIZED_URL"):
        entry._minutes_finalized_config({
            "ZAKI_MINUTES_FINALIZED_ENABLED": "true",
            "ZAKI_MINUTES_FINALIZED_URL": url,
            "ZAKI_MINUTES_FINALIZED_KEY_ID": "minutes-platform-2026-07",
            "ZAKI_MINUTES_FINALIZED_SECRET": FINALIZED_SECRET,
        })


def test_platform_finalized_config_accepts_a_fixed_internal_hub_endpoint():
    assert entry._minutes_finalized_config({
        "ZAKI_MINUTES_FINALIZED_ENABLED": "true",
        "ZAKI_MINUTES_FINALIZED_URL": "http://hub-api:8080/internal/minutes/finalized",
        "ZAKI_MINUTES_FINALIZED_KEY_ID": "minutes-platform-2026-07",
        "ZAKI_MINUTES_FINALIZED_SECRET": FINALIZED_SECRET,
    }) == (
        True,
        "http://hub-api:8080/internal/minutes/finalized",
        "minutes-platform-2026-07",
        FINALIZED_SECRET,
    )


def test_platform_finalized_signer_is_distinct_from_internal_and_erasure_credentials():
    shared_secret = "shared-finalized-secret-0123456789012"
    with pytest.raises(RuntimeError, match="must be distinct"):
        entry._minutes_finalized_config({
            "ZAKI_MINUTES_FINALIZED_ENABLED": "true",
            "ZAKI_MINUTES_FINALIZED_URL": "http://hub-api:8080/internal/minutes/finalized",
            "ZAKI_MINUTES_FINALIZED_KEY_ID": "minutes-platform-2026-07",
            "ZAKI_MINUTES_FINALIZED_SECRET": shared_secret,
            "INTERNAL_API_SECRET": shared_secret,
        })


@pytest.mark.parametrize(
    "secret",
    [
        "short",
        " " + FINALIZED_SECRET,
        FINALIZED_SECRET + " ",
        FINALIZED_SECRET + "\x00",
        FINALIZED_SECRET + "é",
        "x" * 513,
    ],
)
def test_platform_finalized_signer_requires_bounded_unpadded_ascii(secret):
    with pytest.raises(RuntimeError, match="ZAKI_MINUTES_FINALIZED_SECRET"):
        entry._minutes_finalized_config({
            "ZAKI_MINUTES_FINALIZED_ENABLED": "true",
            "ZAKI_MINUTES_FINALIZED_URL": "http://hub-api:8080/internal/minutes/finalized",
            "ZAKI_MINUTES_FINALIZED_KEY_ID": "minutes-platform-2026-07",
            "ZAKI_MINUTES_FINALIZED_SECRET": secret,
        })


@pytest.mark.asyncio
async def test_platform_finalized_drain_loop_is_zero_io_off_and_bounded_on():
    calls = []

    async def drain():
        calls.append("drain")

    async def stop_after_tick(_interval):
        raise asyncio.CancelledError

    await entry._minutes_finalized_drain_loop(
        enabled=False, drain=drain, interval=5, sleep=stop_after_tick,
    )
    assert calls == []

    with pytest.raises(asyncio.CancelledError):
        await entry._minutes_finalized_drain_loop(
            enabled=True, drain=drain, interval=5, sleep=stop_after_tick,
        )
    assert calls == ["drain"]


@pytest.mark.asyncio
async def test_minutes_ttl_loop_is_zero_io_while_disabled():
    calls = []

    async def runner(**kwargs):
        calls.append(kwargs)

    await entry._minutes_ttl_loop(
        enabled=False,
        interval=60,
        limit=100,
        session_factory=object(),
        object_storage=object(),
        redis_client=object(),
        runner=runner,
    )
    assert calls == []


@pytest.mark.asyncio
async def test_minutes_ttl_loop_runs_bounded_batches_when_enabled():
    calls = []
    now = datetime(2026, 7, 15, tzinfo=timezone.utc)

    async def runner(**kwargs):
        calls.append(kwargs)

    async def stop_after_tick(_interval):
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await entry._minutes_ttl_loop(
            enabled=True,
            interval=15,
            limit=23,
            session_factory="sessions",
            object_storage="storage",
            redis_client="redis",
            runner=runner,
            sleep=stop_after_tick,
            clock=lambda: now,
        )
    assert calls == [{
        "enabled": True,
        "now": now,
        "limit": 23,
        "session_factory": "sessions",
        "object_storage": "storage",
        "redis_client": "redis",
    }]


@pytest.mark.asyncio
async def test_minutes_ttl_loop_does_not_log_exception_content(caplog):
    marker = "meeting=41 object=s3://private/transcript provider-secret=leaked"

    async def runner(**_kwargs):
        raise RuntimeError(marker)

    async def stop_after_tick(_interval):
        raise asyncio.CancelledError

    with caplog.at_level("WARNING", logger="meeting_api.entrypoint"):
        with pytest.raises(asyncio.CancelledError):
            await entry._minutes_ttl_loop(
                enabled=True,
                interval=15,
                limit=23,
                session_factory="sessions",
                object_storage="storage",
                redis_client="redis",
                runner=runner,
                sleep=stop_after_tick,
            )

    assert marker not in caplog.text
    assert "Minutes retention TTL tick failed" in caplog.text
