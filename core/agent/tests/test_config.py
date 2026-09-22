"""Config is a validated contract delivered by env (P14): boot from VEXA_*, fail fast, hide secrets."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from shared.spawn import build_worker_env
from shared.config import load_settings
from shared.config import Settings


def test_defaults_boot_clean():
    s = load_settings()
    assert s.agent_profile == "agent"
    assert s.workspace_path == "/workspace"
    assert s.is_secret_present() is False


def test_env_prefix_is_vexa(monkeypatch):
    monkeypatch.setenv("VEXA_AGENT_API_PORT", "9001")
    monkeypatch.setenv("VEXA_AGENT_IDENTITY_TOKEN", "scoped-jwt")
    s = Settings()
    assert s.agent_api_port == 9001
    assert s.is_secret_present() is True


def test_invalid_port_fails_fast(monkeypatch):
    monkeypatch.setenv("VEXA_AGENT_API_PORT", "999999")
    with pytest.raises(ValidationError):
        Settings()


def test_secret_is_not_in_repr(monkeypatch):
    monkeypatch.setenv("VEXA_AGENT_IDENTITY_TOKEN", "super-secret-value")
    s = Settings()
    assert "super-secret-value" not in repr(s)
    assert "super-secret-value" not in str(s)


def test_runtime_control_secret_is_operator_only_and_redacted(monkeypatch):
    monkeypatch.setenv("VEXA_RUNTIME_CONTROL_SECRET", "operator-control-secret")

    settings = Settings()
    worker_env = build_worker_env(
        settings,
        "https://git.example.com/acme/company-memory.git",
    )

    assert settings.runtime_control_secret.get_secret_value() == "operator-control-secret"
    assert "operator-control-secret" not in repr(settings)
    assert "operator-control-secret" not in str(settings)
    assert "VEXA_RUNTIME_CONTROL_SECRET" not in worker_env
    assert "RUNTIME_CONTROL_SECRET" not in worker_env


def test_minutes_read_config_uses_exact_default_off_env_and_hides_its_token(monkeypatch):
    monkeypatch.setenv("ZAKI_MINUTES_READ_ENABLED", "true")
    monkeypatch.setenv("ZAKI_MINUTES_READ_BASE_URL", "https://minutes.internal")
    monkeypatch.setenv("ZAKI_READ_TOKEN_MINUTES", "dedicated-minutes-secret")

    settings = Settings()

    assert settings.minutes_read_enabled is True
    assert settings.minutes_read_base_url == "https://minutes.internal"
    assert settings.minutes_read_token.get_secret_value() == "dedicated-minutes-secret"
    assert "dedicated-minutes-secret" not in repr(settings)
    assert "dedicated-minutes-secret" not in str(settings)


def test_gateway_identity_uses_exact_unprefixed_env_and_hides_its_secret(monkeypatch):
    secret = "gateway-identity-proof-0123456789abcdef"
    previous = "gateway-identity-previous-0123456789abcdef"
    monkeypatch.setenv("GATEWAY_IDENTITY_SECRET", secret)
    monkeypatch.setenv("GATEWAY_IDENTITY_PREVIOUS_SECRET", previous)

    settings = Settings()

    assert settings.gateway_identity_secret.get_secret_value() == secret
    assert settings.gateway_identity_previous_secret.get_secret_value() == previous
    assert secret not in repr(settings)
    assert secret not in str(settings)
    assert previous not in repr(settings)
    assert previous not in str(settings)


def test_agent_erasure_signer_uses_exact_operator_env_and_hides_its_secret(monkeypatch):
    monkeypatch.setenv("ZAKI_AGENT_ERASURE_SIGNING_KEY_ID", "agent-erasure-2026-07")
    monkeypatch.setenv(
        "ZAKI_AGENT_ERASURE_SIGNING_SECRET",
        "agent-erasure-current-secret-01234567890",
    )
    monkeypatch.setenv(
        "ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID",
        "agent-erasure-2026-06",
    )
    monkeypatch.setenv(
        "ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET",
        "agent-erasure-previous-secret-0123456789",
    )

    settings = Settings()

    assert settings.agent_erasure_signing_key_id == "agent-erasure-2026-07"
    assert (
        settings.agent_erasure_signing_secret.get_secret_value()
        == "agent-erasure-current-secret-01234567890"
    )
    assert settings.agent_erasure_previous_verification_key_id == "agent-erasure-2026-06"
    assert (
        settings.agent_erasure_previous_verification_secret.get_secret_value()
        == "agent-erasure-previous-secret-0123456789"
    )
    assert "agent-erasure-current-secret-01234567890" not in repr(settings)
    assert "agent-erasure-previous-secret-0123456789" not in repr(settings)
    assert "agent-erasure-current-secret-01234567890" not in str(settings)
    assert "agent-erasure-previous-secret-0123456789" not in str(settings)


def test_worker_env_matches_runtime_v1_agent_spec():
    """build_worker_env produces exactly the keys of golden runtime.v1/spec-agent.json (P8)."""
    s = load_settings(agent_identity_token="scoped-jwt-token", workspace_ref="main")
    env = build_worker_env(s, "https://git.example.com/acme/company-memory.git")
    assert set(env) == {
        "VEXA_AGENT_IDENTITY_TOKEN",
        "VEXA_WORKSPACE_REPO",
        "VEXA_WORKSPACE_REF",
        "VEXA_WORKSPACE_PATH",
    }
    assert env["VEXA_WORKSPACE_REPO"] == "https://git.example.com/acme/company-memory.git"
    assert env["VEXA_AGENT_IDENTITY_TOKEN"] == "scoped-jwt-token"


def test_worker_env_carries_configured_model():
    s = load_settings(agent_model="deepseek/deepseek-v4-pro")
    env = build_worker_env(s, "https://git.example.com/acme/company-memory.git")
    assert env["VEXA_AGENT_MODEL"] == "deepseek/deepseek-v4-pro"
