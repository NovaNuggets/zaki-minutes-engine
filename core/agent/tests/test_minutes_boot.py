"""Default-off production composition for the Agent-side Minutes read capability."""
from __future__ import annotations

import pytest

from control_plane.minutes_boot import build_minutes_ingestor
from shared.config import load_settings


_READ_TOKEN = "minutes-read-token-0123456789abcdef"
_LEAK_TOKEN = "do-not-leak-this-token-0123456789abcdef"
_INTERNAL_SECRET = "identity-secret-0123456789abcdef"
_ERASURE_SECRET = "agent-erasure-secret-0123456789abcdef"


class _Authorizer:
    def authorize_answer_if_not_erased(self, **_kwargs):
        raise AssertionError("boot composition must not authorize an answer")


class _Completion:
    name = "bounded-http"
    supports_max_tokens = True

    def complete(self, *_args, **_kwargs):
        raise AssertionError("boot composition must not make a model call")


def test_default_off_composition_touches_no_clients_or_completion():
    class ForbiddenFactory:
        def __call__(self, *_args, **_kwargs):
            raise AssertionError("flag-off Minutes must not initialize dependencies")

    assert build_minutes_ingestor(
        load_settings(),
        authorizer=_Authorizer(),
        completion_factory=ForbiddenFactory(),
        read_factory=ForbiddenFactory(),
        identity_factory=ForbiddenFactory(),
        ownership_factory=ForbiddenFactory(),
    ) is None


def test_enabled_boot_reports_the_exact_missing_answer_authorizer_gap():
    settings = load_settings(
        minutes_read_enabled=True,
        minutes_read_base_url="https://minutes.internal",
        minutes_read_token=_LEAK_TOKEN,
        admin_api_url="https://identity.internal",
        internal_api_secret="internal-secret",
    )

    with pytest.raises(
        RuntimeError,
        match="ZAKI_MINUTES_READ_ENABLED requires a tombstone-aware Agent Brain answer authorizer",
    ) as caught:
        build_minutes_ingestor(settings, authorizer=None)

    assert _LEAK_TOKEN not in str(caught.value)


def test_enabled_boot_rejects_an_object_without_the_shared_answer_authorization_gate():
    class LegacyWriteOnly:
        def write_if_not_erased(self, **_kwargs):
            return 0

    settings = load_settings(
        minutes_read_enabled=True,
        minutes_read_base_url="https://minutes.internal",
        minutes_read_token=_LEAK_TOKEN,
        admin_api_url="https://identity.internal",
        internal_api_secret="internal-secret",
    )

    with pytest.raises(
        RuntimeError,
        match="tombstone-aware Agent Brain answer authorizer",
    ) as caught:
        build_minutes_ingestor(settings, authorizer=LegacyWriteOnly())

    assert _LEAK_TOKEN not in str(caught.value)


@pytest.mark.parametrize(
    "weak_token",
    [
        "too-short",
        " " + _READ_TOKEN,
        _READ_TOKEN + " ",
        "a" * 16 + "\n" + "b" * 16,
        "a" * 16 + "\x00" + "b" * 16,
        "a" * 16 + "\x7f" + "b" * 16,
        "é" * 32,
        "a" * 513,
    ],
)
def test_enabled_boot_rejects_read_tokens_the_producer_cannot_compose(weak_token):
    class ForbiddenFactory:
        def __call__(self, *_args, **_kwargs):
            raise AssertionError("weak token must fail before dependency composition")

    settings = load_settings(
        minutes_read_enabled=True,
        minutes_read_base_url="https://minutes.internal",
        minutes_read_token=weak_token,
        admin_api_url="https://identity.internal",
        internal_api_secret="internal-secret",
    )

    with pytest.raises(RuntimeError, match="unpadded printable ASCII between 32 and 512"):
        build_minutes_ingestor(
            settings,
            authorizer=_Authorizer(),
            completion_factory=ForbiddenFactory(),
            read_factory=ForbiddenFactory(),
            identity_factory=ForbiddenFactory(),
            ownership_factory=ForbiddenFactory(),
        )


def test_production_app_refuses_enabled_minutes_before_runtime_composition(monkeypatch):
    from control_plane.api import _build_production_app

    monkeypatch.setenv("ZAKI_MINUTES_CAPTURE_ENABLED", "false")
    monkeypatch.setenv("ZAKI_MINUTES_READ_ENABLED", "true")
    monkeypatch.setenv("ZAKI_MINUTES_READ_BASE_URL", "https://minutes.internal")
    monkeypatch.setenv("ZAKI_READ_TOKEN_MINUTES", _LEAK_TOKEN)
    monkeypatch.setenv("VEXA_ADMIN_API_URL", "https://identity.internal")
    monkeypatch.setenv("VEXA_INTERNAL_API_SECRET", "internal-secret")
    monkeypatch.setenv("VEXA_RUNTIME_CONTROL_SECRET", "runtime-control-secret")
    monkeypatch.setenv(
        "GATEWAY_IDENTITY_SECRET",
        "test-gateway-proof-0123456789abcdef",
    )

    with pytest.raises(
        RuntimeError,
        match="ZAKI_MINUTES_READ_ENABLED requires a tombstone-aware Agent Brain answer authorizer",
    ) as caught:
        _build_production_app()

    assert _LEAK_TOKEN not in str(caught.value)


def test_enabled_composition_builds_fixed_origin_clients_only_after_all_gates_exist():
    seen = {}

    def read_factory(base_url, token):
        seen["read"] = (base_url, token)
        return object()

    def identity_factory(base_url, secret):
        seen["identity"] = (base_url, secret)
        return object()

    def ownership_factory(redis_url):
        seen["ownership"] = redis_url
        return object()

    settings = load_settings(
        minutes_read_enabled=True,
        minutes_read_base_url="https://minutes.internal",
        minutes_read_token=_READ_TOKEN,
        admin_api_url="https://identity.internal",
        internal_api_secret="identity-secret",
    )

    ingestor = build_minutes_ingestor(
        settings,
        authorizer=_Authorizer(),
        completion_factory=lambda: _Completion(),
        read_factory=read_factory,
        identity_factory=identity_factory,
        ownership_factory=ownership_factory,
    )

    assert ingestor is not None
    assert seen == {
        "read": ("https://minutes.internal", _READ_TOKEN),
        "identity": ("https://identity.internal", "identity-secret"),
        "ownership": "redis://redis:6379/0",
    }


def test_enabled_boot_rejects_a_completion_adapter_without_a_token_ceiling():
    class UnboundedCompletion:
        name = "unbounded"
        supports_max_tokens = False

    settings = load_settings(
        minutes_read_enabled=True,
        minutes_read_base_url="https://minutes.internal",
        minutes_read_token=_READ_TOKEN,
        admin_api_url="https://identity.internal",
        internal_api_secret="identity-secret",
    )

    with pytest.raises(RuntimeError, match="token ceiling"):
        build_minutes_ingestor(
            settings,
            authorizer=_Authorizer(),
            completion_factory=lambda: UnboundedCompletion(),
            read_factory=lambda *_args: object(),
            identity_factory=lambda *_args: object(),
            ownership_factory=lambda *_args: object(),
        )


@pytest.mark.parametrize("collision", [_INTERNAL_SECRET, _ERASURE_SECRET])
def test_enabled_read_token_must_be_dedicated_from_internal_and_erasure_keys(collision):
    settings = load_settings(
        minutes_read_enabled=True,
        minutes_read_base_url="https://minutes.internal",
        minutes_read_token=collision,
        admin_api_url="https://identity.internal",
        internal_api_secret=_INTERNAL_SECRET,
        agent_erasure_signing_key_id="agent-erasure-2026-07",
        agent_erasure_signing_secret=_ERASURE_SECRET,
    )

    with pytest.raises(RuntimeError, match="ZAKI_READ_TOKEN_MINUTES must be distinct"):
        build_minutes_ingestor(
            settings,
            authorizer=_Authorizer(),
            completion_factory=lambda: _Completion(),
            read_factory=lambda *_args: object(),
            identity_factory=lambda *_args: object(),
            ownership_factory=lambda *_args: object(),
        )
