"""Fail-closed Agent consumer of Identity's effective Minutes settings."""
from __future__ import annotations

import io
import json

import pytest

import shared.minutes_settings as minutes_settings
from shared.minutes_settings import IdentityMinutesSettingsClient


class _Response(io.BytesIO):
    def __init__(self, body: object, *, status: int = 200, headers: dict | None = None) -> None:
        raw = json.dumps(body, separators=(",", ":")).encode()
        super().__init__(raw)
        self.status = status
        self.headers = {"Content-Length": str(len(raw)), **(headers or {})}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def test_identity_enables_minutes_only_when_both_effective_flags_are_exact_true():
    requests = []

    def open_request(request, *, timeout):
        requests.append((request, timeout))
        return _Response({"read_operator_enabled": True, "agent_read_enabled": True})

    enabled = IdentityMinutesSettingsClient(
        "https://identity.internal:8443",
        "internal-secret",
        open_fn=open_request,
    ).is_enabled(7)

    assert enabled is True
    assert len(requests) == 1
    request, timeout = requests[0]
    assert request.full_url == "https://identity.internal:8443/internal/users/7/minutes"
    assert request.get_header("X-internal-secret") == "internal-secret"
    assert timeout == 5.0


@pytest.mark.parametrize("body", [
    {"read_operator_enabled": False, "agent_read_enabled": True},
    {"read_operator_enabled": True, "agent_read_enabled": False},
    {"read_operator_enabled": 1, "agent_read_enabled": True},
    {"read_operator_enabled": True, "agent_read_enabled": "true"},
    {"agent_read_enabled": True},
    [True, True],
])
def test_identity_rejects_missing_disabled_or_coercible_flags(body):
    client = IdentityMinutesSettingsClient(
        "http://admin-api:8001", "internal-secret",
        open_fn=lambda _request, *, timeout: _Response(body),
    )

    assert client.is_enabled(7) is False


def test_identity_redirect_is_fail_closed_and_never_retried_with_the_secret():
    requests = []

    def redirect(request, *, timeout):
        requests.append(request)
        return _Response({}, status=302, headers={"Location": "https://attacker.invalid/steal"})

    client = IdentityMinutesSettingsClient(
        "http://admin-api:8001", "internal-secret", open_fn=redirect,
    )

    assert client.is_enabled(7) is False
    assert len(requests) == 1
    assert requests[0].full_url.startswith("http://admin-api:8001/")


def test_identity_oversize_response_is_fail_closed():
    response = _Response({"read_operator_enabled": True, "agent_read_enabled": True})
    # urllib's real Message headers are case-insensitive; the shared helper requests this spelling.
    response.headers["content-length"] = str(16 * 1024 + 1)
    client = IdentityMinutesSettingsClient(
        "http://admin-api:8001", "internal-secret",
        open_fn=lambda _request, *, timeout: response,
    )

    assert client.is_enabled(7) is False


def test_identity_request_construction_failure_is_fail_closed(monkeypatch):
    marker = "internal-secret-and-private-user-id"

    def fail_request(*_args, **_kwargs):
        raise RuntimeError(marker)

    monkeypatch.setattr(minutes_settings.urllib.request, "Request", fail_request)
    client = IdentityMinutesSettingsClient(
        "http://admin-api:8001", "internal-secret",
        open_fn=lambda _request, *, timeout: pytest.fail("transport must not run"),
    )

    assert client.is_enabled(7) is False
