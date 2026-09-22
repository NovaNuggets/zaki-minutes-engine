"""Identity-owned Minutes settings client: bounded, redirect-free internal lookup."""
from __future__ import annotations

import json

import httpx
import pytest

from meeting_api.minutes_settings import IdentityMinutesSettings


@pytest.mark.asyncio
async def test_settings_lookup_is_internal_secret_authenticated_and_bounded():
    seen = {}

    def handler(request: httpx.Request):
        seen["request"] = request
        return httpx.Response(200, json={
            "capture_enabled": True,
            "agent_read_enabled": False,
            "policy_version": "minutes-capture.v1",
        })

    lookup = IdentityMinutesSettings(
        "http://admin-api:8080", "internal-secret",
        transport=httpx.MockTransport(handler),
    )
    assert await lookup(7) == {
        "capture_enabled": True,
        "agent_read_enabled": False,
        "policy_version": "minutes-capture.v1",
    }
    assert seen["request"].url.path == "/internal/users/7/minutes"
    assert seen["request"].headers["x-internal-secret"] == "internal-secret"


@pytest.mark.asyncio
async def test_settings_lookup_rejects_redirect_without_forwarding_secret():
    requests = []

    def handler(request: httpx.Request):
        requests.append(request)
        return httpx.Response(302, headers={"location": "http://attacker.invalid/steal"})

    lookup = IdentityMinutesSettings(
        "http://admin-api:8080", "internal-secret",
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(RuntimeError, match="redirect"):
        await lookup(7)
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_settings_lookup_enforces_cap_before_and_while_streaming():
    declared = IdentityMinutesSettings(
        "http://admin-api:8080", "secret",
        transport=httpx.MockTransport(lambda _request: httpx.Response(
            200, headers={"content-length": "20000"}, content=b"{}",
        )),
    )
    with pytest.raises(RuntimeError, match="too large"):
        await declared(7)

    streamed = IdentityMinutesSettings(
        "http://admin-api:8080", "secret",
        transport=httpx.MockTransport(lambda _request: httpx.Response(
            200, content=json.dumps({"padding": "x" * 20_000}).encode(),
        )),
    )
    with pytest.raises(RuntimeError, match="too large"):
        await streamed(7)


@pytest.mark.asyncio
async def test_settings_lookup_maps_unknown_user_to_none_and_fails_other_statuses():
    missing = IdentityMinutesSettings(
        "http://admin-api:8080", "secret",
        transport=httpx.MockTransport(lambda _request: httpx.Response(404)),
    )
    assert await missing(7) is None

    unavailable = IdentityMinutesSettings(
        "http://admin-api:8080", "secret",
        transport=httpx.MockTransport(lambda _request: httpx.Response(503)),
    )
    with pytest.raises(RuntimeError, match="unavailable"):
        await unavailable(7)


@pytest.mark.asyncio
async def test_settings_transport_failure_does_not_retain_the_internal_secret():
    secret = "internal-secret"

    def leaked_request(request: httpx.Request):
        raise httpx.ConnectError(
            f"failed request carried X-Internal-Secret: {secret}", request=request,
        )

    lookup = IdentityMinutesSettings(
        "http://admin-api:8080", secret, transport=httpx.MockTransport(leaked_request),
    )

    with pytest.raises(RuntimeError, match="transport") as raised:
        await lookup(7)

    assert secret not in str(raised.value)
    assert raised.value.__cause__ is None


def test_settings_lookup_rejects_ambiguous_service_urls_and_missing_secret():
    for url in (
        "file:///tmp/admin",
        "http://user:pass@admin-api:8080",
        "http://admin-api:8080/base?token=secret",
    ):
        with pytest.raises(ValueError, match="service URL"):
            IdentityMinutesSettings(url, "secret")
    with pytest.raises(ValueError, match="internal secret"):
        IdentityMinutesSettings("http://admin-api:8080", "")
    for timeout in (0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="timeout"):
            IdentityMinutesSettings(
                "http://admin-api:8080", "secret", timeout_seconds=timeout
            )
