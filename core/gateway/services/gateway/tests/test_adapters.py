"""Production gateway transport bounds that the in-process proxy fake cannot prove."""
import asyncio

import httpx
import pytest

from gateway.adapters import HttpxDownstreamClient, load_gateway_identity_secret
from gateway.ports import DownstreamBodyTooLarge


_VALID_GATEWAY_IDENTITY_SECRET = "gateway-identity-proof-0123456789abcdef"


def test_gateway_identity_secret_is_dedicated_and_strict():
    assert load_gateway_identity_secret({
        "GATEWAY_IDENTITY_SECRET": _VALID_GATEWAY_IDENTITY_SECRET,
        "INTERNAL_API_SECRET": "internal-service-secret-0123456789abcdef",
    }) == _VALID_GATEWAY_IDENTITY_SECRET


@pytest.mark.parametrize("value", [None, "", "too-short", " x" * 16, "x\n" + "y" * 31])
def test_gateway_identity_secret_rejects_missing_or_malformed_material(value):
    environment = {
        "INTERNAL_API_SECRET": "internal-service-secret-0123456789abcdef",
    }
    if value is not None:
        environment["GATEWAY_IDENTITY_SECRET"] = value

    with pytest.raises(RuntimeError, match="GATEWAY_IDENTITY_SECRET"):
        load_gateway_identity_secret(environment)


def test_gateway_identity_secret_rejects_internal_credential_alias():
    with pytest.raises(RuntimeError, match="distinct"):
        load_gateway_identity_secret({
            "GATEWAY_IDENTITY_SECRET": _VALID_GATEWAY_IDENTITY_SECRET,
            "INTERNAL_API_SECRET": _VALID_GATEWAY_IDENTITY_SECRET,
        })


class _MustNotRead(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.read = False

    async def __aiter__(self):
        self.read = True
        yield b"the declared size should reject before this chunk"


def test_buffered_downstream_rejects_declared_oversize_before_reading():
    stream = _MustNotRead()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Length": str(32 * 1024 * 1024 + 1)},
            stream=stream,
            request=request,
        )

    async def exercise() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            downstream = HttpxDownstreamClient(client)
            with pytest.raises(DownstreamBodyTooLarge, match="response too large"):
                await downstream.request("GET", "https://meeting.internal/meetings")

    asyncio.run(exercise())
    assert stream.read is False
