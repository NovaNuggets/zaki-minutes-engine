"""L2: the anthropic completion adapter against a fake transport — Messages-API request shape
(x-api-key, anthropic-version, max_tokens, system as top-level), text-block parsing, 401 taxonomy."""
import json

import httpx
import pytest

from llm import LLMAuthError, LLMConfigError, LLMError
from llm.anthropic_api import AnthropicCompletion

MAX_REQUEST = 4 * 1024 * 1024
MAX_RESPONSE = 4 * 1024 * 1024


class CountingStream(httpx.SyncByteStream):
    def __init__(self, chunks):
        self._chunks = chunks
        self.yielded = 0
        self.closed = False

    def __iter__(self):
        for chunk in self._chunks:
            self.yielded += 1
            yield chunk

    def close(self):
        self.closed = True


def _adapter(handler, **kw):
    kw.setdefault("api_key", "sk-ant-test")
    kw.setdefault("model", "some-model")
    return AnthropicCompletion(transport=httpx.MockTransport(handler), **kw)


def test_request_shape_and_parse(monkeypatch):
    monkeypatch.setenv("VEXA_LLM_MAX_TOKENS", "2048")
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["key"] = request.headers.get("x-api-key")
        seen["version"] = request.headers.get("anthropic-version")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"content": [{"type": "text", "text": "pol"},
                                                     {"type": "text", "text": "ished"}]})

    result = _adapter(handler).complete("clean", system="copilot")
    assert result.text == "polished"
    assert seen["url"] == "https://api.anthropic.com/v1/messages"  # default base
    assert seen["key"] == "sk-ant-test"
    assert seen["version"] == "2023-06-01"
    assert seen["body"]["max_tokens"] == 2048
    assert seen["body"]["system"] == "copilot"
    assert seen["body"]["messages"] == [{"role": "user", "content": "clean"}]


def test_per_call_token_ceiling_overrides_environment_and_invalid_values_never_egress(monkeypatch):
    monkeypatch.setenv("VEXA_LLM_MAX_TOKENS", "4096")
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={"content": [{"type": "text", "text": "ok"}]})

    adapter = _adapter(handler)
    assert adapter.complete("p", max_tokens=2048).text == "ok"
    assert calls[0]["max_tokens"] == 2048
    for invalid in (True, 0, -1, 1.5):
        with pytest.raises(LLMConfigError, match="max_tokens"):
            adapter.complete("p", max_tokens=invalid)
    assert len(calls) == 1


def test_401_raises_auth_error():
    handler = lambda request: httpx.Response(  # noqa: E731
        401,
        text="bad x-api-key: sk-ant-test",
    )
    with pytest.raises(LLMAuthError) as exc:
        _adapter(handler).complete("p")
    assert "sk-ant-test" not in str(exc.value)
    assert "bad x-api-key" not in str(exc.value)


def test_transport_failure_does_not_retain_the_credentialed_request():
    secret = "sk-ant-test"

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"failed x-api-key: {secret}", request=request)

    with pytest.raises(LLMError, match="transport") as raised:
        _adapter(handler).complete("private transcript")

    assert secret not in str(raised.value)
    assert raised.value.__cause__ is None


def test_missing_model_fails_loud(monkeypatch):
    monkeypatch.delenv("VEXA_LLM_MODEL", raising=False)
    with pytest.raises(LLMConfigError):
        AnthropicCompletion(model="").complete("p")


def test_request_is_byte_bounded_before_authorized_egress():
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"content": [{"type": "text", "text": "ok"}]})

    with pytest.raises(LLMError, match="request exceeds limit"):
        _adapter(handler).complete("x" * (MAX_REQUEST + 1))
    assert calls == 0


def test_error_status_does_not_read_or_reflect_untrusted_body():
    stream = CountingStream([b"echoed x-api-key: sk-ant-test"] * 100)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, stream=stream)

    with pytest.raises(LLMError) as exc:
        _adapter(handler).complete("p")
    assert stream.yielded == 0
    assert stream.closed is True
    assert "sk-ant-test" not in str(exc.value)


def test_redirect_is_not_followed_with_api_key_or_prompt():
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((str(request.url), request.headers.get("x-api-key"), request.content))
        return httpx.Response(307, headers={"Location": "https://sink.example/steal"})

    with pytest.raises(LLMError, match="HTTP 307"):
        _adapter(handler).complete("private transcript")
    assert len(requests) == 1
    assert requests[0][0] == "https://api.anthropic.com/v1/messages"


def test_success_response_is_bounded_before_json_decode_for_declared_and_chunked_bodies():
    def declared(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Length": str(MAX_RESPONSE + 1)},
            json={"content": [{"type": "text", "text": "must not parse"}]},
        )

    with pytest.raises(LLMError, match="response exceeds limit"):
        _adapter(declared).complete("p")

    stream = CountingStream([b" " * (64 * 1024)] * 100)

    def chunked(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    with pytest.raises(LLMError, match="response exceeds limit"):
        _adapter(chunked).complete("p")
    assert stream.yielded < 100
    assert stream.closed is True


def test_success_payload_bounds_content_fanout_and_output_text():
    def fanout(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "content": [{"type": "text", "text": "x"} for _ in range(1_001)],
        })

    with pytest.raises(LLMError, match="malformed completion payload"):
        _adapter(fanout).complete("p")

    def huge_output(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "content": [{"type": "text", "text": "x" * 1_000_001}],
        })

    with pytest.raises(LLMError, match="completion output exceeds limit"):
        _adapter(huge_output).complete("p")
