"""L2: the openai-compat completion adapter against a fake transport — request shape (URL, auth
header, messages), response parsing, and the error taxonomy (401→LLMAuthError, 5xx→LLMError,
missing config→LLMConfigError). No network."""
import json

import httpx
import pytest

from llm import LLMAuthError, LLMConfigError, LLMError
from llm.openai_compat import OpenAICompatCompletion

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
    kw.setdefault("base_url", "https://llm.example/v1")
    kw.setdefault("api_key", "sk-test")
    kw.setdefault("model", "some-model")
    return OpenAICompatCompletion(transport=httpx.MockTransport(handler), **kw)


def test_request_shape_and_parse():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"choices": [{"message": {"content": "polished"}}]})

    result = _adapter(handler).complete("clean these lines", system="you are a copilot")
    assert result.text == "polished"
    assert result.model == "some-model"
    assert seen["url"] == "https://llm.example/v1/chat/completions"
    assert seen["auth"] == "Bearer sk-test"
    assert seen["body"]["model"] == "some-model"
    assert seen["body"]["messages"][0] == {"role": "system", "content": "you are a copilot"}
    assert seen["body"]["messages"][1] == {"role": "user", "content": "clean these lines"}


def test_per_call_model_overrides_default():
    def handler(request: httpx.Request) -> httpx.Response:
        assert json.loads(request.content)["model"] == "beat-model"
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    assert _adapter(handler).complete("p", model="beat-model").model == "beat-model"


def test_per_call_token_ceiling_is_sent_and_invalid_values_never_egress():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    adapter = _adapter(handler)
    assert adapter.complete("p", max_tokens=2048).text == "ok"
    assert calls == [{
        "model": "some-model",
        "messages": [{"role": "user", "content": "p"}],
        "max_tokens": 2048,
    }]
    for invalid in (True, 0, -1, 1.5):
        with pytest.raises(LLMConfigError, match="max_tokens"):
            adapter.complete("p", max_tokens=invalid)
    assert len(calls) == 1


def test_no_key_means_no_auth_header(monkeypatch):
    for var in ("VEXA_LLM_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY"):
        monkeypatch.delenv(var, raising=False)

    def handler(request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers  # local runtimes (ollama) need no key
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    adapter = OpenAICompatCompletion(base_url="http://ollama:11434/v1", api_key="",
                                     model="local", transport=httpx.MockTransport(handler))
    assert adapter.complete("p").text == "ok"


def test_401_raises_auth_error():
    handler = lambda request: httpx.Response(  # noqa: E731
        401,
        text="bad Authorization: Bearer sk-test",
    )
    with pytest.raises(LLMAuthError) as exc:
        _adapter(handler).complete("p")
    assert "401" in str(exc.value)
    assert "sk-test" not in str(exc.value)
    assert "bad Authorization" not in str(exc.value)


def test_5xx_raises_llm_error():
    handler = lambda request: httpx.Response(503, text="overloaded")  # noqa: E731
    with pytest.raises(LLMError):
        _adapter(handler).complete("p")


def test_transport_failure_does_not_retain_the_credentialed_request():
    secret = "sk-test"

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(
            f"failed Authorization: Bearer {secret}", request=request,
        )

    with pytest.raises(LLMError, match="transport") as raised:
        _adapter(handler).complete("private transcript")

    assert secret not in str(raised.value)
    assert raised.value.__cause__ is None


def test_missing_base_url_fails_loud(monkeypatch):
    for var in ("VEXA_LLM_BASE_URL", "ANTHROPIC_BASE_URL"):
        monkeypatch.delenv(var, raising=False)
    adapter = OpenAICompatCompletion(base_url="", model="m")
    with pytest.raises(LLMConfigError) as exc:
        adapter.complete("p")
    assert "VEXA_LLM_BASE_URL" in str(exc.value)


def test_missing_model_fails_loud(monkeypatch):
    monkeypatch.delenv("VEXA_LLM_MODEL", raising=False)
    adapter = OpenAICompatCompletion(base_url="https://llm.example/v1", model="")
    with pytest.raises(LLMConfigError) as exc:
        adapter.complete("p")
    assert "VEXA_LLM_MODEL" in str(exc.value)


def test_request_is_byte_bounded_before_authorized_egress():
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    with pytest.raises(LLMError, match="request exceeds limit"):
        _adapter(handler).complete("x" * (MAX_REQUEST + 1))
    assert calls == 0


def test_error_status_does_not_read_or_reflect_untrusted_body():
    stream = CountingStream([b"echoed Bearer sk-test"] * 100)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, stream=stream)

    with pytest.raises(LLMError) as exc:
        _adapter(handler).complete("p")
    assert stream.yielded == 0
    assert stream.closed is True
    assert "sk-test" not in str(exc.value)


def test_redirect_is_not_followed_with_authorization_or_prompt():
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((str(request.url), request.headers.get("authorization"), request.content))
        return httpx.Response(307, headers={"Location": "https://sink.example/steal"})

    with pytest.raises(LLMError, match="HTTP 307"):
        _adapter(handler).complete("private transcript")
    assert len(requests) == 1
    assert requests[0][0] == "https://llm.example/v1/chat/completions"


def test_success_response_is_bounded_before_json_decode_for_declared_and_chunked_bodies():
    def declared(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Length": str(MAX_RESPONSE + 1)},
            json={"choices": [{"message": {"content": "must not parse"}}]},
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


def test_success_payload_bounds_choice_fanout_and_output_text():
    def fanout(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "choices": [{"message": {"content": "x"}} for _ in range(101)],
        })

    with pytest.raises(LLMError, match="malformed completion payload"):
        _adapter(fanout).complete("p")

    def huge_output(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "choices": [{"message": {"content": "x" * 1_000_001}}],
        })

    with pytest.raises(LLMError, match="completion output exceeds limit"):
        _adapter(huge_output).complete("p")
