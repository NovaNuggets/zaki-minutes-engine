"""openai_compat.py — the DEFAULT CompletionPort adapter: any OpenAI-compatible endpoint.

One dialect covers nearly every provider — OpenRouter, Ollama, vLLM, LM Studio, OpenAI itself, and
most gateways all speak ``POST {base}/chat/completions``. Raw httpx, no vendor SDK: the request is
~10 lines and a pinned SDK would be a heavier supply-chain surface than the protocol itself.

Config (constructor args win over env): ``VEXA_LLM_BASE_URL`` (required — e.g.
``https://openrouter.ai/api/v1``, ``http://ollama:11434/v1``; falls back to ``ANTHROPIC_BASE_URL``
for deployments that already point one at a multi-protocol gateway), ``VEXA_LLM_API_KEY`` (falls
back ``ANTHROPIC_AUTH_TOKEN`` → ``ANTHROPIC_API_KEY``; optional — local runtimes need none),
``VEXA_LLM_MODEL`` (the deployment-default model).
"""
from __future__ import annotations

import os
from typing import Optional

import httpx

from llm.errors import LLMAuthError, LLMConfigError, LLMError
from llm.http_safety import (
    MAX_LLM_OUTPUT_CHARS,
    encode_request,
    read_response_json,
)
from llm.ports import CompletionResult


class OpenAICompatCompletion:
    name = "openai-compat"
    supports_max_tokens = True

    def __init__(self, *, base_url: Optional[str] = None, api_key: Optional[str] = None,
                 model: Optional[str] = None, timeout: float = 120.0,
                 transport: Optional[httpx.BaseTransport] = None) -> None:
        self._base = (base_url or os.environ.get("VEXA_LLM_BASE_URL")
                      or os.environ.get("ANTHROPIC_BASE_URL") or "").rstrip("/")
        self._key = (api_key or os.environ.get("VEXA_LLM_API_KEY")
                     or os.environ.get("ANTHROPIC_AUTH_TOKEN")
                     or os.environ.get("ANTHROPIC_API_KEY") or "")
        self._model = model or os.environ.get("VEXA_LLM_MODEL") or ""
        self._client = httpx.Client(
            timeout=timeout,
            transport=transport,
            follow_redirects=False,
        )

    def complete(self, prompt: str, *, system: Optional[str] = None,
                 model: Optional[str] = None,
                 max_tokens: Optional[int] = None) -> CompletionResult:
        target = (model or "").strip() or self._model
        if not self._base:
            raise LLMConfigError(
                "no completion endpoint: set VEXA_LLM_BASE_URL (e.g. https://openrouter.ai/api/v1, "
                "http://ollama:11434/v1) — the openai-compat provider has no default host"
            )
        if not target:
            raise LLMConfigError(
                "no model: set VEXA_LLM_MODEL (deployment default) or a model in the workspace's "
                "agents/meeting.md"
            )
        messages = ([{"role": "system", "content": system}] if system else [])
        messages.append({"role": "user", "content": prompt})
        headers = {"Authorization": f"Bearer {self._key}"} if self._key else {}
        headers["Content-Type"] = "application/json"
        payload: dict = {"model": target, "messages": messages}
        if max_tokens is not None:
            if isinstance(max_tokens, bool) or not isinstance(max_tokens, int) or max_tokens <= 0:
                raise LLMConfigError("max_tokens must be a positive integer")
            payload["max_tokens"] = max_tokens
        body = encode_request(payload)
        try:
            with self._client.stream(
                "POST",
                f"{self._base}/chat/completions",
                content=body,
                headers=headers,
                follow_redirects=False,
            ) as r:
                if r.status_code in (401, 403):
                    raise LLMAuthError(f"completion endpoint rejected credentials (HTTP {r.status_code})")
                if not 200 <= r.status_code < 300:
                    raise LLMError(f"completion endpoint answered HTTP {r.status_code}")
                data = read_response_json(r)
        except (LLMAuthError, LLMError):
            raise
        except httpx.HTTPError:
            # HTTPX retains the prompt- and credential-bearing Request on transport failures.
            raise LLMError("completion transport failure") from None

        if not isinstance(data, dict):
            raise LLMError("malformed completion payload")
        choices = data.get("choices")
        if not isinstance(choices, list) or not choices or len(choices) > 100:
            raise LLMError("malformed completion payload")
        choice = choices[0]
        if not isinstance(choice, dict) or not isinstance(choice.get("message"), dict):
            raise LLMError("malformed completion payload")
        text = choice["message"].get("content")
        if not isinstance(text, str):
            raise LLMError("malformed completion payload")
        if len(text) > MAX_LLM_OUTPUT_CHARS:
            raise LLMError("completion output exceeds limit")
        return CompletionResult(text=text, model=target)
