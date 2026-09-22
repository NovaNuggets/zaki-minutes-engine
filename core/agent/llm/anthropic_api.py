"""anthropic_api.py — CompletionPort adapter for the Anthropic Messages API (and compatibles).

For deployments that point completions directly at ``api.anthropic.com`` — or at any endpoint
speaking the Messages dialect (LiteLLM proxies, DeepSeek/GLM/Kimi Anthropic-compatible endpoints).
Raw httpx, no vendor SDK (same doctrine as ``openai_compat``). Named ``anthropic_api`` to never
shadow the ``anthropic`` pip package.

Config (constructor args win over env): ``VEXA_LLM_BASE_URL`` (default ``https://api.anthropic.com``),
``VEXA_LLM_API_KEY`` (falls back ``ANTHROPIC_AUTH_TOKEN`` → ``ANTHROPIC_API_KEY``),
``VEXA_LLM_MODEL``, ``VEXA_LLM_MAX_TOKENS`` (the Messages API requires max_tokens; default 4096).
"""
from __future__ import annotations

import os
from typing import Optional

import httpx

from llm.errors import LLMAuthError, LLMConfigError, LLMError
from llm.http_safety import (
    MAX_LLM_OUTPUT_CHARS,
    MAX_LLM_RESPONSE_ITEMS,
    encode_request,
    read_response_json,
)
from llm.ports import CompletionResult

_DEFAULT_BASE = "https://api.anthropic.com"
_API_VERSION = "2023-06-01"


def _max_tokens() -> int:
    try:
        return int(os.environ.get("VEXA_LLM_MAX_TOKENS", "4096"))
    except ValueError:
        return 4096


class AnthropicCompletion:
    name = "anthropic"
    supports_max_tokens = True

    def __init__(self, *, base_url: Optional[str] = None, api_key: Optional[str] = None,
                 model: Optional[str] = None, timeout: float = 120.0,
                 transport: Optional[httpx.BaseTransport] = None) -> None:
        self._base = (base_url or os.environ.get("VEXA_LLM_BASE_URL")
                      or _DEFAULT_BASE).rstrip("/")
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
        if not target:
            raise LLMConfigError(
                "no model: set VEXA_LLM_MODEL (deployment default) or a model in the workspace's "
                "agents/meeting.md"
            )
        payload: dict = {
            "model": target,
            "max_tokens": max_tokens if max_tokens is not None else _max_tokens(),
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            payload["system"] = system
        if (
            isinstance(payload["max_tokens"], bool)
            or not isinstance(payload["max_tokens"], int)
            or payload["max_tokens"] <= 0
        ):
            raise LLMConfigError("max_tokens must be a positive integer")
        headers = {
            "x-api-key": self._key,
            "anthropic-version": _API_VERSION,
            "Content-Type": "application/json",
        }
        body = encode_request(payload)
        try:
            with self._client.stream(
                "POST",
                f"{self._base}/v1/messages",
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
        blocks = data.get("content")
        if not isinstance(blocks, list) or len(blocks) > MAX_LLM_RESPONSE_ITEMS:
            raise LLMError("malformed completion payload")
        pieces: list[str] = []
        total_chars = 0
        for block in blocks:
            if not isinstance(block, dict) or not isinstance(block.get("type"), str):
                raise LLMError("malformed completion payload")
            if block["type"] != "text":
                continue
            text = block.get("text")
            if not isinstance(text, str):
                raise LLMError("malformed completion payload")
            total_chars += len(text)
            if total_chars > MAX_LLM_OUTPUT_CHARS:
                raise LLMError("completion output exceeds limit")
            pieces.append(text)
        return CompletionResult(text="".join(pieces), model=target)
