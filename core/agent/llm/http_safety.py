"""Shared byte ceilings for the two credentialed HTTP completion adapters."""
from __future__ import annotations

import json

from llm.errors import LLMError

MAX_LLM_REQUEST_BYTES = 4 * 1024 * 1024
MAX_LLM_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_LLM_OUTPUT_CHARS = 1_000_000
MAX_LLM_RESPONSE_ITEMS = 1_000


def encode_request(payload: dict) -> bytes:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(body) > MAX_LLM_REQUEST_BYTES:
        raise LLMError("completion request exceeds limit")
    return body


def read_response_json(response) -> object:
    """Stream a decoded HTTP response up to the ceiling, then parse JSON.

    ``iter_bytes`` also bounds decompressed content, so a small compressed transfer cannot expand
    without limit. Callers check status before invoking this helper and therefore never read error
    bodies that may echo credentials.
    """
    declared = response.headers.get("Content-Length")
    if declared is not None:
        try:
            declared_length = int(declared)
        except (TypeError, ValueError):
            declared_length = None
        if declared_length is not None and declared_length > MAX_LLM_RESPONSE_BYTES:
            raise LLMError("completion response exceeds limit")

    chunks: list[bytes] = []
    total = 0
    for chunk in response.iter_bytes():
        total += len(chunk)
        if total > MAX_LLM_RESPONSE_BYTES:
            raise LLMError("completion response exceeds limit")
        chunks.append(chunk)
    try:
        return json.loads(b"".join(chunks))
    except (UnicodeDecodeError, ValueError) as exc:
        raise LLMError("malformed completion payload") from exc
