"""HTTP transport helpers for origin-bound agent credentials."""
from __future__ import annotations

import json
import urllib.request
from typing import Any


MAX_INTERNAL_JSON_BYTES = 1024 * 1024
MAX_INTERNAL_REQUEST_BYTES = 1024 * 1024


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Return redirects to the caller instead of replaying a request at another URL."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirectHandler())


def open_no_redirect(request: urllib.request.Request, *, timeout: float):
    """Open exactly ``request.full_url`` without following an HTTP redirect."""

    return _NO_REDIRECT_OPENER.open(request, timeout=timeout)


def read_bounded(response, *, max_bytes: int = MAX_INTERNAL_JSON_BYTES) -> bytes:
    """Read at most ``max_bytes`` from an HTTP response, including chunked responses."""
    declared = response.headers.get("content-length")
    if declared is not None:
        try:
            declared_length = int(declared)
        except (TypeError, ValueError) as error:
            raise ValueError("invalid response content length") from error
        if declared_length < 0 or declared_length > max_bytes:
            raise ValueError("response is too large")

    body = response.read(max_bytes + 1)
    if len(body) > max_bytes:
        raise ValueError("response is too large")
    return body


def read_json_bounded(response, *, max_bytes: int = MAX_INTERNAL_JSON_BYTES) -> Any:
    """Parse bounded JSON without reflecting attacker-controlled decoder details."""
    try:
        return json.loads(read_bounded(response, max_bytes=max_bytes))
    except json.JSONDecodeError:
        raise ValueError("invalid JSON response") from None


def encode_json_bounded(value: Any, *, max_bytes: int = MAX_INTERNAL_REQUEST_BYTES) -> bytes:
    """Serialize one internal JSON request with a deterministic pre-egress byte ceiling."""
    try:
        body = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise ValueError("invalid JSON request") from None
    if len(body) > max_bytes:
        raise ValueError("request is too large")
    return body
