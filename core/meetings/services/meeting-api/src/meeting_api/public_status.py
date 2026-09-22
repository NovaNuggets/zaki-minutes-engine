"""Translate internal lifecycle vocabulary at user-facing API/event boundaries."""
from __future__ import annotations

from typing import Any, Optional


_INTERNAL_TO_PUBLIC = {"needs_help": "needs_human_help"}
_PUBLIC_TO_INTERNAL = {value: key for key, value in _INTERNAL_TO_PUBLIC.items()}


def public_meeting_status(status: Optional[str]) -> Optional[str]:
    """Return the canonical api.v1 product status without mutating persistence."""
    return _INTERNAL_TO_PUBLIC.get(status, status)


def internal_meeting_status(status: Optional[str]) -> Optional[str]:
    """Translate a public status filter back to the lifecycle/database vocabulary."""
    return _PUBLIC_TO_INTERNAL.get(status, status)


def public_meeting_projection(value: Any) -> Any:
    """Shallow-copy a meeting/transcript response and translate its top-level status."""
    if not isinstance(value, dict):
        return value
    projected = dict(value)
    if "status" in projected:
        projected["status"] = public_meeting_status(projected.get("status"))
    return projected

