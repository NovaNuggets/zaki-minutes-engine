"""Canonical public meeting identity validation shared by regular and managed capture."""
from __future__ import annotations

import re


SUPPORTED_PLATFORMS = frozenset({"google_meet", "teams", "zoom", "jitsi"})
_NATIVE_MEETING_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@~-]{0,255}$")


def validate_platform(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("platform is invalid")
    platform = value.strip()
    if platform not in SUPPORTED_PLATFORMS:
        raise ValueError("platform is invalid")
    return platform


def validate_native_meeting_id(value: object, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise ValueError("native_meeting_id is invalid")
    native = value.strip()
    if allow_empty and not native:
        return ""
    if not _NATIVE_MEETING_ID.fullmatch(native):
        raise ValueError("native_meeting_id is invalid")
    return native
