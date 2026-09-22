"""Fail-closed client for Identity's effective per-user Minutes read setting."""
from __future__ import annotations

import json
import math
import re
import urllib.request
from typing import Callable

from shared.http import open_no_redirect, read_bounded
from shared.minutes_read import validate_service_origin


MAX_SETTINGS_BYTES = 16 * 1024
_USER_ID = re.compile(r"^[1-9][0-9]{0,18}$")
_MAX_DB_ID = 2**63 - 1


class IdentityMinutesSettingsClient:
    """Return enabled only for an authoritative literal-boolean dual gate.

    Every transport, status, size, and shape error returns ``False``.  Identity is an access
    authority here, so availability must never become permission.
    """

    def __init__(
        self,
        base_url: str,
        internal_secret: str,
        *,
        timeout: float = 5.0,
        open_fn: Callable = open_no_redirect,
    ) -> None:
        self._origin = validate_service_origin(base_url)
        if not isinstance(internal_secret, str) or not internal_secret:
            raise ValueError("Minutes settings requires an internal secret")
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("Minutes settings timeout must be positive")
        self._secret = internal_secret
        self._timeout = float(timeout)
        self._open = open_fn

    def is_enabled(self, user_id: str | int) -> bool:
        subject = str(user_id)
        if not _USER_ID.fullmatch(subject) or int(subject) > _MAX_DB_ID:
            return False
        try:
            request = urllib.request.Request(
                f"{self._origin}/internal/users/{subject}/minutes",
                headers={"Accept": "application/json", "X-Internal-Secret": self._secret},
                method="GET",
            )
            with self._open(request, timeout=self._timeout) as response:
                status_value = getattr(response, "status", None)
                status = int(status_value if status_value is not None else response.getcode())
                if status != 200:
                    return False
                raw = read_bounded(response, max_bytes=MAX_SETTINGS_BYTES)
            body = json.loads(raw)
        except Exception:
            return False
        return (
            isinstance(body, dict)
            and body.get("read_operator_enabled") is True
            and body.get("agent_read_enabled") is True
        )
