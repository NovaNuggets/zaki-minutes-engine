"""Shared persistence policy for confirmed capture teardown evidence."""
from __future__ import annotations

from .ports import MeetingRepo


async def confirm_capture_teardown_with_retry(
    repo: MeetingRepo,
    *,
    meeting_id: int,
    attempts: int = 3,
) -> bool:
    """Persist hard-stop evidence plus terminal state, retrying transient failures in place."""
    if attempts < 1:
        raise ValueError("attempts must be positive")
    for attempt in range(attempts):
        try:
            return await repo.confirm_capture_teardown(meeting_id=meeting_id)
        except Exception:  # noqa: BLE001 — re-raise only after the bounded retry budget
            if attempt + 1 == attempts:
                raise
    return False  # pragma: no cover — the loop always returns or raises
