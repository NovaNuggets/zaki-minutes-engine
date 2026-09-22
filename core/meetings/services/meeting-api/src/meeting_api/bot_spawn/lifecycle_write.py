"""Durable meeting-status transition guard shared by the SQL adapter and its in-memory mirror."""
from __future__ import annotations

from .ports import LifecycleWriteDisposition

_TERMINAL = frozenset({"completed", "failed"})
_LEGAL_PERSISTED_TRANSITIONS = {
    "requested": frozenset({"joining", "stopping"}),
    "joining": frozenset({"awaiting_admission", "active", "failed", "stopping"}),
    "awaiting_admission": frozenset({"active", "needs_help", "failed", "stopping"}),
    "needs_help": frozenset({"active", "failed", "stopping"}),
    "active": frozenset({"completed", "failed", "stopping"}),
    "stopping": frozenset({"completed", "failed"}),
    "completed": frozenset(),
    "failed": frozenset(),
}


def classify_lifecycle_write(
    current_status: str,
    requested_status: str,
    *,
    force_terminal: bool = False,
) -> LifecycleWriteDisposition:
    """Classify an update against the status locked in the write transaction."""
    if current_status == requested_status:
        return "idempotent"
    if current_status in _TERMINAL:
        return "rejected"
    if force_terminal and requested_status in _TERMINAL:
        return "applied"
    if requested_status in _LEGAL_PERSISTED_TRANSITIONS.get(current_status, frozenset()):
        return "applied"
    return "rejected"
