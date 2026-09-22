"""Injected boundaries for owner-scoped Minutes erasure."""
from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any
from typing import Protocol


@dataclass(frozen=True)
class ErasurePlan:
    """Non-content identifiers and counts required to erase one owned meeting."""

    user_id: str
    meeting_id: str
    transcript_rows: int
    summary_documents: int
    recording_prefixes: tuple[str, ...]
    recording_objects: int | None
    runtime_workload_id: str | None = None
    agent_tombstoned: bool = False
    agent_unit_streams: int = 0
    agent_workspace_documents: int = 0
    agent_brain_records: int = 0
    agent_receipt: dict[str, Any] | None = None


class RetentionRepo(Protocol):
    async def completed_erasure(self, user_id: str, meeting_id: str) -> dict | None:
        """Return the durable content-free receipt for a completed idempotent retry."""

    async def begin_erasure(self, user_id: str, meeting_id: str) -> ErasurePlan | None:
        """For an owned terminal meeting, block writes, drain them, and return the stable plan."""

    async def record_object_census(
        self, plan: ErasurePlan, recording_objects: int
    ) -> ErasurePlan:
        """Persist the pre-delete object count once so retries return a stable receipt."""

    async def purge_carriers(self, plan: ErasurePlan) -> None:
        """Revalidate the durable plan, then purge non-database carriers outside a DB transaction."""

    async def record_agent_erasure(self, plan: ErasurePlan, receipt: dict) -> ErasurePlan:
        """Persist the exact signed Agent receipt on the durable Minutes erasure plan."""

    async def record_erasure_receipt(self, plan: ErasurePlan, receipt: dict) -> dict:
        """Persist or return the stable pre-commit Minutes receipt for this plan."""

    async def commit_erasure(
        self,
        plan: ErasurePlan,
        *,
        erased_at=None,
        policy_version: str | None = None,
        receipt: dict | None = None,
    ) -> dict:
        """Atomically delete database content and durably publish the supplied receipt."""


class RetentionStorage(Protocol):
    async def count_prefix(self, prefix: str) -> int:
        """Count every current object under one validated prefix without returning its keys."""

    async def delete_prefix(self, prefix: str) -> int:
        """Delete every object under one validated prefix; return the number removed."""


class MeetingWriteGate(Protocol):
    def recording_write(self, meeting_id: str) -> AbstractAsyncContextManager[None]:
        """Hold a shared lease for the entire object + database recording mutation."""
