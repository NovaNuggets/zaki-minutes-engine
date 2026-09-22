"""Raw meeting erasure orchestration."""
from __future__ import annotations

from dataclasses import dataclass
from copy import deepcopy
from datetime import datetime
import re
from typing import Callable

from .ports import ErasurePlan, RetentionRepo, RetentionStorage


class ErasureFailed(RuntimeError):
    """The operation did not complete; the message never contains meeting content or storage keys."""


_PREFIX_SEGMENT = re.compile(r"^[A-Za-z0-9._:-]+$")


def _valid_recording_prefix(prefix: str) -> bool:
    if not isinstance(prefix, str) or not prefix.endswith("/"):
        return False
    parts = prefix.split("/")
    return (
        len(parts) >= 5
        and parts[0] == "recordings"
        and parts[-1] == ""
        and all(_PREFIX_SEGMENT.fullmatch(part) and part not in {".", ".."} for part in parts[1:-1])
    )


@dataclass(frozen=True)
class ErasureReceipt:
    user_id: str
    meeting_id: str
    erased_at: datetime
    policy_version: str
    meeting_rows: int
    transcript_rows: int
    summary_documents: int
    recording_objects: int
    agent_tombstoned: bool = False
    agent_unit_streams: int = 0
    agent_workspace_documents: int = 0
    agent_brain_records: int = 0

    def as_dict(self) -> dict:
        receipt = {
            "user_id": self.user_id,
            "meeting_id": self.meeting_id,
            "erased_at": self.erased_at.isoformat(),
            "policy_version": self.policy_version,
            "deleted": {
                "meeting_rows": self.meeting_rows,
                "transcript_rows": self.transcript_rows,
                "summary_documents": self.summary_documents,
                "recording_objects": self.recording_objects,
            },
        }
        if self.agent_tombstoned:
            receipt["agent_tombstoned"] = True
            receipt["deleted"].update({
                "agent_unit_streams": self.agent_unit_streams,
                "agent_workspace_documents": self.agent_workspace_documents,
                "agent_brain_records": self.agent_brain_records,
            })
        return receipt


@dataclass(frozen=True)
class SignedErasureReceipt:
    """An exact durable ``erasure.v1`` receipt returned by the managed path."""

    receipt: dict

    def as_dict(self) -> dict:
        return deepcopy(self.receipt)

    @property
    def recording_objects(self) -> int:
        return int(self.receipt["counts"]["recording_objects"])

    @property
    def agent_tombstoned(self) -> bool:
        counts = self.receipt.get("counts", {})
        return all(
            type(counts.get(field)) is int
            for field in (
                "agent_unit_streams",
                "agent_workspace_documents",
                "agent_brain_records",
            )
        )


async def erase_meeting(
    repo: RetentionRepo,
    storage: RetentionStorage,
    *,
    user_id: str,
    meeting_id: str,
    erased_at: datetime,
    policy_version: str,
    receipt_factory: Callable[[ErasurePlan], dict] | None = None,
    receipt_verifier: Callable[[dict, ErasurePlan], bool] | None = None,
    agent_receipt_verifier: Callable[[object, ErasurePlan], bool] | None = None,
) -> ErasureReceipt | SignedErasureReceipt | None:
    """Erase one owned meeting without exposing whether an absent meeting belongs to another user."""

    try:
        plan = await repo.begin_erasure(user_id, meeting_id)
    except Exception:
        raise ErasureFailed("meeting erasure planning requires retry") from None
    if plan is None:
        return None
    if agent_receipt_verifier is not None:
        try:
            if (
                not plan.agent_tombstoned
                or plan.agent_receipt is None
                or not agent_receipt_verifier(plan.agent_receipt, plan)
            ):
                raise RuntimeError("Agent erasure receipt verification failed")
        except Exception:
            raise ErasureFailed("Agent erasure receipt requires retry") from None
    prefixes = plan.recording_prefixes
    if any(not _valid_recording_prefix(prefix) for prefix in prefixes):
        raise ErasureFailed("meeting erasure plan is invalid")

    if plan.recording_objects is None:
        try:
            recording_objects = sum(
                [await storage.count_prefix(prefix) for prefix in prefixes]
            )
            plan = await repo.record_object_census(plan, recording_objects)
        except Exception:
            raise ErasureFailed("meeting erasure census requires retry") from None

    if (
        plan.recording_objects is None
        or plan.recording_objects < 0
        or (not prefixes and plan.recording_objects != 0)
    ):
        raise ErasureFailed("meeting erasure plan is invalid")

    stable_receipt = None
    if receipt_factory is not None:
        try:
            candidate = receipt_factory(plan)
            stable_receipt = await repo.record_erasure_receipt(plan, candidate)
            if receipt_verifier is None or not receipt_verifier(stable_receipt, plan):
                raise RuntimeError("stable receipt verification failed")
        except Exception:
            raise ErasureFailed("meeting erasure receipt requires retry") from None
    elif receipt_verifier is not None:
        raise ErasureFailed("meeting erasure receipt requires retry")

    try:
        for prefix in plan.recording_prefixes:
            await storage.delete_prefix(prefix)
        if any([await storage.count_prefix(prefix) for prefix in plan.recording_prefixes]):
            raise RuntimeError("recording prefix remains non-empty")
    except Exception:
        raise ErasureFailed("meeting erasure failed before database commit") from None

    try:
        await repo.purge_carriers(plan)
    except Exception:
        raise ErasureFailed("meeting erasure carrier purge requires retry") from None

    try:
        commit_time = erased_at
        if stable_receipt is not None:
            commit_time = datetime.fromisoformat(
                stable_receipt["issued_at"].replace("Z", "+00:00")
            )
        committed = await repo.commit_erasure(
            plan,
            erased_at=commit_time,
            policy_version=policy_version,
            receipt=stable_receipt,
        )
    except Exception:
        raise ErasureFailed("meeting erasure requires retry") from None

    if stable_receipt is not None:
        if committed != stable_receipt:
            raise ErasureFailed("meeting erasure receipt requires retry")
        return SignedErasureReceipt(receipt=deepcopy(committed))

    deleted = committed
    return ErasureReceipt(
        user_id=plan.user_id,
        meeting_id=plan.meeting_id,
        erased_at=erased_at,
        policy_version=policy_version,
        meeting_rows=deleted.get("meeting_rows", 0),
        transcript_rows=deleted.get("transcript_rows", 0),
        summary_documents=deleted.get("summary_documents", 0),
        recording_objects=plan.recording_objects,
        agent_tombstoned=plan.agent_tombstoned,
        agent_unit_streams=plan.agent_unit_streams,
        agent_workspace_documents=plan.agent_workspace_documents,
        agent_brain_records=plan.agent_brain_records,
    )
