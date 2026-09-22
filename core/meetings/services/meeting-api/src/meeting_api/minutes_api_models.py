"""Exact public response models for the managed Minutes boundary."""
from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, RootModel


MAX_DATABASE_ID = 2**63 - 1


def _bounded_row_id(value: str) -> str:
    if int(value) > MAX_DATABASE_ID:
        raise ValueError("row identifier exceeds PostgreSQL signed-bigint range")
    return value


CanonicalRowId = Annotated[
    str,
    Field(strict=True, pattern=r"^[1-9][0-9]{0,18}$"),
    AfterValidator(_bounded_row_id),
]


class MinutesProductStatus(StrEnum):
    REQUESTED = "requested"
    JOINING = "joining"
    AWAITING_ADMISSION = "awaiting_admission"
    ACTIVE = "active"
    NEEDS_HUMAN_HELP = "needs_human_help"
    STOPPING = "stopping"
    COMPLETED = "completed"
    FAILED = "failed"


class MinutesCompletionReason(StrEnum):
    STOPPED = "stopped"
    LEFT_ALONE = "left_alone"
    STARTUP_ALONE = "startup_alone"
    EVICTED = "evicted"
    AWAITING_ADMISSION_TIMEOUT = "awaiting_admission_timeout"
    AWAITING_ADMISSION_REJECTED = "awaiting_admission_rejected"
    JOIN_FAILURE = "join_failure"
    VALIDATION_ERROR = "validation_error"
    MAX_BOT_TIME_EXCEEDED = "max_bot_time_exceeded"


class MinutesFailureStage(StrEnum):
    REQUESTED = "requested"
    JOINING = "joining"
    AWAITING_ADMISSION = "awaiting_admission"
    ACTIVE = "active"


class _ExactModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MinutesCaptureResponse(_ExactModel):
    id: CanonicalRowId
    status: MinutesProductStatus


class MinutesWithdrawalResponse(_ExactModel):
    meeting_id: CanonicalRowId
    state: Literal["withdrawn"]
    changed: bool
    withdrawn_at: datetime


class MinutesNonterminalStatusResponse(_ExactModel):
    meeting_id: CanonicalRowId
    status: Literal[
        MinutesProductStatus.REQUESTED,
        MinutesProductStatus.JOINING,
        MinutesProductStatus.AWAITING_ADMISSION,
        MinutesProductStatus.ACTIVE,
        MinutesProductStatus.NEEDS_HUMAN_HELP,
        MinutesProductStatus.STOPPING,
    ]


class MinutesCompletedStatusResponse(_ExactModel):
    meeting_id: CanonicalRowId
    status: Literal[MinutesProductStatus.COMPLETED]
    completion_reason: MinutesCompletionReason


class MinutesFailedStatusResponse(_ExactModel):
    meeting_id: CanonicalRowId
    status: Literal[MinutesProductStatus.FAILED]
    failure_stage: MinutesFailureStage


MinutesStatusVariant = Annotated[
    MinutesNonterminalStatusResponse
    | MinutesCompletedStatusResponse
    | MinutesFailedStatusResponse,
    Field(discriminator="status"),
]


class MinutesStatusResponse(RootModel[MinutesStatusVariant]):
    """Exact discriminated public status: terminal attribution exists on one branch only."""


class MinutesErasureSubject(_ExactModel):
    user_id: CanonicalRowId
    meeting_id: CanonicalRowId


class MinutesAccountErasureSubject(_ExactModel):
    user_id: CanonicalRowId


class MinutesErasureCounts(_ExactModel):
    meeting_rows: int = Field(ge=0, le=2_147_483_647)
    transcript_rows: int = Field(ge=0, le=2_147_483_647)
    summary_documents: int = Field(ge=0, le=2_147_483_647)
    recording_objects: int = Field(ge=0, le=2_147_483_647)
    agent_unit_streams: int = Field(ge=0, le=2_147_483_647)
    agent_workspace_documents: int = Field(ge=0, le=2_147_483_647)
    agent_brain_records: int = Field(ge=0, le=2_147_483_647)


class MinutesErasureResponse(_ExactModel):
    """Exact signed ``erasure.v1`` meeting receipt returned by Minutes."""

    version: Literal["erasure.v1"]
    owner: Literal["minutes"]
    scope: Literal["meeting"]
    subject: MinutesErasureSubject
    counts: MinutesErasureCounts
    issued_at: datetime
    key_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    nonce: str = Field(pattern=r"^[A-Za-z0-9_-]{16,128}$")
    digest: str = Field(pattern=r"^sha256=[0-9a-f]{64}$")
    signature: str = Field(pattern=r"^sha256=[0-9a-f]{64}$")


class MinutesAccountErasureResponse(_ExactModel):
    """Reserved exact ``erasure.v1`` account receipt returned by Minutes orchestration."""

    version: Literal["erasure.v1"]
    owner: Literal["minutes"]
    scope: Literal["account"]
    subject: MinutesAccountErasureSubject
    counts: MinutesErasureCounts
    issued_at: datetime
    key_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    nonce: str = Field(pattern=r"^[A-Za-z0-9_-]{16,128}$")
    digest: str = Field(pattern=r"^sha256=[0-9a-f]{64}$")
    signature: str = Field(pattern=r"^sha256=[0-9a-f]{64}$")
