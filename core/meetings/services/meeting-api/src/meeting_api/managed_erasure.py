"""Authenticated user erasure orchestrator spanning Agent-owned and Minutes-owned carriers."""
from __future__ import annotations

from datetime import datetime, timezone
import re
from collections.abc import Mapping
from typing import Callable, Optional

from fastapi import APIRouter, Header
from fastapi.responses import JSONResponse

from .retention import ErasureFailed, erase_meeting
from .erasure_receipts import sign_erasure_receipt, verify_erasure_receipt
from .minutes_api_models import MinutesErasureResponse
from .managed_auth import hub_token_matches, validate_hub_token


_ROW_ID = re.compile(r"^[1-9][0-9]{0,18}$")
_MAX_DB_ID = 2**63 - 1
ERASURE_POLICY_VERSION = "minutes-erasure.v1"


def _json(body: dict, status_code: int = 200) -> JSONResponse:
    return JSONResponse(
        content=body,
        status_code=status_code,
        headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
    )


def _user_id(value: Optional[str]) -> Optional[int]:
    try:
        parsed = int(value) if value is not None else 0
    except (TypeError, ValueError):
        return None
    return parsed if 0 < parsed <= _MAX_DB_ID else None


def build_router(
    *,
    repo,
    storage,
    agent_eraser,
    runtime_scrubber,
    now: Callable[[], datetime],
    signing_key_id: str,
    signing_secret: str | bytes,
    verification_keys: Mapping[str, str | bytes],
    nonce_factory: Callable[[], str],
    hub_token: str,
) -> APIRouter:
    hub_token = validate_hub_token(hub_token)
    router = APIRouter(prefix="/minutes")

    def hub_is_authorized(value: Optional[str]) -> bool:
        return hub_token_matches(value, hub_token)

    def _receipt_is_valid(
        receipt: object,
        user_id: int | str,
        meeting_id: str,
        *,
        expected_counts: dict | None = None,
    ) -> bool:
        verification_secret = (
            verification_keys.get(receipt.get("key_id"))
            if isinstance(receipt, dict)
            else None
        )
        return (
            verification_secret is not None
            and isinstance(receipt.get("counts"), dict)
            and set(receipt["counts"]) == {
                "meeting_rows",
                "transcript_rows",
                "summary_documents",
                "recording_objects",
                "agent_unit_streams",
                "agent_workspace_documents",
                "agent_brain_records",
            }
            and (expected_counts is None or receipt["counts"] == expected_counts)
            and verify_erasure_receipt(
                receipt,
                verification_secret,
                expected_owner="minutes",
                expected_scope="meeting",
                expected_user_id=user_id,
                expected_meeting_id=meeting_id,
                now=now,
            )
        )

    @router.delete("/meetings/{meeting_id}", response_model=MinutesErasureResponse)
    async def erase_owned_meeting(
        meeting_id: str,
        x_zaki_minutes_token: Optional[str] = Header(
            default=None, alias="X-Zaki-Minutes-Token"
        ),
        x_user_id: Optional[str] = Header(default=None),
    ):
        if not hub_is_authorized(x_zaki_minutes_token):
            return _json({"detail": "Unauthorized"}, 401)
        user_id = _user_id(x_user_id)
        if user_id is None:
            return _json({"detail": "Missing user identity"}, 401)
        if not _ROW_ID.fullmatch(meeting_id) or int(meeting_id) > _MAX_DB_ID:
            return _json({"error": {"code": "meeting_not_found"}}, 404)
        try:
            completed = await repo.completed_erasure(str(user_id), meeting_id)
        except Exception:
            return _json({"error": {"code": "erasure_pending"}}, 503)
        if completed is not None:
            if not _receipt_is_valid(completed, user_id, meeting_id):
                return _json({"error": {"code": "erasure_pending"}}, 503)
            return _json(completed)

        try:
            plan = await repo.begin_erasure(str(user_id), meeting_id)
        except Exception:
            return _json({"error": {"code": "erasure_pending"}}, 503)
        if plan is None:
            # Close the concurrent-completion race: a peer can commit after our first receipt read
            # but before this owner lookup acquires the meeting lock.
            try:
                completed = await repo.completed_erasure(str(user_id), meeting_id)
            except Exception:
                return _json({"error": {"code": "erasure_pending"}}, 503)
            if completed is None:
                return _json({"error": {"code": "meeting_not_found"}}, 404)
            if not _receipt_is_valid(completed, user_id, meeting_id):
                return _json({"error": {"code": "erasure_pending"}}, 503)
            return _json(completed)

        # A stopped container/Pod and the runtime's durable lifecycle record are separate
        # secret-bearing carriers. Reclaim them before Agent or Minutes content deletion so a 200
        # receipt can never coexist with a retained meeting URL, passcode, or provider token.
        if plan.runtime_workload_id is not None:
            try:
                await runtime_scrubber.scrub_workload(plan.runtime_workload_id)
            except Exception:
                return _json({"error": {"code": "erasure_pending"}}, 503)

        if not plan.agent_tombstoned:
            try:
                agent_receipt = await agent_eraser(
                    user_id=user_id, meeting_id=int(meeting_id)
                )
                plan = await repo.record_agent_erasure(
                    plan, agent_receipt.as_dict()
                )
            except Exception:
                return _json({"error": {"code": "erasure_pending"}}, 503)
        if not plan.agent_tombstoned:
            return _json({"error": {"code": "erasure_pending"}}, 503)

        erased_at = now()
        if (
            not isinstance(erased_at, datetime)
            or erased_at.tzinfo is None
            or erased_at.utcoffset() is None
        ):
            return _json({"error": {"code": "erasure_pending"}}, 503)
        erased_at = erased_at.astimezone(timezone.utc)
        try:
            def _receipt_factory(stable_plan):
                return sign_erasure_receipt(
                    owner="minutes",
                    scope="meeting",
                    user_id=stable_plan.user_id,
                    meeting_id=stable_plan.meeting_id,
                    counts={
                        "meeting_rows": 1,
                        "transcript_rows": stable_plan.transcript_rows,
                        "summary_documents": stable_plan.summary_documents,
                        "recording_objects": stable_plan.recording_objects,
                        "agent_unit_streams": stable_plan.agent_unit_streams,
                        "agent_workspace_documents": stable_plan.agent_workspace_documents,
                        "agent_brain_records": stable_plan.agent_brain_records,
                    },
                    issued_at=erased_at,
                    key_id=signing_key_id,
                    nonce=nonce_factory(),
                    secret=signing_secret,
                )

            def _receipt_verifier(stable_receipt, stable_plan):
                return _receipt_is_valid(
                    stable_receipt,
                    stable_plan.user_id,
                    stable_plan.meeting_id,
                    expected_counts={
                        "meeting_rows": 1,
                        "transcript_rows": stable_plan.transcript_rows,
                        "summary_documents": stable_plan.summary_documents,
                        "recording_objects": stable_plan.recording_objects,
                        "agent_unit_streams": stable_plan.agent_unit_streams,
                        "agent_workspace_documents": stable_plan.agent_workspace_documents,
                        "agent_brain_records": stable_plan.agent_brain_records,
                    },
                )

            def _agent_receipt_verifier(stable_receipt, stable_plan):
                return agent_eraser.verify_durable_receipt(
                    stable_receipt,
                    user_id=int(stable_plan.user_id),
                    meeting_id=int(stable_plan.meeting_id),
                )

            receipt = await erase_meeting(
                repo,
                storage,
                user_id=str(user_id),
                meeting_id=meeting_id,
                erased_at=erased_at,
                policy_version=ERASURE_POLICY_VERSION,
                receipt_factory=_receipt_factory,
                receipt_verifier=_receipt_verifier,
                agent_receipt_verifier=_agent_receipt_verifier,
            )
        except ErasureFailed:
            return _json({"error": {"code": "erasure_pending"}}, 503)
        if receipt is None or not receipt.agent_tombstoned:
            return _json({"error": {"code": "erasure_pending"}}, 503)
        return _json(receipt.as_dict())

    return router
