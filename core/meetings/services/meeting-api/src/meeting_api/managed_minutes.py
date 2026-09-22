"""User-facing, flag-gated composition over the managed Minutes consent core."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import inspect
import json
import re
import secrets
from typing import Callable, Optional

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from .bot_spawn import DuplicateMeeting, SpawnFailed, TranscriptionNotConfigured
from .bot_spawn.identifiers import (
    validate_native_meeting_id,
    validate_platform,
)
from .bot_spawn.router import _resolve_max_concurrent
from .capture import (
    CaptureAuthority,
    CaptureDenial,
    CaptureDenied,
    CaptureTeardownUnconfirmed,
    request_capture,
    withdraw_capture,
)
from .retention import ScopeExpiries
from .minutes_api_models import (
    MinutesCaptureResponse,
    MinutesStatusResponse,
    MinutesWithdrawalResponse,
)
from .public_status import public_meeting_status
from .managed_auth import hub_token_matches, validate_hub_token


MAX_CAPTURE_BODY_BYTES = 16 * 1024
_CAPTURE_REQUEST_FIELDS = frozenset({
    "platform", "native_meeting_id", "meeting_url", "passcode", "language", "task",
})
MAX_DATABASE_ID = 2**63 - 1
_ROW_ID = re.compile(r"^[1-9][0-9]{0,18}$")
def _json(body: dict, status_code: int = 200) -> JSONResponse:
    return JSONResponse(
        content=body,
        status_code=status_code,
        headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
    )


def _resolve_user(value: Optional[str]) -> Optional[int]:
    try:
        user_id = int(value) if value is not None else 0
    except (TypeError, ValueError):
        return None
    return user_id if 0 < user_id <= MAX_DATABASE_ID else None


def _public_capture_denial(denial: CaptureDenial) -> tuple[str, int]:
    """Collapse internal authority detail into the sealed minutes-api.v1 taxonomy."""
    if denial is CaptureDenial.QUOTA_EXHAUSTED:
        return "quota_exhausted", 429
    if denial is CaptureDenial.MEETING_URL_INVALID:
        return "meeting_url_invalid", 422
    if denial in {
        CaptureDenial.OPERATOR_DISABLED,
        CaptureDenial.TENANT_DISABLED,
        CaptureDenial.USER_NOT_REQUESTED,
    }:
        return "capture_disabled", 403
    return "capture_policy_invalid", 403


async def _body(request: Request) -> dict:
    declared_values = [
        value for name, value in request.scope.get("headers", [])
        if name.lower() == b"content-length"
    ]
    if len(declared_values) > 1:
        raise ValueError("request length is invalid")
    if declared_values:
        try:
            declared = declared_values[0].decode("ascii")
        except UnicodeDecodeError:
            raise ValueError("request length is invalid") from None
        if not declared or not declared.isdecimal():
            raise ValueError("request length is invalid")
        if int(declared) > MAX_CAPTURE_BODY_BYTES:
            raise OverflowError("request body is too large") from None
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > MAX_CAPTURE_BODY_BYTES:
            raise OverflowError("request body is too large")
        chunks.append(chunk)
    try:
        value = json.loads(b"".join(chunks))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("invalid JSON body") from None
    if not isinstance(value, dict):
        raise ValueError("body must be an object")
    return value


def _time(value: object) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


async def _settings(provider, user_id: int) -> Optional[dict]:
    result = provider(user_id)
    result = await result if inspect.isawaitable(result) else result
    return result if isinstance(result, dict) else None


def _retention_days(settings: dict) -> Optional[dict[str, int]]:
    value = settings.get("retention_days")
    if not isinstance(value, dict):
        return None
    out: dict[str, int] = {}
    for scope in ("audio", "transcript", "summary"):
        days = value.get(scope)
        if isinstance(days, bool) or not isinstance(days, int) or not 1 <= days <= 3_650:
            return None
        out[scope] = days
    if out["audio"] > out["transcript"] or out["summary"] > out["transcript"]:
        return None
    return out


def _bounded_string(value: object, *, maximum: int) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError
    value = value.strip()
    if not value or len(value) > maximum or any(ord(character) < 32 for character in value):
        raise ValueError
    return value


def build_router(
    *, repo, runtime, publisher, carrier_fencer, settings_provider,
    now: Callable[[], datetime], token_secret: Optional[str] = None,
    redis_url: Optional[str] = None, meeting_api_url: Optional[str] = None,
    hub_token: str,
    operator_enabled: bool = True,
) -> APIRouter:
    if not isinstance(operator_enabled, bool):
        raise ValueError("managed Minutes operator flag must be boolean")
    hub_token = validate_hub_token(hub_token)
    router = APIRouter(prefix="/minutes")

    def hub_is_authorized(value: Optional[str]) -> bool:
        return hub_token_matches(value, hub_token)

    @router.post(
        "/captures",
        response_model=MinutesCaptureResponse,
        status_code=201,
    )
    async def create_managed_capture(
        request: Request,
        x_zaki_minutes_token: Optional[str] = Header(
            default=None, alias="X-Zaki-Minutes-Token"
        ),
        x_user_id: Optional[str] = Header(default=None),
        x_user_limits: Optional[str] = Header(default=None),
    ):
        if not hub_is_authorized(x_zaki_minutes_token):
            return _json({"detail": "Unauthorized"}, 401)
        user_id = _resolve_user(x_user_id)
        if user_id is None:
            return _json({"detail": "Missing user identity"}, 401)
        # Keep this management edge mounted during an operator rollback so an
        # existing capture remains visible and stoppable.  The rollback gate is
        # evaluated before quota, request-body, settings, or runtime work.
        if not operator_enabled:
            return _json({"error": {"code": "capture_disabled"}}, 403)
        max_concurrent = _resolve_max_concurrent(x_user_limits)
        if max_concurrent is None:
            return _json({"detail": "Missing trusted quota context"}, 401)
        if max_concurrent <= 0:
            return _json({"error": {"code": "quota_exhausted"}}, 429)
        try:
            payload = await _body(request)
        except OverflowError:
            return _json({"error": {"code": "request_too_large"}}, 413)
        except ValueError as error:
            return _json({"detail": str(error)}, 422)
        if set(payload) - _CAPTURE_REQUEST_FIELDS:
            return _json({"detail": "unsupported capture request field"}, 422)
        try:
            user_settings = await _settings(settings_provider, user_id)
        except Exception:
            return _json({"error": {"code": "capture_authority_unavailable"}}, 503)
        if user_settings is None:
            return _json({"error": {"code": "capture_disabled"}}, 403)
        if user_settings.get("capture_enabled") is not True:
            return _json({"error": {"code": "capture_disabled"}}, 403)
        policy = user_settings.get("policy_version")
        attested_at = _time(user_settings.get("attested_at"))
        days = _retention_days(user_settings)
        evaluated_at = now()
        if (
            evaluated_at.tzinfo is None
            or evaluated_at.utcoffset() is None
            or not isinstance(policy, str)
            or not policy.strip()
            or len(policy) > 128
            or attested_at is None
            or attested_at > evaluated_at
            or days is None
        ):
            return _json({"error": {"code": "capture_policy_invalid"}}, 403)
        try:
            platform = validate_platform(payload.get("platform"))
            native = validate_native_meeting_id(payload.get("native_meeting_id"))
        except ValueError:
            return _json({"detail": "invalid platform or meeting identifier"}, 422)
        meeting_url = payload.get("meeting_url")
        if meeting_url is not None and not isinstance(meeting_url, str):
            return _json({"detail": "meeting_url must be a string"}, 422)
        try:
            passcode = _bounded_string(payload.get("passcode"), maximum=512)
            language = _bounded_string(payload.get("language"), maximum=35)
            task = _bounded_string(payload.get("task"), maximum=256)
        except ValueError:
            return _json({"detail": "capture option is invalid"}, 422)
        tenant_id = f"user:{user_id}"
        authority = CaptureAuthority(
            operator_enabled=True,
            tenant_enabled=True,
            tenant_attested=True,
            tenant_policy_version=policy.strip(),
            tenant_attested_at=attested_at,
            user_requested=True,
            quota_permitted=True,
            subject_user_id=user_id,
            tenant_id=tenant_id,
            meeting_platform=platform,
            native_meeting_id=native,
            authorized_at=evaluated_at,
            valid_until=evaluated_at + timedelta(minutes=5),
            scope_expiries=ScopeExpiries(**{
                scope: evaluated_at + timedelta(days=days[scope])
                for scope in ("audio", "transcript", "summary")
            }),
            grant_id=secrets.token_urlsafe(32),
            meeting_url_sha256=(
                hashlib.sha256(meeting_url.strip().encode()).hexdigest()
                if isinstance(meeting_url, str) else None
            ),
        )
        try:
            meeting = await request_capture(
                repo, runtime,
                authority=authority,
                tenant_id=tenant_id,
                user_id=user_id,
                platform=platform,
                native_meeting_id=native,
                passcode=passcode,
                meeting_url=meeting_url,
                language=language,
                task=task,
                max_concurrent=max_concurrent,
                redis_url=redis_url,
                meeting_api_url=meeting_api_url,
                token_secret=token_secret,
                evaluated_at=evaluated_at,
            )
        except CaptureDenied as error:
            code, status = _public_capture_denial(error.code)
            return _json({"error": {"code": code}}, status)
        except DuplicateMeeting:
            return _json({"error": {"code": "capture_already_active"}}, 409)
        except TranscriptionNotConfigured:
            return _json({"error": {"code": "transcription_unavailable"}}, 503)
        except (SpawnFailed, CaptureTeardownUnconfirmed):
            return _json({"error": {"code": "capture_start_failed"}}, 502)
        response = MinutesCaptureResponse.model_validate({
            "id": str(meeting.get("id")),
            "status": public_meeting_status(meeting.get("status")),
        })
        return _json(response.model_dump(mode="json"), 201)

    @router.delete(
        "/captures/{platform}/{native_meeting_id}",
        response_model=MinutesWithdrawalResponse,
    )
    async def withdraw_managed_capture(
        platform: str,
        native_meeting_id: str,
        x_zaki_minutes_token: Optional[str] = Header(
            default=None, alias="X-Zaki-Minutes-Token"
        ),
        x_user_id: Optional[str] = Header(default=None),
    ):
        if not hub_is_authorized(x_zaki_minutes_token):
            return _json({"detail": "Unauthorized"}, 401)
        user_id = _resolve_user(x_user_id)
        if user_id is None:
            return _json({"detail": "Missing user identity"}, 401)
        try:
            platform = validate_platform(platform)
            native_meeting_id = validate_native_meeting_id(native_meeting_id)
        except ValueError:
            return _json({"detail": "invalid platform or meeting identifier"}, 422)
        try:
            receipt = await withdraw_capture(
                repo,
                publisher,
                carrier_fencer=carrier_fencer,
                tenant_id=f"user:{user_id}",
                user_id=user_id,
                platform=platform,
                native_meeting_id=native_meeting_id,
                runtime=runtime,
                withdrawn_at=now(),
            )
        except CaptureDenied:
            return _json({"error": {"code": "capture_not_found"}}, 404)
        except CaptureTeardownUnconfirmed:
            return _json({"error": {"code": "capture_withdrawal_pending"}}, 503)
        response = MinutesWithdrawalResponse.model_validate({
            **receipt,
            "meeting_id": str(receipt.get("meeting_id")),
        })
        return _json(response.model_dump(mode="json"))

    @router.get(
        "/meetings/{meeting_id}/status",
        response_model=MinutesStatusResponse,
    )
    async def get_managed_capture_status(
        meeting_id: str,
        x_zaki_minutes_token: Optional[str] = Header(
            default=None, alias="X-Zaki-Minutes-Token"
        ),
        x_user_id: Optional[str] = Header(default=None),
    ):
        if not hub_is_authorized(x_zaki_minutes_token):
            return _json({"detail": "Unauthorized"}, 401)
        user_id = _resolve_user(x_user_id)
        if user_id is None:
            return _json({"detail": "Missing user identity"}, 401)
        if not _ROW_ID.fullmatch(meeting_id):
            return _json({"error": {"code": "meeting_not_found"}}, 404)
        row_id = int(meeting_id)
        if row_id <= 0 or row_id > MAX_DATABASE_ID:
            return _json({"error": {"code": "meeting_not_found"}}, 404)
        try:
            meeting = await repo.find_owned_minutes(user_id=user_id, meeting_id=row_id)
        except Exception:
            return _json({"error": {"code": "capture_authority_unavailable"}}, 503)
        if meeting is None:
            return _json({"error": {"code": "meeting_not_found"}}, 404)
        data = meeting.get("data") if isinstance(meeting.get("data"), dict) else {}
        status = public_meeting_status(meeting.get("status"))
        payload: dict[str, object] = {"meeting_id": meeting_id, "status": status}
        if status == "completed":
            payload["completion_reason"] = data.get("completion_reason")
        elif status == "failed":
            payload["failure_stage"] = data.get("failure_stage")
        try:
            response = MinutesStatusResponse.model_validate(payload)
        except ValidationError:
            # Fail closed on legacy/corrupt lifecycle rows; do not leak internal vocabulary or data.
            return _json({"error": {"code": "capture_authority_unavailable"}}, 503)
        return _json(response.model_dump(mode="json", exclude_none=True))

    return router
