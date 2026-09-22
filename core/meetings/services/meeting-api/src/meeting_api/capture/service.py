"""Fail-closed ZAKI capture authorization and spawn composition."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import hmac
import json
import math
from typing import Any, Optional, Protocol

from ..bot_spawn import (
    CaptureGrantConsumed,
    MaxBotsExceeded,
    MeetingRepo,
    QuotaExceeded,
    RuntimeClient,
    TeardownUnconfirmed,
    UnsafeMeetingUrl,
    request_bot,
    validate_meeting_url,
    confirm_capture_teardown_with_retry,
)
from ..collector.carriers import fence_meeting_redis_carriers
from ..retention import ScopeExpiries, materialize_scope_expiries
from ..lifecycle.stop import leave_command_channel, leave_command_payload
from ..obs import log_event


ZAKI_NOTETAKER_NAME = "ZAKI Notetaker"
MAX_AUTHORITY_LIFETIME = timedelta(minutes=5)


class CaptureDenial(str, Enum):
    """Stable, content-free reasons why capture did not start."""

    OPERATOR_DISABLED = "operator_disabled"
    OPERATOR_POLICY_INVALID = "operator_policy_invalid"
    TENANT_DISABLED = "tenant_disabled"
    TENANT_ATTESTATION_REQUIRED = "tenant_attestation_required"
    TENANT_POLICY_INVALID = "tenant_policy_invalid"
    USER_NOT_REQUESTED = "user_not_requested"
    USER_REQUEST_INVALID = "user_request_invalid"
    QUOTA_EXHAUSTED = "quota_exhausted"
    QUOTA_POLICY_INVALID = "quota_policy_invalid"
    AUTHORITY_SCOPE_MISMATCH = "authority_scope_mismatch"
    AUTHORITY_EXPIRED = "authority_expired"
    AUTHORITY_REPLAYED = "authority_replayed"
    RETENTION_POLICY_INVALID = "retention_policy_invalid"
    MEETING_URL_INVALID = "meeting_url_invalid"


class CaptureDenied(Exception):
    """Capture did not start; the exception contains no meeting or participant data."""

    def __init__(self, code: CaptureDenial):
        self.code = code
        super().__init__(code.value)


class CaptureTeardownUnconfirmed(TeardownUnconfirmed):
    """Withdrawal is durable, but its Redis fence and/or terminal stop is unconfirmed."""


class CaptureStopPublisher(Protocol):
    """The internal leave-command publisher used after withdrawal is durable."""

    async def publish(self, channel: str, message: str) -> Any:
        ...


class CaptureCarrierFencer(Protocol):
    """Install the permanent raw/processed Redis tombstone for one numeric meeting row."""

    async def __call__(
        self, meeting_id: int, *, raw: bool, processed: bool
    ) -> None:
        ...


class RedisCaptureCarrierFencer:
    """Production adapter over the shared monotonic Redis carrier-fence contract."""

    def __init__(self, redis_client: Any) -> None:
        self._redis_client = redis_client

    async def __call__(
        self, meeting_id: int, *, raw: bool, processed: bool
    ) -> None:
        from ..webhooks.platform_finalized import RedisTranscriptFinalizedOutbox
        from ..webhooks.retry import purge_meeting_webhook_state

        await purge_meeting_webhook_state(self._redis_client, meeting_id)
        await RedisTranscriptFinalizedOutbox(self._redis_client).cancel(meeting_id)
        await fence_meeting_redis_carriers(
            self._redis_client,
            meeting_id,
            raw=raw,
            processed=processed,
        )


@dataclass(frozen=True)
class CaptureAuthority:
    """The independently stored inputs to the effective capture decision.

    Optional annotations are intentional: deserializers can represent missing fields, and this
    boundary rejects them rather than allowing Python truthiness to invent an enabling default.
    """

    operator_enabled: Optional[bool]
    tenant_enabled: Optional[bool]
    tenant_attested: Optional[bool]
    tenant_policy_version: Optional[str]
    tenant_attested_at: Optional[datetime]
    user_requested: Optional[bool]
    quota_permitted: Optional[bool]
    subject_user_id: Optional[int]
    tenant_id: Optional[str]
    meeting_platform: Optional[str]
    native_meeting_id: Optional[str]
    authorized_at: Optional[datetime]
    valid_until: Optional[datetime]
    scope_expiries: Optional[ScopeExpiries]
    grant_id: Optional[str] = None
    meeting_url_sha256: Optional[str] = None


def _require_strict_bool(value: object, invalid: CaptureDenial) -> bool:
    if type(value) is not bool:
        raise CaptureDenied(invalid)
    return value


def _capture_evidence(
    authority: CaptureAuthority,
    *,
    tenant_id: Optional[str],
    user_id: int,
    platform: str,
    native_meeting_id: str,
    evaluated_at: datetime,
) -> dict:
    if (
        isinstance(authority.subject_user_id, bool)
        or not isinstance(authority.subject_user_id, int)
        or authority.subject_user_id <= 0
        or authority.subject_user_id != user_id
    ):
        raise CaptureDenied(CaptureDenial.AUTHORITY_SCOPE_MISMATCH)
    if (
        not isinstance(tenant_id, str)
        or not tenant_id.strip()
        or len(tenant_id) > 128
        or any(ord(character) < 32 for character in tenant_id)
        or authority.tenant_id != tenant_id
    ):
        raise CaptureDenied(CaptureDenial.AUTHORITY_SCOPE_MISMATCH)
    if (
        not isinstance(platform, str)
        or not platform
        or authority.meeting_platform != platform
    ):
        raise CaptureDenied(CaptureDenial.AUTHORITY_SCOPE_MISMATCH)
    if (
        not isinstance(native_meeting_id, str)
        or not native_meeting_id
        or authority.native_meeting_id != native_meeting_id
    ):
        raise CaptureDenied(CaptureDenial.AUTHORITY_SCOPE_MISMATCH)
    if (
        not isinstance(evaluated_at, datetime)
        or evaluated_at.tzinfo is None
        or evaluated_at.utcoffset() is None
        or not isinstance(authority.authorized_at, datetime)
        or authority.authorized_at.tzinfo is None
        or authority.authorized_at.utcoffset() is None
        or not isinstance(authority.valid_until, datetime)
        or authority.valid_until.tzinfo is None
        or authority.valid_until.utcoffset() is None
        or not authority.authorized_at <= evaluated_at < authority.valid_until
        or not timedelta(0) < authority.valid_until - authority.authorized_at <= MAX_AUTHORITY_LIFETIME
    ):
        raise CaptureDenied(CaptureDenial.AUTHORITY_EXPIRED)
    grant_id = authority.grant_id
    if (
        not isinstance(grant_id, str)
        or not grant_id.strip()
        or len(grant_id) > 128
        or any(ord(character) < 32 for character in grant_id)
    ):
        raise CaptureDenied(CaptureDenial.AUTHORITY_SCOPE_MISMATCH)
    grant_id = grant_id.strip()
    grant_id_sha256 = hashlib.sha256(grant_id.encode()).hexdigest()
    operator_enabled = _require_strict_bool(
        authority.operator_enabled, CaptureDenial.OPERATOR_POLICY_INVALID
    )
    if not operator_enabled:
        raise CaptureDenied(CaptureDenial.OPERATOR_DISABLED)

    tenant_enabled = _require_strict_bool(
        authority.tenant_enabled, CaptureDenial.TENANT_POLICY_INVALID
    )
    if not tenant_enabled:
        raise CaptureDenied(CaptureDenial.TENANT_DISABLED)

    tenant_attested = _require_strict_bool(
        authority.tenant_attested, CaptureDenial.TENANT_POLICY_INVALID
    )
    if not tenant_attested:
        raise CaptureDenied(CaptureDenial.TENANT_ATTESTATION_REQUIRED)

    policy_version = authority.tenant_policy_version
    if (
        not isinstance(policy_version, str)
        or not policy_version.strip()
        or len(policy_version) > 128
        or any(ord(character) < 32 for character in policy_version)
    ):
        raise CaptureDenied(CaptureDenial.TENANT_POLICY_INVALID)
    policy_version = policy_version.strip()

    attested_at = authority.tenant_attested_at
    if (
        not isinstance(attested_at, datetime)
        or attested_at.tzinfo is None
        or attested_at.utcoffset() is None
        or attested_at > authority.authorized_at
    ):
        raise CaptureDenied(CaptureDenial.TENANT_POLICY_INVALID)

    user_requested = _require_strict_bool(
        authority.user_requested, CaptureDenial.USER_REQUEST_INVALID
    )
    if not user_requested:
        raise CaptureDenied(CaptureDenial.USER_NOT_REQUESTED)

    quota_permitted = _require_strict_bool(
        authority.quota_permitted, CaptureDenial.QUOTA_POLICY_INVALID
    )
    if not quota_permitted:
        raise CaptureDenied(CaptureDenial.QUOTA_EXHAUSTED)

    metadata = {
        "zaki_capture": {
            "bot_name": ZAKI_NOTETAKER_NAME,
            "tenant_id": tenant_id,
            "state": "authorized",
            "tenant_attested": True,
            "tenant_policy_version": policy_version,
            "tenant_attested_at": attested_at.isoformat(),
            "user_requested": True,
            "authorized_at": authority.authorized_at.isoformat(),
            "authority_valid_until": authority.valid_until.isoformat(),
            "grant_id_sha256": grant_id_sha256,
        }
    }
    try:
        expiries = materialize_scope_expiries(authority.scope_expiries)
    except (TypeError, ValueError):
        raise CaptureDenied(CaptureDenial.RETENTION_POLICY_INVALID) from None
    if any(
        getattr(expiries, scope) <= evaluated_at
        for scope in ("audio", "transcript", "summary")
    ):
        raise CaptureDenied(CaptureDenial.RETENTION_POLICY_INVALID)
    metadata["zaki_retention"] = {
        "state": "open",
        "scope_expiries": {
            scope: getattr(expiries, scope).isoformat()
            for scope in ("audio", "transcript", "summary")
        },
        "expired_scopes": [],
    }
    return metadata


class _CaptureEvidenceRepo:
    """Narrow MeetingRepo decorator that adds validated evidence to a fresh spawn write."""

    def __init__(self, delegate: MeetingRepo, metadata: dict):
        self._delegate = delegate
        self._metadata = dict(metadata)

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)

    async def create_meeting_guarded(
        self,
        *,
        user_id: int,
        platform: str,
        native_meeting_id: str,
        data: dict,
        max_concurrent: Optional[int] = None,
        exclude_meeting_id: Optional[int] = None,
    ) -> dict:
        meeting_data = dict(data)
        meeting_data.update(self._metadata)
        return await self._delegate.create_meeting_guarded(
            user_id=user_id,
            platform=platform,
            native_meeting_id=native_meeting_id,
            data=meeting_data,
            max_concurrent=max_concurrent,
            exclude_meeting_id=exclude_meeting_id,
        )

    async def mark_spawn_rejected(
        self, *, meeting_id: int, reason: str, data: Optional[dict] = None
    ) -> Optional[dict]:
        capture = dict(self._metadata["zaki_capture"])
        capture["state"] = "denied"
        capture["denial"] = reason
        patch = dict(data or {})
        patch["zaki_capture"] = capture
        return await self._delegate.mark_spawn_rejected(
            meeting_id=meeting_id,
            reason=reason,
            data=patch,
        )


async def request_capture(
    repo: MeetingRepo,
    runtime: RuntimeClient,
    *,
    authority: CaptureAuthority,
    tenant_id: str,
    user_id: int,
    platform: str,
    native_meeting_id: str,
    passcode: Optional[str] = None,
    meeting_url: Optional[str] = None,
    language: Optional[str] = None,
    task: Optional[str] = None,
    max_concurrent: Optional[int] = None,
    redis_url: Optional[str] = None,
    meeting_api_url: Optional[str] = None,
    token_secret: Optional[str] = None,
    evaluated_at: Optional[datetime] = None,
) -> dict:
    """Authorize and start one ZAKI-managed capture through the existing spawn pipeline.

    Authority validation completes before any repository or runtime call. The caller cannot choose
    a less visible bot identity, disable transcription, or supply its own consent evidence.
    """
    evaluated_at = evaluated_at or datetime.now(timezone.utc)
    if meeting_url is not None:
        try:
            meeting_url = validate_meeting_url(meeting_url, platform=platform)
        except UnsafeMeetingUrl:
            raise CaptureDenied(CaptureDenial.MEETING_URL_INVALID) from None
        expected_url_hash = authority.meeting_url_sha256
        actual_url_hash = hashlib.sha256(meeting_url.encode()).hexdigest()
        if (
            not isinstance(expected_url_hash, str)
            or len(expected_url_hash) != 64
            or not hmac.compare_digest(expected_url_hash, actual_url_hash)
        ):
            raise CaptureDenied(CaptureDenial.AUTHORITY_SCOPE_MISMATCH)
    elif authority.meeting_url_sha256 is not None:
        raise CaptureDenied(CaptureDenial.AUTHORITY_SCOPE_MISMATCH)
    metadata = _capture_evidence(
        authority,
        tenant_id=tenant_id,
        user_id=user_id,
        platform=platform,
        native_meeting_id=native_meeting_id,
        evaluated_at=evaluated_at,
    )
    scope_expiries = metadata["zaki_retention"]["scope_expiries"]
    capture_expires_at = min(
        datetime.fromisoformat(scope_expiries[scope])
        for scope in ("audio", "transcript", "summary")
    )
    capture_lifetime_seconds = max(
        1, math.ceil((capture_expires_at - evaluated_at).total_seconds())
    )
    capture_repo = _CaptureEvidenceRepo(repo, metadata)
    try:
        return await request_bot(
            capture_repo,
            runtime,
            user_id=user_id,
            platform=platform,
            native_meeting_id=native_meeting_id,
            bot_name=ZAKI_NOTETAKER_NAME,
            passcode=passcode,
            meeting_url=meeting_url,
            language=language,
            task=task,
            recording_enabled=True,
            transcribe_enabled=True,
            operator_transcription_only=True,
            continue_meeting=False,
            max_concurrent=max_concurrent,
            redis_url=redis_url,
            meeting_api_url=meeting_api_url,
            token_secret=token_secret,
            capture_expires_at=capture_expires_at.isoformat(),
            invocation_contract="invocation.v2",
            managed_retention={
                "policyVersion": authority.tenant_policy_version,
                "scopeExpiresAt": {
                    scope: scope_expiries[scope]
                    for scope in ("audio", "transcript", "summary")
                },
            },
            max_lifetime_sec=capture_lifetime_seconds,
        )
    except CaptureGrantConsumed as error:
        raise CaptureDenied(CaptureDenial.AUTHORITY_REPLAYED) from error
    except (MaxBotsExceeded, QuotaExceeded) as error:
        raise CaptureDenied(CaptureDenial.QUOTA_EXHAUSTED) from error
    except TeardownUnconfirmed:
        raise CaptureTeardownUnconfirmed() from None


async def withdraw_capture(
    repo: MeetingRepo,
    publisher: CaptureStopPublisher,
    *,
    carrier_fencer: CaptureCarrierFencer,
    tenant_id: str,
    user_id: int,
    platform: str,
    native_meeting_id: str,
    runtime: Optional[RuntimeClient] = None,
    withdrawn_at: Optional[datetime] = None,
) -> dict:
    """Durably withdraw capture, then ask the bot to leave.

    The repository takes the exclusive side of the meeting write barrier before storing the
    withdrawal. Immediately afterward, the permanent Redis raw+processed fence closes the bot and
    Agent egress paths before runtime teardown starts. A fence failure never skips authoritative
    teardown, but it does return the content-free retryable pending outcome. The receipt is
    content-free.
    """
    withdrawn_at = withdrawn_at if withdrawn_at is not None else datetime.now(timezone.utc)
    if (
        not isinstance(withdrawn_at, datetime)
        or withdrawn_at.tzinfo is None
        or withdrawn_at.utcoffset() is None
    ):
        raise CaptureDenied(CaptureDenial.USER_REQUEST_INVALID)
    if (
        isinstance(user_id, bool)
        or not isinstance(user_id, int)
        or user_id <= 0
        or not isinstance(tenant_id, str)
        or not tenant_id.strip()
        or len(tenant_id) > 128
        or any(ord(character) < 32 for character in tenant_id)
        or not isinstance(platform, str)
        or not platform
        or not isinstance(native_meeting_id, str)
        or not native_meeting_id
    ):
        raise CaptureDenied(CaptureDenial.AUTHORITY_SCOPE_MISMATCH)
    result = await repo.withdraw_capture(
        tenant_id=tenant_id,
        user_id=user_id,
        platform=platform,
        native_meeting_id=native_meeting_id,
        withdrawn_at=withdrawn_at.isoformat(),
    )
    if result is None:
        raise CaptureDenied(CaptureDenial.AUTHORITY_SCOPE_MISMATCH)
    meeting = result["meeting"]
    carrier_fence_confirmed = False
    try:
        await carrier_fencer(meeting["id"], raw=True, processed=True)
        carrier_fence_confirmed = True
    except Exception as error:  # noqa: BLE001 — stop still runs; caller receives retryable pending
        log_event(
            "capture_withdraw_carrier_fence_failed",
            audience="system",
            level="error",
            span="capture.withdraw",
            user_id=user_id,
            meeting_id=str(meeting["id"]),
            fields={"error_type": type(error).__name__},
        )
    if result["should_stop"]:
        meeting_id = meeting["id"]
        capture = meeting["data"]["zaki_capture"]
        teardown_already_confirmed = capture.get("teardown_state") == "confirmed"
        if teardown_already_confirmed:
            # Upgrade legacy rows that recorded physical teardown before the terminal projection
            # was introduced. The same narrow CAS is safe and must not re-delete the workload.
            try:
                legacy_terminalized = await confirm_capture_teardown_with_retry(
                    repo,
                    meeting_id=meeting_id,
                )
            except Exception as error:  # noqa: BLE001 — durable terminal evidence is still missing
                log_event(
                    "capture_withdraw_confirmation_persist_failed",
                    audience="system",
                    level="error",
                    span="capture.withdraw",
                    user_id=user_id,
                    meeting_id=str(meeting_id),
                    fields={"error_type": type(error).__name__},
                )
                raise CaptureTeardownUnconfirmed() from None
            if not legacy_terminalized:
                log_event(
                    "capture_withdraw_confirmation_persist_failed",
                    audience="system",
                    level="error",
                    span="capture.withdraw",
                    user_id=user_id,
                    meeting_id=str(meeting_id),
                    fields={"reason": "legacy_terminal_cas_not_applied"},
                )
                raise CaptureTeardownUnconfirmed() from None
            meeting["status"] = "completed"
            meeting["data"]["completion_reason"] = "stopped"
        else:
            # The runtime kernel is the primary stop path. It may terminate gracefully itself, and
            # its successful 2xx is authoritative. Running it before best-effort pub/sub prevents a
            # fast bot exit from turning the subsequent DELETE into an ambiguous 404.
            workload_id = meeting.get("bot_container_id")
            hard_teardown_confirmed = False
            if runtime is None or not workload_id:
                log_event(
                    "capture_withdraw_teardown_unconfirmed",
                    audience="system",
                    level="error",
                    span="capture.withdraw",
                    user_id=user_id,
                    meeting_id=str(meeting_id),
                    fields={"reason": "runtime_or_workload_missing"},
                )
            else:
                try:
                    await runtime.delete_workload(workload_id)
                    hard_teardown_confirmed = True
                except Exception as error:  # noqa: BLE001 — report a stable pending outcome
                    log_event(
                        "capture_withdraw_teardown_unconfirmed",
                        audience="system",
                        level="error",
                        span="capture.withdraw",
                        user_id=user_id,
                        meeting_id=str(meeting_id),
                        fields={"error_type": type(error).__name__},
                    )

            confirmation_persisted = False
            if hard_teardown_confirmed:
                try:
                    confirmation_persisted = await confirm_capture_teardown_with_retry(
                        repo,
                        meeting_id=meeting_id,
                    )
                    if not confirmation_persisted:
                        log_event(
                            "capture_withdraw_confirmation_persist_failed",
                            audience="system",
                            level="error",
                            span="capture.withdraw",
                            user_id=user_id,
                            meeting_id=str(meeting_id),
                            fields={"reason": "cas_not_applied"},
                        )
                except Exception as error:  # noqa: BLE001 — physical stop lacks durable evidence
                    log_event(
                        "capture_withdraw_confirmation_persist_failed",
                        audience="system",
                        level="error",
                        span="capture.withdraw",
                        user_id=user_id,
                        meeting_id=str(meeting_id),
                        fields={"error_type": type(error).__name__},
                    )

            # Pub/sub remains a courtesy signal only. Its subscriber count is intentionally ignored:
            # delivery says nothing about command execution or workload termination.
            try:
                await publisher.publish(
                    leave_command_channel(meeting_id),
                    json.dumps(leave_command_payload(meeting_id)),
                )
            except Exception as error:  # noqa: BLE001 — kernel result remains authoritative
                log_event(
                    "capture_withdraw_leave_publish_failed",
                    audience="system",
                    level="warning",
                    span="capture.withdraw",
                    user_id=user_id,
                    meeting_id=str(meeting_id),
                    fields={"error_type": type(error).__name__},
                )

            # A physical stop without its durable confirmed+terminal CAS is not enough to unblock
            # GDPR erasure. Surface the same content-free pending outcome for either missing proof.
            if not hard_teardown_confirmed or not confirmation_persisted:
                raise CaptureTeardownUnconfirmed() from None
            meeting["data"]["zaki_capture"] = {
                **capture,
                "teardown_state": "confirmed",
            }
            meeting["status"] = "completed"
            meeting["data"]["completion_reason"] = "stopped"
    if not carrier_fence_confirmed:
        raise CaptureTeardownUnconfirmed() from None
    capture = meeting["data"]["zaki_capture"]
    return {
        "meeting_id": meeting["id"],
        "state": "withdrawn",
        "changed": result["changed"],
        "withdrawn_at": capture["withdrawn_at"],
    }
