"""Build the bot's invocation (BOT_CONFIG) + runtime workload spec against an explicit contract.

Ordinary bots remain on sealed ``invocation.v1`` + runtime profile ``meeting-bot``. Managed
Minutes capture uses ``invocation.v2`` + the separately enabled ``meeting-bot-v2`` profile.

The parent ``meetings.request_bot`` assembled a ``BOT_CONFIG`` dict, minted a stateless
``MeetingToken`` (HS256 JWT) into it, and POSTed a spawn request to the runtime API. This carve
ports the CORE of that:

  * ``mint_meeting_token(...)`` — a hand-rolled HS256 MeetingToken signed only by the dedicated
    ``MEETING_TOKEN_SECRET``;
    claims: meeting_id/user_id/platform/native_meeting_id/scope/iss/aud/iat/exp/jti). The bot carries
    it and the recording-upload endpoint re-verifies it.
  * ``build_invocation(...)`` — the parent's ``BOT_CONFIG`` as an ``invocation.v1`` ``Invocation``
    (camelCase fields, ``None`` stripped). Validated against the sealed schema before it ships.
  * ``build_workload_spec(...)`` — wrap the invocation as the ONE env var the bot reads
    (``BOT_CONFIG``) inside a ``runtime.v1`` ``WorkloadSpec`` (``profile="meeting-bot"``), validated
    against the sealed schema.

continue_meeting / max-bots / join-retry are P3 — NOT here; ``request_bot`` leaves the seam.
"""
from __future__ import annotations

import base64
import binascii
import hmac
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

import jsonschema
from referencing import Registry, Resource

# ── sealed-schema loaders (the seam, P8 — by path, not import) ──────────────────────────────────


def _load_schema(rel: Path) -> dict:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / rel
        if candidate.is_file():
            return json.loads(candidate.read_text())
    raise FileNotFoundError(f"sealed contract not found by path: {rel}")


_INVOCATION_SCHEMAS = {
    version: _load_schema(
        Path("meetings") / "contracts" / version / "invocation.schema.json"
    )
    for version in ("invocation.v1", "invocation.v2")
}
_RUNTIME_SCHEMA = _load_schema(
    Path("runtime") / "contracts" / "runtime.v1" / "runtime.schema.json"
)
_INV_REGISTRIES = {
    version: Registry().with_resource(schema["$id"], Resource.from_contents(schema))
    for version, schema in _INVOCATION_SCHEMAS.items()
}
_RT_REGISTRY = Registry().with_resource(
    _RUNTIME_SCHEMA["$id"], Resource.from_contents(_RUNTIME_SCHEMA)
)


def _conforms(obj: dict, schema: dict, registry: Registry, shape: str) -> None:
    jsonschema.Draft202012Validator(
        {"$ref": f"{schema['$id']}#/$defs/{shape}"}, registry=registry
    ).validate(obj)


def conforms_invocation(obj: dict, *, contract_version: str = "invocation.v1") -> None:
    """Validate ``obj`` against the selected invocation contract (v1 is the legacy default)."""
    schema = _INVOCATION_SCHEMAS.get(contract_version)
    registry = _INV_REGISTRIES.get(contract_version)
    if schema is None or registry is None:
        raise ValueError(f"unsupported invocation contract: {contract_version}")
    _conforms(obj, schema, registry, "Invocation")


def conforms_workload_spec(obj: dict) -> None:
    """Validate ``obj`` against ``runtime.v1#/$defs/WorkloadSpec`` (raises on non-conformance)."""
    _conforms(obj, _RUNTIME_SCHEMA, _RT_REGISTRY, "WorkloadSpec")


def conforms_runtime_event(obj: dict) -> None:
    """Validate one runtime callback against ``runtime.v1#/$defs/RuntimeEvent``."""
    jsonschema.Draft202012Validator(
        {"$ref": f"{_RUNTIME_SCHEMA['$id']}#/$defs/RuntimeEvent"},
        registry=_RT_REGISTRY,
    ).validate(obj)
    # The lean meeting-api jsonschema install intentionally has no optional RFC3339 format plugin,
    # so enforce the contract's date-time assertion explicitly instead of silently treating it as
    # an annotation.
    try:
        at = obj["at"]
        parsed = datetime.fromisoformat(at[:-1] + "+00:00" if at.endswith("Z") else at)
    except (KeyError, TypeError, ValueError, AttributeError):
        raise jsonschema.ValidationError("'at' must be an RFC3339 date-time") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise jsonschema.ValidationError("'at' must include a UTC offset")


# ── MeetingToken (HS256 JWT) ───────────────────────────────────────────────────────────────────


_MEETING_TOKEN_ISSUER = "meeting-api"
_MEETING_TOKEN_AUDIENCES = ["transcription-collector", "meeting-lifecycle"]
_MEETING_TOKEN_SCOPE = "transcribe:write lifecycle:write"


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def mint_meeting_token(
    meeting_id: int,
    user_id: int,
    platform: str,
    native_meeting_id: str,
    *,
    session_uid: str,
    ttl_seconds: int = 7200,
    secret: Optional[str] = None,
) -> str:
    """Mint a stateless MeetingToken (HS256 JWT), signed with ``MEETING_TOKEN_SECRET`` (or ``secret``).

    The opaque invocation token is deliberately multi-endpoint but tightly bounded: it can write a
    transcript/recording and report lifecycle only for one immutable meeting/session pair. It never
    carries a platform-wide service credential or the signing key itself.
    """
    secret = secret if secret is not None else os.environ.get("MEETING_TOKEN_SECRET")
    if not secret:
        raise ValueError("MEETING_TOKEN_SECRET not configured; cannot mint MeetingToken")
    if isinstance(meeting_id, bool) or not isinstance(meeting_id, int) or not 0 < meeting_id <= 2**63 - 1:
        raise ValueError("MeetingToken meeting identity missing or invalid")
    if not isinstance(session_uid, str) or not session_uid or len(session_uid) > 256:
        raise ValueError("MeetingToken session identity missing or invalid")
    if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int) or ttl_seconds < 1:
        raise ValueError("MeetingToken lifetime must be a positive integer")
    now = int(datetime.now(timezone.utc).timestamp())
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "meeting_id": meeting_id,
        "user_id": user_id,
        "platform": platform,
        "native_meeting_id": native_meeting_id,
        "session_uid": session_uid,
        "scope": _MEETING_TOKEN_SCOPE,
        "iss": _MEETING_TOKEN_ISSUER,
        "aud": _MEETING_TOKEN_AUDIENCES,
        "iat": now,
        "exp": now + ttl_seconds,
        "jti": str(uuid.uuid4()),
    }
    header_b64 = _b64url(json.dumps(header, separators=(",", ":")).encode())
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    signature = hmac.new(secret.encode(), signing_input, digestmod="sha256").digest()
    return f"{header_b64}.{payload_b64}.{_b64url(signature)}"


def _decode_b64url(segment: str) -> bytes:
    if not isinstance(segment, str) or not segment:
        raise ValueError("malformed MeetingToken")
    try:
        return base64.b64decode(
            segment + "=" * (-len(segment) % 4), altchars=b"-_", validate=True
        )
    except (ValueError, binascii.Error):
        raise ValueError("malformed MeetingToken") from None


def verify_meeting_token(
    token: str,
    *,
    purpose: Literal["recording", "lifecycle", "transcript"],
    secret: Optional[str] = None,
) -> dict[str, Any]:
    """Strictly verify one spawn-scoped MeetingToken for an intended write path.

    Every allowed purpose is explicit in the minted token. Exact JOSE, audience, and scope values
    reject algorithm confusion, single-purpose legacy tokens, and permission stuffing. Callers must
    additionally bind ``meeting_id`` and ``session_uid`` to their authoritative repository lookup.
    """
    if purpose not in {"recording", "lifecycle", "transcript"}:
        raise ValueError("unsupported MeetingToken purpose")
    secret = secret if secret is not None else os.environ.get("MEETING_TOKEN_SECRET")
    if not isinstance(secret, str) or not secret:
        raise ValueError("MEETING_TOKEN_SECRET not configured; cannot verify MeetingToken")
    if not isinstance(token, str):
        raise ValueError("malformed MeetingToken")
    try:
        header_b64, payload_b64, sig_b64 = token.split(".")
        header = json.loads(_decode_b64url(header_b64))
        claims = json.loads(_decode_b64url(payload_b64))
        got = _decode_b64url(sig_b64)
    except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("malformed MeetingToken") from None
    if header != {"alg": "HS256", "typ": "JWT"} or not isinstance(claims, dict):
        raise ValueError("MeetingToken JOSE profile mismatch")
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    expected = hmac.new(secret.encode(), signing_input, digestmod="sha256").digest()
    if len(got) != len(expected) or not hmac.compare_digest(expected, got):
        raise ValueError("MeetingToken signature mismatch")
    if (
        claims.get("iss") != _MEETING_TOKEN_ISSUER
        or claims.get("aud") != _MEETING_TOKEN_AUDIENCES
        or claims.get("scope") != _MEETING_TOKEN_SCOPE
    ):
        raise ValueError("MeetingToken purpose mismatch")
    required_audience = {
        "recording": "transcription-collector",
        "lifecycle": "meeting-lifecycle",
        "transcript": "transcription-collector",
    }[purpose]
    if required_audience not in claims["aud"]:
        raise ValueError("MeetingToken purpose mismatch")
    meeting_id = claims.get("meeting_id")
    if (
        isinstance(meeting_id, bool)
        or not isinstance(meeting_id, int)
        or not 0 < meeting_id <= 2**63 - 1
    ):
        raise ValueError("MeetingToken meeting identity missing or invalid")
    session_uid = claims.get("session_uid")
    if not isinstance(session_uid, str) or not session_uid or len(session_uid) > 256:
        raise ValueError("MeetingToken session identity missing or invalid")
    issued_at = claims.get("iat")
    expires_at = claims.get("exp")
    if type(issued_at) is not int or type(expires_at) is not int or expires_at <= issued_at:
        raise ValueError("MeetingToken expiry missing or invalid")
    now = int(datetime.now(timezone.utc).timestamp())
    if issued_at > now + 60:
        raise ValueError("MeetingToken issued-at time is invalid")
    if now >= expires_at:
        raise ValueError("MeetingToken expired")
    return claims


# ── invocation + workload-spec builders ─────────────────────────────────────────────────────────


def build_invocation(
    *,
    meeting_id: int,
    platform: str,
    meeting_url: Optional[str],
    bot_name: str,
    passcode: Optional[str] = None,
    token: str,
    native_meeting_id: Optional[str],
    connection_id: str,
    language: Optional[str] = None,
    task: Optional[str] = None,
    transcription_tier: str = "realtime",
    redis_url: Optional[str] = None,
    transcript_ingest_url: Optional[str] = None,
    retention_fence_url: Optional[str] = None,
    automatic_leave: Optional[dict] = None,
    meeting_api_callback_url: Optional[str] = None,
    transcribe_enabled: bool = True,
    recording_enabled: bool = False,
    capture_modes: Optional[list[str]] = None,
    recording_upload_url: Optional[str] = None,
    transcription_service_url: Optional[str] = None,
    transcription_service_token: Optional[str] = None,
    capture_expires_at: Optional[str] = None,
    managed_retention: Optional[dict] = None,
    contract_version: str = "invocation.v1",
) -> dict:
    """Assemble a version-routed invocation (the parent's ``BOT_CONFIG``).

    v1 is unchanged and rejects managed-only fields. v2 requires the immutable per-scope
    retention authority and serializes ``meeting_id`` as a canonical decimal string.
    """
    if contract_version == "invocation.v1":
        if capture_expires_at is not None or managed_retention is not None:
            raise ValueError("managed retention requires invocation.v2")
        if not isinstance(redis_url, str) or not redis_url:
            raise ValueError("invocation.v1 requires redis_url")
        if transcript_ingest_url is not None or retention_fence_url is not None:
            raise ValueError("managed transcript HTTP ingress requires invocation.v2")
        wire_meeting_id: int | str = meeting_id
    elif contract_version == "invocation.v2":
        if capture_expires_at is None or managed_retention is None:
            raise ValueError("invocation.v2 requires capture deadline and managed retention")
        if redis_url is not None:
            raise ValueError("invocation.v2 forbids direct Redis authority")
        if (
            not isinstance(transcript_ingest_url, str)
            or not transcript_ingest_url
            or not isinstance(retention_fence_url, str)
            or not retention_fence_url
        ):
            raise ValueError("invocation.v2 requires managed transcript HTTP endpoints")
        if isinstance(meeting_id, bool) or not isinstance(meeting_id, int) or not 0 < meeting_id <= 2**63 - 1:
            raise ValueError("invocation.v2 meeting_id must be a positive signed-bigint")
        wire_meeting_id = str(meeting_id)
    else:
        raise ValueError(f"unsupported invocation contract: {contract_version}")

    invocation: dict[str, Any] = {
        "contractVersion": contract_version if contract_version == "invocation.v2" else None,
        "platform": platform,
        "meetingUrl": meeting_url,
        "botName": bot_name,
        "passcode": passcode,
        "nativeMeetingId": native_meeting_id,
        "token": token,
        "connectionId": connection_id,
        "meeting_id": wire_meeting_id,
        "redisUrl": redis_url if contract_version == "invocation.v1" else None,
        "transcriptIngestUrl": (
            transcript_ingest_url if contract_version == "invocation.v2" else None
        ),
        "retentionFenceUrl": (
            retention_fence_url if contract_version == "invocation.v2" else None
        ),
        "language": language,
        "task": task,
        "transcriptionTier": transcription_tier,
        "transcribeEnabled": transcribe_enabled,
        "transcriptionServiceUrl": transcription_service_url,
        "transcriptionServiceToken": transcription_service_token,
        "recordingEnabled": recording_enabled,
        "captureModes": capture_modes,
        "recordingUploadUrl": recording_upload_url,
        "meetingApiCallbackUrl": meeting_api_callback_url,
        "automaticLeave": automatic_leave,
        "captureExpiresAt": capture_expires_at,
        "managedRetention": managed_retention,
    }
    invocation = {k: v for k, v in invocation.items() if v is not None}
    conforms_invocation(invocation, contract_version=contract_version)
    if contract_version == "invocation.v2":
        scope_expiries = managed_retention.get("scopeExpiresAt")
        try:
            values = [
                datetime.fromisoformat(str(scope_expiries[scope]).replace("Z", "+00:00"))
                for scope in ("audio", "transcript", "summary")
            ]
            capture_deadline = datetime.fromisoformat(capture_expires_at.replace("Z", "+00:00"))
        except (AttributeError, KeyError, TypeError, ValueError):
            raise ValueError("invocation.v2 retention timestamps are invalid") from None
        if (
            capture_deadline.tzinfo is None
            or capture_deadline.utcoffset() is None
            or any(value.tzinfo is None or value.utcoffset() is None for value in values)
            or capture_deadline != min(values)
        ):
            raise ValueError("captureExpiresAt must equal the earliest managed retention expiry")
    return invocation


def build_workload_spec(
    *,
    workload_id: str,
    invocation: dict,
    callback_url: Optional[str] = None,
    extra_env: Optional[dict[str, str]] = None,
    max_lifetime_sec: Optional[int] = None,
    invocation_contract: str = "invocation.v1",
    profile: str = "meeting-bot",
) -> dict:
    """Wrap ``invocation`` as the bot's ONE config env var (``VEXA_BOT_CONFIG``) inside a ``runtime.v1``
    ``WorkloadSpec`` (``profile="meeting-bot"``). The bot image resolves from the kernel's profile
    registry — NOT carried in the spec. Validated against the sealed schema.

    The sealed ``invocation.v1`` contract (ADR-0002) names this env var ``VEXA_BOT_CONFIG`` — what the
    carved v0.12 bot (``config.ts``) and the runtime profile read. We ALSO emit the legacy ``BOT_CONFIG``
    alias so the 0.11-derived published image (``vexaai/vexa-bot:dev``) still boots; ``VEXA_BOT_CONFIG``
    is authoritative. (The mock-bot L3 lane surfaced this: the carved bot got no config under ``BOT_CONFIG``.)"""
    expected_profile = {
        "invocation.v1": "meeting-bot",
        "invocation.v2": "meeting-bot-v2",
    }.get(invocation_contract)
    if expected_profile is None:
        raise ValueError(f"unsupported invocation contract: {invocation_contract}")
    if profile != expected_profile:
        raise ValueError(
            f"{invocation_contract} requires runtime profile {expected_profile}; got {profile}"
        )
    conforms_invocation(invocation, contract_version=invocation_contract)
    payload = json.dumps(invocation, separators=(",", ":"))
    env: dict[str, str] = {"VEXA_BOT_CONFIG": payload, "BOT_CONFIG": payload}
    if invocation_contract == "invocation.v2":
        env["VEXA_INVOCATION_CONTRACT"] = invocation_contract
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items()})
    spec: dict[str, Any] = {
        "workloadId": workload_id,
        "profile": profile,
        "env": env,
    }
    if callback_url:
        spec["callbackUrl"] = callback_url
    if max_lifetime_sec is not None:
        spec["maxLifetimeSec"] = max_lifetime_sec
    conforms_workload_spec(spec)
    return spec
