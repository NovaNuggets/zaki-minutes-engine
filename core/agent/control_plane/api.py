"""api.py — the agent-api HTTP front door (the unit control plane's entrypoint).

A thin FastAPI surface mirroring ``runtime_kernel/api.py``. Routes (the gateway api.v1 proxies these):
  POST /invocations          — the dispatcher sink: a unit.v1 dispatch → a runtime.v1 agent spawn
  POST /api/chat             — a chat *now*-dispatch, streamed back as an SSE VIEW of its Stream
  POST /api/chat/reset       — drop a session
  GET  /api/sessions         — list a subject's sessions
  GET  /api/routines …       — routines (compile to schedule.v1 cron jobs)
  POST /events               — the generic event ingress (event.v1 → unit.v1)
  GET  /api/workspace/…      — read the workspace tree/file
  GET  /health               — liveness

Chat is **not** run in-process (agents never run in the control plane). ``/api/chat`` builds a now
dispatch, asks the Dispatcher to spawn the isolated container, then RELAYS the dispatch's output Stream
(``unit:<id>:out``) as SSE via the injected ``StreamReader``. When no reader is wired it answers ``501``
honestly. Built lazily (PEP 562) so ``uvicorn control_plane.api:app`` wires the real adapters at startup.
"""
from __future__ import annotations

import asyncio
import os

from collections.abc import Mapping
import hashlib
import hmac
import json
import logging
import re
import secrets
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterator, Optional
from urllib.parse import unquote, urlsplit

from fastapi import Body, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse
from jsonschema.exceptions import ValidationError
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from control_plane import meeting_steering
from control_plane.minutes_erasure import ErasureNotFound, ErasurePending
from control_plane.minutes_ingest import MinutesIngestDisabled, MinutesIngestError
from control_plane.gateway_identity import (
    GatewayReplayUnavailable,
    GatewayIdentityVerifier,
    InMemoryGatewayReplayStore,
    RedisGatewayReplayStore,
)
from control_plane import schedule_digest as schedule_digest_mod
from control_plane import routines as routines_mod
from control_plane.config_preflight import NOT_CONFIGURED, capability_state, missing_capability_keys
from shared import units
from control_plane import workspace_routines as workspace_routines_mod
from shared.agent_config import default_meeting_model, load_meeting_config
from shared.meeting_retention import activate_processing_if_writable, bind_processing_deadline
from shared.http import open_no_redirect, read_json_bounded
from shared.seeding import resolve_seed_dir, seed_workspace, validate_seed
from control_plane.workspace_attach import (
    CloneError,
    activate_workspace,
    active_workspaces,
    attached_workspaces,
    create_shared_workspace_dir,
    create_workspace,
    deactivate_workspace,
    delete_workspace,
    ensure_workspace_private,
    ensure_workspace_shareable,
    rename_workspace,
    set_archived,
    set_shared_active,
    shared_active_mounts,
    swap_workspace,
    workspace_dir_for,
)
from control_plane.workspace_publish import PublishError, RepoExistsError, publish_workspace, published_remote_url
from control_plane.workspace_git_sync import RemoteSyncError, pull_origin, push_origin, remote_status
from control_plane.workspace_purpose import read_purpose, write_purpose
from control_plane import workspace_membership as membership_mod
from control_plane import git_credentials as git_creds
from control_plane import system_mounts
from control_plane.workspace_membership import MembershipError, MembershipIndex, InMemoryMembershipIndex
from control_plane.dispatch import Dispatcher
from control_plane.events import event_to_invocation
from shared.ports import SchedulerPort, StreamReader
from control_plane.workspace_reader import WorkspaceReader

logger = logging.getLogger("agent_api.api")
MAX_UPLOAD_BYTES = 25 * 1024 * 1024
MAX_UPLOAD_TOTAL_BYTES = 25 * 1024 * 1024
MAX_UPLOAD_FILES = 100
UPLOAD_READ_CHUNK_BYTES = 64 * 1024
MEETING_STREAM_TRANSCRIPT_REPLAY = 80
MEETING_STREAM_OUTPUT_REPLAY = 160
MAX_MINUTES_ERASURE_REQUEST_BYTES = 1024
MAX_GATEWAY_SIGNED_BODY_BYTES = 32 * 1024 * 1024
GATEWAY_SIGNED_BODY_READ_TIMEOUT_SECONDS = 10.0
_TRUE_OPERATOR_FLAGS = frozenset({"1", "true", "yes", "on"})
_FALSE_OPERATOR_FLAGS = frozenset({"", "0", "false", "no", "off"})
# How long the SSE keeps draining after session_end when the copilot HAS written notes but its
# view_end marker hasn't arrived (the final beat is ~10s of LLM; a dead worker never marks) —
# the bounded cap that replaces the old one-empty-poll guess (ADR 0027).
MEETING_STREAM_ENDING_CAP_SEC = 45.0


def _upload_filename(name: str | None) -> str:
    base = (name or "upload").replace("\\", "/").rsplit("/", 1)[-1].strip()
    base = re.sub(r"\s+", "_", base)
    base = re.sub(r"[^A-Za-z0-9._-]", "_", base).strip("._-")
    return base[:160] or "upload"


def _truncate_title(text: str, *, limit: int = 60) -> str:
    """A session's default title — the first prompt, single-lined + truncated."""
    title = " ".join((text or "").split())
    return title[: limit - 1] + "…" if len(title) > limit else title


def _stream_tail_id(redis_url: str | None, stream: str) -> str | None:
    if not redis_url:
        return None
    try:
        import redis

        r = redis.from_url(redis_url, decode_responses=True)
        rows = r.xrevrange(stream, count=1)
        return str(rows[0][0]) if rows else "0-0"
    except Exception as exc:
        logger.warning("could not resolve transcript stream tail for %s: %s", stream, exc)
        return None


# How long a turn's start-cursor record lives — covers the client's whole resume window (its hard
# timeout is minutes); after this a stale nonce simply falls back to the fresh-dispatch path.
CHAT_TURN_HEAD_TTL_SEC = 900


def _chat_turn_head_key(unit_id: str) -> str:
    return f"unit:{unit_id}:turnhead"


def _record_chat_turn_head(redis_url: str | None, unit_id: str, turn_id: str, start: str) -> None:
    """Remember, per warm chat unit, the CURRENT turn's nonce + the out-Stream id it started after.
    Best-effort (redis-less unit tests skip it): losing the record only degrades a no-cursor retry
    back to today's behavior, it never breaks the turn."""
    if not redis_url:
        return
    try:
        import redis

        r = redis.from_url(redis_url, decode_responses=True)
        r.set(_chat_turn_head_key(unit_id), json.dumps({"turn_id": turn_id, "start": start}),
              ex=CHAT_TURN_HEAD_TTL_SEC)
    except Exception as exc:  # noqa: BLE001
        logger.warning("could not record chat turn head for %s: %s", unit_id, exc)


def _chat_turn_head(redis_url: str | None, unit_id: str, turn_id: str) -> str | None:
    """The recorded start cursor of the turn ``turn_id`` — or None when it isn't the current turn."""
    if not redis_url or not turn_id:
        return None
    try:
        import redis

        r = redis.from_url(redis_url, decode_responses=True)
        raw = r.get(_chat_turn_head_key(unit_id))
        head = json.loads(raw) if raw else None
        return head["start"] if head and head.get("turn_id") == turn_id else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("could not read chat turn head for %s: %s", unit_id, exc)
        return None


class _Sessions:
    """Durable, per-subject chat-session index. Each session carries a created + last-active stamp and an
    optional title (default the first prompt, truncated). ``list`` returns them most-recent first.

    Backed by redis when a client is wired (one hash per session under ``agent:sessions:<subject>`` +
    the per-subject id set), with an in-memory fallback so the unit tests need no redis. Multiple
    conversation threads live in the ONE user workspace — this indexes the threads, not workspaces."""

    def __init__(self, redis_client=None) -> None:
        self._redis = redis_client
        self._mem: dict[str, dict[str, dict]] = {}  # subject → {session → {created,last_active,title}}

    # ── redis key helpers ──
    @staticmethod
    def _ids_key(subject: str) -> str:
        return f"agent:sessions:{subject}"

    @staticmethod
    def _meta_key(subject: str, session: str) -> str:
        return f"agent:session:{subject}:{session}"

    def _now(self) -> float:
        import time

        return time.time()

    def upsert(self, subject: str, session: str, *, title: str | None = None) -> None:
        """Record the session on use: create it (stamping ``created`` + a default ``title``) or touch its
        ``last_active``. An explicit ``title`` overrides; otherwise the first prompt seeds it once."""
        now = self._now()
        if self._redis is not None:
            mkey = self._meta_key(subject, session)
            existing = self._redis.hgetall(mkey) or {}
            fields = {"last_active": str(now)}
            if not existing:
                fields["created"] = str(now)
                fields["title"] = title or session
            elif title is not None:
                fields["title"] = title
            self._redis.hset(mkey, mapping=fields)
            self._redis.sadd(self._ids_key(subject), session)
            return
        rec = self._mem.setdefault(subject, {}).get(session)
        if rec is None:
            self._mem[subject][session] = {"created": now, "last_active": now, "title": title or session}
        else:
            rec["last_active"] = now
            if title is not None:
                rec["title"] = title

    def list(self, subject: str) -> list[dict]:
        """The subject's sessions, most-recently-active first."""
        rows: list[dict] = []
        if self._redis is not None:
            for session in self._redis.smembers(self._ids_key(subject)) or set():
                meta = self._redis.hgetall(self._meta_key(subject, session)) or {}
                rows.append({
                    "session": session,
                    "title": meta.get("title") or session,
                    "created": float(meta.get("created", 0) or 0),
                    "last_active": float(meta.get("last_active", 0) or 0),
                })
        else:
            for session, meta in self._mem.get(subject, {}).items():
                rows.append({
                    "session": session, "title": meta.get("title") or session,
                    "created": meta.get("created", 0.0), "last_active": meta.get("last_active", 0.0),
                })
        rows.sort(key=lambda r: r["last_active"], reverse=True)
        return rows

    def drop(self, subject: str, session: str) -> None:
        if self._redis is not None:
            self._redis.srem(self._ids_key(subject), session)
            self._redis.delete(self._meta_key(subject, session))
            return
        self._mem.get(subject, {}).pop(session, None)


# P21 (ADR 0027 family — the panel's stale-live finding): a registry entry is "live" only while
# segments actually FLOW. The watcher re-adds on every batch (~2s apart), so this much silence
# means the meeting is over even when the session_end frame was lost (e.g. a hot-reload racing the
# wire leaves it pending-unacked) — the server-side stale-"live" the terminal's durableTerminal
# guard was papering over.
LIVE_SILENCE_TTL_SEC = 60.0

# The processing desired-state flag's set-time backstop TTL (the watcher refreshes a rolling TTL per
# armed batch while segments flow — transcription_watcher.PROC_FLAG_ROLLING_TTL_SEC). Bounds a flag
# whose meeting never produces a segment; generous because the toggle is only offered on live rows.
PROC_FLAG_BACKSTOP_TTL_SEC = 4 * 3600


_AGENT_API_DIRECT_CREDENTIAL_ENV_NAMES = (
    # Agent-owned runtime, identity, dispatch, bot, STT, and cross-spoke boundaries.
    "VEXA_RUNTIME_CONTROL_SECRET",
    "RUNTIME_CONTROL_SECRET",
    "VEXA_AGENT_IDENTITY_TOKEN",
    "VEXA_DISPATCH_SIGNING_KEY",
    "VEXA_BOT_API_KEY",
    "TRANSCRIPTION_SERVICE_TOKEN",
    "ZAKI_READ_TOKEN_MINUTES",
    "GATEWAY_IDENTITY_SECRET",
    "GATEWAY_IDENTITY_PREVIOUS_SECRET",
    "VEXA_INTERNAL_API_SECRET",
    "INTERNAL_API_SECRET",
    # Model-provider credentials brokered through agent-api into isolated workers.
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "VEXA_LLM_API_KEY",
    # Raw Git credentials are not part of the supported brokered PAT path, but reject aliasing if
    # an operator has nevertheless made one ambient to agent-api.
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "GITLAB_TOKEN",
    "GL_TOKEN",
    "BITBUCKET_TOKEN",
    "GIT_TOKEN",
    "VEXA_GIT_TOKEN",
    # These credentials are normally absent/explicitly cleared on agent-api.  Inventorying them
    # closes direct-start and extra-environment projection mistakes without granting their use.
    "ADMIN_TOKEN",
    "ADMIN_API_TOKEN",
    "RUNTIME_CALLBACK_SECRET",
    "MEETING_TOKEN_SECRET",
    "REDIS_PASSWORD",
    "VEXA_REDIS_PASSWORD",
    "NEXTAUTH_SECRET",
    "DB_PASSWORD",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "GOOGLE_CLIENT_SECRET",
    "MICROSOFT_CLIENT_SECRET",
    "VEXA_API_KEY",
    "ZAKI_MINUTES_HUB_TOKEN",
    "ZAKI_AGENT_ERASURE_VERIFICATION_SECRET",
    "ZAKI_MINUTES_ERASURE_SIGNING_SECRET",
    "ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET",
    "ZAKI_MINUTES_FINALIZED_SECRET",
)
_AGENT_API_CREDENTIAL_URL_ENV_NAMES = (
    "VEXA_REDIS_URL",
    "REDIS_URL",
    "DATABASE_URL",
    "ANTHROPIC_BASE_URL",
    "VEXA_LLM_BASE_URL",
    "TRANSCRIPTION_SERVICE_URL",
    "ZAKI_MINUTES_READ_BASE_URL",
    "VEXA_RUNTIME_API_URL",
    "VEXA_GATEWAY_URL",
    "VEXA_ADMIN_API_URL",
    "VEXA_MEETING_API_URL",
)


def _agent_api_credential_values(
    environment: Mapping[str, object] | None,
    *,
    excluded_names: frozenset[str] = frozenset(),
) -> tuple[str, ...]:
    """Return credential material visible to agent-api without retaining names in failures."""

    if environment is None:
        return ()
    if not isinstance(environment, Mapping):
        raise RuntimeError("Agent erasure credential inventory is invalid")
    values = [
        value
        for name in _AGENT_API_DIRECT_CREDENTIAL_ENV_NAMES
        if name not in excluded_names
        if isinstance((value := environment.get(name)), str) and value
    ]
    for name in _AGENT_API_CREDENTIAL_URL_ENV_NAMES:
        if name in excluded_names:
            continue
        raw = environment.get(name)
        if not isinstance(raw, str) or not raw:
            continue
        try:
            parsed = urlsplit(raw)
            embedded = (parsed.username, parsed.password)
        except ValueError:
            continue
        values.extend(unquote(value) for value in embedded if value)
    return tuple(values)


def _valid_agent_erasure_secret(value: object) -> bool:
    return (
        isinstance(value, str)
        and 32 <= len(value) <= 512
        and value == value.strip()
        and all(0x20 <= ord(character) <= 0x7E for character in value)
    )


def _agent_erasure_secret_matches(left: str, right: object) -> bool:
    return (
        isinstance(right, str)
        and right.isascii()
        and hmac.compare_digest(left, right)
    )


def _validate_gateway_identity_config(
    *,
    required: bool,
    secret: object,
    previous_secret: object = "",
    internal_secret: object = "",
    credential_environment: "Mapping[str, object] | None" = None,
) -> None:
    """Validate the dedicated Gateway→Agent proof without exposing its material.

    A cluster-wide internal credential cannot prove Gateway origin because every holder could mint
    a request signature. The dedicated HMAC key is therefore required for hardened ingress/Minutes
    routes and must be both strong and distinct from every other credential visible to agent-api.
    """

    if secret in (None, ""):
        if previous_secret not in (None, ""):
            raise RuntimeError(
                "GATEWAY_IDENTITY_PREVIOUS_SECRET requires GATEWAY_IDENTITY_SECRET"
            )
        if required:
            raise RuntimeError(
                "GATEWAY_IDENTITY_SECRET is required when gateway identity is enforced"
            )
        return
    if not _valid_agent_erasure_secret(secret):
        raise RuntimeError(
            "GATEWAY_IDENTITY_SECRET must be unpadded printable ASCII between "
            "32 and 512 characters"
        )
    assert isinstance(secret, str)
    if previous_secret not in (None, ""):
        if not _valid_agent_erasure_secret(previous_secret):
            raise RuntimeError(
                "GATEWAY_IDENTITY_PREVIOUS_SECRET must be unpadded printable ASCII between "
                "32 and 512 characters"
            )
        assert isinstance(previous_secret, str)
        if _agent_erasure_secret_matches(secret, previous_secret):
            raise RuntimeError("Gateway identity rotation secrets must be distinct")
    other_credentials = (
        ((internal_secret,) if internal_secret else ())
        + _agent_api_credential_values(
            credential_environment,
            excluded_names=frozenset({
                "GATEWAY_IDENTITY_SECRET",
                "GATEWAY_IDENTITY_PREVIOUS_SECRET",
            }),
        )
    )
    gateway_secrets = (secret,) + (
        (previous_secret,) if isinstance(previous_secret, str) and previous_secret else ()
    )
    if any(
        _agent_erasure_secret_matches(gateway_secret, credential)
        for gateway_secret in gateway_secrets
        for credential in other_credentials
    ):
        raise RuntimeError(
            "GATEWAY_IDENTITY_SECRET must be distinct from every agent-api credential"
        )


def _security_redis_from_url(redis_url: str):
    """Build the synchronous security-state client with finite failure latency."""
    from redis import Redis

    return Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=2.0,
        socket_timeout=2.0,
        retry_on_timeout=False,
        health_check_interval=30,
    )


def _validate_agent_erasure_signing_config(
    *,
    capture_enabled: "str | None",
    key_id: "str | None",
    secret: "str | None",
    previous_key_id: "str | None" = None,
    previous_secret: "str | None" = None,
    internal_secret: "str | None" = None,
    credential_environment: "Mapping[str, object] | None" = None,
) -> None:
    """Require the operator-owned receipt signer only when managed capture is active."""

    raw = (capture_enabled or "").strip().lower()
    if raw not in _TRUE_OPERATOR_FLAGS:
        if raw not in _FALSE_OPERATOR_FLAGS:
            raise RuntimeError("ZAKI_MINUTES_CAPTURE_ENABLED must be a boolean operator flag")
        previous_configured = any(
            value is not None and value != ""
            for value in (previous_key_id, previous_secret)
        )
        if previous_configured:
            raise RuntimeError(
                "Agent erasure previous verification key requires "
                "ZAKI_MINUTES_CAPTURE_ENABLED=true"
            )
        return
    if not isinstance(key_id, str) or not _ERASURE_KEY_ID.fullmatch(key_id):
        raise RuntimeError(
            "ZAKI_MINUTES_CAPTURE_ENABLED requires ZAKI_AGENT_ERASURE_SIGNING_KEY_ID"
        )
    if not isinstance(secret, str) or not secret:
        raise RuntimeError(
            "ZAKI_MINUTES_CAPTURE_ENABLED requires ZAKI_AGENT_ERASURE_SIGNING_SECRET"
        )
    if not _valid_agent_erasure_secret(secret):
        raise RuntimeError(
            "ZAKI_AGENT_ERASURE_SIGNING_SECRET must be unpadded printable ASCII "
            "between 32 and 512 characters"
        )
    has_previous_id = previous_key_id is not None and previous_key_id != ""
    has_previous_secret = previous_secret is not None and previous_secret != ""
    if has_previous_id != has_previous_secret:
        raise RuntimeError(
            "Agent erasure previous verification key id and secret must be configured together"
    )
    if has_previous_id and (
        not isinstance(previous_key_id, str)
        or not _ERASURE_KEY_ID.fullmatch(previous_key_id)
        or previous_key_id == key_id
        or not _valid_agent_erasure_secret(previous_secret)
        or _agent_erasure_secret_matches(secret, previous_secret)
    ):
        raise RuntimeError("Agent erasure previous verification key is invalid")
    erasure_secrets = (secret,) + ((previous_secret,) if has_previous_secret else ())
    other_credentials = (
        ((internal_secret,) if internal_secret else ())
        + _agent_api_credential_values(credential_environment)
    )
    if any(
        _agent_erasure_secret_matches(erasure_secret, credential)
        for erasure_secret in erasure_secrets
        for credential in other_credentials
    ):
        raise RuntimeError(
            "Agent erasure secrets must be distinct from every agent-api credential"
        )


def _require_minutes_erasure_backend(
    *, capture_enabled: "str | None", eraser: "object | None",
) -> "object | None":
    """Fail boot when Minutes capture is mounted without its Agent-owned erasure stores.

    A route that merely returns 503 is not an erasure implementation: capture could create Agent
    derivatives while production had no Brain provenance purge. Keep activation impossible until a
    production composition supplies the real transactional eraser.
    """

    raw = (capture_enabled or "").strip().lower()
    if raw in _TRUE_OPERATOR_FLAGS:
        if eraser is None:
            raise RuntimeError(
                "ZAKI_MINUTES_CAPTURE_ENABLED requires an Agent Brain provenance eraser"
            )
        return eraser
    if raw in _FALSE_OPERATOR_FLAGS:
        return eraser
    raise RuntimeError("ZAKI_MINUTES_CAPTURE_ENABLED must be a boolean operator flag")


def _wrap_signed_minutes_eraser(
    eraser: "object | None",
    *,
    redis_client: object | None,
    key_id: str,
    secret: str,
    previous_key_id: str | None = None,
    previous_secret: str | None = None,
    internal_secret: str = "",
    credential_environment: "Mapping[str, object] | None" = None,
    clock: "Callable[[], datetime] | None" = None,
    nonce: "Callable[[], str] | None" = None,
) -> "object | None":
    """Compose the canonical persisted ``erasure.v1`` proof around a real raw eraser.

    ``None`` is deliberately inert so a default-off production boot does not construct Redis or
    signer dependencies.  A present raw eraser is never exposed directly: missing/colliding signer
    material fails composition before the HTTP route can mount it.
    """

    _validate_agent_erasure_signing_config(
        capture_enabled="true" if eraser is not None else "false",
        key_id=key_id,
        secret=secret,
        previous_key_id=previous_key_id,
        previous_secret=previous_secret,
        internal_secret=internal_secret,
        credential_environment=credential_environment,
    )
    if eraser is None:
        return None
    if redis_client is None:
        raise RuntimeError("Agent Minutes erasure requires its durable receipt store")
    from control_plane.agent_erasure_receipts import (
        AgentErasureV1Signer,
        RedisAgentErasureReceiptStore,
        SignedAgentMinutesErasure,
    )

    signer = AgentErasureV1Signer(
        key_id=key_id,
        secret=secret,
        previous_key_id=previous_key_id,
        previous_secret=previous_secret,
        clock=clock or (lambda: datetime.now(timezone.utc)),
        nonce=nonce or (lambda: secrets.token_urlsafe(24)),
    )
    return SignedAgentMinutesErasure(
        eraser=eraser,
        receipts=RedisAgentErasureReceiptStore(redis_client, signer=signer),
    )


_AGENT_ERASURE_RECEIPT_FIELDS = frozenset({
    "version", "owner", "scope", "subject", "counts", "issued_at",
    "key_id", "nonce", "digest", "signature",
})
_AGENT_ERASURE_COUNT_FIELDS = frozenset({
    "agent_unit_streams", "agent_workspace_documents", "agent_brain_records",
})
_ERASURE_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_ERASURE_NONCE = re.compile(r"^[A-Za-z0-9_-]{16,128}$")
_ERASURE_SHA256 = re.compile(r"^sha256=[0-9a-f]{64}$")


def _is_exact_agent_meeting_erasure_receipt(
    receipt: object, *, user_id: int, meeting_id: str,
) -> bool:
    """Shape/context guard for the already-signed in-process erasure result.

    Cryptographic verification belongs to the persistent wrapper/store.  This final mediation gate
    prevents a raw or legacy eraser from being accidentally mounted and returned over the wire.
    """

    if not isinstance(receipt, dict) or set(receipt) != _AGENT_ERASURE_RECEIPT_FIELDS:
        return False
    if (
        receipt.get("version") != "erasure.v1"
        or receipt.get("owner") != "agent"
        or receipt.get("scope") != "meeting"
        or receipt.get("subject") != {
            "user_id": str(user_id), "meeting_id": meeting_id,
        }
    ):
        return False
    counts = receipt.get("counts")
    if not isinstance(counts, dict) or set(counts) != _AGENT_ERASURE_COUNT_FIELDS:
        return False
    if any(
        type(counts.get(field)) is not int or not 0 <= counts[field] <= 2_147_483_647
        for field in _AGENT_ERASURE_COUNT_FIELDS
    ):
        return False
    if (
        not isinstance(receipt.get("issued_at"), str)
        or not isinstance(receipt.get("key_id"), str)
        or not _ERASURE_KEY_ID.fullmatch(receipt["key_id"])
        or not isinstance(receipt.get("nonce"), str)
        or not _ERASURE_NONCE.fullmatch(receipt["nonce"])
        or not isinstance(receipt.get("digest"), str)
        or not _ERASURE_SHA256.fullmatch(receipt["digest"])
        or not isinstance(receipt.get("signature"), str)
        or not _ERASURE_SHA256.fullmatch(receipt["signature"])
    ):
        return False
    try:
        issued = datetime.fromisoformat(receipt["issued_at"].replace("Z", "+00:00"))
    except ValueError:
        return False
    return (
        issued.tzinfo is not None
        and issued.utcoffset() is not None
        and issued.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        == receipt["issued_at"]
    )


def _owned_processing_deadline_ms(owned: dict) -> int | None:
    """Resolve the immutable processed-content cutoff from one already owner-checked row.

    Ordinary upstream meetings carry neither ZAKI authority object and keep their legacy unbounded
    processing semantics. Any partial/malformed managed authority fails closed.
    """
    data = owned.get("data")
    if not isinstance(data, dict):
        return None
    capture = data.get("zaki_capture")
    retention = data.get("zaki_retention")
    if capture is None and retention is None:
        return None
    if (
        not isinstance(capture, dict)
        or capture.get("state") != "authorized"
        or not isinstance(retention, dict)
        or retention.get("state") != "open"
    ):
        raise ValueError("managed meeting processing authority is unavailable")
    expired = retention.get("expired_scopes", [])
    if (
        not isinstance(expired, list)
        or any(scope in expired for scope in ("transcript", "summary"))
    ):
        raise ValueError("managed meeting processing authority is unavailable")
    expiries = retention.get("scope_expiries")
    if not isinstance(expiries, dict):
        raise ValueError("managed meeting processing authority is unavailable")
    parsed: list[datetime] = []
    for scope in ("transcript", "summary"):
        value = expiries.get(scope)
        if not isinstance(value, str):
            raise ValueError("managed meeting processing authority is unavailable")
        try:
            expiry = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("managed meeting processing authority is unavailable") from None
        if expiry.tzinfo is None or expiry.utcoffset() != timedelta(0):
            raise ValueError("managed meeting processing authority is unavailable")
        parsed.append(expiry)
    return int(min(parsed).timestamp() * 1000)


class _LiveMeetings:
    """In-memory registry of meeting copilots — the terminal's 'meetings' feed. Keyed by session_uid (the
    native Meet code). A stopped/ended meeting is KEPT (``status='stopped'``) so the terminal can offer to
    send the bot back; ``add`` (re)marks it live. Liveness is EVIDENCE, not a latch: ``add`` stamps
    ``last_seen`` and ``list`` demotes an entry silent past LIVE_SILENCE_TTL_SEC to stopped (P21 —
    absence of the expected signal is itself a reportable state). The dev-tier foundation."""

    def __init__(self) -> None:
        self._by_uid: dict[str, dict] = {}

    def add(self, meeting: dict) -> None:
        m = dict(meeting)
        m["status"] = "live"
        m["last_seen"] = time.monotonic()
        self._by_uid[meeting["session_uid"]] = m

    def stop(self, session_uid: str) -> None:
        m = self._by_uid.get(session_uid)
        if m:
            m["status"] = "stopped"

    def drop(self, session_uid: str) -> None:
        # the meeting ended — keep the row (stopped) so 'send the bot back' stays available
        self.stop(session_uid)

    def erase(self, session_uid: str) -> None:
        """Forget an erased row entirely; unlike an ordinary end, it must not remain discoverable."""
        self._by_uid.pop(session_uid, None)

    def list(self) -> list[dict]:
        now = time.monotonic()
        for m in self._by_uid.values():
            if m.get("status") == "live" and now - m.get("last_seen", now) > LIVE_SILENCE_TTL_SEC:
                m["status"] = "stopped"  # earned liveness expired — the segment flow went silent
        return list(self._by_uid.values())


class ChatContextBody(BaseModel):
    """The terminal-state CONTEXT BUNDLE (slice 1). ``extra="ignore"`` on purpose — forward-
    tolerant: a newer terminal adding bundle fields must never 422 against this server."""
    model_config = {"extra": "ignore"}
    tz: Optional[str] = None            # IANA tz for digest rendering (invalid → UTC)
    surface: Optional[dict] = None      # {list?: str, tab?: {kind: str}} — the ambient gate signal
    focus: Optional[dict] = None        # the focused thing (meeting/file/workspace/today); None = cleared
    include: Optional[dict] = None      # {schedule?: bool} — explicit user toggle beats the gate


class ChatBody(BaseModel):
    model_config = {"extra": "forbid"}
    prompt: str
    # subject is DERIVED server-side from X-User-Id (P20) — kept here only so a client that still sends it
    # doesn't 422 (extra=forbid); the value is IGNORED. Dropped from the client in Stage 4.
    subject: Optional[str] = None
    session: Optional[str] = None
    # LEGACY single-focus grounding ({kind, ref}) — still honored when ``context`` is absent, so
    # old clients keep byte-identical behavior. The terminal now sends ``context`` (below) too.
    active: Optional[dict] = None
    # the terminal-state context bundle; when present it is AUTHORITATIVE (including
    # ``focus: null`` = the user cleared the focus chip — legacy ``active`` is then ignored).
    context: Optional[ChatContextBody] = None
    # Client-minted TURN NONCE (one per user turn, constant across that turn's reconnect attempts).
    # Lets the server tell a no-cursor RETRY (the stream dropped before the client ever saw an ``id:``,
    # so it can't send Last-Event-ID) from a genuinely new turn with identical text ("yes" twice):
    # a matching nonce re-attaches from the turn's recorded start — no second dispatch, no lost events.
    turn_id: Optional[str] = None


class ResetBody(BaseModel):
    """Body for POST /api/chat/reset — the docs (api/agent.mdx) say it's just ``{session?}``. reset only
    needs the session; ``prompt``/``subject``/``active`` are accepted-and-ignored so a client reusing the
    chat-body shape doesn't 422 (reset must NOT require a prompt the way the chat turn does)."""
    model_config = {"extra": "forbid"}
    session: Optional[str] = None
    subject: Optional[str] = None
    prompt: Optional[str] = None
    active: Optional[dict] = None
    context: Optional[ChatContextBody] = None  # accepted-and-ignored, same rationale


class RoutineCreate(BaseModel):
    """The Routines surface / ``/routine`` create form — compiles to a routine.v1 + a schedule.v1 job."""
    model_config = {"extra": "forbid"}
    subject: Optional[str] = None  # DERIVED from X-User-Id (P20); ignored if sent. Dropped client-side in Stage 4.
    name: str
    cron: str
    prompt: str
    run_now: bool = True  # fire one immediate run so the author sees a result without waiting for cron


class RoutineEnabledPatch(BaseModel):
    model_config = {"extra": "forbid"}
    enabled: bool


class WorkspaceSwapBody(BaseModel):
    """Attach a custom external git repo as the subject's workspace. Omit ``repo`` to swap back to seed."""
    model_config = {"extra": "forbid"}
    repo: Optional[str] = None   # git URL to clone (None → swap back to the seeded default)
    ref: Optional[str] = None    # branch/tag/sha to check out (defaults to main)
    slug: Optional[str] = None   # target a parked slot DIRECTLY (e.g. a no-repo backup) — restores, no re-clone
    fresh: bool = False          # swap-to-seed only: rebuild the default from template (start fresh) vs restore the park
    token: Optional[str] = None  # access token for a PRIVATE repo — used for the clone only, never stored (P15)


class WorkspacePublishBody(BaseModel):
    """Publish the subject's vexa-born workspace to GitHub — create the repo (unless ``remote_url``
    targets a pre-created one) and push the current branch's full history. ``token`` is the caller's
    PAT, used server-side for this call only, NEVER stored (P15)."""
    model_config = {"extra": "forbid"}
    repo_name: Optional[str] = None    # name of the repo to create (required unless remote_url is given)
    private: bool = True               # create the repo private (default) or public
    token: Optional[str] = None        # GitHub PAT (repo-creation + push); OPTIONAL — falls back to the caller's SAVED token
    org: Optional[str] = None          # create under this org instead of the user's account
    remote_url: Optional[str] = None   # skip creation and push to this (pre-created/empty) repo
    slug: Optional[str] = None         # target workspace (own slot or shared membership); omitted = the seed-slot workspace


class WorkspaceRenameBody(BaseModel):
    """Set a workspace slot's DISPLAY name (label only — the slug/parked dir are unchanged). Empty clears it."""
    model_config = {"extra": "forbid"}
    slug: str
    name: Optional[str] = None


class WorkspacePushBody(BaseModel):
    """Push a workspace's current branch to its GitHub home (origin / vexa-publish), fast-forward only.
    ``slug`` targets one of the caller's workspaces (default = the primary); ``token`` is the caller's PAT.
    OPTIONAL — when omitted, the caller's SAVED reusable GitHub token (git_credentials) is used. Whichever
    token applies is used for this push only and NEVER stored on the workspace remote (P15)."""
    model_config = {"extra": "forbid"}
    slug: Optional[str] = None
    token: Optional[str] = None


class GitTokenBody(BaseModel):
    """Save (or, with an empty/omitted ``token``, CLEAR) the caller's reusable GitHub token — stored ONCE,
    server-side, and reused as the fallback credential for every git op across all their repos."""
    model_config = {"extra": "forbid"}
    token: Optional[str] = None


class WorkspacePullBody(BaseModel):
    """Fetch + fast-forward a workspace from its GitHub home. ``slug`` targets one of the caller's
    workspaces (default = primary); ``token`` (optional — public repos need none) is used for the fetch
    only and NEVER stored (P15). A divergence is refused, not merged/rebased/forced."""
    model_config = {"extra": "forbid"}
    slug: Optional[str] = None
    token: Optional[str] = None


class WorkspacePurposeBody(BaseModel):
    """Set a workspace's PURPOSE — a one-line statement of what it's for, stored IN the workspace so it
    travels when shared and is read into the agent's mount preamble. ``slug`` targets one of the caller's
    workspaces (default = primary); an empty ``purpose`` clears it."""
    model_config = {"extra": "forbid"}
    slug: Optional[str] = None
    purpose: str = ""


class InviteCreateBody(BaseModel):
    """Mint a scoped invite for a shared workspace (owner/contributor only). Returns the token ONCE."""
    model_config = {"extra": "forbid"}
    workspace_id: str
    role: str = "viewer"                 # viewer | contributor (never owner)
    expires_in_sec: int = 604800         # 7 days
    max_uses: int = 1
    mode: str = "open"                   # open (anyone-with-link) | restricted (allowed_emails only)
    allowed_emails: Optional[list[str]] = None  # restricted mode: the verified emails permitted to redeem


class InviteAcceptBody(BaseModel):
    """Redeem an invite token (any logged-in user). Idempotent per user."""
    model_config = {"extra": "forbid"}
    token: str


class RoleSetBody(BaseModel):
    """Flip a member's role (owner only) — the "change read/write permissions" DoD item."""
    model_config = {"extra": "forbid"}
    role: str                            # viewer | contributor | owner


class SharedNewBody(BaseModel):
    """CREATE a new shared workspace (top-level, caller becomes owner) — the bootstrap that makes a
    workspace shareable so invites can be minted against it. ``name`` → display + workspace-id base."""
    model_config = {"extra": "forbid"}
    name: str = "Shared workspace"


class SharedActiveBody(BaseModel):
    """Switch a shared workspace ON (mount) or OFF (hide) in the caller's active set — per-user, membership
    is unchanged."""
    model_config = {"extra": "forbid"}
    active: bool


class ArchiveBody(BaseModel):
    """Archive (collapse, keep) or un-archive one of the caller's own workspaces."""
    model_config = {"extra": "forbid"}
    archived: bool = True


class WorkspaceActivateBody(BaseModel):
    """ADD a workspace to the subject's active set (the additive mount set — WP-A2.1). Pass ``repo`` to
    clone/restore a git repo, or ``slug`` to activate an already-parked slot. Unlike swap it does NOT park
    the others — the private baseline and any other active workspaces stay mounted."""
    model_config = {"extra": "forbid"}
    repo: Optional[str] = None   # git URL to clone (first time) / restore (thereafter)
    ref: Optional[str] = None    # branch/tag/sha (defaults to main)
    slug: Optional[str] = None   # activate an already-parked slot directly (no repo needed)
    token: Optional[str] = None  # access token for a PRIVATE repo — clone only, never stored (P15)


class WorkspaceNewBody(BaseModel):
    """CREATE a brand-new BLANK workspace (seeded from the template) at a fresh slug and ADD it to the
    active set — the additive-model "new workspace" action. NOT a swap: nothing is parked/rebuilt/backed
    up. ``name`` (optional) → the new workspace's display label (default a unique "New workspace")."""
    model_config = {"extra": "forbid"}
    name: Optional[str] = None


class WorkspaceDeactivateBody(BaseModel):
    """REMOVE a workspace from the active set (park it — never destroyed). The private baseline cannot be
    deactivated (it is the subject's durable memory root)."""
    model_config = {"extra": "forbid"}
    slug: str


class MeetingStart(BaseModel):
    """Launch a live-meeting copilot for a REAL meeting. The vexa-cloud bridge POSTs this once it has a
    bot in the meeting; the dispatch then tails ``tc:meeting:{native_id}`` (the stream the bridge feeds)."""
    model_config = {"extra": "forbid"}
    platform: str               # google_meet | teams | zoom
    native_id: str              # the platform meeting id (e.g. a Google Meet code abc-defg-hij)
    subject: Optional[str] = None  # DERIVED from X-User-Id (P20); ignored if sent.
    title: Optional[str] = None


class MeetingProcess(BaseModel):
    """Toggle copilot PROCESSING for a meeting. on=false → no processing (raw transcript only);
    on=true → process the meeting (full-history backfill the first time, else resume live)."""
    model_config = {"extra": "forbid"}
    native_id: str
    platform: str = "google_meet"
    on: bool
    # P0: the meetings-domain ROW id (unique per owner/run). The handler requires a positive numeric
    # value, owner-scopes it, and binds `native_id` to that row; there is intentionally no native-id
    # fallback because native links collide across tenants and have no permanent retention fence.
    meeting_id: Optional[str] = None
    subject: Optional[str] = None  # DERIVED from X-User-Id (P20); ignored if sent.


# The meeting copilot's start brief. The in-container worker drives per-beat extraction with its own
# CARD_PROMPT; this is the envelope's entrypoint (continuity = the session file in the workspace).
_MEETING_BRIEF = (
    "You are the live meeting copilot. Watch the meeting transcript as it streams in and surface the "
    "people, companies, products, and projects worth tagging."
)


def _encode_sse_cursor(last: dict, tkey: str, okey: str, pkey: str | None = None) -> str:
    """Pack the per-stream redis cursors into ONE SSE event id (the browser echoes it as
    Last-Event-ID on reconnect → we resume EXACTLY from here, gapless). '-' = not-yet-read.
    Three parts since ADR 0027 (transcript|output|processed); the third is the proc-stream cursor."""
    parts = [last.get(tkey, "-"), last.get(okey, "-")]
    if pkey is not None:
        parts.append(last.get(pkey, "-"))
    return "|".join(str(p) for p in parts)


def _decode_sse_cursor(raw: str | None) -> "tuple[str | None, str | None, str | None]":
    """Last-Event-ID → (transcript_id, output_id, processed_id). None when absent/malformed (fresh
    connect). PAD-tolerant: a pre-ADR-0027 two-part id decodes with processed_id None — the caller
    replays the proc stream from the start (notes upsert by id client-side, so replay is idempotent
    and never drops the reconnect gap)."""
    if not raw or "|" not in raw:
        return (None, None, None)
    parts = (raw.split("|") + [None, None, None])[:3]
    return tuple(p if p and p != "-" else None for p in parts)  # type: ignore[return-value]


def _sse(events) -> Iterator[str]:
    for item in events:
        # ``None`` is the reader's idle tick → an SSE comment keepalive. Proxies (and the client's own
        # idle-stall detector) cut a byte-silent stream in ~18-30s; a long agent think is byte-silent for
        # minutes. The comment is invisible to EventSource parsing but keeps bytes flowing.
        if item is None:
            yield ": keepalive\n\n"
            continue
        # Each item is either a bare event dict, or (event, sse_id) — the id makes reconnects resumable.
        ev, sid = item if isinstance(item, tuple) else (item, None)
        prefix = f"id: {sid}\n" if sid else ""
        yield f"{prefix}data: {json.dumps(ev)}\n\n"


def _has_custom_model_endpoint(cfg: dict) -> bool:
    """True iff a per-user Settings → Models config actually delivers a credential to the worker.
    Mirrors overlay_model_config's inertness rule (dispatch.py): only ``mode=custom`` WITH a
    ``base_url`` stamps auth env; ``api_key`` is optional (a keyless local gateway is legitimate)."""
    return (cfg.get("mode") or "").strip() == "custom" and bool((cfg.get("base_url") or "").strip())


def _model_creds_error_message() -> str:
    keys = ", ".join(missing_capability_keys("model_inference"))
    return (
        "No model credentials are configured, so the agent cannot run. "
        f"Set one of {keys} in the deployment environment "
        "(deploy/compose/.env for the compose stack, then `make all`), "
        "or add a custom endpoint under Settings → Models."
    )


def _model_blocked_error_message() -> str:
    return (
        "Your personal model configuration is blocked by operator policy, so the agent cannot run. "
        "Choose an approved endpoint under Settings → Models or ask an operator to approve the current one."
    )


MEETING_CHAT_TRANSCRIPT_SEGMENTS = 400  # bound the live transcript folded into a meeting-chat prompt
# A transcript entry may refine prior segment ids, so read a small bounded multiple of the desired
# unique lines. This is a hard Redis fanout cap: chat grounding never XRANGEs an entire long meeting.
MEETING_CHAT_STREAM_ENTRY_MULTIPLIER = 4
MEETING_CHAT_STREAM_MAX_ENTRIES = 1600


def _meeting_chat_entry_budget(limit: int) -> int:
    return min(MEETING_CHAT_STREAM_MAX_ENTRIES, max(1, limit * MEETING_CHAT_STREAM_ENTRY_MULTIPLIER))


def _fold_meeting_transcript(redis_url: "str | None", stream_key: str, *, limit: int) -> str:
    """Fold the live transcript Stream ``tc:meeting:{stream_key}`` — the SAME stream the meeting copilot
    tails (worker/meeting.py) and the terminal renders — into ordered ``speaker: text`` lines for chat
    grounding. ``stream_key`` is the meetings-domain ROW id (P0 cross-tenant leak fix: the carrier keys
    on the row id, never the native id which collides across tenants/re-sends). Refining live drafts are
    upserted by ``segment_id`` (latest text wins, no duplicate), arrival order preserved, bounded to the
    last ``limit`` segments. Best-effort: returns "" when redis is unwired or the stream is empty."""
    if not redis_url:
        return ""
    try:
        import redis

        r = redis.from_url(redis_url, decode_responses=True)
        # XREVRANGE applies COUNT server-side. Reverse the bounded tail locally so refinement/order
        # semantics remain chronological without materializing the complete meeting.
        rows = list(reversed(r.xrevrange(
            f"tc:meeting:{stream_key}", count=_meeting_chat_entry_budget(limit)
        )))
    except Exception as exc:  # noqa: BLE001 — grounding is best-effort; never fail the chat turn
        logger.warning("could not read transcript for %s: %s", stream_key, exc)
        return ""
    order: list[str] = []
    seg_by_id: dict[str, dict] = {}
    for entry_id, fields in rows:
        try:
            payload = json.loads(fields.get("payload", "{}"))
        except (TypeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        if payload.get("type") == "session_end":
            continue
        segments = payload.get("segments", [])
        if not isinstance(segments, list):
            continue
        for i, seg in enumerate(segments):
            if not isinstance(seg, dict):
                continue
            sid = str(seg.get("segment_id") or f"{entry_id}:{i}")
            if sid not in seg_by_id:
                order.append(sid)
            seg_by_id[sid] = seg
    lines: list[str] = []
    for sid in order[-limit:]:
        seg = seg_by_id[sid]
        text = str(seg.get("text") or "").strip()
        if not text:
            continue
        speaker = str(seg.get("speaker") or "Speaker").strip()
        lines.append(f"{speaker}: {text}")
    return "\n".join(lines)


def _fold_meeting_processed(redis_url: "str | None", stream_key: str, *, limit: int) -> str:
    """Fold the PROCESSED-notes Stream ``proc:meeting:{stream_key}`` (processed-notes.v1 — the copilot's
    cleaned transcript; single writer worker/meeting.py) into ordered ``speaker: text`` lines for
    post-meeting chat grounding. Notes upsert by id (a refining pass upgrades in place), the ``view_end``
    terminal marker is skipped, order preserved, bounded to the last ``limit`` notes. Best-effort:
    returns "" when redis is unwired, the stream is empty, or entries are malformed."""
    if not redis_url:
        return ""
    try:
        import redis

        r = redis.from_url(redis_url, decode_responses=True)
        rows = list(reversed(r.xrevrange(
            f"proc:meeting:{stream_key}", count=_meeting_chat_entry_budget(limit)
        )))
    except Exception as exc:  # noqa: BLE001 — grounding is best-effort; never fail the chat turn
        logger.warning("could not read processed notes for %s: %s", stream_key, exc)
        return ""
    order: list[str] = []
    note_by_id: dict[str, dict] = {}
    for entry_id, fields in rows:
        if fields.get("type") == "view_end":
            continue
        raw = fields.get("note")
        if not raw:
            continue
        try:
            note = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if not isinstance(note, dict):
            continue
        nid = str(note.get("id") or entry_id)
        if nid not in note_by_id:
            order.append(nid)
        note_by_id[nid] = note
    lines: list[str] = []
    for nid in order[-limit:]:
        note = note_by_id[nid]
        text = str(note.get("text") or "").strip()
        if not text:
            continue
        speaker = str(note.get("speaker") or "Speaker").strip()
        lines.append(f"{speaker}: {text}")
    return "\n".join(lines)


def _meeting_grounding(
    active: "dict | None", session: str, prompt: str, redis_url: "str | None"
) -> "tuple[dict, list[str], str]":
    """Cookbook #1 — chat grounding in the terminal's ACTIVE meeting, branched by the meeting's
    LIFECYCLE PHASE (design-spec meeting-lifecycle-v2, W4; steering templates + the _global override
    live in control_plane/meeting_steering.py):

      prep (idle/scheduled)          — no transcript fold (none exists); steer toward preparation,
                                       naming the bound prep workspace when the client sent one.
      live/post                     — never copy raw/processed meeting content into the generic-chat
                                      prompt. Require the dedicated bounded Minutes read path and fail
                                      honestly while it is disabled or unavailable.

    Generic chat prompts are durable carriers (Redis warm delivery plus harness continuity), so they
    are not an acceptable transport for transcript PII. Returns the plain (none-context, no tools,
    prompt) when the active tab isn't a meeting."""
    a = active or {}
    if a.get("kind") != "meeting":
        return ({"kind": "none", "session": session}, [], prompt)
    m = a.get("meeting") or a  # tolerate {kind, meeting:{…}} or a flat {kind, platform, native_id}
    row_id = str(m.get("meeting_id") or "").strip()
    if not re.fullmatch(r"[1-9][0-9]{0,18}", row_id):
        return ({"kind": "none", "session": session}, [], prompt)
    native = m.get("native_id") or m.get("ref") or row_id
    if not native:
        return ({"kind": "none", "session": session}, [], prompt)
    platform = m.get("platform") or "google_meet"
    # A chat turn (trigger "message"), not a live-meeting serve — the transcript travels in the prompt,
    # so the dispatch context stays plain (no meeting env / serve path is engaged for a chat).
    ctx = {"kind": "none", "session": session}
    status = str(m.get("status") or "").strip().lower()
    phase = meeting_steering.phase_for(status)
    fields = {
        "title": str(m.get("title") or "").strip() or str(native),
        "platform": platform,
        "native": str(native),
    }

    if phase == "prep":
        when = str(m.get("scheduled_at") or "").strip()
        fields["when"] = f", scheduled for {when}" if when else " (no time set yet)"
        workspace = str(m.get("workspace_id") or "").strip()
        fields["workspace"] = (
            f"The prep workspace bound to this meeting is \"{workspace}\" — ground your research and "
            f"write the brief there (its kg/ entities cover the attendees and companies). "
            if workspace
            else (
                "No shared prep workspace is bound to this meeting — the brief lives (or will live) "
                "as this meeting's note under kg/entities/meeting/ in the user's OWN workspace, "
                "reused across the meeting's series. "
            )
        )
        return (ctx, [], meeting_steering.render("prep", fields) + prompt)

    # The legacy path folded tc:/proc: Redis content into `prompt`. Dispatcher then XADDed that full
    # prompt to a durable generic-chat input stream and the harness linked it into durable continuity
    # state. A row-scoped Minutes eraser cannot prove those secondary copies gone. Never inspect the
    # carriers here: the bounded, non-persistent Minutes path is the sole allowed meeting-content read.
    return (ctx, [], meeting_steering.MINUTES_READ_REQUIRED + prompt)


# The grounding/user-message boundary marker. Every server-folded context block (kg-links + mounts,
# added by the WORKER, land BEFORE the user prompt too; schedule digest; meeting/workspace grounding)
# sits BEFORE this sentinel; the user's actual words are AFTER it. The terminal strips everything up to
# and including it in ONE cut — robust to preamble wording drift, unlike per-block regexes. An HTML
# comment so the model treats it as inert. Old chats (no sentinel) fall back to the client's regexes.
CONTEXT_SENTINEL = "<!--vexa:user-input-below-->"


# ── terminal-state context bundle (slice 1) — the grounding orchestrator ────────────────────────────
# A chat turn's prompt is assembled [ambient <schedule> digest] + [focus fold] + user prompt.
#   ambient — the schedule digest, SURFACE-GATED (Meetings list / Today tab / meeting-ish tab focused)
#             with the user's explicit include.schedule toggle beating the gate either way.
#   focus   — meeting/prep (delegates to _meeting_grounding, ENRICHED with the server row so a cold
#             client store can't ground a planned meeting as live), workspace (purpose + README head),
#             today (the full-day digest REPLACES ambient), file/none (unchanged).
# Everything stays inside the trusted control plane and rides the prompt (P15); ctx stays "none".

_AMBIENT_TAB_KINDS = {"today", "meeting", "meetingPrep"}


def _ambient_gated(context: "ChatContextBody | None") -> bool:
    """Digest on/off: explicit ``include.schedule`` wins; absent → on iff the user is on a
    meetings-relevant surface. No context (legacy client) → off (old behavior)."""
    if context is None:
        return False
    include = context.include or {}
    if isinstance(include.get("schedule"), bool):
        return include["schedule"]
    surface = context.surface or {}
    if surface.get("list") == "meetings":
        return True
    tab = surface.get("tab") or {}
    if tab.get("kind") in _AMBIENT_TAB_KINDS:
        return True
    focus = context.focus or {}
    return focus.get("kind") == "today"


_WORKSPACE_README_LINES = 60
_WORKSPACE_README_CHARS = 3000


def _fold_workspace_grounding(mounts: "list", slug: str) -> str:
    """The workspace-focus preamble: purpose + README head for the mount matching ``slug``.
    FAIL-CLOSED: a slug outside the caller's active/shared mounts folds nothing — the mount set
    IS the authorization; we never read a workspace the turn couldn't see."""
    mount = next((m for m in mounts if getattr(m, "slug", None) == slug
                  or getattr(m, "workspace_id", None) == slug), None)
    if mount is None:
        return ""
    name = str(getattr(mount, "name", "") or slug)
    try:
        purpose = read_purpose(mount.path) or ""
    except Exception:  # noqa: BLE001
        purpose = ""
    purpose_part = f" Its purpose: {purpose.strip()}." if purpose.strip() else ""
    readme = ""
    try:
        text = (Path(mount.path) / "README.md").read_text(encoding="utf-8")
        readme = "\n".join(text.splitlines()[:_WORKSPACE_README_LINES])[:_WORKSPACE_README_CHARS]
    except OSError:
        readme = ""
    fields = {"name": name, "slug": slug, "purpose": purpose_part, "readme": readme}
    if not readme.strip():
        return meeting_steering.NO_README_WORKSPACE_FOCUS.format(**fields)
    return meeting_steering.render("workspace_focus", fields)


def _enriched_meeting_focus(focus: dict, rows: "list[dict]") -> "dict | None":
    """Overlay the SERVER row's truth onto the client-sent meeting focus — status/title/
    scheduled_at/workspace_id come from the meetings domain when the row is found; the client's
    values remain only as display fallbacks after an owned server row is found. ``None`` means the
    caller-supplied identity was not present in the authenticated user's meeting list and must not be
    used to address Redis carriers."""
    nid = focus.get("native_id") or focus.get("ref")
    row = schedule_digest_mod.find_row(
        rows, meeting_id=focus.get("meeting_id"), platform=focus.get("platform"), native_id=nid)
    if row is None and nid is not None:
        # The terminal's tab param is the ROW id for planned meetings without a link (native is
        # NULL there) — it rides in native_id, so retry it as the row id before giving up.
        row = schedule_digest_mod.find_row(rows, meeting_id=nid)
    if row is None:
        return None
    data = row.get("data") or {}
    merged = dict(focus)
    merged["meeting_id"] = row.get("id", focus.get("meeting_id"))
    merged["status"] = row.get("status") or focus.get("status")
    if row.get("platform") and row.get("platform") != "unknown":
        merged["platform"] = row["platform"]
    if row.get("native_meeting_id"):
        merged["native_id"] = row["native_meeting_id"]
    for src_key, dst_key in (("title", "title"), ("scheduled_at", "scheduled_at"), ("workspace_id", "workspace_id")):
        if data.get(src_key):
            merged[dst_key] = data[src_key]
    return merged


def _context_grounding(
    body: "ChatBody", session: str, redis_url: "str | None", *,
    schedule_rows: "Callable[[], list[dict]]",
    workspace_mounts: "Callable[[], list]",
) -> "tuple[dict, list[str], str]":
    """Assemble the turn's grounding from the context bundle (or the legacy ``active``).
    ``schedule_rows`` / ``workspace_mounts`` are LAZY — fetched only for the branches that
    need them, and both degrade to empty on failure (a bundle must never fail the turn)."""
    prompt = body.prompt
    context = body.context
    focus = context.focus if context is not None else body.active
    ctx = {"kind": "none", "session": session}

    ambient = _ambient_gated(context)
    kind = (focus or {}).get("kind")
    need_rows = ambient or kind in ("meeting", "today")
    rows: "list[dict]" = []
    if need_rows:
        try:
            rows = schedule_rows() or []
        except Exception:  # noqa: BLE001 — best-effort by contract
            rows = []

    tz = context.tz if context is not None else None
    preamble = ""
    if kind == "today":
        digest = schedule_digest_mod.build_schedule_digest(rows, tz=tz, full_day=True)
        if digest:
            preamble = digest + meeting_steering.render("schedule", {})
        return (ctx, [], preamble + prompt)

    if ambient:
        digest = schedule_digest_mod.build_schedule_digest(rows, tz=tz)
        if digest:
            preamble = digest + meeting_steering.render("schedule", {})

    if kind == "meeting":
        enriched = _enriched_meeting_focus(dict(focus), rows) if rows else None
        if enriched is None:
            # Meeting context is an active data read, not harmless presentation metadata. If the
            # owner-scoped schedule source is empty/down or does not contain this row, never fold a
            # client-supplied Redis id/native link into the model prompt.
            return (ctx, [], preamble + prompt)
        _c, _t, folded_prompt = _meeting_grounding(enriched, session, prompt, redis_url)
        return (_c, _t, preamble + folded_prompt if preamble else folded_prompt)

    if kind == "workspace" and (focus or {}).get("slug"):
        try:
            mounts = workspace_mounts() or []
        except Exception:  # noqa: BLE001
            mounts = []
        preamble += _fold_workspace_grounding(mounts, str(focus["slug"]))
        return (ctx, [], preamble + prompt)

    # file focus stays client-side-preambled; none/unknown kinds fold nothing extra
    return (ctx, [], preamble + prompt)


# ── SSE ownership gate (P0 cross-tenant leak fix — the SSE sibling of the by-id REST check) ──────────
# The live SSE feed `GET /api/meeting/stream` is keyed on a CALLER-SUPPLIED row id (`meeting_id`) and a
# `session_uid`. Row ids are sequential ints, so without an ownership check any authenticated user B could
# `EventSource(...?meeting_id=<A_row>&session_uid=<A_native>)` and stream tenant A's live transcript +
# copilot cards — an ACTIVE, enumerable cross-tenant read. We mirror the WS `/ws` pattern (gateway
# `authorize_subscribe` → `Meeting.user_id == user_id`) and the by-id REST path (`get_transcript_by_id`
# owner-scopes in SQL): verify the caller OWNS the row BEFORE opening the redis stream. Fail CLOSED.
#
# agent-api has no meetings DB; it asks meeting-api `GET /meetings/{meeting_id}` forwarding the
# gateway-injected `X-User-Id` (meeting-api's `_resolve_user_id` trusts it exactly as its by-id path does)
# — a row owned by another user (or absent) returns 404 there → we treat it as NOT-OWNED. The returned
# record is revalidated here rather than trusted as an untyped transport response. The canonical row is
# the ONLY transcript/copilot carrier address: native meeting links can be reused across tenants, so they
# must never select a Redis stream. Returns the owned meeting record (dict) on success, else None.
# Injectable so the L2 suite drives it over a fake.
def _http_meeting_owner_lookup(meeting_api_url: str):
    """Build the default owner-lookup: GET {meeting_api_url}/meetings/{id} with the caller's X-User-Id.
    Returns a callable ``(user_id: str, meeting_id: str) -> dict | None`` (the owned meeting record, or
    None when the row is absent / owned by someone else / meeting-api is unreachable — fail-closed)."""
    import urllib.error
    import urllib.request

    base = (meeting_api_url or "").rstrip("/")

    def _lookup(user_id: str, meeting_id: str) -> "dict | None":
        if not base or not user_id or not str(meeting_id).isdigit():
            return None  # non-numeric row id can't be an owned meeting row → fail closed
        try:
            req = urllib.request.Request(
                f"{base}/meetings/{int(meeting_id)}", headers={"X-User-Id": str(user_id)})
            with open_no_redirect(req, timeout=5) as resp:
                if resp.status != 200:
                    return None
                record = read_json_bounded(resp)
                return record if isinstance(record, dict) else None
        except urllib.error.HTTPError:
            return None   # 404 (not owned / absent) or any other status → refuse
        except Exception:  # noqa: BLE001 — meeting-api unreachable → fail CLOSED, never open the stream
            return None

    return _lookup


def create_app(
    dispatcher: Dispatcher,
    *,
    stream_reader: Optional[StreamReader] = None,
    sessions: Optional[_Sessions] = None,
    reader: Optional[WorkspaceReader] = None,
    scheduler: Optional[SchedulerPort] = None,
    invocations_url: Optional[str] = None,
    redis_url: Optional[str] = None,
    membership_index: Optional[MembershipIndex] = None,
    meeting_owner_lookup: "Optional[object]" = None,
    schedule_source: "Optional[Callable[[str], list]]" = None,
    minutes_eraser: "Optional[object]" = None,
    minutes_ingestor: "Optional[object]" = None,
    gateway_replay_store: "Optional[object]" = None,
) -> FastAPI:
    if sessions is not None:
        sess = sessions
    elif redis_url:
        import redis as _redis

        sess = _Sessions(_redis.from_url(redis_url, decode_responses=True))
    else:
        sess = _Sessions()
    live = _LiveMeetings()
    bind_live = getattr(minutes_eraser, "bind_live_registry", None)
    if callable(bind_live):
        bind_live(live)
    wsr = reader or WorkspaceReader("/workspaces")
    mindex: MembershipIndex = membership_index if membership_index is not None else InMemoryMembershipIndex()
    app = FastAPI(title="vexa-agent-api", version="0.12.0")
    app.state.dispatcher = dispatcher
    app.state.sessions = sess
    app.state.live_meetings = live
    app.state.scheduler = scheduler
    settings = dispatcher.settings if dispatcher is not None else None
    # The SSE ownership gate's owner-lookup (P0): default = HTTP to meeting-api; injectable for L2 tests.
    _meeting_owner_lookup = meeting_owner_lookup or _http_meeting_owner_lookup(
        settings.meeting_api_url if settings is not None else "")
    # The ambient schedule digest's rows source (context bundle): TTL-cached meeting-api fetch;
    # injectable for L2 tests, same seam style as meeting_owner_lookup.
    _schedule_source = schedule_source or schedule_digest_mod.digest_source(
        settings.meeting_api_url if settings is not None else "", mindex.list)

    # TOPOLOGY BOUNDARY (Lane M vector 3): agent-api trusts X-User-Id / X-User-Email as ground truth.
    # That trust is only SOUND when the gateway is the SOLE ingress — the gateway strips any client-sent
    # x-user-id/x-user-email and re-injects the values it resolved from the verified api-key. In the
    # current dev/direct topology the terminal and host-local clients reach agent-api WITHOUT the gateway
    # hop (compose loopback + VEXA_AGENT_DEFAULT_SUBJECT fallback), so those headers are spoofable and
    # restricted-mode invites MUST NOT be relied on as a security boundary here. A hardened deploy sets
    # VEXA_REQUIRE_GATEWAY_IDENTITY=1: agent-api then rejects any request lacking a fresh Gateway HMAC
    # bound to the exact method/path/user. The signing key never crosses the network and every nonce is
    # claimed once. OFF by default so the dev/direct topology keeps working. Full fix = route the
    # terminal through the gateway (Stage 4) and make the gateway the only ingress to agent-api.
    _require_gateway_identity = (
        settings.require_gateway_identity if settings is not None else False
    )
    _gateway_identity_secret = (
        settings.gateway_identity_secret.get_secret_value()
        if settings is not None else ""
    )
    _gateway_identity_previous_secret = (
        settings.gateway_identity_previous_secret.get_secret_value()
        if settings is not None else ""
    )
    _internal_api_secret = (
        settings.internal_api_secret.get_secret_value()
        if settings is not None else ""
    )
    _validate_gateway_identity_config(
        required=_require_gateway_identity or minutes_ingestor is not None,
        secret=_gateway_identity_secret,
        previous_secret=_gateway_identity_previous_secret,
        internal_secret=_internal_api_secret,
    )
    _gateway_identity_verifier = None
    if _require_gateway_identity or minutes_ingestor is not None:
        replay_store = gateway_replay_store or InMemoryGatewayReplayStore()
        _gateway_identity_verifier = GatewayIdentityVerifier(
            _gateway_identity_secret,
            previous_secret=_gateway_identity_previous_secret,
            replay_store=replay_store,
        )

    def _gateway_error(
        status_code: int, detail: str, *, headers: "Mapping[str, str] | None" = None,
    ) -> JSONResponse:
        return JSONResponse(
            {"detail": detail},
            status_code=status_code,
            headers={"Cache-Control": "no-store", **dict(headers or {})},
        )

    def _requires_gateway_proof(request: Request) -> bool:
        path = request.url.path
        if minutes_ingestor is not None and path == "/api/minutes/summarize-last":
            return True
        return (
            _require_gateway_identity
            and path.startswith("/api/")
            and not path.startswith("/api/admin/")
        )

    @app.middleware("http")
    async def _verify_gateway_request(request: Request, call_next):
        """Authenticate metadata first, then bounded body bytes, then claim the nonce.

        Gateway already drains and caps the public body before signing, so the internal body should
        arrive promptly. Repeating both the byte and time bounds here prevents a direct/slow caller
        from turning proof verification into unbounded buffering or event-loop blocking.
        """
        if not _requires_gateway_proof(request):
            return await call_next(request)
        subject = request.headers.get("x-user-id", "")
        try:
            query = request.scope.get("query_string", b"").decode("ascii")
        except (AttributeError, UnicodeDecodeError):
            return _gateway_error(401, "verified gateway identity required")
        if _gateway_identity_verifier is None or not subject:
            return _gateway_error(401, "verified gateway identity required")
        proof = _gateway_identity_verifier.authenticate_metadata(
            method=request.method,
            path=request.url.path,
            user_id=subject,
            query=query,
            headers=request.headers,
        )
        if proof is None:
            return _gateway_error(401, "verified gateway identity required")

        declared = request.headers.get("content-length")
        if declared is not None:
            try:
                declared_bytes = int(declared)
            except ValueError:
                return _gateway_error(400, "invalid request body length")
            if declared_bytes < 0 or declared_bytes > MAX_GATEWAY_SIGNED_BODY_BYTES:
                return _gateway_error(413, "request body too large")
        chunks: list[bytes] = []
        received = 0
        try:
            async with asyncio.timeout(GATEWAY_SIGNED_BODY_READ_TIMEOUT_SECONDS):
                async for chunk in request.stream():
                    received += len(chunk)
                    if received > MAX_GATEWAY_SIGNED_BODY_BYTES:
                        return _gateway_error(413, "request body too large")
                    chunks.append(chunk)
        except TimeoutError:
            return _gateway_error(408, "request body timed out")
        except Exception:
            return _gateway_error(400, "request body is invalid")
        body = b"".join(chunks)
        request._body = body  # Starlette's wrapped receive replays this cache to FastAPI parsers.
        try:
            verified = await run_in_threadpool(
                _gateway_identity_verifier.verify_body_and_claim,
                proof,
                body,
            )
        except GatewayReplayUnavailable:
            return _gateway_error(
                503,
                "gateway identity boundary is unavailable",
                headers={"Retry-After": "1"},
            )
        if not verified:
            return _gateway_error(401, "verified gateway identity required")
        request.state.gateway_identity_scope = (request.method, request.url.path, subject)
        return await call_next(request)

    def _gateway_subject(request: Request, *, detail: str) -> str:
        subject = request.headers.get("x-user-id", "")
        proof_scope = (request.method, request.url.path, subject)
        if getattr(request.state, "gateway_identity_scope", None) == proof_scope:
            return subject
        raise HTTPException(status_code=401, detail=detail)

    def subject_of(request: Request) -> str:
        """The authenticated subject (P20). The gateway resolves the api-key → user_id and injects
        ``X-User-Id``; agent-api derives the workspace/chat/quota partition from THAT, never from the
        client body/query. Fail-closed (401) when the header is absent, unless a single-user fallback
        (``VEXA_AGENT_DEFAULT_SUBJECT``) is configured for a direct/self-host deploy with no gateway in front.

        When ``VEXA_REQUIRE_GATEWAY_IDENTITY`` is set, the request must additionally carry a fresh,
        request-bound Gateway signature. This does not change the default dev/direct topology."""
        if _require_gateway_identity:
            return _gateway_subject(
                request,
                detail="verified gateway identity required (VEXA_REQUIRE_GATEWAY_IDENTITY)",
            )
        uid = request.headers.get("x-user-id")
        if uid:
            return uid
        fallback = settings.agent_default_subject if settings is not None else ""
        if fallback:
            return fallback
        raise HTTPException(status_code=401, detail="missing X-User-Id (agent-api is fronted by the gateway)")

    if minutes_ingestor is not None:
        _validate_gateway_identity_config(
            required=True,
            secret=_gateway_identity_secret,
            previous_secret=_gateway_identity_previous_secret,
            internal_secret=_internal_api_secret,
        )

        def minutes_subject_of(request: Request) -> str:
            """Return only a gateway-attested subject for the sensitive Minutes route.

            The optional compatibility posture used by ordinary Agent routes is deliberately not
            inherited here: once the bundled reference route is composed, a direct caller cannot
            turn a spoofed ``X-User-Id`` into a transcript read.
            """
            return _gateway_subject(
                request,
                detail="verified gateway identity required for Minutes",
            )

        @app.post("/api/minutes/summarize-last")
        def summarize_last_minutes(request: Request):
            """Invoke the server-owned, non-chat Minutes pipeline for this authenticated subject.

            No request body participates in addressing or model context. Only the validated derived
            answer and non-sensitive execution counts leave the isolated ingestion stage.
            """
            subject = minutes_subject_of(request)
            try:
                result = minutes_ingestor.summarize_last_meeting(subject)
            except MinutesIngestDisabled:
                raise HTTPException(status_code=403, detail="Minutes read is disabled") from None
            except MinutesIngestError:
                raise HTTPException(status_code=503, detail="Minutes read is unavailable") from None
            return JSONResponse(
                {
                    "answer": result.answer,
                    "candidates_quarantined": result.candidates_quarantined,
                    "summary_fallback": result.summary_fallback,
                },
                headers={"Cache-Control": "no-store"},
            )

    @app.get("/health")
    def health():
        ok = dispatcher is not None
        # ADDITIVE config.v1 rows (ADR-0026): the agent plane's capability tri-states (bot_gateway ·
        # model_inference). They never affect `status`/`checks` or the status code — an unconfigured
        # capability degrades a FEATURE (e.g. 'add bot from URL', worker model credentials), not the
        # process; the runtime's /health carries the credentials-file probe for the mount mechanics.
        from control_plane.config_preflight import capability_health

        return JSONResponse(
            {"status": "ok" if ok else "degraded", "service": "agent-api", "checks": {"dispatcher": ok},
             "capabilities": capability_health()},
            status_code=200 if ok else 503,
        )

    @app.post("/internal/minutes/meetings/{meeting_id}/erase")
    async def internal_minutes_meeting_erase(meeting_id: str, request: Request):
        """Agent-owned, internal half of a Minutes meeting erasure receipt."""
        secret = settings.internal_api_secret.get_secret_value() if settings is not None else ""
        provided = request.headers.get("x-internal-secret", "")
        if not secret or not hmac.compare_digest(provided, secret):
            return JSONResponse(
                {"error": {"code": "forbidden"}}, status_code=403,
                headers={"Cache-Control": "no-store"},
            )
        if (
            not re.fullmatch(r"[1-9][0-9]{0,18}", meeting_id)
            or int(meeting_id) > 9_223_372_036_854_775_807
        ):
            return JSONResponse(
                {"error": {"code": "not_found"}}, status_code=404,
                headers={"Cache-Control": "no-store"},
            )

        declared_length = request.headers.get("content-length")
        if declared_length:
            try:
                if int(declared_length) > MAX_MINUTES_ERASURE_REQUEST_BYTES:
                    raise OverflowError
            except (ValueError, OverflowError):
                return JSONResponse(
                    {"error": {"code": "request_too_large"}}, status_code=413,
                    headers={"Cache-Control": "no-store"},
                )
        chunks: list[bytes] = []
        total = 0
        async for chunk in request.stream():
            total += len(chunk)
            if total > MAX_MINUTES_ERASURE_REQUEST_BYTES:
                return JSONResponse(
                    {"error": {"code": "request_too_large"}}, status_code=413,
                    headers={"Cache-Control": "no-store"},
                )
            chunks.append(chunk)
        try:
            body = json.loads(b"".join(chunks))
        except (UnicodeDecodeError, json.JSONDecodeError):
            body = None
        if (
            not isinstance(body, dict)
            or set(body) != {"user_id"}
            or not isinstance(body.get("user_id"), str)
            or not re.fullmatch(r"[1-9][0-9]{0,18}", body["user_id"])
            or int(body["user_id"]) > 9_223_372_036_854_775_807
        ):
            return JSONResponse(
                {"error": {"code": "invalid_request"}}, status_code=400,
                headers={"Cache-Control": "no-store"},
            )
        user_id = int(body["user_id"])
        if minutes_eraser is None:
            return JSONResponse(
                {"error": {"code": "erasure_pending"}}, status_code=503,
                headers={"Cache-Control": "no-store", "Retry-After": "5"},
            )
        try:
            receipt = minutes_eraser.erase(user_id=user_id, meeting_id=meeting_id)
        except ErasureNotFound:
            return JSONResponse(
                {"error": {"code": "not_found"}}, status_code=404,
                headers={"Cache-Control": "no-store"},
            )
        except ErasurePending:
            return JSONResponse(
                {"error": {"code": "erasure_pending"}}, status_code=503,
                headers={"Cache-Control": "no-store", "Retry-After": "5"},
            )
        except Exception as error:  # noqa: BLE001 - never expose private store/transport details
            logger.error(
                "Agent Minutes erasure failed row=%s user=%s kind=%s",
                meeting_id, user_id, type(error).__name__,
            )
            return JSONResponse(
                {"error": {"code": "erasure_pending"}}, status_code=503,
                headers={"Cache-Control": "no-store", "Retry-After": "5"},
            )
        if not _is_exact_agent_meeting_erasure_receipt(
            receipt, user_id=user_id, meeting_id=meeting_id,
        ):
            return JSONResponse(
                {"error": {"code": "erasure_pending"}}, status_code=503,
                headers={"Cache-Control": "no-store", "Retry-After": "5"},
            )
        # Return the wrapper's signed proof byte-for-structure.  The route must never re-sign or
        # project counts: retries replay the exact persisted receipt (same nonce/signature).
        return JSONResponse(receipt, headers={"Cache-Control": "no-store"})

    @app.get("/api/models")
    def models(request: Request):
        subject = subject_of(request)
        streaming_model = settings.meeting_model or default_meeting_model() or "default"
        try:
            # A workspace-pinned model (free string) wins; an unpinned workspace ("" — deployment
            # default) must NOT blank the label out.
            workspace_model = load_meeting_config(wsr.workspace_dir(subject)).model
            if workspace_model:
                streaming_model = workspace_model
        except ValueError:
            pass
        chat_model = settings.agent_model or "default"
        return {
            "chat_model": chat_model,
            "agent_model": chat_model,
            "streaming_model": streaming_model,
            "meeting_model": streaming_model,
        }

    @app.post("/invocations", status_code=202)
    def invocations(invocation: dict = Body(...)):
        """The dispatcher sink — any trigger source POSTs a unit.v1 dispatch here."""
        try:
            workload_id = dispatcher.dispatch(invocation)
        except ValidationError as e:  # non-conformant unit.v1 envelope — fail loud (P18)
            raise HTTPException(status_code=400, detail=f"invalid unit.v1 dispatch: {e.message}")
        return {"workload_id": workload_id}

    @app.post("/api/meeting/start", status_code=202)
    def meeting_start(body: MeetingStart, request: Request):
        """Retired native-only launcher.

        A native meeting id is neither tenant-unique nor connected to the numeric retention fence, so
        this legacy bridge could bypass owner-scoped consent and launch an unfenced copilot.  Managed
        capture now discovers the numeric row through the watcher and users opt in through the
        owner-bound ``/api/meeting/process`` desired-state endpoint.
        """
        subject_of(request)  # authenticate consistently even though the retired route never mutates
        raise HTTPException(
            status_code=410,
            detail="native-only meeting start is no longer supported",
        )

    @app.get("/api/meeting/relay-health")
    def meeting_relay_health(request: Request):
        """P18 (ADR 0010) — the transcript relay's observable health: is the numeric→native resolve OK,
        and are segments arriving? A stale `VEXA_BOT_API_KEY` (401 on `/meetings`) shows here as a typed
        `native_resolve: {ok:false, kind:'unauthorized', detail:…}` instead of silent dead air."""
        subject_of(request)
        from control_plane import transcription_watcher as _txw
        return _txw.relay_health()

    @app.get("/api/admin/overview")
    def admin_overview(request: Request):
        """Read-only infra + pipeline introspection for the terminal's hidden admin panel: every
        runtime.v1 workload (agent workers + meeting bots, classified) plus the per-meeting redis
        pipeline carriers (proc/tc streams, opt-in flag, cursor, active_meetings membership).

        INTERNAL-TIER ONLY (fail-closed): the caller must present ``X-Internal-Secret`` matching
        ``VEXA_INTERNAL_API_SECRET`` — the terminal's Next server holds it and fronts this with its
        own email-allowlist gate; an unconfigured secret means NOBODY gets in (403), and the check
        holds regardless of ingress (direct or via the gateway's /agent/* proxy)."""
        from control_plane import admin_panel

        secret = settings.internal_api_secret.get_secret_value() if settings is not None else ""
        provided = request.headers.get("x-internal-secret", "")
        if not secret or not hmac.compare_digest(provided, secret):
            raise HTTPException(status_code=403, detail="internal secret required")

        overview: dict = {"workloads": [], "meetings": []}
        try:
            overview["workloads"] = admin_panel.fetch_workloads(
                settings.runtime_api_url,
                control_secret=settings.runtime_control_secret.get_secret_value(),
            )
        except Exception as e:  # noqa: BLE001 — typed partial failure (P18): the panel shows the section error
            overview["workloads_error"] = f"{type(e).__name__}: {e}"
        if redis_url:
            import redis as _redis

            try:
                r = _redis.from_url(redis_url, decode_responses=True)
                overview["meetings"] = admin_panel.pipeline_snapshot(r, live.list())
            except Exception as e:  # noqa: BLE001
                overview["meetings_error"] = f"{type(e).__name__}: {e}"
        else:
            overview["meetings_error"] = "no redis_url configured"
        return overview

    @app.post("/api/admin/probe")
    def admin_probe(request: Request):
        """Run the transcription-pipeline golden smoke probe (gateway → meeting-api → runtime →
        redis carriers → transcript relay). Same internal-tier gate as the overview; POST because
        it actively exercises the path (a redis write/read round-trip on scratch keys)."""
        from control_plane import admin_panel
        from control_plane import transcription_watcher as _txw

        secret = settings.internal_api_secret.get_secret_value() if settings is not None else ""
        provided = request.headers.get("x-internal-secret", "")
        if not secret or not hmac.compare_digest(provided, secret):
            raise HTTPException(status_code=403, detail="internal secret required")

        r = None
        if redis_url:
            import redis as _redis

            try:
                r = _redis.from_url(redis_url, decode_responses=True)
            except Exception:  # noqa: BLE001 — the probe's redis stage reports the fault
                r = None
        # Workloads cross-check the in-memory live registry (a stale "live" entry must not turn
        # relay quiet into a false FAIL). Unknown (kernel unreachable) → None = trust the registry.
        try:
            workloads = admin_panel.fetch_workloads(
                settings.runtime_api_url,
                control_secret=settings.runtime_control_secret.get_secret_value(),
            )
        except Exception:  # noqa: BLE001
            workloads = None
        return admin_panel.run_probe(settings, r, live.list(), relay_health=_txw.relay_health(),
                                     workloads=workloads)

    @app.post("/api/meeting/process", status_code=202)
    def meeting_process(body: MeetingProcess, request: Request):
        """Owner-controlled copilot PROCESSING for one numeric meeting row (ADR 0027).

        ON atomically retention-checks and writes an opaque desired-state generation; the watcher is
        the one dispatch arbiter and resumes from the frozen per-row cursor. OFF first revokes that
        generation, then authoritatively stops the active runtime workload. Workers prove the exact
        generation before transcript/model/derivative operations, so a race-spawned or pre-reenable
        worker is inert. The cursor remains frozen for a later, newly consented gap-fill.
        """
        # Authentication and BOLA protection precede Redis construction/mutation for BOTH ON and OFF.
        # Numeric meeting rows are the only managed identity: native ids collide across tenants and do
        # not map to the permanent retention fence, so the legacy native fallback is deliberately gone.
        subject = subject_of(request)
        row_id = str(body.meeting_id or "").strip()
        # Canonical signed-64-bit decimal only.  Accepting "041" would owner-check row 41 over HTTP
        # but key Redis/fences on 041, silently escaping the row's retention authority.
        if not re.fullmatch(r"[1-9][0-9]{0,18}", row_id):
            raise HTTPException(status_code=404, detail="meeting not found")
        owned = _meeting_owner_lookup(subject, row_id)
        if not isinstance(owned, dict):
            # Unknown and foreign rows are intentionally indistinguishable.
            raise HTTPException(status_code=404, detail="meeting not found")
        if str(owned.get("id") or "") != row_id or str(owned.get("user_id") or "") != subject:
            # A malformed or mis-scoped authority response cannot bless a Redis key.
            raise HTTPException(status_code=404, detail="meeting not found")
        owned_native = str(owned.get("native_meeting_id") or "").strip()
        if not body.native_id or len(body.native_id) > 512 or (
            owned_native and body.native_id != owned_native
        ):
            raise HTTPException(status_code=404, detail="meeting not found")

        import redis as _redis

        r = _redis.from_url(redis_url, decode_responses=True)
        key = row_id
        # The opt-in flag has its OWN key suffix — it must NOT collide with the processed-notes STREAM
        # ``proc:meeting:{key}`` the worker XADDs (worker.py), else a GET on the flag hits a stream →
        # WRONGTYPE (crashes the watcher's arm loop). ``:cursor`` is likewise a distinct sibling key.
        flag = f"proc:meeting:{key}:on"
        cursor_key = f"proc:meeting:{key}:cursor"
        if not body.on:
            try:
                r.delete(flag)  # cursor is intentionally LEFT in place (frozen) for the next re-enable
            except Exception as error:  # noqa: BLE001 — never report OFF without revocation authority
                logger.error(
                    "meeting processing revocation authority unavailable for %s (%s)",
                    key,
                    type(error).__name__,
                )
                raise HTTPException(
                    status_code=503,
                    detail="meeting processing authority is unavailable",
                ) from None
            try:
                # Desired-state deletion prevents new work; runtime stop cancels the already-running
                # worker.  A race-spawned worker carries the deleted generation and self-rejects before
                # reading transcript content, while a later ON receives a different generation.
                dispatcher.stop_workload(f"agent-meet-{key}")
            except Exception as error:  # noqa: BLE001 — stop must be positively acknowledged
                logger.error(
                    "meeting processing runtime stop unconfirmed for %s (%s)",
                    key,
                    type(error).__name__,
                )
                raise HTTPException(
                    status_code=503,
                    detail="meeting processing stop could not be confirmed",
                ) from None
            return {
                "native_id": owned_native or body.native_id,
                "meeting_id": row_id,
                "processing": False,
            }
        try:
            # TTL'd desired state (P21/P22 — verified on the eyeball: NO session_end frame ever
            # crosses the wire on the stop path, so the watcher's reap there is belt-only and the
            # flag used to persist forever). This backstop bounds a flag that never sees a segment;
            # the watcher REFRESHES a rolling TTL while segments actually flow, so the flag outlives
            # any real meeting and self-cleans within ~an hour of the flow stopping.
            expires_at_ms = _owned_processing_deadline_ms(owned)
            generation = secrets.token_urlsafe(24)
            if expires_at_ms is not None:
                generation = bind_processing_deadline(generation, expires_at_ms)
            allowed, cursor = activate_processing_if_writable(
                r,
                key,
                flag_key=flag,
                cursor_key=cursor_key,
                ttl_seconds=PROC_FLAG_BACKSTOP_TTL_SEC,
                token=generation,
                expires_at_ms=expires_at_ms,
            )
        except Exception as error:  # noqa: BLE001 — authority loss must never report processing on
            logger.error(
                "meeting processing activation authority unavailable for %s (%s)",
                key,
                type(error).__name__,
            )
            raise HTTPException(
                status_code=503,
                detail="meeting processing authority is unavailable",
            ) from None
        if not allowed:
            raise HTTPException(
                status_code=410,
                detail="meeting processing is no longer available",
            )
        # `resumed_from` reports where the watcher's arm WILL resume (the frozen cursor, else the
        # start of the transcript) — informational for the client; the dispatch itself happens on
        # the watcher's next segment (≤ one batch), keyed and started from the same cursor.
        start_id = cursor or "0-0"
        return {
            "native_id": owned_native or body.native_id,
            "meeting_id": row_id,
            "processing": True,
            "resumed_from": start_id,
        }

    @app.post("/api/chat")
    def chat(body: ChatBody, request: Request):
        """A chat *now*-dispatch: spawn the isolated container, stream its Stream back as SSE.

        RESUMABLE (mirrors /api/meeting/stream): every SSE event carries an ``id:`` = the unit output
        Stream cursor. A dropped view (per-dispatch worker cold-start races the SSE, a transient proxy
        drop) reconnects with ``Last-Event-ID`` — we then RE-ATTACH to the SAME warm unit and resume the
        read from that cursor (gapless) WITHOUT dispatching a second turn. The turn was never lost (the
        worker completes + commits regardless); resume just re-shows the output the client missed."""
        if stream_reader is None:
            raise HTTPException(status_code=501, detail="stream relay not wired")
        subject = subject_of(request)  # server-derived (P20); body.subject is ignored
        session = body.session or units.DEFAULT_CHAT_SESSION
        # A reconnect carries Last-Event-ID (the last Stream cursor the client rendered). On resume we
        # DON'T re-dispatch — we re-attach to the existing warm unit and read from the cursor onward.
        resume = request.headers.get("last-event-id") or None
        # Ground meeting metadata/prep context when safe. Raw/processed meeting content is never folded
        # into this durable generic-chat prompt; it requires the dedicated bounded Minutes read path.
        ctx, tools, prompt = _context_grounding(
            body, session, redis_url,
            schedule_rows=lambda: _schedule_source(subject),
            workspace_mounts=lambda: (active_workspaces(wsr.root, subject)
                                      + shared_active_mounts(wsr.root, subject, mindex.list(subject))),
        )
        # Mark the grounding→user boundary so the terminal strips ALL folded context in one cut. Every
        # branch returns `<grounding> + body.prompt`, so the user's words are the exact suffix; insert the
        # sentinel right before them (no-op when nothing was folded). The kg/mounts preambles the worker
        # prepends land before `prompt`, hence before the sentinel too — so they're stripped as well.
        if body.prompt and prompt.endswith(body.prompt) and len(prompt) > len(body.prompt):
            prompt = prompt[: len(prompt) - len(body.prompt)] + CONTEXT_SENTINEL + body.prompt
        # Attribute this turn's commits to the human editor by EMAIL (gateway-injected, trusted) rather
        # than the bare subject id — the git author NAME becomes the email; the synthetic author email
        # (<subject>@vexa.local) stays for the you/member classification (workspace_reader.git_state_at).
        _email = (request.headers.get("x-user-email") or "").strip()
        inv = units.make_dispatch(
            subject=subject, trigger="message",
            start=units.entrypoint(inline=prompt), context=ctx, tools=tools,
            principal={"name": _email} if _email else None,
        )
        if resume:
            # Re-attach only — the warm unit id is deterministic from (subject, session); resume reads
            # its durable output Stream from the cursor. No new turn, no session re-title.
            unit_id = units.dispatch_id(inv)
        else:
            unit_id = units.dispatch_id(inv)
            retry_from = _chat_turn_head(redis_url, unit_id, body.turn_id) if body.turn_id else None
            if retry_from is not None:
                # No-cursor RETRY of the current turn (the stream dropped before the client saw any
                # ``id:``): re-attach from the turn's recorded start — the whole turn replays, including
                # a terminal event the worker wrote while the client was gone. NO second dispatch.
                resume = retry_from
            else:
                # Fresh turn — credential preflight FIRST (config.v1 ``model_inference``, the
                # request-path oracle): with no deployment credential AND no per-user custom
                # endpoint, the worker's claude CLI can only fail with its own "Not logged in ·
                # Please run /login" — an adapter internal that means nothing to an API consumer.
                # Refuse HERE with an actionable frame instead: no worker spawn, no ghost session
                # entry. A FAILED config lookup (None) fails OPEN — a down identity service must
                # never block a turn; the worker-side auth taxonomy still catches it cleanly.
                cfg = dispatcher.resolve_model_config(subject)
                if cfg is not None and cfg.get("blocked"):
                    return StreamingResponse(
                        _sse([{"type": "error", "message": _model_blocked_error_message()},
                              {"type": "turn-complete"}]),
                        media_type="text/event-stream",
                        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no",
                                 "X-Unit-Id": unit_id, "X-Chat-Session": session},
                    )
                if capability_state("model_inference") == NOT_CONFIGURED:
                    if cfg is not None and not _has_custom_model_endpoint(cfg):
                        return StreamingResponse(
                            _sse([{"type": "error", "message": _model_creds_error_message()},
                                  {"type": "turn-complete"}]),
                            media_type="text/event-stream",
                            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no",
                                     "X-Unit-Id": unit_id, "X-Chat-Session": session},
                        )
                # Snapshot the out-Stream tail BEFORE dispatching and attach the reader from
                # it — attaching at ``$`` raced the worker (events written between dispatch and attach,
                # or a whole turn that finished in the gap, were invisible: the 'Reconnecting' hang).
                # The thread's Stream holds PRIOR turns too, so the snapshot (not stream start) is the
                # earliest safe attach point — the client appends and stops on any ``turn-complete``.
                start = _stream_tail_id(redis_url, units.output_topic(unit_id)) or None
                # Upsert the durable index on first use of a thread: a new thread is titled by its first
                # prompt; an existing one just bumps last_active (title preserved).
                is_new = not any(r["session"] == session for r in sess.list(subject))
                sess.upsert(subject, session,
                            title=_truncate_title(body.prompt) if is_new else None)
                unit_id = dispatcher.dispatch(inv)  # spawn-or-touch the thread's warm chat unit
                if body.turn_id and start is not None:
                    _record_chat_turn_head(redis_url, unit_id, body.turn_id, start)
                resume = start
        return StreamingResponse(
            _sse(stream_reader.read(unit_id, resume=resume)),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no",
                     "X-Unit-Id": unit_id, "X-Chat-Session": session},
        )

    @app.post("/api/chat/reset")
    def chat_reset(body: ResetBody, request: Request):
        """Drop a conversation thread: remove it from the index AND delete its continuity file so a
        future turn on the same name starts a fresh conversation (not a resume of the old one)."""
        subject = subject_of(request)
        session = body.session or units.DEFAULT_CHAT_SESSION
        sess.drop(subject, session)
        try:
            wsr.drop_session(subject, session)
        except Exception:  # noqa: BLE001 — index drop is the contract; the file delete is best-effort
            logger.exception("dropping continuity file failed subject=%s session=%s", subject, session)
        return {"ok": True}

    @app.get("/api/sessions")
    def list_sessions(request: Request):
        return {"sessions": sess.list(subject_of(request))}

    @app.get("/api/sessions/{session}/history")
    def session_history(session: str, request: Request):
        """The session's prior conversation, as simplified turns the terminal can render (so clicking a
        saved chat re-opens its history). Tolerant: a missing/empty transcript returns ``{turns: []}``;
        an invalid subject/session never 500s."""
        subject = subject_of(request)
        # The turn's cwd FOLLOWS the active set (flat model), so a thread's continuity may sit under
        # any currently-mounted workspace dir — hand the reader those candidates. Best-effort: a
        # failing mount resolution only narrows the search to _system + home.
        extra: list = []
        try:
            ms = active_workspaces(wsr.root, subject) + shared_active_mounts(wsr.root, subject, mindex.list(subject))
            extra = [m.path for m in ms]
        except Exception:  # noqa: BLE001
            logger.warning("mount resolution for history failed subject=%s — searching anchored roots only", subject)
        try:
            turns = wsr.history(subject, session, extra_roots=extra)
        except Exception:  # noqa: BLE001 — history is best-effort; a bad path → empty, never an error
            logger.exception("loading session history failed subject=%s session=%s", subject, session)
            turns = []
        return {"turns": turns}

    # ── routines (MVP2) — a scheduled routine compiles to a schedule.v1 cron job whose body is a
    #    unit.v1 dispatch POSTed back to /invocations when due (the runtime owns the durable cron) ──
    @app.post("/api/routines", status_code=201)
    def create_routine(body: RoutineCreate, request: Request):
        if scheduler is None or not invocations_url:
            raise HTTPException(status_code=501, detail="scheduler not wired")
        try:
            routine = routines_mod.make_routine(
                subject=subject_of(request), name=body.name, cron=body.cron, prompt=body.prompt,
            )
            job_spec = routines_mod.compile_to_job(routine, invocations_url=invocations_url)
        except (ValueError, ValidationError) as e:  # bad cron form / non-conformant routine — fail loud
            raise HTTPException(status_code=400, detail=str(getattr(e, "message", e)))
        job = scheduler.schedule(job_spec)
        ran_now = False
        if body.run_now:
            # Fire one immediate run via the dispatcher (no HTTP hop) so the author sees a result now.
            try:
                dispatcher.dispatch(job_spec["request"]["body"])
                ran_now = True
            except Exception:  # noqa: BLE001 — the routine is still scheduled even if the demo run fails
                ran_now = False
        return {"routine": routine, "job_id": job.get("job_id"), "ran_now": ran_now}

    @app.get("/api/routines")
    def list_routines(request: Request):
        if scheduler is None:
            return {"routines": []}
        cards = workspace_routines_mod.routine_cards_for_subject(
            subject_of(request),
            jobs=scheduler.list_jobs(limit=1000),
            workspaces_dir=wsr.root,
        )
        return {"routines": cards}

    @app.patch("/api/routines/{name}/enabled")
    def set_routine_enabled(name: str, body: RoutineEnabledPatch, request: Request):
        if scheduler is None or not invocations_url:
            raise HTTPException(status_code=501, detail="scheduler not wired")
        subject = subject_of(request)
        try:
            workspace_routines_mod.set_routine_file_enabled(
                subject,
                name,
                enabled=body.enabled,
                workspaces_dir=wsr.root,
            )
            result = workspace_routines_mod.reconcile_workspace_routines(
                subject,
                scheduler=scheduler,
                invocations_url=invocations_url,
                workspaces_dir=wsr.root,
            )
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="unknown routine")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {
            "ok": True,
            "name": name,
            "enabled": body.enabled,
            "reconcile": result.__dict__,
        }

    @app.delete("/api/routines/{routine_id}")
    def delete_routine(routine_id: str, request: Request):
        if scheduler is None:
            raise HTTPException(status_code=501, detail="scheduler not wired")
        subject = subject_of(request)
        for job in scheduler.list_jobs():
            meta = job.get("metadata") or {}
            if meta.get("routine_id") == routine_id and meta.get("owner") == subject:
                scheduler.cancel_job(job["job_id"])
                return {"ok": True, "routine_id": routine_id}
        raise HTTPException(status_code=404, detail="unknown routine")

    # ── events (MVP3) — the GENERIC event-source ingress: any event.v1 Event → a unit.v1 dispatch →
    #    the one Dispatcher. agent-api knows no tool/domain; the unit reaches email/calendar via its
    #    toolbelt. Email-triage, post-meeting, news all POST here (one front door, P6) ──
    @app.post("/events", status_code=202)
    def events(event: dict = Body(...)):
        try:
            invocation = event_to_invocation(event)
        except ValidationError as e:
            raise HTTPException(status_code=400, detail=f"invalid event.v1: {e.message}")
        except ValueError as e:  # no plan carried — fail loud (P18)
            raise HTTPException(status_code=422, detail=str(e))
        workload_id = dispatcher.dispatch(invocation)
        return {"workload_id": workload_id, "trigger": invocation["trigger"]}

    def _read_target(request: Request, slug: Optional[str]) -> Path:
        """Resolve which workspace dir a READ (tree/file) targets, returning its ABSOLUTE PATH. Default (no
        slug) = the caller's primary baseline. A `slug` addresses ANOTHER mount in the caller's active set —
        their own non-primary private workspaces (which live under .attached, NOT <root>/<slug>) OR a SHARED
        workspace they're a member of. Authorization is by construction: the set is built for THIS subject
        (own actives + shared_active_mounts over their memberships), so a slug not in it → 403. This is what
        lets the KNOWLEDGE panel render one section per active mount without leaking arbitrary workspaces."""
        subject = subject_of(request)
        target = (slug or "").strip()
        # _system — the caller's OWN private-system workspace (RW, surfaced hidden-by-default in the files
        # panel). It's a per-subject dispatch mount, not in the active set, so authorize it directly here:
        # it can only ever resolve to THIS subject's own .system store — never another user's.
        if target == system_mounts.SYSTEM_SLUG:
            return system_mounts.system_store_path(wsr.root, subject)
        mounts = active_workspaces(wsr.root, subject)  # own actives (real .attached paths); may raise ValueError
        try:
            mounts = mounts + shared_active_mounts(wsr.root, subject, mindex.list(subject))
        except Exception:  # noqa: BLE001 — a shared-mount hiccup must not break a plain own-workspace read
            pass
        if not target or target == subject:
            primary = next((m for m in mounts if m.primary), None)
            return Path(primary.path) if primary else (wsr.root / subject)
        for m in mounts:
            if m.slug == target:
                return Path(m.path)
        raise HTTPException(status_code=403, detail="not authorized for this workspace")

    def _manage_dir(subject: str, slug: Optional[str], *, write: bool = False) -> Path:
        """Resolve a workspace dir for a MANAGEMENT op (git sync, purpose) — unlike ``_read_target`` this
        also reaches the caller's PARKED slots (a workspace need not be mounted to manage it). Own slots
        first (active or parked); a slug that isn't one of them but IS a shared workspace the caller belongs
        to resolves to the shared dir. Shared mutations require contributor-or-owner; viewers retain the
        status/purpose reads. Neither path can ever reach another user's private workspace."""
        try:
            return workspace_dir_for(wsr.root, subject, slug)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid subject")
        except KeyError:
            pass
        target = (slug or "").strip()
        if target and membership_mod.is_member(wsr.root, target, subject) is not None:
            if write:
                try:
                    membership_mod.require_role(wsr.root, target, subject, "contributor")
                except MembershipError as exc:
                    raise HTTPException(status_code=exc.status, detail=str(exc)) from None
            return membership_mod._ws_dir(wsr.root, target)
        raise HTTPException(status_code=404, detail="workspace not found")

    @app.get("/api/workspace/tree")
    def ws_tree(request: Request, hidden: bool = False, slug: Optional[str] = None):
        try:
            return {"files": wsr.tree_at(_read_target(request, slug), hidden=hidden)}
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid subject")

    @app.post("/api/workspace/upload")
    async def ws_upload(request: Request, files: list[UploadFile] = File(...)):
        if not files:
            raise HTTPException(status_code=400, detail="no files uploaded")
        if len(files) > MAX_UPLOAD_FILES:
            raise HTTPException(status_code=413, detail="too many upload files")
        subject = subject_of(request)
        try:
            ws = wsr.workspace_dir(subject)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid subject")
        uploads = ws / "uploads"
        pending: list[tuple[bytes, str, str]] = []
        aggregate_bytes = 0
        try:
            for file in files:
                chunks: list[bytes] = []
                file_bytes = 0
                try:
                    while True:
                        chunk = await file.read(UPLOAD_READ_CHUNK_BYTES)
                        if not chunk:
                            break
                        file_bytes += len(chunk)
                        if file_bytes > MAX_UPLOAD_BYTES:
                            raise HTTPException(status_code=413, detail="upload exceeds 25MB")
                        if aggregate_bytes + file_bytes > MAX_UPLOAD_TOTAL_BYTES:
                            raise HTTPException(status_code=413, detail="upload batch exceeds 25MB")
                        chunks.append(chunk)
                except HTTPException:
                    raise
                except Exception:
                    raise HTTPException(status_code=400, detail="could not read upload") from None

                content = b"".join(chunks)
                aggregate_bytes += file_bytes
                safe_name = _upload_filename(file.filename)
                digest = hashlib.sha256(content).hexdigest()
                stored_name = f"{digest[:16]}-{safe_name}"
                pending.append((content, stored_name, f"uploads/{stored_name}"))
        finally:
            for file in files:
                try:
                    await file.close()
                except Exception:  # noqa: BLE001 — close is best-effort; never replace the route outcome
                    logger.warning("closing an uploaded file failed")

        # All request-controlled validation completes before the first filesystem mutation. Keeping
        # the aggregate equal to the per-file ceiling bounds both this pending list and the write phase.
        # A worker may write symlinks inside its own workspace. Never resolve/follow an `uploads`
        # symlink from agent-api's all-workspaces mount: that could redirect a later authenticated
        # upload into another tenant. O_NOFOLLOW closes the check/open race; all file writes are
        # relative to the verified directory fd and atomically replace the directory entry itself.
        uploaded: list[dict[str, str]] = []
        directory_fd: int | None = None
        try:
            if uploads.is_symlink():
                raise HTTPException(status_code=400, detail="invalid upload directory")
            uploads.mkdir(parents=True, exist_ok=True)
            directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            directory_fd = os.open(uploads, directory_flags)
            for content, stored_name, path in pending:
                temporary_name = f".{stored_name}.{secrets.token_hex(8)}.tmp"
                file_fd: int | None = None
                try:
                    file_flags = (
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
                    )
                    file_fd = os.open(temporary_name, file_flags, 0o600, dir_fd=directory_fd)
                    with os.fdopen(file_fd, "wb", closefd=True) as output:
                        file_fd = None
                        output.write(content)
                    os.replace(
                        temporary_name,
                        stored_name,
                        src_dir_fd=directory_fd,
                        dst_dir_fd=directory_fd,
                    )
                finally:
                    if file_fd is not None:
                        os.close(file_fd)
                    try:
                        os.unlink(temporary_name, dir_fd=directory_fd)
                    except FileNotFoundError:
                        pass
                uploaded.append({"name": stored_name, "path": path})
        except HTTPException:
            raise
        except OSError:
            raise HTTPException(status_code=507, detail="could not store upload") from None
        finally:
            if directory_fd is not None:
                os.close(directory_fd)
        return {"files": uploaded}

    @app.get("/api/workspace/file")
    def ws_file(request: Request, path: str, slug: Optional[str] = None):
        try:
            content = wsr.read_at(_read_target(request, slug), path)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid path")
        if content is None:
            raise HTTPException(status_code=404, detail="not found")
        return {"path": path, "content": content}

    @app.get("/api/workspace/git")
    def ws_git(request: Request, slug: Optional[str] = None):
        """Author-attributed source-control state (branch · working changes · recent commits) of a
        workspace. No ``slug`` → the caller's own primary. A ``slug`` addresses a SHARED workspace the
        caller is a member of (same authorized resolution as tree/file reads) — its commits carry
        ``author`` + ``kind`` so the terminal can show OTHER members' agent pushes as they land."""
        try:
            target = _read_target(request, slug)  # authorizes: a slug outside the caller's mount set → 403
            return wsr.git_state_at(target, viewer=subject_of(request))
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid subject")

    @app.get("/api/workspace/git/show")
    def ws_git_show(request: Request, sha: str, slug: Optional[str] = None, path: Optional[str] = None):
        """Unified diff of ONE commit (optionally one file) — same authorized resolution as ws_git — so
        the terminal can highlight exactly what a commit changed."""
        try:
            target = _read_target(request, slug)  # authorizes: a slug outside the caller's mount set → 403
            return wsr.git_diff_at(target, sha, path)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid subject")

    # ── workspace lifecycle (SCAFFOLD / TODO(phase-6)) — init from a validated template, swap which
    # validated workspace/template the next dispatch mounts. The seams exist downstream (seeding.seed_workspace
    # for init; VEXA_WORKSPACE_REPO/REF in dispatch/spawn for swap, bridge resolves per-meeting) — Phase 6
    # surfaces them here and wires the slim-client init_workspace()/use_workspace().
    @app.post("/api/workspace/init", status_code=201)
    def ws_init(request: Request):
        """EAGERLY provision this subject's workspace tiers — the "on account creation" seam (so the
        Personal baseline + the private `_system` tier exist BEFORE the first dispatch, instead of being
        lazily seeded on first turn). Materializes the baseline from the VALIDATED workspace-seed template
        (shared.seeding.seed_workspace) and ensures `_system` (system_mounts.ensure_system_workspace).
        Idempotent — existing tiers (`.git` present) are returned untouched, so it's safe to call on every
        login. The same seams the worker uses lazily on first dispatch, surfaced as a control."""
        subject = subject_of(request)
        ws = wsr.workspace_dir(subject)
        # Select the seed out of the registry root (default template for now; per-request template
        # selection lands with the second seed). VEXA_WORKSPACE_SEED_DIR still overrides.
        seed_dir = resolve_seed_dir(
            settings.default_template if settings is not None else None,
            seeds_root=settings.workspace_seeds_dir if settings is not None else None,
        )
        problems = validate_seed(seed_dir)
        if problems:
            raise HTTPException(status_code=500, detail="invalid workspace seed: " + "; ".join(problems))
        existed = (ws / ".git").exists()
        seed_workspace(ws, seed_dir)
        # The PRIVATE SYSTEM tier (`_system`) — always-mounted, holds the light identity reference. Ensure
        # it up front too so identity + chats/settings have a home from the very first turn. Idempotent.
        system_existed = (system_mounts.system_store_path(wsr.root, subject) / ".git").exists()
        system_mounts.ensure_system_workspace(str(wsr.root), subject)
        return {"workspace": str(ws), "seeded": not existed, "already_initialized": existed,
                "system_seeded": not system_existed}

    @app.get("/api/workspace/attached")
    def ws_attached(request: Request):
        """The subject's attachment view: the active slug + the parked workspaces available to swap back
        to, plus ``published_url`` — where the ACTIVE workspace was published (the ``vexa-publish``
        remote's token-free URL), or null when it never was. The client renders a published workspace
        with a link to its GitHub home instead of the publish action."""
        subject = subject_of(request)
        try:
            state = attached_workspaces(wsr.root, subject)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid subject")
        state["published_url"] = published_remote_url(wsr.workspace_dir(subject))
        return state

    @app.post("/api/workspace/swap")
    def ws_swap(request: Request, body: WorkspaceSwapBody = Body(default=WorkspaceSwapBody())):
        """Attach a CUSTOM external git repo as this subject's active workspace (swap). The currently
        active workspace is PARKED (kept, never destroyed) so it can be swapped back to; the requested
        repo is restored from a prior park or cloned fresh. Omit ``repo`` to swap back to the seed.

        Mounting is by-folder (``<root>/<subject>`` is what the next dispatch mounts), so the swapped
        tree takes effect on the subject's next turn — no dispatch change needed."""
        subject = subject_of(request)
        _tok = (body.token or "").strip() or git_creds.read_github_token(wsr.root, subject)
        try:
            result = swap_workspace(wsr.root, subject, body.repo, body.ref or "main",
                                    slug=body.slug or None, fresh=body.fresh, token=_tok or None)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid subject")
        except KeyError:
            raise HTTPException(status_code=404, detail="unknown workspace")
        except CloneError as exc:
            # message is already token-redacted (P15); private repo without/with a bad token lands here.
            raise HTTPException(status_code=502, detail=f"git clone failed: {exc}")
        return {
            "subject": result.subject,
            "active": result.active_slug,
            "repo": result.repo,
            "ref": result.ref,
            "swapped": result.swapped,
            "cloned": result.cloned,
            "parked": result.parked_slug,
            "nested": result.nested,
        }

    # ── the additive mount set (WP-A2.1): ACTIVE-SET membership over swap's park/restore machinery ──────
    @app.get("/api/workspace/active")
    def ws_active(request: Request):
        """The subject's ordered ACTIVE SET — the workspaces the next dispatch mounts (the private baseline
        first, then any activated extras). Each: ``slug, repo, ref, role, path, write, primary``."""
        subject = subject_of(request)
        try:
            mounts = active_workspaces(wsr.root, subject)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid subject")
        # Lane A: append the SHARED workspaces the subject is a member of. The index (users.data.memberships[])
        # only ENUMERATES candidates; shared_active_mounts re-checks the role authoritatively per workspace.
        # A failing index costs the "shared" section of the set, never the subject's own private mounts.
        try:
            mounts = mounts + shared_active_mounts(wsr.root, subject, mindex.list(subject))
        except Exception:  # noqa: BLE001 — a shared-mount resolution hiccup must not break the active-set read
            logger.warning("shared-mount resolution failed for subject=%s — returning private mounts only", subject)
        return {
            "subject": subject,
            "active": [
                {"slug": m.slug, "repo": m.repo, "ref": m.ref, "role": m.role,
                 "path": m.path, "write": m.write, "primary": m.primary, "name": m.name}
                for m in mounts
            ],
        }

    @app.post("/api/workspace/activate")
    def ws_activate(request: Request, body: WorkspaceActivateBody = Body(default=WorkspaceActivateBody())):
        """ADD a workspace to the active set WITHOUT parking the others (the additive counterpart of swap).
        Clones/restores the target if needed. Idempotent — an already-active workspace is a no-op."""
        subject = subject_of(request)
        _tok = (body.token or "").strip() or git_creds.read_github_token(wsr.root, subject)
        try:
            result = activate_workspace(wsr.root, subject, body.repo, body.ref or "main",
                                        slug=body.slug or None, token=_tok or None)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid subject")
        except KeyError:
            raise HTTPException(status_code=404, detail="unknown workspace")
        except CloneError as exc:
            raise HTTPException(status_code=502, detail=f"git clone failed: {exc}")
        return {"subject": result.subject, "slug": result.slug, "changed": result.changed,
                "cloned": result.cloned, "nested": result.nested}

    @app.post("/api/workspace/new", status_code=201)
    def ws_new(request: Request, body: WorkspaceNewBody = Body(default=WorkspaceNewBody())):
        """CREATE a brand-new BLANK workspace (seeded from the template) at a fresh unique slug and ADD it
        to the active set (additive — the "new workspace" action). Nothing is parked/rebuilt/backed up: the
        private baseline and every other active workspace stay exactly as they were."""
        subject = subject_of(request)
        try:
            result = create_workspace(wsr.root, subject, name=body.name or None)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid subject")
        return {"subject": result.subject, "slug": result.slug, "changed": result.changed,
                "added": True}

    @app.post("/api/workspace/deactivate")
    def ws_deactivate(request: Request, body: WorkspaceDeactivateBody = Body(...)):
        """REMOVE a workspace from the active set (park it — never destroyed). The private baseline can be
        switched off too (sets ``baseline_hidden``; its home tree is untouched, re-activate to switch it back
        on). Idempotent — an already-off / not-active slug is a no-op."""
        subject = subject_of(request)
        try:
            result = deactivate_workspace(wsr.root, subject, body.slug)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid subject")
        return {"subject": result.subject, "slug": result.slug, "changed": result.changed}

    @app.post("/api/workspace/publish")
    def ws_publish(request: Request, body: WorkspacePublishBody = Body(...)):
        """Publish this subject's vexa-born workspace to GitHub — the counterpart of swap/attach.
        Creates the repo under the caller's account (or ``org``) with their per-call PAT, then pushes
        the active workspace's current branch (FULL history) over the token-scrubbed dedicated remote.
        ``remote_url`` skips creation (pre-created/empty repo). Re-publish = plain push (fast-forward
        or a clear error on divergence — never a force push). The token is used server-side for this
        call only and never stored; every error is token-redacted (P15)."""
        subject = subject_of(request)
        token = (body.token or "").strip() or git_creds.read_github_token(wsr.root, subject)
        if not token:
            raise HTTPException(status_code=400, detail="a GitHub token is required — pass one or save a reusable token")
        try:
            result = publish_workspace(
                wsr.root, subject,
                token=token, repo_name=body.repo_name, private=body.private,
                org=body.org or None, remote_url=body.remote_url or None,
                # slug → any workspace the caller can manage (own parked slot or shared membership,
                # resolved + permission-checked by _manage_dir); omitted keeps the legacy seed target.
                ws_dir=_manage_dir(subject, body.slug, write=True) if body.slug else None,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc) or "invalid subject")
        except RepoExistsError as exc:
            raise HTTPException(status_code=409, detail=str(exc))   # already token-redacted (P15)
        except PublishError as exc:
            raise HTTPException(status_code=502, detail=str(exc))   # already token-redacted (P15)
        return {
            "repo_url": result.repo_url,
            "pushed_ref": result.pushed_ref,
            "head_sha": result.head_sha,
            "created": result.created,
        }

    @app.post("/api/workspace/rename")
    def ws_rename(request: Request, body: WorkspaceRenameBody = Body(...)):
        """Rename a workspace slot — a DISPLAY label only. The slug and the parked tree are unchanged, so
        swap-back and repo re-attach keep matching. Pass an empty ``name`` to clear the label."""
        subject = subject_of(request)
        try:
            return rename_workspace(wsr.root, subject, body.slug, body.name)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid subject")
        except KeyError:
            raise HTTPException(status_code=404, detail="unknown workspace")

    @app.get("/api/workspace/git-token")
    def ws_git_token_get(request: Request):
        """Whether the caller has a SAVED reusable GitHub token, and a masked (last-4) preview of it. The
        clear value is NEVER returned — server-side only (git_credentials)."""
        subject = subject_of(request)
        return {"set": git_creds.read_github_token(wsr.root, subject) is not None,
                "masked": git_creds.masked_github_token(wsr.root, subject)}

    @app.post("/api/workspace/git-token")
    def ws_git_token_set(request: Request, body: GitTokenBody = Body(default=GitTokenBody())):
        """Save (or CLEAR, with an empty token) the caller's reusable GitHub token — stored once, server-
        side, and applied as the fallback credential for every git op across all their repos. Returns the
        masked state, never the clear value."""
        subject = subject_of(request)
        try:
            stored = git_creds.set_github_token(wsr.root, subject, body.token)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"set": stored, "masked": git_creds.masked_github_token(wsr.root, subject)}

    @app.get("/api/workspace/git-remote-status")
    def ws_git_remote_status(request: Request, slug: Optional[str] = None):
        """The GitHub-sync state of a workspace (default = the caller's primary; ``slug`` = one of their
        own or shared workspaces). Read-only + no network: reports the home remote (origin / vexa-publish),
        its URL, the branch, and ahead/behind counts vs the last-fetched tracking ref. No token needed."""
        subject = subject_of(request)
        ws = _manage_dir(subject, slug)
        s = remote_status(ws)
        return {
            "has_home": s.has_home, "remote": s.remote, "url": s.url, "branch": s.branch,
            "tracked": s.tracked, "ahead": s.ahead, "behind": s.behind,
        }

    @app.post("/api/workspace/push")
    def ws_push(request: Request, body: WorkspacePushBody = Body(...)):
        """Push a workspace's current branch to its GitHub home (origin for attached clones, vexa-publish
        for published vexa-born), fast-forward only — NEVER a force push. The token authenticates the push
        and is never stored; a diverged remote fails loud (pull first). Every error is token-redacted (P15)."""
        subject = subject_of(request)
        ws = _manage_dir(subject, body.slug, write=True)
        token = (body.token or "").strip() or git_creds.read_github_token(wsr.root, subject)
        if not token:
            raise HTTPException(status_code=400, detail="a GitHub token is required — pass one or save a reusable token")
        try:
            r = push_origin(ws, token=token)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except RemoteSyncError as exc:
            raise HTTPException(status_code=502, detail=str(exc))  # already token-redacted (P15)
        return {"remote": r.remote, "url": r.url, "branch": r.branch, "head_sha": r.head_sha}

    @app.post("/api/workspace/pull")
    def ws_pull(request: Request, body: WorkspacePullBody = Body(default=WorkspacePullBody())):
        """Fetch + FAST-FORWARD a workspace from its GitHub home. A divergence (local commits the remote
        lacks) is refused — no merge/rebase/force — so it is resolved deliberately. The token (optional for
        public repos) is used for the fetch only and never stored (P15)."""
        subject = subject_of(request)
        ws = _manage_dir(subject, body.slug, write=True)
        token = (body.token or "").strip() or git_creds.read_github_token(wsr.root, subject)  # None ⇒ public-repo fetch
        try:
            r = pull_origin(ws, token=token)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except RemoteSyncError as exc:
            raise HTTPException(status_code=502, detail=str(exc))  # already token-redacted (P15)
        return {"remote": r.remote, "url": r.url, "branch": r.branch, "head_sha": r.head_sha,
                "updated": r.updated, "behind_before": r.behind_before}

    @app.get("/api/workspace/purpose")
    def ws_purpose_get(request: Request, slug: Optional[str] = None):
        """Read a workspace's PURPOSE one-liner (default = the caller's primary; ``slug`` = one of their
        own or shared workspaces). ``""`` when unset."""
        subject = subject_of(request)
        ws = _manage_dir(subject, slug)
        return {"purpose": read_purpose(ws)}

    @app.post("/api/workspace/purpose")
    def ws_purpose_set(request: Request, body: WorkspacePurposeBody = Body(default=WorkspacePurposeBody())):
        """Set (or clear) a workspace's PURPOSE — stored in the workspace + committed so it travels when
        shared and feeds the mount preamble. Returns the normalized purpose actually stored."""
        subject = subject_of(request)
        ws = _manage_dir(subject, body.slug, write=True)
        return {"purpose": write_purpose(ws, body.purpose)}

    @app.get("/api/meeting/stream")
    def meeting_stream(meeting_id: str, session_uid: str, request: Request):
        """SSE feed for a LIVE meeting — merges the transcript Stream (`tc:meeting:{id}`) and the
        copilot's output Stream (`unit:agent-meet-{sid}:out`) into one feed the terminal renders:
        transcript lines + proactive `card`s + the agent working (`message-delta`/`tool-call`).

        RESUMABLE: every event carries an SSE ``id:`` = the per-stream redis cursors. On reconnect the
        browser echoes the last one as ``Last-Event-ID``; we resume EXACTLY from there (redis streams are
        durable + id-addressable) instead of re-seeding only the last N entries. Without this, a transient
        disconnect (the 'Live stream disconnected — reconnecting' path) dropped every segment published in
        the gap beyond the bounded replay window from the LIVE view — the real-time transcript-loss bug
        (the durable store kept them, so they only reappeared post-time)."""
        # P0 (cross-tenant leak fix — SSE sibling of the by-id REST ownership check): OWNER-SCOPE the live
        # feed BEFORE opening any redis stream. `meeting_id` (row id) + `session_uid` arrive from the
        # caller's query params; row ids are sequential ints, so without this an authenticated user B could
        # `EventSource(...?meeting_id=<A_row>&session_uid=<A_native>)` and stream tenant A's live transcript
        # + copilot cards (an ACTIVE, enumerable cross-tenant read). Mirror the WS `/ws` path: derive the
        # caller identity (`subject_of` → 401 on no gateway-injected X-User-Id) and verify the caller OWNS
        # the requested row (meeting-api `GET /meetings/{id}` owner-scopes in SQL: `Meeting.user_id ==
        # user_id` → 404 for a foreign/absent row). Fail CLOSED (403) BEFORE the stream opens.
        # OWNER-ONLY for now (matches the WS path today); a shared-workspace membership grant would extend
        # `_meeting_owner_lookup` — the clean seam — but is intentionally NOT honored here yet.
        subject = subject_of(request)  # 401 if no (gateway-injected) identity — fail closed
        # Canonical positive decimal rows are the only carrier address. In particular, accepting "010"
        # would owner-check row 10 but tail separately keyed Redis streams under 010.
        if not re.fullmatch(r"[1-9][0-9]{0,18}", meeting_id):
            raise HTTPException(status_code=403, detail="not authorized for this meeting")
        if not redis_url:
            raise HTTPException(status_code=501, detail="redis not wired")
        owned = _meeting_owner_lookup(subject, meeting_id)
        if not isinstance(owned, dict):
            # Absent row, or a row owned by a DIFFERENT tenant → refuse (404-equivalent, no stream opened).
            raise HTTPException(status_code=403, detail="not authorized for this meeting")
        if str(owned.get("id") or "") != meeting_id or str(owned.get("user_id") or "") != subject:
            # A malformed, stale, or mis-scoped authority response cannot bless a carrier key.
            raise HTTPException(status_code=403, detail="not authorized for this meeting")
        # `session_uid` is caller-supplied and selects `unit:agent-meet-{session_uid}:out`. Bind it to the
        # owner-proven row only. Native meeting links are not tenant-unique and the retired legacy native
        # fallback could expose an older tenant's out-stream after link reuse.
        if session_uid != meeting_id:
            raise HTTPException(status_code=403, detail="session_uid does not match this meeting")

        resume_t, resume_o, resume_p = _decode_sse_cursor(request.headers.get("last-event-id"))

        def gen():
            import time as _time

            import redis

            r = redis.from_url(redis_url, decode_responses=True)
            tkey = f"tc:meeting:{meeting_id}"
            okey = f"unit:agent-meet-{session_uid}:out"
            # ADR 0027: the SSE tails the processed-notes stream DIRECTLY (processed-notes.v1) —
            # baseline cleaned notes reach the view seconds after a segment instead of waiting for
            # an LLM beat on the out-stream, and the worker's `view_end` marker (not a quiet-poll
            # guess) tells us processing is complete.
            pkey = f"proc:meeting:{meeting_id}"
            # Resume EXACTLY from the client's last-seen cursors when present (gapless reconnect);
            # otherwise seed then live-tail (fresh connect). A missing proc cursor (old 2-part id)
            # resumes from 0-0 — a full replay the client's upsert-by-id absorbs, never a gap.
            last = {tkey: resume_t or "$", okey: resume_o or "$", pkey: resume_p or "0-0"}
            idle = 0
            ending = False        # transcript hit session_end — drain notes/cards before meeting-end
            ending_at = 0.0       # when the drain started (monotonic) — bounds a markerless worker
            view_end_seen = False  # the worker's completion marker arrived on the proc stream

            def cursor():
                return _encode_sse_cursor(last, tkey, okey, pkey)

            def seg_events(payload):
                for seg in payload.get("segments", []):
                    yield ({"type": "transcript", "speaker": seg.get("speaker"),
                            "text": seg.get("text"), "t": seg.get("start"),
                            "tsMs": seg.get("abs_start_ms"),
                            "completed": seg.get("completed", True),
                            "id": seg.get("segment_id")}, cursor())

            def note_events(entry_fields):
                """One proc-stream entry → the SAME `note` SSE event the out-stream used to carry
                (meetingLive.ts upserts by note.id). The `view_end` marker flips completion instead."""
                nonlocal view_end_seen
                if entry_fields.get("type") == "view_end":
                    view_end_seen = True
                    return
                try:
                    note = json.loads(entry_fields.get("note") or "null")
                except (json.JSONDecodeError, ValueError):
                    return
                if isinstance(note, dict) and note.get("id") and note.get("text"):
                    yield ({"type": "note", "note": note}, cursor())

            if resume_t is None:   # fresh connect → seed the bounded recent transcript tail
                seed_rows = list(reversed(r.xrevrange(tkey, count=MEETING_STREAM_TRANSCRIPT_REPLAY) or []))
                for entry_id, fields in seed_rows:
                    last[tkey] = entry_id
                    payload = json.loads(fields.get("payload", "{}"))
                    if payload.get("type") == "session_end":
                        ending = True
                        ending_at = _time.monotonic()
                        last.pop(tkey, None)
                        continue
                    yield from seg_events(payload)
            if resume_o is None:   # fresh connect → seed the output (cards/agent-activity) replay
                output_seed_rows = list(reversed(r.xrevrange(okey, count=MEETING_STREAM_OUTPUT_REPLAY) or []))
                for entry_id, fields in output_seed_rows:
                    last[okey] = entry_id
                    yield (json.loads(fields.get("event", "{}")), cursor())
            # The proc stream needs no separate seed pass: the 0-0 resume cursor makes the first
            # xread below deliver its ENTIRE history (bounded by the notes' 1:1 segment cardinality),
            # so a mid-meeting connect renders the complete processed view.

            while True:
                # once the transcript ends, keep polling briefly — the copilot's FINAL beat is still
                # running (~10s of LLM); its notes + the view_end marker arrive on the proc stream.
                resp = r.xread(last, count=500, block=1500 if ending else 15000)
                if not resp:
                    if ending:
                        # End when processing is COMPLETE (view_end drained — evidence, P21), when no
                        # copilot ever wrote (empty proc stream — nothing to wait for), or at the
                        # bounded cap (a worker that died markerless must not hold the view open).
                        try:
                            has_proc = bool(r.exists(pkey))
                        except Exception:  # noqa: BLE001 — an unreadable stream must not wedge the close
                            has_proc = False
                        if (view_end_seen or not has_proc
                                or _time.monotonic() - ending_at > MEETING_STREAM_ENDING_CAP_SEC):
                            live.drop(session_uid)  # leaves the terminal's live-meetings feed
                            yield ({"type": "meeting-end"}, cursor())
                            return
                        continue  # the final beat is still writing — keep draining
                    idle += 15000
                    if idle >= 600000:
                        return
                    yield ({"type": "ping"}, cursor())
                    continue
                idle = 0
                for stream, entries in resp:
                    for entry_id, fields in entries:
                        last[stream] = entry_id
                        if stream == tkey:
                            payload = json.loads(fields.get("payload", "{}"))
                            if payload.get("type") == "session_end":
                                ending = True            # don't end yet — drain the final beat first
                                ending_at = _time.monotonic()
                                last.pop(tkey, None)     # session_end is the last transcript entry
                                break
                            yield from seg_events(payload)
                        elif stream == pkey:
                            yield from note_events(fields)
                        else:
                            yield (json.loads(fields.get("event", "{}")), cursor())

        return StreamingResponse(
            _sse(gen()), media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )


    # ── workspace membership + invites + roles (Lane M) ───────────────────────────────────────────
    # The access layer for SHARED workspaces. Authoritative store = policy/members.json + policy/
    # invites.json in the workspace's OWN git repo (PLATFORM-WRITE-ONLY, committed via
    # membership_mod.policy_commit); mirror = users.data.memberships[] over the injected index.
    # is_member(workspace_id, subject) -> role|None is the seam Lane A calls for mount/subscribe authz.
    def _pc(ws, message):
        return membership_mod.policy_commit(ws, message)

    def _member_error(exc: MembershipError):
        return HTTPException(status_code=exc.status, detail=str(exc))

    @app.post("/api/workspace/shared/{workspace_id}/active")
    def ws_shared_active(workspace_id: str, request: Request, body: SharedActiveBody = Body(...)):
        """Switch a SHARED workspace ON/OFF in the caller's active set (mount vs hide). Membership is
        unchanged — this is a per-user mount preference so a member can 'switch it off' without leaving."""
        subject = subject_of(request)
        try:
            set_shared_active(wsr.root, subject, workspace_id, body.active)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid workspace")
        return {"workspace_id": workspace_id, "active": body.active}

    @app.post("/api/workspace/{slug}/archive")
    def ws_archive(slug: str, request: Request, body: ArchiveBody = Body(default=ArchiveBody())):
        """Archive (collapse, keep the data) or un-archive one of the caller's own workspaces."""
        subject = subject_of(request)
        try:
            set_archived(wsr.root, subject, slug, body.archived)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except KeyError:
            raise HTTPException(status_code=404, detail="workspace not found")
        return {"slug": slug, "archived": body.archived}

    @app.delete("/api/workspace/{slug}")
    def ws_delete(slug: str, request: Request):
        """DELETE one of the caller's own workspaces — removes the data irreversibly. Baseline is refused."""
        subject = subject_of(request)
        try:
            delete_workspace(wsr.root, subject, slug)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except KeyError:
            raise HTTPException(status_code=404, detail="workspace not found")
        return {"slug": slug, "deleted": True}

    @app.post("/api/workspace/{workspace_id}/unshare")
    def ws_unshare(workspace_id: str, request: Request):
        """UN-SHARE a workspace (owner only) — move it back into the caller's PRIVATE store and drop every
        member's index entry, so it stops being shared (mirror of share-enable). Returns the new private slug."""
        subject = subject_of(request)
        try:
            membership_mod.require_role(wsr.root, workspace_id, subject, "owner")
            members = membership_mod.read_members(wsr.root, workspace_id)
            new_slug = ensure_workspace_private(wsr.root, subject, workspace_id)
        except MembershipError as exc:
            raise _member_error(exc)
        except KeyError:
            raise HTTPException(status_code=404, detail="workspace not found")
        for m in members:  # best-effort: the shared workspace is gone, so drop the derived index entries
            try:
                mindex.remove(m.get("subject"), workspace_id)
            except Exception:  # noqa: BLE001
                pass
        return {"slug": new_slug}

    @app.post("/api/workspace/{slug}/share-enable")
    def ws_share_enable(slug: str, request: Request):
        """Make one of the caller's OWN workspaces shareable (promote a private workspace to a top-level
        shared one if needed) and ensure the caller is its owner. Returns the shareable workspace_id — the
        caller then mints invites against it. This is what lets ANY workspace be shared AFTER creation, with
        no share-vs-not decision at create time."""
        subject = subject_of(request)
        try:
            workspace_id, promoted = ensure_workspace_shareable(wsr.root, subject, slug)
            if promoted:
                membership_mod.ensure_owner(wsr.root, workspace_id, subject, index=mindex,
                                            email=request.headers.get("x-user-email"), commit_fn=_pc)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except KeyError:
            raise HTTPException(status_code=404, detail="workspace not found")
        except MembershipError as exc:
            raise _member_error(exc)
        return {"workspace_id": workspace_id, "promoted": promoted}

    @app.post("/api/workspace/shared/new", status_code=201)
    def ws_shared_new(request: Request, body: SharedNewBody = Body(default=SharedNewBody())):
        """CREATE a new shared workspace and make the caller its OWNER — the bootstrap for the share flow.
        A fresh top-level workspace (git-inited + seeded) is created at <root>/<workspace_id>; the caller is
        granted owner in BOTH stores (policy/members.json + the index). The caller can then mint invites."""
        subject = subject_of(request)
        try:
            wid = create_shared_workspace_dir(wsr.root, body.name)
            membership_mod.ensure_owner(wsr.root, wid, subject, index=mindex,
                                        email=request.headers.get("x-user-email"), commit_fn=_pc)
        except MembershipError as exc:
            raise _member_error(exc)
        except Exception as exc:  # noqa: BLE001 — surface a clean 500 (dir/seed failure) rather than a stack
            logger.exception("shared-workspace create failed for subject=%s", subject)
            raise HTTPException(status_code=500, detail="could not create shared workspace")
        return {"workspace_id": wid, "role": "owner", "name": body.name}

    @app.post("/api/workspace/invites", status_code=201)
    def ws_invite_create(request: Request, body: InviteCreateBody = Body(...)):
        """Mint a scoped invite token for a shared workspace. Auth: owner OR contributor of the target.
        The workspace must be shareable (reserved/own-private refused). The token is returned ONCE; only
        its hash is persisted in policy/invites.json."""
        subject = subject_of(request)
        try:
            membership_mod.require_role(wsr.root, body.workspace_id, subject, "contributor")
            minted = membership_mod.mint_invite(
                wsr.root, body.workspace_id, role=body.role, created_by=subject,
                expires_in_sec=body.expires_in_sec, max_uses=body.max_uses,
                mode=body.mode, allowed_emails=body.allowed_emails, commit_fn=_pc,
            )
        except MembershipError as exc:
            raise _member_error(exc)
        # The client composes the accept URL; we hand back the token + id + terms once.
        return {
            "id": minted.id, "token": minted.token, "role": minted.role,
            "workspace_id": body.workspace_id, "expires_at": minted.expires_at,
            "max_uses": minted.max_uses, "mode": body.mode,
            "accept_path": "/api/workspace/invites/accept",
        }

    @app.get("/api/workspace/invites/preview")
    def ws_invite_preview(request: Request, token: str):
        """READ-ONLY preview of an invite — the target workspace + terms — WITHOUT granting anything.
        Powers the pre-join CONSENT screen: the invitee sees what the workspace is (its purpose), the role
        they'd get, and who shared it BEFORE they log in / join. Capability-gated by the token (whoever
        holds the link may preview it); no membership is checked or created, no use is consumed. 404 for a
        token that matches nothing (never enumerates workspaces)."""
        info = membership_mod.preview_invite(wsr.root, token)
        if info is None:
            raise HTTPException(status_code=404, detail="invalid invite")
        wsid = info["workspace_id"]
        # Human context for the card: the workspace's purpose + who shared it (their email when we've
        # stored it — see the members roster; else the opaque subject as a last resort).
        purpose = read_purpose(membership_mod._ws_dir(wsr.root, wsid))
        shared_by = info.get("created_by")
        for m in membership_mod.read_members(wsr.root, wsid):
            if m.get("subject") == info.get("created_by") and m.get("email"):
                shared_by = m["email"]
                break
        return {
            "workspace_id": wsid, "name": wsid, "purpose": purpose,
            "role": info["role"], "mode": info["mode"], "expires_at": info["expires_at"],
            "shared_by": shared_by, "valid": info["valid"], "reason": info["reason"],
        }

    @app.post("/api/workspace/invites/accept")
    def ws_invite_accept(request: Request, body: InviteAcceptBody = Body(...)):
        """Redeem an invite token (any logged-in user) → membership in BOTH stores, use-count bumped.
        Idempotent per user (accepting twice = one membership, no extra use consumed). The token carries
        NO workspace id — we resolve it by scanning the shareable workspaces' invites for its hash.
        Post-auth redeem (AMENDMENT 5): the caller is an already-authenticated user (X-User-Id); a
        RESTRICTED invite additionally requires their VERIFIED email (X-User-Email, gateway-injected)
        to be in the invite's allowed_emails."""
        subject = subject_of(request)
        # SECURITY BOUNDARY: X-User-Email is trusted as the caller's VERIFIED email ONLY because the
        # gateway strips any client-sent x-user-email and re-injects the value it resolved from the
        # api-key. That invariant holds solely when the gateway is agent-api's SOLE ingress. Today the
        # terminal / host-local clients reach agent-api directly (no gateway hop), so on the direct edge
        # this header is spoofable — restricted-mode invites are NOT a security boundary until agent-api
        # is gateway-fronted (Stage 4). VEXA_REQUIRE_GATEWAY_IDENTITY (checked in subject_of) lets a
        # hardened deploy reject non-gateway callers. See the TOPOLOGY BOUNDARY note in create_app.
        subject_email = request.headers.get("x-user-email")
        h = membership_mod.hash_token(body.token)
        # Resolve which shared workspace this token belongs to by hash (never trust a client-declared id).
        target_ws = None
        root = wsr.root
        for child in sorted(p for p in root.iterdir() if p.is_dir()) if root.exists() else []:
            slug = child.name
            if slug.startswith(".") or slug in membership_mod.RESERVED_SLUGS:
                continue
            for inv in membership_mod._read_json_list(child, membership_mod.INVITES_FILE):
                if inv.get("hash") == h:
                    target_ws = slug
                    break
            if target_ws:
                break
        if target_ws is None:
            raise HTTPException(status_code=404, detail="invalid invite")
        try:
            result = membership_mod.accept_invite(
                wsr.root, target_ws, token=body.token, subject=subject, subject_email=subject_email,
                index=mindex, commit_fn=_pc,
            )
        except MembershipError as exc:
            raise _member_error(exc)
        return result

    @app.delete("/api/workspace/invites/{invite_id}")
    def ws_invite_revoke(invite_id: str, request: Request, workspace_id: str):
        """Revoke an invite (owner/contributor of the workspace)."""
        subject = subject_of(request)
        try:
            membership_mod.require_role(wsr.root, workspace_id, subject, "contributor")
            membership_mod.revoke_invite(wsr.root, workspace_id, invite_id, commit_fn=_pc)
        except MembershipError as exc:
            raise _member_error(exc)
        return {"ok": True, "invite_id": invite_id}

    @app.get("/api/workspace/invites")
    def ws_invites_list(request: Request, workspace_id: str):
        """List a workspace's invites (owner/contributor). Hashes are never surfaced."""
        subject = subject_of(request)
        try:
            membership_mod.require_role(wsr.root, workspace_id, subject, "contributor")
            return {"invites": membership_mod.list_invites(wsr.root, workspace_id)}
        except MembershipError as exc:
            raise _member_error(exc)

    @app.get("/api/workspace/members")
    def ws_members_list(request: Request, workspace_id: str):
        """List a workspace's members (owner/contributor). Opportunistically records the CALLER's own
        verified email onto their member row (self-healing for members granted before emails were stored)
        so the roster shows human labels, not opaque subject ids."""
        subject = subject_of(request)
        try:
            membership_mod.require_role(wsr.root, workspace_id, subject, "contributor")
            try:  # best-effort label refresh — never fail the list on a backfill hiccup
                membership_mod.backfill_member_email(
                    wsr.root, workspace_id, subject,
                    request.headers.get("x-user-email"), commit_fn=_pc)
            except Exception:  # noqa: BLE001
                logger.debug("member email backfill skipped for %s in %s", subject, workspace_id, exc_info=True)
            return {"members": membership_mod.read_members(wsr.root, workspace_id)}
        except MembershipError as exc:
            raise _member_error(exc)

    @app.delete("/api/workspace/members/{member_subject}")
    def ws_member_remove(member_subject: str, request: Request, workspace_id: str):
        """Remove a member (owner only)."""
        subject = subject_of(request)
        try:
            membership_mod.require_role(wsr.root, workspace_id, subject, "owner")
            membership_mod.remove_member(wsr.root, workspace_id, member_subject, index=mindex, commit_fn=_pc)
        except MembershipError as exc:
            raise _member_error(exc)
        return {"ok": True, "subject": member_subject}

    @app.post("/api/workspace/members/{member_subject}/role")
    def ws_member_role(member_subject: str, request: Request, workspace_id: str,
                       body: RoleSetBody = Body(...)):
        """Flip a member's role (owner only) — read <-> read/write permissions."""
        subject = subject_of(request)
        try:
            membership_mod.require_role(wsr.root, workspace_id, subject, "owner")
            rec = membership_mod.set_role(
                wsr.root, workspace_id, member_subject, body.role,
                changed_by=subject, index=mindex, commit_fn=_pc,
            )
        except MembershipError as exc:
            raise _member_error(exc)
        return rec

    @app.post("/api/workspace/{workspace_id}/leave")
    def ws_member_leave(workspace_id: str, request: Request):
        """LEAVE a shared workspace — the caller removes THEMSELVES (any role; no owner gate). The
        last-owner guard still applies: a sole creator must unshare or hand off ownership rather than
        orphan the workspace, so their leave is refused (409) with that message."""
        subject = subject_of(request)
        if membership_mod.is_member(wsr.root, workspace_id, subject) is None:
            raise HTTPException(status_code=404, detail="not a member of this workspace")
        try:
            membership_mod.remove_member(wsr.root, workspace_id, subject, index=mindex, commit_fn=_pc)
        except MembershipError as exc:
            raise _member_error(exc)
        return {"ok": True, "left": workspace_id}

    @app.get("/api/workspace/shared")
    def ws_shared_list(request: Request):
        """The "workspaces shared with me" listing from the index (users.data.memberships[])."""
        subject = subject_of(request)
        try:
            return {"memberships": mindex.list(subject)}
        except Exception:
            return {"memberships": []}

    # ── Settings → Models "Test" buttons (on-demand credential tests, fail-loud surface) ────────
    # Both test the caller's EFFECTIVE config. STT selects one complete settings backend or the
    # complete env backend, so a URL can never inherit credentials from another tier.

    @app.get("/api/models/test")
    def models_test(request: Request):
        """Test the effective model credentials NOW: custom mode = a real 1-token completion
        against the endpoint; subscription = mounted-credentials expiry check (the recurring
        stale-Keychain 401 surfaces here with its remedy instead of at the next chat turn)."""
        from control_plane import config_test as _ct
        subject = subject_of(request)
        cfg: dict = {}
        mc = getattr(dispatcher, "_model_config", None)
        if mc is not None:
            try:
                cfg = mc.resolve(subject) or {}
            except Exception:
                # Resolver failure must not turn a normal user's Test click into a deployment-key
                # spend. Report only generic operator-managed status.
                return _ct.managed_models_status({})
        credential_owner = cfg.pop("credential_owner", "operator")
        if credential_owner != "user":
            return _ct.managed_models_status(cfg)
        return _ct.run_models_test(cfg)

    @app.get("/api/transcription/test")
    def transcription_test(request: Request):
        """Probe the effective STT backend with its token (GET /balance): catches dead URLs,
        rejected tokens, and the zero-balance-external-account case that 402s every segment."""
        from control_plane import config_test as _ct
        subject = subject_of(request)
        url, token, source = "", "", "env"
        credential_owner = "operator"
        settings = dispatcher.settings
        admin = (settings.admin_api_url or "").rstrip("/")
        if admin:  # same internal edge bot_spawn uses (bot-context carries the resolved override)
            import urllib.request as _ur
            try:
                req = _ur.Request(f"{admin}/internal/users/{subject}/bot-context",
                                  headers={"X-Internal-Secret":
                                           settings.internal_api_secret.get_secret_value()})
                # The internal secret is an origin-bound credential. Refuse redirects instead of
                # allowing urllib to replay it to an arbitrary Location target.
                with _ct.open_no_redirect(req, timeout=5) as r:
                    body = json.loads(_ct._read_bounded_response(r))
                credential_owner = body.get("transcription_credential_owner", "operator")
                t = body.get("transcription") or {}
                if t.get("blocked"):
                    return {
                        "ok": False,
                        "summary": (
                            "Personal transcription configuration is blocked by operator policy; "
                            "choose an approved endpoint or Deployment default."
                        ),
                        "source": "settings",
                        "blocked": True,
                    }
                if t.get("url"):
                    url, token, source = t.get("url") or "", t.get("token") or "", "settings"
            except Exception:
                credential_owner = "operator"
        if credential_owner == "user" and url:
            return _ct.run_transcription_test(url, token, source)
        if not url or credential_owner != "user":
            url = os.environ.get("TRANSCRIPTION_SERVICE_URL", "")
            token = os.environ.get("TRANSCRIPTION_SERVICE_TOKEN", "")
        return _ct.managed_transcription_status(url, token)
    return app


# ── ASGI entrypoint (PEP 562) — `uvicorn control_plane.api:app` resolves this lazily ──────────────────
def _build_production_app() -> FastAPI:
    from shared.adapters import AdminApiMembershipIndex, AdminApiModelConfig, LocalIdentityMinter, RedisStreamReader, RuntimeHttpClient, SchedulerHttpClient
    from shared.config import load_settings
    from control_plane.config_preflight import preflight
    from control_plane.minutes_boot import build_minutes_ingestor
    from control_plane.workspace_routines import start_workspace_routine_reconciler

    _validate_agent_erasure_signing_config(
        capture_enabled=os.environ.get("ZAKI_MINUTES_CAPTURE_ENABLED"),
        key_id=os.environ.get("ZAKI_AGENT_ERASURE_SIGNING_KEY_ID"),
        secret=os.environ.get("ZAKI_AGENT_ERASURE_SIGNING_SECRET"),
        previous_key_id=os.environ.get(
            "ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID"
        ),
        previous_secret=os.environ.get(
            "ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET"
        ),
        internal_secret=os.environ.get("VEXA_INTERNAL_API_SECRET"),
        credential_environment=os.environ,
    )

    # There is deliberately no fake/partial production eraser. Until the Brain provenance adapter is
    # composed, mounting capture on Agent must fail before any runtime/watchers start.
    raw_minutes_eraser = _require_minutes_erasure_backend(
        capture_enabled=os.environ.get("ZAKI_MINUTES_CAPTURE_ENABLED"), eraser=None,
    )

    # config.v1 boot preflight (ADR-0026): agent-api has no required-explicit keys today, so this
    # logs the capability tri-states (bot_gateway · model_inference) — a deploy that cannot add bots
    # from URL or whose workers will have NO model credentials says so in the boot log and on
    # /health, instead of failing at first chat with 'Model inference failed: Not logged in'.
    preflight()

    settings = load_settings()
    _validate_gateway_identity_config(
        required=settings.require_gateway_identity or settings.minutes_read_enabled,
        secret=settings.gateway_identity_secret.get_secret_value(),
        previous_secret=settings.gateway_identity_previous_secret.get_secret_value(),
        internal_secret=settings.internal_api_secret.get_secret_value(),
        credential_environment=os.environ,
    )
    boundary_redis = None
    if (
        raw_minutes_eraser is not None
        or settings.require_gateway_identity
        or settings.minutes_read_enabled
    ):
        boundary_redis = _security_redis_from_url(settings.redis_url)
    erasure_redis = boundary_redis if raw_minutes_eraser is not None else None
    gateway_replay_store = (
        RedisGatewayReplayStore(boundary_redis)
        if settings.require_gateway_identity or settings.minutes_read_enabled
        else None
    )
    minutes_eraser = _wrap_signed_minutes_eraser(
        raw_minutes_eraser,
        redis_client=erasure_redis,
        key_id=settings.agent_erasure_signing_key_id,
        secret=settings.agent_erasure_signing_secret.get_secret_value(),
        previous_key_id=settings.agent_erasure_previous_verification_key_id or None,
        previous_secret=(
            settings.agent_erasure_previous_verification_secret.get_secret_value() or None
        ),
        internal_secret=settings.internal_api_secret.get_secret_value(),
        credential_environment=os.environ,
    )
    # The read client, live Identity dual gate, and isolated completion stage are ready to compose,
    # but this tree has no canonical Nullalis tombstone-aware answer authorizer. Flag-off returns
    # None without touching those dependencies; flag-on fails at that exact content-free seam.
    # Model-derived Minutes text remains ephemeral and never crosses into Brain here.
    minutes_ingestor = build_minutes_ingestor(settings, authorizer=None)
    runtime_control_secret = settings.runtime_control_secret.get_secret_value()
    runtime = RuntimeHttpClient(
        settings.runtime_api_url,
        control_secret=runtime_control_secret,
    )
    scheduler = SchedulerHttpClient(
        settings.runtime_api_url,
        control_secret=runtime_control_secret,
    )
    identity = LocalIdentityMinter(settings.dispatch_signing_key.get_secret_value())
    invocations_url = settings.agent_api_self_url.rstrip("/") + "/invocations"
    # Lane M: the membership index mirror (users.data.memberships[]) over the admin-api internal edge.
    # Empty admin_api_url → the in-memory index (git files stay authoritative; only "shared with me"
    # listing is degraded, per Q6). create_app defaults to InMemoryMembershipIndex when None is passed.
    membership_index = None
    model_config = None
    if settings.admin_api_url:
        membership_index = AdminApiMembershipIndex(
            settings.admin_api_url, settings.internal_api_secret.get_secret_value(),
        )
        # Settings → Models: per-subject effective model config (user pref > platform setting)
        # over the same internal edge; None (no admin-api) → deployment env defaults only.
        model_config = AdminApiModelConfig(
            settings.admin_api_url, settings.internal_api_secret.get_secret_value(),
        )
    # Lane A: the Dispatcher takes the SAME index so shared workspaces the subject is a member of enter
    # the dispatch mount set (read-only for Slice 1), not just the /active listing.
    dispatcher = Dispatcher(settings, runtime, identity, membership_index=membership_index,
                            model_config=model_config)
    app = create_app(
        dispatcher,
        stream_reader=RedisStreamReader(settings.redis_url),
        reader=WorkspaceReader(settings.workspaces_dir),
        scheduler=scheduler,
        invocations_url=invocations_url,
        redis_url=settings.redis_url,
        membership_index=membership_index,
        minutes_eraser=minutes_eraser,
        minutes_ingestor=minutes_ingestor,
        gateway_replay_store=gateway_replay_store,
    )
    app.state.workspace_routine_reconciler = start_workspace_routine_reconciler(
        scheduler=scheduler,
        invocations_url=invocations_url,
        workspaces_dir=settings.workspaces_dir,
        interval_sec=settings.routine_reconcile_interval_sec,
    )

    @app.on_event("shutdown")
    def _stop_workspace_routine_reconciler() -> None:
        handle = getattr(app.state, "workspace_routine_reconciler", None)
        if handle is not None:
            handle.stop()

    # The in-process meetings Integration (replaces the standalone bridge container): a daemon thread
    # tails transcription_segments and arms the copilot only after meeting-api binds the exact numeric
    # row to its authoritative owner over the secret-protected internal edge. There is no production
    # fallback subject: owner authority uncertainty leaves the row unregistered and undispatched.
    from control_plane import transcription_watcher
    owner_lookup = transcription_watcher._http_owner_lookup(
        settings.meeting_api_url,
        settings.internal_api_secret.get_secret_value(),
    )
    transcription_watcher.start(
        settings.redis_url,
        dispatcher,
        app.state.live_meetings,
        owner_lookup=owner_lookup,
    )
    return app


def __getattr__(name: str):
    if name == "app":
        return _build_production_app()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
