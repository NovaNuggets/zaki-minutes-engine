"""``python -m meeting_api`` — the production meeting-api (P4 compose CMD).

Assembles the unified modular-monolith (``meeting_api.create_app``) with the REAL per-module
adapters (SQLAlchemy + redis + MinIO/S3 + httpx-runtime), then — per P4 — ALSO starts the
control-plane background loops alongside the HTTP app via the FastAPI lifespan:

  * **collector segment consumer** — drains the ``transcription_segments`` redis stream
    (``consume_segments`` → ``ingest`` → publish ``tc:…:mutable``) on a poll interval.
  * **db-writer** — the RESTORED parent flush loop (0.10 ``process_redis_to_postgres``): each tick
    moves immutable live segments from the redis hash ``meeting:{id}:segments`` into the
    ``transcriptions`` table (upsert on segment identity; redis trimmed only after the confirmed
    write) and drains the copilot's ``proc:meeting:{id}`` notes into ``meeting.data`` JSONB.
  * **webhook retry-drain** — one ``drain_retry_queue`` sweep per interval over the redis retry
    queue (failed ``meeting.status_change`` deliveries are retried with backoff).
  * **scheduler tick** — fires due ``schedule.v1`` jobs (this also drives the join-retry re-spawns
    that ``JoinRetryController`` schedules) on the tick interval.

Each loop is a single-tick function the eval drives explicitly; here the entrypoint wraps it in the
``while True: tick; sleep`` poll the deployment uses. uvicorn-target: ``uvicorn meeting_api.__main__:app``.
"""
from __future__ import annotations

import asyncio
import hmac
import logging
import math
import os
import re
import secrets
from contextlib import asynccontextmanager
from urllib.parse import quote, unquote, urlsplit

log = logging.getLogger("meeting_api.entrypoint")
_DATABASE_SSL_MODES = frozenset({"disable", "require", "verify-ca", "verify-full"})


def _validate_operator_secret(name: str, value: object) -> str:
    """Validate one bounded, unpadded printable-ASCII bearer/HMAC secret."""

    if (
        not isinstance(value, str)
        or not 32 <= len(value) <= 512
        or value != value.strip()
        or any(not 0x20 <= ord(character) <= 0x7E for character in value)
    ):
        raise RuntimeError(
            f"{name} must be unpadded printable ASCII between 32 and 512 characters"
        )
    return value


def _validate_service_secret_isolation(**values: str | None) -> None:
    """Reject credential aliasing across independently authorized service boundaries.

    A shared value turns the weakest holder into a signing/authentication oracle for every aliased
    boundary. Empty optional values are ignored; every configured value must be unique.
    """
    seen: dict[str, str] = {}
    for name, value in values.items():
        if not isinstance(value, str) or not value:
            continue
        prior = seen.get(value)
        if prior is not None:
            raise RuntimeError(f"{name} must be distinct from {prior}")
        seen[value] = name


def _url_password(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        encoded = urlsplit(value).password
    except ValueError:
        raise RuntimeError("credential URL userinfo is invalid") from None
    if encoded is None:
        return None
    if re.search(r"%(?![0-9A-Fa-f]{2})", encoded):
        raise RuntimeError("credential URL userinfo is invalid")
    try:
        return unquote(encoded, encoding="utf-8", errors="strict")
    except UnicodeDecodeError:
        raise RuntimeError("credential URL userinfo is invalid") from None


def _meeting_api_secret_values(env=None) -> dict[str, str | None]:
    """Inventory every credential-bearing environment value visible to meeting-api."""

    source = os.environ if env is None else env
    database_url = source.get("DATABASE_URL")
    redis_url = source.get("REDIS_URL")
    names = (
        "MEETING_TOKEN_SECRET",
        "DB_PASSWORD",
        "DATABASE_URL",
        "REDIS_URL",
        "RUNTIME_CONTROL_SECRET",
        "RUNTIME_CALLBACK_SECRET",
        "INTERNAL_API_SECRET",
        "TRANSCRIPTION_SERVICE_TOKEN",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        "ZAKI_MINUTES_HUB_TOKEN",
        "ZAKI_READ_TOKEN_MINUTES",
        "ZAKI_AGENT_ERASURE_VERIFICATION_SECRET",
        "ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET",
        "ZAKI_MINUTES_ERASURE_SIGNING_SECRET",
        "ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET",
        "ZAKI_MINUTES_FINALIZED_SECRET",
    )
    values = {name: source.get(name) for name in names}
    values["DATABASE_URL_PASSWORD"] = _url_password(database_url)
    values["REDIS_URL_PASSWORD"] = _url_password(redis_url)
    return values


def _operator_flag(name: str, default: bool = False) -> bool:
    """Parse an operator-owned activation flag without truthy-string surprises."""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{name} must be an explicit boolean")


def _minutes_auto_join_config(env=None) -> bool:
    """Keep calendar-driven bot spawning unavailable until it carries managed authority.

    Calendar discovery may continue to create planned rows, but launch v1 has no consent,
    retention, attestation, or withdrawal-fence contract for unattended joins.  There is therefore
    no legacy ``request_bot`` fallback behind this flag: enabling it is an explicit boot error.
    """

    source = os.environ if env is None else env
    raw = str(source.get("ZAKI_MINUTES_AUTO_JOIN_ENABLED", "false")).strip().lower()
    if raw in {"0", "false", "no", "off", ""}:
        return False
    if raw in {"1", "true", "yes", "on"}:
        raise RuntimeError(
            "ZAKI_MINUTES_AUTO_JOIN_ENABLED requires a managed consent and retention "
            "implementation; calendar auto-join is unavailable in launch v1"
        )
    raise RuntimeError("ZAKI_MINUTES_AUTO_JOIN_ENABLED must be an explicit boolean")


def _minutes_invocation_v2_config(env=None, *, capture_enabled: bool) -> bool:
    """Independent producer-side gate for the managed v2 bot profile.

    Capture may not activate merely because the service has v2 code installed. The operator must
    also assert that runtime has the isolated, v2-capable ``meeting-bot-v2`` profile.
    """
    source = os.environ if env is None else env
    raw = str(source.get("ZAKI_MINUTES_INVOCATION_V2_ENABLED", "false")).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        enabled = True
    elif raw in {"0", "false", "no", "off", ""}:
        enabled = False
    else:
        raise RuntimeError("ZAKI_MINUTES_INVOCATION_V2_ENABLED must be an explicit boolean")
    if capture_enabled and not enabled:
        raise RuntimeError(
            "ZAKI_MINUTES_CAPTURE_ENABLED requires "
            "ZAKI_MINUTES_INVOCATION_V2_ENABLED and a v2-capable runtime profile"
        )
    return enabled


def _minutes_ttl_config(env=None) -> tuple[bool, float, int]:
    """Parse the default-off retention worker controls before any carrier is touched."""
    source = os.environ if env is None else env
    raw_enabled = str(source.get("MINUTES_TTL_ENABLED", "false")).strip().lower()
    if raw_enabled in {"1", "true", "yes", "on"}:
        enabled = True
    elif raw_enabled in {"0", "false", "no", "off"}:
        enabled = False
    else:
        raise RuntimeError("MINUTES_TTL_ENABLED must be an explicit boolean")
    try:
        interval = float(source.get("MINUTES_TTL_INTERVAL_S", "60"))
    except (TypeError, ValueError):
        raise RuntimeError("MINUTES_TTL_INTERVAL_S must be a positive number") from None
    if not math.isfinite(interval) or interval <= 0:
        raise RuntimeError("MINUTES_TTL_INTERVAL_S must be a positive number")
    try:
        limit = int(source.get("MINUTES_TTL_BATCH_SIZE", "100"))
    except (TypeError, ValueError):
        raise RuntimeError("MINUTES_TTL_BATCH_SIZE must be an integer from 1 to 500") from None
    if not 1 <= limit <= 500:
        raise RuntimeError("MINUTES_TTL_BATCH_SIZE must be an integer from 1 to 500")
    return enabled, interval, limit


def _validate_minutes_activation(
    *, capture_enabled: bool, read_enabled: bool, ttl_enabled: bool,
    read_token: str | None,
    hub_token: str | None = None,
    managed_routes_enabled: bool = False,
    agent_verification_key_id: str | None = None,
    agent_verification_secret: str | None = None,
    minutes_signing_key_id: str | None = None,
    minutes_signing_secret: str | None = None,
    internal_secret: str | None = None,
) -> None:
    """Refuse privacy-incomplete operator combinations before the service starts."""
    if read_enabled:
        if not isinstance(read_token, str) or not read_token:
            raise RuntimeError(
                "ZAKI_MINUTES_READ_ENABLED requires ZAKI_READ_TOKEN_MINUTES"
            )
        _validate_operator_secret("ZAKI_READ_TOKEN_MINUTES", read_token)
    required = (
        ("ZAKI_AGENT_ERASURE_VERIFICATION_KEY_ID", agent_verification_key_id),
        ("ZAKI_AGENT_ERASURE_VERIFICATION_SECRET", agent_verification_secret),
        ("ZAKI_MINUTES_ERASURE_SIGNING_KEY_ID", minutes_signing_key_id),
        ("ZAKI_MINUTES_ERASURE_SIGNING_SECRET", minutes_signing_secret),
    )
    configured = tuple(isinstance(value, str) and bool(value.strip()) for _, value in required)
    if (capture_enabled or any(configured)) and not ttl_enabled:
        raise RuntimeError(
            "managed Minutes data/erasure requires MINUTES_TTL_ENABLED=true"
        )
    if not capture_enabled and not any(configured):
        if read_enabled and internal_secret and read_token == internal_secret:
            raise RuntimeError(
                "ZAKI_READ_TOKEN_MINUTES must be distinct from INTERNAL_API_SECRET"
            )
        if managed_routes_enabled:
            from .managed_auth import validate_hub_token

            try:
                validate_hub_token(hub_token)
            except ValueError:
                raise RuntimeError(
                    "managed Minutes routes require ZAKI_MINUTES_HUB_TOKEN"
                ) from None
        return
    for name, value in required:
        if not isinstance(value, str) or not value.strip():
            activation = (
                "ZAKI_MINUTES_CAPTURE_ENABLED"
                if capture_enabled else "Minutes erasure configuration"
            )
            raise RuntimeError(f"{activation} requires {name}")
    if _ERASURE_KEY_ID.fullmatch(agent_verification_key_id) is None:
        raise RuntimeError("ZAKI_AGENT_ERASURE_VERIFICATION_KEY_ID is invalid")
    if _ERASURE_KEY_ID.fullmatch(minutes_signing_key_id) is None:
        raise RuntimeError("ZAKI_MINUTES_ERASURE_SIGNING_KEY_ID is invalid")
    _validate_operator_secret(
        "ZAKI_AGENT_ERASURE_VERIFICATION_SECRET", agent_verification_secret
    )
    _validate_operator_secret(
        "ZAKI_MINUTES_ERASURE_SIGNING_SECRET", minutes_signing_secret
    )
    if internal_secret and agent_verification_secret == internal_secret:
        raise RuntimeError(
            "ZAKI_AGENT_ERASURE_VERIFICATION_SECRET must be distinct from "
            "INTERNAL_API_SECRET"
        )
    if minutes_signing_secret == agent_verification_secret:
        raise RuntimeError(
            "ZAKI_MINUTES_ERASURE_SIGNING_SECRET must be independent from the Agent erasure key"
        )
    if internal_secret and minutes_signing_secret == internal_secret:
        raise RuntimeError(
            "ZAKI_MINUTES_ERASURE_SIGNING_SECRET must be distinct from INTERNAL_API_SECRET"
        )
    if read_enabled and read_token in {
        internal_secret,
        agent_verification_secret,
        minutes_signing_secret,
    }:
        raise RuntimeError(
            "ZAKI_READ_TOKEN_MINUTES must be distinct from internal and erasure secrets"
        )
    from .managed_auth import validate_hub_token

    try:
        validate_hub_token(hub_token)
    except ValueError:
        raise RuntimeError(
            "managed Minutes routes require ZAKI_MINUTES_HUB_TOKEN"
        ) from None


_ERASURE_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


def _minutes_erasure_verification_keyring(
    *,
    current_key_id: str | None,
    current_secret: str | None,
    previous_key_id: str | None,
    previous_secret: str | None,
) -> dict[str, str]:
    """Build the bounded Minutes receipt verifier overlap used during one key rotation.

    The current signer is always in the verifier set.  An optional previous pair lets an erasure
    draft created before rotation finish and replay without ever restoring the old signing key as
    current.  Partial or alias pairs fail boot.
    """

    if (
        not isinstance(current_key_id, str)
        or not _ERASURE_KEY_ID.fullmatch(current_key_id)
        or not isinstance(current_secret, str)
    ):
        raise RuntimeError("Minutes erasure current signing key is invalid")
    _validate_operator_secret(
        "ZAKI_MINUTES_ERASURE_SIGNING_SECRET", current_secret
    )
    has_previous_id = isinstance(previous_key_id, str) and bool(previous_key_id)
    has_previous_secret = isinstance(previous_secret, str) and bool(previous_secret)
    if has_previous_id != has_previous_secret:
        raise RuntimeError(
            "Minutes erasure previous verification key id and secret must be configured together"
        )
    keys = {current_key_id: current_secret}
    if not has_previous_id:
        return keys
    try:
        _validate_operator_secret(
            "ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET", previous_secret
        )
    except RuntimeError:
        raise RuntimeError("Minutes erasure previous verification key is invalid") from None
    if (
        not _ERASURE_KEY_ID.fullmatch(previous_key_id)
        or previous_key_id == current_key_id
        or previous_secret == current_secret
    ):
        raise RuntimeError("Minutes erasure previous verification key is invalid")
    keys[previous_key_id] = previous_secret
    return keys


def _agent_erasure_verification_keyring(
    *,
    current_key_id: str | None,
    current_secret: str | None,
    previous_key_id: str | None,
    previous_secret: str | None,
) -> dict[str, str]:
    """Build the bounded current-plus-one-previous verifier for Agent-owned proofs."""

    if (
        not isinstance(current_key_id, str)
        or not _ERASURE_KEY_ID.fullmatch(current_key_id)
        or not isinstance(current_secret, str)
    ):
        raise RuntimeError("Agent erasure current verification key is invalid")
    try:
        _validate_operator_secret(
            "ZAKI_AGENT_ERASURE_VERIFICATION_SECRET", current_secret
        )
    except RuntimeError:
        raise RuntimeError("Agent erasure current verification key is invalid") from None
    has_previous_id = isinstance(previous_key_id, str) and bool(previous_key_id)
    has_previous_secret = isinstance(previous_secret, str) and bool(previous_secret)
    if has_previous_id != has_previous_secret:
        raise RuntimeError(
            "Agent erasure previous verification key id and secret must be configured together"
        )
    keys = {current_key_id: current_secret}
    if not has_previous_id:
        return keys
    try:
        _validate_operator_secret(
            "ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET", previous_secret
        )
    except RuntimeError:
        raise RuntimeError("Agent erasure previous verification key is invalid") from None
    if (
        not _ERASURE_KEY_ID.fullmatch(previous_key_id)
        or previous_key_id == current_key_id
        or hmac.compare_digest(previous_secret, current_secret)
    ):
        raise RuntimeError("Agent erasure previous verification key is invalid")
    keys[previous_key_id] = previous_secret
    return keys


def _minutes_finalized_config(env=None) -> tuple[bool, str | None, str | None, str | None]:
    """Parse the default-off, operator-owned platform webhook without exposing its secret."""

    source = os.environ if env is None else env
    raw = str(source.get("ZAKI_MINUTES_FINALIZED_ENABLED", "false")).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        enabled = True
    elif raw in {"0", "false", "no", "off", ""}:
        enabled = False
    else:
        raise RuntimeError("ZAKI_MINUTES_FINALIZED_ENABLED must be an explicit boolean")
    if not enabled:
        return False, None, None, None

    names = (
        "ZAKI_MINUTES_FINALIZED_URL",
        "ZAKI_MINUTES_FINALIZED_KEY_ID",
        "ZAKI_MINUTES_FINALIZED_SECRET",
    )
    values: list[str] = []
    for name in names:
        value = source.get(name)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"ZAKI_MINUTES_FINALIZED_ENABLED requires {name}")
        values.append(value)
    url, key_id, secret = values
    if _ERASURE_KEY_ID.fullmatch(key_id) is None:
        raise RuntimeError("ZAKI_MINUTES_FINALIZED_KEY_ID is invalid")
    _validate_operator_secret("ZAKI_MINUTES_FINALIZED_SECRET", secret)
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except (TypeError, ValueError):
        raise RuntimeError("ZAKI_MINUTES_FINALIZED_URL is unsafe") from None
    if (
        len(url) > 2_048
        or url != url.strip()
        or parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or port is not None and not 1 <= port <= 65_535
        or any(ord(character) < 32 for character in url)
    ):
        raise RuntimeError("ZAKI_MINUTES_FINALIZED_URL is unsafe")
    for candidate_name in (
        "INTERNAL_API_SECRET",
        "ZAKI_READ_TOKEN_MINUTES",
        "ZAKI_AGENT_ERASURE_VERIFICATION_SECRET",
        "ZAKI_MINUTES_ERASURE_SIGNING_SECRET",
        "ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET",
    ):
        candidate = source.get(candidate_name)
        if isinstance(candidate, str) and candidate and secret == candidate:
            raise RuntimeError(
                "ZAKI_MINUTES_FINALIZED_SECRET must be distinct from authentication, "
                "read, and erasure secrets"
            )
    return True, url, key_id, secret


async def _minutes_finalized_drain_loop(
    *, enabled: bool, drain, interval: float, sleep=asyncio.sleep,
) -> None:
    """Periodically drain the bounded durable outbox; the off path performs no carrier I/O."""

    if not enabled:
        return
    while True:
        try:
            await drain()
        except asyncio.CancelledError:
            raise
        except Exception:
            # Do not interpolate the delivery exception: upstream/provider errors can contain URLs
            # or request headers. The durable outbox is the retry signal.
            log.warning("Minutes platform finalized drain tick failed")
        await sleep(interval)


async def _minutes_ttl_loop(
    *,
    enabled: bool,
    interval: float,
    limit: int,
    session_factory,
    object_storage,
    redis_client,
    runner=None,
    sleep=None,
    clock=None,
) -> None:
    """Run bounded TTL sweeps in-process; the false path performs exactly zero carrier I/O."""
    if not enabled:
        return
    from datetime import datetime, timezone

    if runner is None:
        from .retention import run_production_ttl_once

        runner = run_production_ttl_once
    sleeper = sleep or asyncio.sleep
    utcnow = clock or (lambda: datetime.now(timezone.utc))
    while True:
        try:
            await runner(
                enabled=True,
                now=utcnow(),
                limit=limit,
                session_factory=session_factory,
                object_storage=object_storage,
                redis_client=redis_client,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # Runner exceptions can carry meeting ids, object keys, or provider request context.
            # The next bounded tick is the retry signal; emit no exception args or traceback.
            log.warning("Minutes retention TTL tick failed; retrying on next tick")
        await sleeper(interval)


def _database_url() -> str:
    explicit = os.getenv("DATABASE_URL")
    if explicit:
        return explicit
    host = os.getenv("DB_HOST", "postgres")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "vexa")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    ssl_mode = os.getenv("DB_SSL_MODE", "disable")
    if ssl_mode not in _DATABASE_SSL_MODES:
        raise RuntimeError(
            "DB_SSL_MODE must be one of: disable, require, verify-ca, verify-full"
        )
    # Secret-backed DB_* values are raw credentials. Encode userinfo and the
    # database path so generated secrets cannot change the URL structure.
    url = (
        "postgresql+asyncpg://"
        f"{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}/"
        f"{quote(name, safe='')}"
    )
    return url if ssl_mode == "disable" else f"{url}?ssl={ssl_mode}"


def _require_config(env: "os._Environ | dict | None" = None) -> None:
    """Fail-fast on missing required config (A4), driven by the config.v1 declaration (ADR-0026).

    ``config.v1.json`` (next to this module) declares every env key the service consumes; the
    vendored shared preflight raises ``ConfigError`` (a ``RuntimeError``) naming every missing
    *required-explicit* keys include MEETING_TOKEN_SECRET, which HS256-signs the MeetingToken every
    spawn mints (invocation.mint_meeting_token) AND the bot-write verifiers check; unset, the
    deploy would 500 every POST /bots, so it refuses to boot instead. Capability tri-states
    (stt · object_storage, incl. the STT live auth probe) are logged here and exposed on
    ``/health``; they never block boot.
    """
    from .config_preflight import preflight

    preflight(env)


def build_production_app():
    """Wire the unified meeting-api with the real adapters + the lifespan-driven loops."""
    _require_config()  # A4: refuse to boot a misconfigured deploy (no MEETING_TOKEN_SECRET → every spawn 500s).
    _minutes_auto_join_config()
    managed_minutes_only = _operator_flag("ZAKI_MINUTES_MANAGED_ONLY", False)
    finalized_config = _minutes_finalized_config()

    import redis.asyncio as aioredis
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from . import create_app
    from .bot_spawn.adapters import HttpRuntimeClient, SqlAlchemyMeetingRepo
    from .collector.adapters import (
        RedisStreamBus,
        SqlAlchemyTranscriptStore,
        redis_client_options,
    )
    from .recordings.adapters import S3Storage, SqlAlchemyRecordingRepo
    from .retention.adapters import S3RetentionStorage, SqlAlchemyRetentionRepo

    database_url = _database_url()
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    runtime_api_url = os.getenv("RUNTIME_API_URL", "http://runtime:8090")
    runtime_control_secret = os.getenv("RUNTIME_CONTROL_SECRET") or ""
    if not runtime_control_secret:
        raise RuntimeError(
            "RUNTIME_CONTROL_SECRET is required to authenticate runtime operations"
        )
    runtime_callback_secret = os.getenv("RUNTIME_CALLBACK_SECRET") or ""
    if not runtime_callback_secret:
        raise RuntimeError(
            "RUNTIME_CALLBACK_SECRET is required to authenticate runtime callbacks"
        )
    # MeetingToken signing is its own service boundary. It is never the admin API credential or the
    # general platform-internal credential, and it is projected only into meeting-api.
    token_secret = os.getenv("MEETING_TOKEN_SECRET") or None

    engine = create_async_engine(database_url, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    redis_client = aioredis.from_url(
        redis_url, decode_responses=True, **redis_client_options()
    )

    # Per-module production adapters (each module's adapters.* builders) injected into create_app.
    transcript_store = SqlAlchemyTranscriptStore(session_factory, redis_client=redis_client)
    segment_bus = RedisStreamBus(redis_client)
    meeting_repo = SqlAlchemyMeetingRepo(session_factory)

    import httpx

    runtime_http = httpx.AsyncClient(timeout=30.0)
    runtime_client = HttpRuntimeClient(
        runtime_http,
        runtime_api_url,
        control_secret=runtime_control_secret,
    )

    # One Identity-owned settings lookup serves both managed capture and the Agent read opt-in.
    # The read plane cannot boot enabled without this authority: an outage/omission must never
    # become an implicit tenant opt-in.
    from .minutes_settings import IdentityMinutesSettings

    admin_api_url = (os.getenv("ADMIN_API_URL") or "").rstrip("/")
    internal_secret = os.getenv("INTERNAL_API_SECRET") or ""
    if not internal_secret:
        raise RuntimeError(
            "INTERNAL_API_SECRET is required for platform service-to-service calls"
        )
    minutes_settings = (
        IdentityMinutesSettings(admin_api_url, internal_secret)
        if admin_api_url and internal_secret
        else None
    )
    zaki_read_enabled = _operator_flag("ZAKI_MINUTES_READ_ENABLED", False)
    zaki_read_token = os.getenv("ZAKI_READ_TOKEN_MINUTES") or None
    if zaki_read_enabled and minutes_settings is None:
        raise RuntimeError(
            "ZAKI_MINUTES_READ_ENABLED requires ADMIN_API_URL and INTERNAL_API_SECRET"
        )
    minutes_capture_enabled = _operator_flag("ZAKI_MINUTES_CAPTURE_ENABLED", False)
    minutes_hub_token = os.getenv("ZAKI_MINUTES_HUB_TOKEN")
    agent_verification_key_id = os.getenv("ZAKI_AGENT_ERASURE_VERIFICATION_KEY_ID")
    agent_verification_secret = os.getenv("ZAKI_AGENT_ERASURE_VERIFICATION_SECRET")
    minutes_signing_key_id = os.getenv("ZAKI_MINUTES_ERASURE_SIGNING_KEY_ID")
    minutes_signing_secret = os.getenv("ZAKI_MINUTES_ERASURE_SIGNING_SECRET")
    erasure_boundary_configured = all((
        agent_verification_key_id,
        agent_verification_secret,
        minutes_signing_key_id,
        minutes_signing_secret,
    ))
    minutes_invocation_v2_enabled = _minutes_invocation_v2_config(
        capture_enabled=minutes_capture_enabled
    )
    if (minutes_capture_enabled or erasure_boundary_configured) and minutes_settings is None:
        raise RuntimeError(
            "managed Minutes capture/erasure requires ADMIN_API_URL and INTERNAL_API_SECRET"
        )
    ttl_config = _minutes_ttl_config()
    _validate_minutes_activation(
        capture_enabled=minutes_capture_enabled,
        read_enabled=zaki_read_enabled,
        ttl_enabled=ttl_config[0],
        read_token=zaki_read_token,
        hub_token=minutes_hub_token,
        # MANAGED_ONLY is only the legacy-create denial gate. Hub-trusting user routes instead
        # follow active capture or a complete historical erasure boundary.
        managed_routes_enabled=minutes_capture_enabled or erasure_boundary_configured,
        agent_verification_key_id=agent_verification_key_id,
        agent_verification_secret=agent_verification_secret,
        minutes_signing_key_id=minutes_signing_key_id,
        minutes_signing_secret=minutes_signing_secret,
        internal_secret=internal_secret or None,
    )
    from .capture import RedisCaptureCarrierFencer

    minutes_capture_fencer = RedisCaptureCarrierFencer(redis_client)

    recording_repo = SqlAlchemyRecordingRepo(session_factory)
    storage = S3Storage(
        bucket=os.getenv("MINIO_BUCKET", os.getenv("RECORDING_BUCKET", "vexa")),
        endpoint_url=os.getenv("S3_ENDPOINT") or _minio_endpoint_url(),
        access_key=os.getenv("S3_ACCESS_KEY") or os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("S3_SECRET_KEY") or os.getenv("MINIO_SECRET_KEY"),
    )
    retention_storage = S3RetentionStorage(
        bucket=os.getenv("MINIO_BUCKET", os.getenv("RECORDING_BUCKET", "vexa")),
        endpoint_url=os.getenv("S3_ENDPOINT") or _minio_endpoint_url(),
        access_key=os.getenv("S3_ACCESS_KEY") or os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("S3_SECRET_KEY") or os.getenv("MINIO_SECRET_KEY"),
    )
    minutes_retention_repo = SqlAlchemyRetentionRepo(
        session_factory, redis_client=redis_client
    )
    minutes_agent_eraser = None
    previous_agent_key_id = os.getenv(
        "ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_KEY_ID"
    )
    previous_agent_secret = os.getenv(
        "ZAKI_AGENT_ERASURE_PREVIOUS_VERIFICATION_SECRET"
    )
    previous_minutes_key_id = os.getenv(
        "ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_KEY_ID"
    )
    previous_minutes_secret = os.getenv(
        "ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET"
    )
    _validate_service_secret_isolation(**_meeting_api_secret_values())
    erasure_configured = erasure_boundary_configured
    agent_api_url = (os.getenv("AGENT_API_URL") or "http://agent-api:8080").rstrip("/")
    minutes_verification_keys = None
    agent_verification_keys = None
    if any((previous_agent_key_id, previous_agent_secret)) and not erasure_configured:
        raise RuntimeError(
            "Agent erasure previous verification key requires the current erasure boundary"
        )
    if any((previous_minutes_key_id, previous_minutes_secret)) and not erasure_configured:
        raise RuntimeError(
            "Minutes erasure previous verification key requires the current erasure boundary"
        )
    if erasure_configured:
        if not agent_api_url or not internal_secret:
            raise RuntimeError(
                "Minutes erasure configuration requires AGENT_API_URL and INTERNAL_API_SECRET"
            )
        from datetime import datetime, timezone

        from .agent_erasure import AgentMinutesEraser

        try:
            agent_verification_keys = _agent_erasure_verification_keyring(
                current_key_id=agent_verification_key_id,
                current_secret=agent_verification_secret,
                previous_key_id=previous_agent_key_id,
                previous_secret=previous_agent_secret,
            )
            minutes_verification_keys = _minutes_erasure_verification_keyring(
                current_key_id=minutes_signing_key_id,
                current_secret=minutes_signing_secret,
                previous_key_id=previous_minutes_key_id,
                previous_secret=previous_minutes_secret,
            )
            if previous_minutes_secret and previous_minutes_secret in {
                internal_secret or None,
                agent_verification_secret,
                zaki_read_token if zaki_read_enabled else None,
            }:
                raise RuntimeError(
                    "Minutes erasure previous verification secret must be distinct from "
                    "authentication and Agent erasure secrets"
                )
            if previous_agent_secret and previous_agent_secret in {
                internal_secret or None,
                minutes_signing_secret,
                previous_minutes_secret,
                zaki_read_token if zaki_read_enabled else None,
            }:
                raise RuntimeError(
                    "Agent erasure previous verification secret must be distinct from "
                    "authentication and Minutes erasure secrets"
                )
            minutes_agent_eraser = AgentMinutesEraser(
                agent_api_url,
                internal_secret,
                verification_keys=agent_verification_keys,
                now=lambda: datetime.now(timezone.utc),
            )
        except ValueError:
            raise RuntimeError("Minutes Agent erasure verification config is invalid") from None
    if minutes_capture_enabled and minutes_agent_eraser is None:
        raise RuntimeError(
            "ZAKI_MINUTES_CAPTURE_ENABLED requires AGENT_API_URL and INTERNAL_API_SECRET "
            "for erasure"
        )

    # Per-user webhook delivery (WebhookSink: SSRF-guard → event-filter → sign → POST → enqueue-retry).
    # httpx transport; failures route to the redis RetryQueue the background drain loop sweeps.
    # WH2: the transport is IP-PINNED — it re-resolves + re-validates the host at connect time and
    # dials the validated IP (preserving Host + TLS SNI), closing the DNS-rebinding TOCTOU window
    # between submit-time validate_webhook_url and the actual socket connect.
    from .webhooks import RetryQueue, WebhookSink
    from .webhooks.ssrf import build_pinned_transport

    async def _webhook_transport(url: str, body: bytes, headers: dict):
        async with httpx.AsyncClient(timeout=10.0, transport=build_pinned_transport()) as client:
            return await client.post(url, content=body, headers=headers)

    webhook_sink = WebhookSink(_webhook_transport, queue=RetryQueue(redis_client))

    minutes_finalized_outbox = None
    minutes_finalized_sink = None
    if finalized_config[0]:
        from .webhooks.platform_finalized import (
            MinutesPlatformWebhookSink,
            RedisTranscriptFinalizedOutbox,
        )

        async def _minutes_platform_transport(url: str, body: bytes, headers: dict):
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
                return await client.post(url, content=body, headers=headers)

        minutes_finalized_outbox = RedisTranscriptFinalizedOutbox(redis_client)
        try:
            minutes_finalized_sink = MinutesPlatformWebhookSink(
                url=finalized_config[1],
                key_id=finalized_config[2],
                secret=finalized_config[3],
                transport=_minutes_platform_transport,
            )
        except ValueError:
            raise RuntimeError(
                "ZAKI_MINUTES_FINALIZED_ENABLED requires valid operator webhook configuration"
            ) from None

    # Completion finalization: when the lifecycle FSM lands on a terminal status the callback runs
    # this — flush the meeting's remaining redis segments to Postgres (threshold 0) + persist the
    # processed doc into meeting.data, so a finished meeting is durable IMMEDIATELY (not `whenever
    # the next db-writer tick happens to run`).
    from .collector.db_writer import finalize_meeting

    async def _transcript_finalizer(meeting_id: int) -> None:
        await finalize_meeting(redis_client, transcript_store, meeting_id)

    # Calendar-sync user edges (GET/POST /user/calendar/sync): the SAME one-user pass the
    # background sweep runs, on demand — paste-a-feed gets an immediate result instead of a
    # silent wait for the next tick (fail loud to the user). None-returns mean "no feed / sync
    # unavailable" and the route answers 404/503 accordingly.
    async def _calendar_sync_now(user_id: int):
        admin_api_url = (os.getenv("ADMIN_API_URL") or "").rstrip("/")
        internal_secret = os.getenv("INTERNAL_API_SECRET") or ""
        if not (admin_api_url and internal_secret):
            return None
        import json as _json

        from .calendar_sync import fetch_configs, run_user_sync, store_stamp

        configs = await fetch_configs(admin_api_url, internal_secret)
        cfg = next((c for c in configs or [] if c.get("user_id") == user_id), None)
        if cfg is None:
            return None

        async def _pub(uid, entry):
            frame = {"type": "meeting.status", "meeting_id": entry["id"],
                     "native": entry.get("native"), "status": entry.get("status"),
                     "when": entry.get("when")}
            try:
                await redis_client.publish(f"u:{uid}:meetings", _json.dumps(frame))
            except Exception:
                pass

        stamp = await run_user_sync(transcript_store, cfg, publish=_pub)
        await store_stamp(redis_client, user_id, stamp)
        return stamp

    async def _calendar_sync_status(user_id: int):
        from .calendar_sync import read_stamp
        return await read_stamp(redis_client, user_id)

    app = create_app(
        transcript_store=transcript_store,
        redis=segment_bus,
        meeting_repo=meeting_repo,
        runtime=runtime_client,
        recording_repo=recording_repo,
        storage=storage,
        token_secret=token_secret,
        runtime_callback_secret=runtime_callback_secret,
        # The user-stop route (DELETE /bots) publishes the bot's `leave` command on redis pub/sub.
        # redis.asyncio's client satisfies the CommandPublisher port directly (async publish()).
        command_publisher=redis_client,
        webhook_sink=webhook_sink,
        transcript_finalizer=_transcript_finalizer,
        minutes_finalized_enabled=finalized_config[0],
        minutes_finalized_outbox=minutes_finalized_outbox,
        minutes_finalized_sink=minutes_finalized_sink,
        calendar_sync_now=_calendar_sync_now,
        calendar_sync_status=_calendar_sync_status,
        zaki_read_enabled=zaki_read_enabled,
        zaki_read_token=zaki_read_token,
        zaki_read_scope=minutes_settings,
        minutes_capture_enabled=minutes_capture_enabled,
        minutes_invocation_v2_enabled=minutes_invocation_v2_enabled,
        minutes_settings=minutes_settings,
        minutes_capture_fencer=minutes_capture_fencer,
        minutes_redis_url=redis_url,
        minutes_meeting_api_url=os.getenv("MEETING_API_URL") or "http://meeting-api:8080",
        minutes_hub_token=minutes_hub_token,
        managed_minutes_only=managed_minutes_only,
        minutes_retention_repo=(minutes_retention_repo if minutes_agent_eraser else None),
        minutes_retention_storage=(retention_storage if minutes_agent_eraser else None),
        minutes_agent_eraser=minutes_agent_eraser,
        minutes_erasure_signing_key_id=(
            minutes_signing_key_id if minutes_agent_eraser else None
        ),
        minutes_erasure_signing_secret=(
            minutes_signing_secret if minutes_agent_eraser else None
        ),
        minutes_erasure_verification_keys=(
            minutes_verification_keys if minutes_agent_eraser else None
        ),
        minutes_erasure_nonce_factory=(
            (lambda: secrets.token_urlsafe(24))
            if minutes_agent_eraser else None
        ),
    )

    _attach_background_loops(
        app,
        transcript_store,
        segment_bus,
        redis_client,
        meeting_repo,
        runtime_client,
        ttl_session_factory=session_factory,
        ttl_storage=retention_storage,
        ttl_config=ttl_config,
    )
    return app


def _minio_endpoint_url() -> str:
    """Build an http(s) MinIO URL from MINIO_ENDPOINT (host:port) + MINIO_SECURE, mirroring 0.11."""
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    scheme = "https" if os.getenv("MINIO_SECURE", "false").lower() == "true" else "http"
    return f"{scheme}://{endpoint}"


def _attach_background_loops(
    app,
    transcript_store,
    segment_bus,
    redis_client,
    meeting_repo=None,
    runtime=None,
    *,
    ttl_session_factory=None,
    ttl_storage=None,
    ttl_config=None,
) -> None:
    """Register the FastAPI lifespan that starts/stops the control-plane poll loops."""
    from .collector.ingest import consume_segments

    seg_interval = float(os.getenv("SEGMENT_CONSUMER_INTERVAL", "0.5"))
    webhook_interval = float(os.getenv("WEBHOOK_DRAIN_INTERVAL", "5"))
    scheduler_interval = float(os.getenv("SCHEDULER_TICK_INTERVAL", "1"))
    # The db-writer cadence — the parent's BACKGROUND_TASK_INTERVAL (10s); either env name works.
    db_writer_interval = float(
        os.getenv("DB_WRITER_INTERVAL_S", os.getenv("BACKGROUND_TASK_INTERVAL", "10"))
    )
    # Stop-reconcile backstop: a meeting whose bot was told to leave but never sent its own terminal
    # callback would stay `stopping` forever. After a grace window, complete it through the same
    # lifecycle callback the bot uses — so the FSM, webhook, and ws status frame all fire identically.
    stop_grace = float(os.getenv("STOP_RECONCILE_GRACE_S", "45"))
    stop_interval = float(os.getenv("STOP_RECONCILE_INTERVAL_S", "15"))
    # GENERAL reconcile: ANY non-terminal status whose bot is gone (its row quiet past the grace) is
    # converged to a terminal state through the same lifecycle callback. `stopping` uses stop_grace
    # (a stop was requested); `active`/etc. use `active_grace`. The active-reap is ADDITIONALLY gated on
    # runtime WORKLOAD liveness (reconcile.py `_bot_workload_gone`): a meeting whose bot workload is still
    # alive is NEVER reaped, even past the grace — so a quiet-but-live (silent) bot is safe regardless of
    # this window. With that gate in place, 300s is a SANE default again (the 86400 env stopgap, which
    # only worked because it disabled the time-based reap entirely, is no longer needed).
    active_grace = float(os.getenv("RECONCILE_ACTIVE_GRACE_S", "300"))
    # Bounded untracked escalation (the zombie-loop fix): a meeting whose workload stays UNTRACKED
    # (runtime 404) CONTINUOUSLY past this window — no runtime re-adoption, no bot callback — is
    # presumed lost (runtime restart on the process backend / external removal) and advanced to
    # `failed` with the evidence note, instead of retrying an error + dead DELETE every sweep forever.
    untracked_grace = float(os.getenv("MEETING_UNTRACKED_GRACE_SEC", "600"))
    ttl_enabled, ttl_interval, ttl_limit = ttl_config or _minutes_ttl_config()
    if ttl_enabled and (ttl_session_factory is None or ttl_storage is None):
        raise RuntimeError("enabled Minutes TTL requires database and object storage adapters")

    async def _segment_consumer_loop() -> None:
        # Drain the transcription_segments stream → persist + publish tc:…:mutable.
        while True:
            try:
                await consume_segments(transcript_store, segment_bus)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("segment consumer tick failed")
            await asyncio.sleep(seg_interval)

    async def _db_writer_loop() -> None:
        # The RESTORED parent db-writer (0.10 process_redis_to_postgres): each tick, flush every
        # active meeting's IMMUTABLE redis-hash segments into the transcriptions table (upsert on
        # (meeting_id, segment_id)) and drain its processed-notes stream into meeting.data JSONB.
        # Redis is trimmed only AFTER the confirmed durable write. Without this loop nothing ever
        # moved segments to Postgres — the transcriptions table stayed EMPTY and a redis eviction
        # was unrecoverable transcript loss (the 0.12 release blocker).
        from .collector.db_writer import db_writer_tick

        if not hasattr(transcript_store, "upsert_segments"):
            return  # a store without a durable sink (bare fake) — nothing to flush into
        while True:
            try:
                await db_writer_tick(redis_client, transcript_store)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("db-writer tick failed")
            await asyncio.sleep(db_writer_interval)

    async def _webhook_drain_loop() -> None:
        import httpx

        from .webhooks.retry import drain_retry_queue
        from .webhooks.ssrf import build_pinned_transport

        # The injected Transport: POST the signed envelope; return the response (its .status_code
        # drives the retry/permanent decision in retry._deliver_one). WH2: IP-pinned at connect
        # (re-resolve + re-validate + dial the validated IP) so a rebinding flip can't slip an
        # internal target into a retry sweep either.
        async def _transport(url: str, body: bytes, headers: dict):
            async with httpx.AsyncClient(timeout=10.0, transport=build_pinned_transport()) as client:
                return await client.post(url, content=body, headers=headers)

        while True:
            try:
                await drain_retry_queue(redis_client, _transport)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("webhook retry-drain tick failed")
            await asyncio.sleep(webhook_interval)

    async def _scheduler_tick_loop() -> None:
        # The scheduler fires due schedule.v1 jobs — including the join-retry re-spawns that
        # JoinRetryController enqueues. The Scheduler instance lives on app.state when wired.
        scheduler = getattr(app.state, "scheduler", None)
        if scheduler is None:
            return
        while True:
            try:
                scheduler.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("scheduler tick failed")
            await asyncio.sleep(scheduler_interval)

    async def _stop_reconcile_loop() -> None:
        # Complete meetings stuck in `stopping` past the grace window AND kill any orphan workload (CC6 /
        # ADR-0024) — through the importable reconcile sweep, reusing the SAME in-process lifecycle logic
        # so the FSM → persist → webhook → ws-publish path fires identically (no duplicate logic).
        if meeting_repo is None or not hasattr(meeting_repo, "list_stale_stopping"):
            return
        from .lifecycle.machine import TransitionSource as _TS
        from .lifecycle.reconcile import (
            reconcile_stale_nonterminal_sweep,
            reconcile_stale_stopping_sweep,
        )

        # Drive the sweep's synthetic terminals through the in-process lifecycle entry — NOT an httpx
        # POST to 127.0.0.1:PORT. The sweeps only ever post a TERMINAL status after their own evidence
        # gate (confirmed teardown / bounded untracked escalation), so they are a runtime-destroy-class
        # advance: `force_terminal_on_destroy=True` lets the terminal edge land even when the in-process
        # FSM record is a stale non-terminal state the DB already moved past (the loopback self-POST
        # 409'd on exactly that — `joining → completed` for a bot stopped before it reported active —
        # leaving the meeting `stopping` and the reaper re-DELETEing every tick forever).
        apply_lifecycle_event = app.state.apply_lifecycle_event

        async def _post_lifecycle(body: dict):
            status_code, _content = await apply_lifecycle_event(
                body,
                transition_source=_TS.RUNTIME_DESTROY,
                force_terminal_on_destroy=True,
            )
            return status_code

        # The general sweep (any stale non-terminal status whose bot is gone) subsumes the stale-
        # stopping sweep, but we keep the latter as the guaranteed orphan-kill backstop for `stopping`.
        has_general = hasattr(meeting_repo, "list_stale_nonterminal")
        while True:
            try:
                if has_general:
                    await reconcile_stale_nonterminal_sweep(
                        meeting_repo, runtime, _post_lifecycle,
                        stop_grace=stop_grace, active_grace=active_grace, log=log,
                        untracked_grace=untracked_grace,
                    )
                await reconcile_stale_stopping_sweep(
                    meeting_repo, runtime, _post_lifecycle, stop_grace=stop_grace, log=log,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("stop-reconcile tick failed")
            await asyncio.sleep(stop_interval)

    admin_api_url = (os.getenv("ADMIN_API_URL") or "").rstrip("/")
    internal_secret = os.getenv("INTERNAL_API_SECRET") or ""

    # Calendar sync: each sweep discovers every user with a connected ICS feed (admin-api internal
    # edge), fetches it over the SSRF-pinned transport, and upserts planned meetings (one row per
    # calendar UID — next occurrence only). Per-user try/except: one bad feed never stalls the
    # sweep. Unset ADMIN_API_URL/INTERNAL_API_SECRET → no-op (capability degrade, not boot-fail).
    calendar_interval = float(os.getenv("CALENDAR_SYNC_INTERVAL_S", "300"))

    async def _cal_publish(user_id, entry):
        import json as _json
        frame = {"type": "meeting.status", "meeting_id": entry["id"],
                 "native": entry.get("native"), "status": entry.get("status"),
                 "when": entry.get("when")}
        try:
            await redis_client.publish(f"u:{user_id}:meetings", _json.dumps(frame))
        except Exception:
            pass

    async def _calendar_sync_loop() -> None:
        if not (admin_api_url and internal_secret):
            return
        if not hasattr(transcript_store, "create_planned_meeting"):
            return
        from .calendar_sync import fetch_configs, run_user_sync, store_stamp

        while True:
            try:
                configs = await fetch_configs(admin_api_url, internal_secret)
                for cfg in configs or []:
                    try:  # one bad feed never stalls the sweep
                        stamp = await run_user_sync(transcript_store, cfg, publish=_cal_publish)
                    except Exception:
                        log.exception("calendar sync failed for user %s", cfg.get("user_id"))
                        continue
                    await store_stamp(redis_client, cfg["user_id"], stamp)
            except Exception:
                log.exception("calendar sync tick failed")
            await asyncio.sleep(calendar_interval)

    async def _retention_ttl_loop() -> None:
        await _minutes_ttl_loop(
            enabled=ttl_enabled,
            interval=ttl_interval,
            limit=ttl_limit,
            session_factory=ttl_session_factory,
            object_storage=ttl_storage,
            redis_client=redis_client,
        )

    async def _platform_finalized_loop() -> None:
        await _minutes_finalized_drain_loop(
            enabled=getattr(app.state, "minutes_finalized_enabled", False) is True,
            drain=app.state.minutes_finalized_drain,
            interval=webhook_interval,
        )

    @asynccontextmanager
    async def lifespan(_app):
        tasks = [
            asyncio.create_task(_segment_consumer_loop(), name="segment-consumer"),
            asyncio.create_task(_db_writer_loop(), name="db-writer"),
            asyncio.create_task(_webhook_drain_loop(), name="webhook-drain"),
            asyncio.create_task(_scheduler_tick_loop(), name="scheduler-tick"),
            asyncio.create_task(_stop_reconcile_loop(), name="stop-reconcile"),
            asyncio.create_task(_calendar_sync_loop(), name="calendar-sync"),
            asyncio.create_task(_retention_ttl_loop(), name="minutes-retention-ttl"),
            asyncio.create_task(_platform_finalized_loop(), name="minutes-finalized-drain"),
        ]
        log.info("meeting-api background loops started: %s", [t.get_name() for t in tasks])
        try:
            yield
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    # FastAPI supports assigning .router.lifespan_context post-construction.
    app.router.lifespan_context = lifespan


# uvicorn ``meeting_api.__main__:app`` resolves this. Exposed LAZILY via PEP 562 so merely importing
# this module never wires SQLAlchemy/asyncpg/boto3 (NOT in the offline gate venv). The app + loops
# are constructed only when uvicorn touches ``__main__.app`` at boot; the loops start under the
# lifespan, once the event loop is running.
def __getattr__(name: str):
    if name == "app":
        return build_production_app()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def main() -> None:
    import uvicorn

    uvicorn.run(
        build_production_app(),
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8080")),
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )


if __name__ == "__main__":
    main()
