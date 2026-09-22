"""The admin-api FastAPI surface — v0.12 carve of `services/admin-api/app/main.py`.

Derived (re-read, reimplemented clean) — the load-bearing identity surface that O-STACK-3
exercises:

  3 auth tiers (parent §):
    - admin   : `X-Admin-API-Key` == ADMIN_API_TOKEN (hmac.compare_digest)  → user/token CRUD
    - user    : `X-API-Key` resolves to an APIToken with a valid scope       → /user/* self-serve
    - internal: `X-Internal-Secret` == INTERNAL_API_SECRET, FAIL-CLOSED      → /internal/validate

  /internal/validate (the gateway's authz oracle): returns user_id + scopes + max_concurrent +
  email, plus webhook_url/secret/events from user.data; rejects expired tokens; bumps
  last_used_at; FAILS CLOSED when INTERNAL_API_SECRET is unset (503) and on a bad secret (403).

  Token mint: scoped {bot,tx,browser,agent}, optional multi-scope `?scopes=bot,tx`, optional expiry
  `?expires_in=<sec>`; an invalid scope → 422.
"""
import hmac
import ipaddress
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request, Response, Security, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, StrictBool
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..schema.models import APIToken, PlatformSetting, User
from ..token_scope import (
    CONTRACT_SCOPES,
    IDENTITY_V1,
    IDENTITY_V2,
    VALID_SCOPES,
    generate_prefixed_token,
)
from .db import get_db

ADMIN_KEY_HEADER = APIKeyHeader(name="X-Admin-API-Key", auto_error=False)
USER_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def _admin_token() -> Optional[str]:
    return os.getenv("ADMIN_API_TOKEN")


def _internal_secret() -> str:
    return os.environ.get("INTERNAL_API_SECRET", "")


def _validate_auth_domain_separation() -> None:
    """Refuse aliasing the external admin key with internal service authority."""

    admin = _admin_token()
    internal = _internal_secret()
    if admin and internal and hmac.compare_digest(admin, internal):
        raise RuntimeError(
            "ADMIN_API_TOKEN must be distinct from INTERNAL_API_SECRET"
        )


def _dev_mode() -> bool:
    return os.getenv("DEV_MODE", "false").lower() == "true"


def _is_explicit_loopback_host(hostname: str) -> bool:
    """Allow cleartext ICS only for a lexically explicit loopback development host."""
    host = (hostname or "").strip().lower().rstrip(".")
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


async def verify_admin_token(admin_api_key: str = Security(ADMIN_KEY_HEADER)):
    token = _admin_token()
    if not token:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Admin authentication is not configured on the server.")
    if not admin_api_key or not hmac.compare_digest(admin_api_key, token):
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Invalid or missing admin token.")


async def _authenticate_user(
    api_key: str,
    db: AsyncSession,
    *,
    required_scope: Optional[str] = None,
) -> User:
    if not api_key:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Missing API Key")
    row = (await db.execute(select(APIToken).where(APIToken.token == api_key))).scalars().first()
    if not row:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Invalid API Key")
    if row.expires_at is not None:
        now = datetime.now(timezone.utc)
        expiry = row.expires_at
        if expiry.tzinfo is None:
            now = now.replace(tzinfo=None)
        if expiry <= now:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    token_scopes = set(row.scopes) if row.scopes else set()
    if not token_scopes & VALID_SCOPES:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Token scope not authorized for this endpoint")
    if required_scope is not None and required_scope not in token_scopes:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail=f"Token scope not authorized for this endpoint; {required_scope} required",
        )
    user = (await db.execute(select(User).where(User.id == row.user_id))).scalars().first()
    if not user:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Invalid API Key")
    return user


async def get_current_user(api_key: str = Security(USER_KEY_HEADER),
                           db: AsyncSession = Depends(get_db)) -> User:
    return await _authenticate_user(api_key, db)


async def get_current_browser_user(api_key: str = Security(USER_KEY_HEADER),
                                   db: AsyncSession = Depends(get_db)) -> User:
    """Resolve an interactive user; machine credentials cannot mutate human consent."""
    return await _authenticate_user(api_key, db, required_scope="browser")


# --- request/response models ---
class UserCreate(BaseModel):
    email: str
    name: Optional[str] = None
    max_concurrent_bots: int = 3


class UserResponse(BaseModel):
    id: int
    email: str
    name: Optional[str] = None
    max_concurrent_bots: int

    model_config = {"from_attributes": True}


class TokenResponse(BaseModel):
    id: int
    token: str
    user_id: int
    scopes: List[str]

    model_config = {"from_attributes": True}


class TokenInfo(BaseModel):
    """A token as listed — metadata only, NEVER the secret value (mint is the only place it crosses)."""
    id: int
    user_id: int
    scopes: List[str]
    name: Optional[str] = None
    created_at: Optional[datetime] = None
    last_used_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class WebhookUpdate(BaseModel):
    webhook_url: str
    webhook_secret: Optional[str] = None
    webhook_events: Optional[Dict[str, bool]] = None


class CalendarUpdate(BaseModel):
    """The user's secret ICS feed URL (``null`` disconnects). ``auto_join`` remains a
    compatibility input; launch v1 accepts false and rejects true."""
    ics_url: Optional[str] = None
    auto_join: Optional[bool] = None


class MinutesUpdate(BaseModel):
    """User-owned Minutes choices only; deployment policy and attestations are server-owned."""

    capture_enabled: Optional[StrictBool] = None
    agent_read_enabled: Optional[StrictBool] = None
    retention_days: Optional[Dict[str, object]] = None


_MINUTES_RETENTION_DEFAULTS = {"audio": 7, "transcript": 30, "summary": 30}
_MINUTES_RETENTION_KEYS = frozenset(_MINUTES_RETENTION_DEFAULTS)
_MINUTES_POLICY_VERSION = "minutes-capture.v1"


def _minutes_operator_flag(name: str) -> bool:
    """Fail closed at the policy boundary; deployment preflight reports malformed values."""
    return os.getenv(name, "false").strip().lower() == "true"


def _validated_minutes_retention(value: object) -> Dict[str, int]:
    if not isinstance(value, dict) or set(value) != _MINUTES_RETENTION_KEYS:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="retention_days must contain exactly audio, transcript, and summary",
        )
    if any(type(days) is not int or days < 1 or days > 3650 for days in value.values()):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="retention_days values must be integers between 1 and 3650",
        )
    normalized = {name: int(value[name]) for name in _MINUTES_RETENTION_DEFAULTS}
    if normalized["audio"] > normalized["transcript"]:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="audio retention cannot exceed transcript retention",
        )
    if normalized["summary"] > normalized["transcript"]:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="summary retention cannot exceed transcript retention",
        )
    return normalized


def _minutes_view(user: User) -> dict:
    data = user.data if isinstance(user.data, dict) else {}
    stored = data.get("minutes_prefs")
    prefs = stored if isinstance(stored, dict) else {}
    retention_valid = True
    if "retention_days" not in prefs:
        retention = dict(_MINUTES_RETENTION_DEFAULTS)
    else:
        try:
            retention = _validated_minutes_retention(prefs.get("retention_days"))
        except HTTPException:
            # Keep the response shape stable for repair UI, but never turn corrupt policy into
            # a silent retention extension for capture authority.
            retention = dict(_MINUTES_RETENTION_DEFAULTS)
            retention_valid = False

    attested_at = prefs.get("attested_at")
    try:
        parsed_attestation = datetime.fromisoformat(
            attested_at.replace("Z", "+00:00")
        ) if isinstance(attested_at, str) else None
    except ValueError:
        parsed_attestation = None
    attestation_valid = (
        parsed_attestation is not None
        and parsed_attestation.tzinfo is not None
        and parsed_attestation.utcoffset() is not None
        and parsed_attestation <= datetime.now(timezone.utc)
    )

    operator_enabled = _minutes_operator_flag("ZAKI_MINUTES_CAPTURE_ENABLED")
    read_operator_enabled = _minutes_operator_flag("ZAKI_MINUTES_READ_ENABLED")
    capture_requested = prefs.get("capture_enabled") is True
    agent_read_requested = prefs.get("agent_read_enabled") is True
    capture_enabled = (
        operator_enabled
        and retention_valid
        and attestation_valid
        and capture_requested
        and prefs.get("policy_version") == _MINUTES_POLICY_VERSION
    )
    agent_read_enabled = read_operator_enabled and agent_read_requested
    attested_at = attested_at if capture_enabled else None
    capture_repair = None
    if operator_enabled and capture_requested and not capture_enabled:
        capture_repair = (
            "retention_repair_required"
            if not retention_valid
            else "reconsent_required"
        )
    return {
        "operator_enabled": operator_enabled,
        "read_operator_enabled": read_operator_enabled,
        "capture_enabled": capture_enabled,
        "agent_read_enabled": agent_read_enabled,
        "capture_requested": capture_requested,
        "agent_read_requested": agent_read_requested,
        "retention_days": retention,
        "policy_version": _MINUTES_POLICY_VERSION,
        "attested_at": attested_at,
        # Bounded UI state only: never expose corrupt persisted values or parse diagnostics.
        "capture_repair": capture_repair,
    }


# ── model + transcription config (per-user prefs and the platform-wide defaults) ──
# One vocabulary everywhere: a MODELS config is {mode, model, meeting_model, base_url, api_key}
# (mode "subscription" = the deployment's brokered credential — the mounted Claude Code
# subscription or a deployment API key; mode "custom" = a user/operator-supplied
# Anthropic-/OpenAI-compatible endpoint + key, e.g. a LiteLLM/OpenRouter gateway in front of an
# open-source model). A TRANSCRIPTION config is {url, token} — the STT service the bot invocation
# rides. Per-user copies live in users.data["model_prefs"] / ["transcription_prefs"]; the
# platform defaults live in platform_settings rows "models" / "transcription". Model config
# resolves field-by-field. A transcription backend is an atomic URL+credential boundary: selecting
# a user URL selects only that user's backend fields, never a platform credential. The process env
# stays the bottom fallback downstream (dispatch/bot_spawn only override what is set here).
MODEL_MODES = ("subscription", "custom")
_MODELS_FIELDS = ("mode", "model", "meeting_model", "base_url", "api_key")
_TRANSCRIPTION_FIELDS = ("url", "token")
# "setup" tracks the admin first-run wizard: per-step state ("done" / "skipped") + overall
# completion — the terminal re-surfaces the wizard until it reads completed. Plain strings,
# no secrets, admin-gated like the other keys.
_SETUP_FIELDS = ("models", "transcription", "completed")
SETTING_KEYS = {"models": _MODELS_FIELDS, "transcription": _TRANSCRIPTION_FIELDS,
                "setup": _SETUP_FIELDS}


class ModelPrefsUpdate(BaseModel):
    """Partial update — only fields the caller SENDS change; an empty string clears a field."""
    mode: Optional[str] = None
    model: Optional[str] = None
    meeting_model: Optional[str] = None
    base_url: Optional[str] = None
    api_key: Optional[str] = None


class TranscriptionPrefsUpdate(BaseModel):
    url: Optional[str] = None
    token: Optional[str] = None


_BLOCKED_USER_ENDPOINT_HOSTS = frozenset({
    "metadata.google.internal",
    "metadata.amazonaws.com",
})


def _browser_ipv4(host: str) -> Optional[ipaddress.IPv4Address]:
    """Parse legacy numeric IPv4 forms normalized by WHATWG-compatible HTTP clients."""
    pieces = host.split(".")
    if pieces[-1] == "":
        pieces.pop()
    if not pieces or len(pieces) > 4:
        return None
    numbers: list[int] = []
    for piece in pieces:
        if not piece:
            return None
        base, digits = 10, piece
        if piece.lower().startswith("0x"):
            base, digits = 16, piece[2:]
        elif len(piece) > 1 and piece.startswith("0"):
            base, digits = 8, piece[1:]
        if not digits:
            digits = "0"
        try:
            numbers.append(int(digits, base))
        except ValueError:
            return None
    if any(number < 0 for number in numbers):
        return None
    if any(number > 255 for number in numbers[:-1]):
        return None
    remaining_bytes = 5 - len(numbers)
    if numbers[-1] >= 256**remaining_bytes:
        return None
    value = numbers[-1]
    for index, number in enumerate(numbers[:-1]):
        value += number * 256 ** (3 - index)
    return ipaddress.IPv4Address(value)


def _validate_user_endpoint_url(
    value: str,
    *,
    allowed_hosts_env: str,
    endpoint: str,
) -> None:
    """Validate one user-owned inference origin against a dedicated operator allowlist."""
    from urllib.parse import urlparse

    if "\\" in value or any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"user {endpoint} url contains an unsafe delimiter")
    try:
        parsed = urlparse(value)
        host = parsed.hostname
        port = parsed.port
    except ValueError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"user {endpoint} url is malformed") from None
    if parsed.scheme != "https" or not host:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"user {endpoint} url must be an https URL")
    if parsed.username is not None or parsed.password is not None:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"user {endpoint} url cannot contain credentials")
    if port not in (None, 443):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"user {endpoint} url must use the approved https origin")
    if parsed.query or parsed.fragment:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"user {endpoint} url must be a query-free base URL")
    canonical_host = host.lower().rstrip(".")
    if any(
        not label or label.startswith("-") or label.endswith("-")
        for label in canonical_host.split(".")
    ):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"user {endpoint} url hostname is malformed")
    if "%" in canonical_host:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"user {endpoint} url hostname cannot use percent encoding")
    if (
        canonical_host in _BLOCKED_USER_ENDPOINT_HOSTS
        or canonical_host == "localhost"
        or canonical_host.endswith((".localhost", ".local", ".internal"))
    ):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"user {endpoint} url cannot target an internal host")
    browser_ip = _browser_ipv4(canonical_host)
    try:
        address = browser_ip or ipaddress.ip_address(canonical_host)
    except ValueError:
        address = None
    if address is not None and not address.is_global:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"user {endpoint} url cannot target a private network")
    if address is None and "." not in canonical_host:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"user {endpoint} url cannot target a single-label host")
    allowed_hosts = {
        configured.strip().lower().rstrip(".")
        for configured in os.getenv(allowed_hosts_env, "").split(",")
        if configured.strip()
    }
    if canonical_host not in allowed_hosts:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"user {endpoint} url hostname is not operator-approved")


def _validate_user_transcription_url(value: str) -> None:
    _validate_user_endpoint_url(
        value,
        allowed_hosts_env="VEXA_USER_TRANSCRIPTION_ALLOWED_HOSTS",
        endpoint="transcription",
    )


def _validate_user_model_url(value: str) -> None:
    _validate_user_endpoint_url(
        value,
        allowed_hosts_env="VEXA_USER_MODEL_ALLOWED_HOSTS",
        endpoint="model",
    )


def _mask_secret(secret: Optional[str]) -> Optional[str]:
    """The webhook-secret masking rule: never echo a stored secret in the clear — last 4 chars
    behind asterisks, enough to recognize WHICH secret is set."""
    if not secret:
        return None
    return "********" + (secret[-4:] if len(secret) > 8 else "")


def _validate_config_fields(update: dict, *, kind: str) -> dict:
    """Shared field validation for both the per-user prefs and the platform settings writers
    (one rulebook, whichever tier writes). Returns the cleaned update dict."""
    from urllib.parse import urlparse

    cleaned: dict = {}
    for field, raw in update.items():
        value = (raw or "").strip() if isinstance(raw, str) else raw
        if value in (None, ""):
            cleaned[field] = ""  # explicit clear
            continue
        if not isinstance(value, str) or len(value) > 2048:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"{field} must be a string under 2048 chars")
        if field == "mode" and value not in MODEL_MODES:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"mode must be one of {sorted(MODEL_MODES)}")
        if field in ("base_url", "url"):
            try:
                parsed = urlparse(value)
                hostname = parsed.hostname
                parsed.port
            except ValueError:
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"{field} must be a valid http(s) URL") from None
            if parsed.scheme not in ("http", "https") or not hostname:
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"{field} must be an http(s) URL")
            if field == "url" and kind == "transcription_prefs":
                _validate_user_transcription_url(value)
            if field == "base_url" and kind == "model_prefs":
                _validate_user_model_url(value)
        cleaned[field] = value
    return cleaned


def _apply_config_update(stored: dict, cleaned: dict) -> dict:
    """Overlay a cleaned partial update onto a stored config: set non-empty, drop cleared."""
    out = dict(stored or {})
    for field, value in cleaned.items():
        if value == "":
            out.pop(field, None)
        else:
            out[field] = value
    return out


def _resolve_model_backend(user_cfg: dict, platform_cfg: dict) -> dict:
    """Resolve model names flexibly while keeping every endpoint/key pair on one owner tier."""
    user_mode = (user_cfg.get("mode") or "").strip()
    user_url = (user_cfg.get("base_url") or "").strip()

    # Managed subscription is an explicit user choice: model names may inherit, endpoint/key may
    # not. Stale custom fields are intentionally ignored.
    if user_mode == "subscription":
        out = {"mode": "subscription"}
        for field in ("model", "meeting_model"):
            value = user_cfg.get(field) or platform_cfg.get(field)
            if value:
                out[field] = value
        return out

    # Custom mode is an atomic user tier. A missing URL/key stays missing (fail loud downstream)
    # instead of being filled with a platform credential. Revalidate on every read so an operator
    # allowlist revocation takes effect for already-stored preferences.
    if user_mode == "custom":
        names = {
            field: user_cfg.get(field) or platform_cfg.get(field)
            for field in ("model", "meeting_model")
        }
        if not user_url:
            return {
                "mode": "custom",
                **names,
                "blocked": True,
                "config_status": "incomplete",
                "validation_error": "Personal model endpoint is incomplete; set an approved Base URL.",
            }
        try:
            _validate_user_model_url(user_url)
        except HTTPException:
            return {
                "mode": "custom",
                **names,
                "blocked": True,
                "config_status": "blocked",
                "validation_error": "Personal model endpoint is no longer operator-approved.",
            }
        return {
            field: user_cfg[field]
            for field in _MODELS_FIELDS
            if user_cfg.get(field)
        }

    # No user provider selected: platform owns the provider bundle; users may still choose model
    # names without changing the credential origin.
    platform_mode = (platform_cfg.get("mode") or "").strip()
    if platform_mode == "custom":
        out = {
            field: platform_cfg[field]
            for field in ("mode", "base_url", "api_key")
            if platform_cfg.get(field)
        }
    elif platform_mode == "subscription":
        out = {"mode": "subscription"}
    else:
        # An unset provider delegates to deployment defaults. Ignore any legacy custom origin/key
        # still present from older writers so hidden credentials cannot become active again.
        out = {}
    for field in ("model", "meeting_model"):
        value = user_cfg.get(field) or platform_cfg.get(field)
        if value:
            out[field] = value
    return out


def _resolve_transcription_backend(user_cfg: dict, platform_cfg: dict) -> dict:
    """Choose one complete STT backend tier without crossing its credential boundary."""
    if user_cfg.get("url"):
        try:
            _validate_user_transcription_url(user_cfg["url"])
        except HTTPException:
            return {
                "blocked": True,
                "config_status": "blocked",
                "validation_error": "Personal transcription endpoint is no longer operator-approved.",
            }
        else:
            return {
                field: user_cfg[field]
                for field in _TRANSCRIPTION_FIELDS
                if user_cfg.get(field)
            }
    return {
        field: platform_cfg[field]
        for field in _TRANSCRIPTION_FIELDS
        if platform_cfg.get(field)
    }


def create_app() -> FastAPI:
    _validate_auth_domain_separation()
    app = FastAPI(title="Vexa Admin API (v0.12)")

    # --- liveness probe (gate:health): process-up, no DB dependency. Readiness (DB reachable)
    # is a separate concern — keeping /health a pure liveness check makes it green without a
    # live Postgres, matching the long-running-service health contract {status:"ok", service}.
    @app.get("/health")
    async def health():
        return {"status": "ok", "service": "admin-api"}

    # --- admin tier: user + token CRUD ---
    @app.get("/admin/capabilities", dependencies=[Depends(verify_admin_token)])
    async def admin_capabilities():
        """Content-free version negotiation for admin clients.

        Old admin-api deployments do not have this route (404), which is the explicit v1 signal.
        A client may use the Agent scope only after this exact response advertises identity.v2.
        """
        return {
            "contracts": {
                "identity": {
                    "versions": [IDENTITY_V1, IDENTITY_V2],
                    "preferred": IDENTITY_V2,
                }
            }
        }

    @app.post("/admin/users", response_model=UserResponse,
              dependencies=[Depends(verify_admin_token)])
    async def create_user(user_in: UserCreate, response: Response,
                          db: AsyncSession = Depends(get_db)):
        existing = (await db.execute(select(User).where(User.email == user_in.email))).scalars().first()
        if existing:
            response.status_code = status.HTTP_200_OK
            return UserResponse.model_validate(existing)
        u = User(email=user_in.email, name=user_in.name,
                 max_concurrent_bots=user_in.max_concurrent_bots)
        db.add(u)
        await db.commit()
        await db.refresh(u)
        response.status_code = status.HTTP_201_CREATED
        return UserResponse.model_validate(u)

    # --- GET /admin/users/email/{email} → resolve an existing user by email (api.v1). The dashboard
    # login (send-magic-link → findUserByEmail) calls this to find an existing account before minting a
    # session token, so a returning user resolves to their own identity (and meetings) rather than a new
    # one. Mirrors create_user's lookup.
    @app.get("/admin/users/email/{email}", response_model=UserResponse,
             dependencies=[Depends(verify_admin_token)])
    async def get_user_by_email(email: str, db: AsyncSession = Depends(get_db)):
        user = (await db.execute(select(User).where(User.email == email))).scalars().first()
        if not user:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="User not found")
        return UserResponse.model_validate(user)

    @app.post("/admin/users/{user_id}/tokens", response_model=TokenResponse,
              status_code=status.HTTP_201_CREATED, dependencies=[Depends(verify_admin_token)])
    async def create_token_for_user(user_id: int, scope: str = "bot",
                                    scopes: Optional[str] = None,
                                    contract_version: str = IDENTITY_V1,
                                    name: Optional[str] = None,
                                    expires_in: Optional[int] = None,
                                    db: AsyncSession = Depends(get_db)):
        user = await db.get(User, user_id)
        if not user:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="User not found")
        scope_list = ([s.strip() for s in scopes.split(",") if s.strip()]
                      if scopes is not None else [scope])
        allowed_scopes = CONTRACT_SCOPES.get(contract_version)
        if allowed_scopes is None:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Unknown identity contract {contract_version!r}. "
                    f"Valid: {sorted(CONTRACT_SCOPES)}"
                ),
            )
        invalid = [s for s in scope_list if s not in allowed_scopes]
        if invalid:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=(f"Invalid scope(s) for {contract_version}: {invalid}. "
                                        f"Valid: {sorted(allowed_scopes)}"))
        token_value = generate_prefixed_token(scope_list[0])
        expires_at = None
        if expires_in is not None and expires_in > 0:
            expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
        tok = APIToken(token=token_value, user_id=user_id, scopes=scope_list,
                       name=name, created_at=datetime.utcnow(), expires_at=expires_at)
        db.add(tok)
        await db.commit()
        await db.refresh(tok)
        return TokenResponse.model_validate(tok)

    # --- GET /admin/users/{user_id}/tokens → the user's tokens, metadata only (no secret values).
    # Added for the terminal's token self-serve surface: it lists on the user's behalf (admin tier,
    # scoped server-side to the logged-in user) and verifies ownership before forwarding a revoke.
    @app.get("/admin/users/{user_id}/tokens", response_model=List[TokenInfo],
             dependencies=[Depends(verify_admin_token)])
    async def list_tokens_for_user(user_id: int, db: AsyncSession = Depends(get_db)):
        user = await db.get(User, user_id)
        if not user:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="User not found")
        rows = (await db.execute(
            select(APIToken).where(APIToken.user_id == user_id).order_by(APIToken.id)
        )).scalars().all()
        return [TokenInfo.model_validate(t) for t in rows]

    @app.delete("/admin/tokens/{token_id}", status_code=status.HTTP_204_NO_CONTENT,
                dependencies=[Depends(verify_admin_token)])
    async def delete_token(token_id: int, db: AsyncSession = Depends(get_db)):
        tok = await db.get(APIToken, token_id)
        if not tok:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Token not found")
        await db.delete(tok)
        await db.commit()
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # --- user tier: webhook self-serve (writes to user.data JSONB) ---
    @app.put("/user/webhook", response_model=UserResponse)
    async def set_user_webhook(webhook_update: WebhookUpdate,
                               user: User = Depends(get_current_user),
                               db: AsyncSession = Depends(get_db)):
        from sqlalchemy.orm import attributes
        data = dict(user.data or {})
        data["webhook_url"] = webhook_update.webhook_url
        if webhook_update.webhook_secret:
            data["webhook_secret"] = webhook_update.webhook_secret
        if webhook_update.webhook_events is not None:
            data["webhook_events"] = webhook_update.webhook_events
        user.data = data
        attributes.flag_modified(user, "data")
        db.add(user)
        await db.commit()
        await db.refresh(user)
        return UserResponse.model_validate(user)

    @app.get("/user/webhook")
    async def get_user_webhook(user: User = Depends(get_current_user)):
        """Read back the caller's webhook config. The secret NEVER leaves in the clear —
        it is masked to its last 4 chars (`********abcd`), enough to recognize which secret
        is set without disclosing it."""
        data = user.data if isinstance(user.data, dict) else {}
        secret = data.get("webhook_secret")
        masked = None
        if secret:
            masked = "********" + (secret[-4:] if len(secret) > 8 else "")
        return {
            "webhook_url": data.get("webhook_url"),
            "webhook_secret_set": bool(secret),
            "webhook_secret": masked,
            "webhook_events": data.get("webhook_events"),
        }

    # --- user tier: Minutes consent, per-carrier retention, and Agent-read opt-in ---
    @app.get("/user/minutes")
    async def get_user_minutes(response: Response, user: User = Depends(get_current_browser_user)):
        response.headers["Cache-Control"] = "no-store"
        return _minutes_view(user)

    @app.put("/user/minutes")
    async def set_user_minutes(minutes_update: MinutesUpdate, response: Response,
                               user: User = Depends(get_current_browser_user),
                               db: AsyncSession = Depends(get_db)):
        from sqlalchemy.orm import attributes

        fields = minutes_update.model_fields_set
        if ("capture_enabled" in fields and minutes_update.capture_enabled is True
                and not _minutes_operator_flag("ZAKI_MINUTES_CAPTURE_ENABLED")):
            raise HTTPException(status.HTTP_403_FORBIDDEN,
                                detail="Minutes capture is disabled by the operator")
        if ("agent_read_enabled" in fields and minutes_update.agent_read_enabled is True
                and not _minutes_operator_flag("ZAKI_MINUTES_READ_ENABLED")):
            raise HTTPException(status.HTTP_403_FORBIDDEN,
                                detail="Minutes Agent read is disabled by the operator")

        # Serialize preference updates for this user. Without a row lock, concurrent requests that
        # toggle capture and Agent reads can each copy stale JSONB and the later commit can silently
        # resurrect a withdrawn consent or discard a retention change. ``populate_existing`` is
        # required because the auth dependency already loaded this identity in the same session.
        locked_user = (await db.execute(
            select(User)
            .where(User.id == user.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )).scalar_one()
        data = dict(locked_user.data or {})
        current = data.get("minutes_prefs")
        prefs = dict(current) if isinstance(current, dict) else {}
        if "retention_days" in fields:
            if minutes_update.retention_days is None:
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail="retention_days cannot be null")
            prefs["retention_days"] = _validated_minutes_retention(
                minutes_update.retention_days
            )
            # Changing the lifetime of sensitive content is itself a consent decision. Refresh
            # an existing current-policy attestation; never use this to accept a newer policy.
            if (
                prefs.get("capture_enabled") is True
                and prefs.get("policy_version") == _MINUTES_POLICY_VERSION
            ):
                prefs["attested_at"] = datetime.now(timezone.utc).isoformat()
        if "capture_enabled" in fields:
            enabled = minutes_update.capture_enabled is True
            prefs["capture_enabled"] = enabled
            prefs["attested_at"] = (
                datetime.now(timezone.utc).isoformat() if enabled else None
            )
            prefs["policy_version"] = _MINUTES_POLICY_VERSION if enabled else None
        if "agent_read_enabled" in fields:
            prefs["agent_read_enabled"] = minutes_update.agent_read_enabled is True

        data["minutes_prefs"] = prefs
        locked_user.data = data
        attributes.flag_modified(locked_user, "data")
        db.add(locked_user)
        await db.commit()
        response.headers["Cache-Control"] = "no-store"
        return _minutes_view(locked_user)

    # --- user tier: calendar-sync self-serve (writes to user.data JSONB, like webhook) ---
    @app.put("/user/calendar")
    async def set_user_calendar(calendar_update: CalendarUpdate,
                                user: User = Depends(get_current_user),
                                db: AsyncSession = Depends(get_db)):
        """Set/clear the caller's secret ICS feed URL. ``ics_url: null`` disconnects the calendar.
        Automatic capture is unavailable in launch v1. The URL is a SECRET
        (Google/Outlook secret-address feeds) — it is stored, never echoed in the clear."""
        from urllib.parse import urlparse

        from sqlalchemy.orm import attributes
        if calendar_update.auto_join is True:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                detail="Calendar auto-join is unavailable in launch v1",
            )
        data = dict(user.data or {})
        if "ics_url" in calendar_update.model_fields_set:
            url = (calendar_update.ics_url or "").strip()
            if url:
                if len(url) > 2048:
                    raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                                        detail="ics_url too long")
                try:
                    parsed = urlparse(url)
                    hostname = parsed.hostname
                    parsed.port
                except ValueError:
                    raise HTTPException(
                        status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail="ics_url must be a valid http(s) URL",
                    ) from None
                if parsed.scheme not in ("http", "https") or not hostname:
                    raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                                        detail="ics_url must be an http(s) URL")
                if parsed.scheme == "http" and not _is_explicit_loopback_host(hostname):
                    raise HTTPException(
                        status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail="hosted ics_url feeds must use https",
                    )
                # Catch the #1 paste mistake up front: Google Calendar's EMBED page (HTML, not a
                # feed). The real feed is Settings -> Integrate calendar -> 'Secret address in
                # iCal format' (ends in .ics). Content-level checks happen at fetch time.
                if "/calendar/embed" in (parsed.path or "").lower():
                    raise HTTPException(
                        status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=("that's the calendar's embed page, not its feed - in Google "
                                "Calendar open Settings -> Integrate calendar and copy the "
                                "'Secret address in iCal format' (ends in .ics)"))
                data["calendar_ics_url"] = url
            else:
                data.pop("calendar_ics_url", None)
        if calendar_update.auto_join is not None:
            data["calendar_auto_join"] = bool(calendar_update.auto_join)
        user.data = data
        attributes.flag_modified(user, "data")
        db.add(user)
        await db.commit()
        return await get_user_calendar(user)  # the masked read-back shape

    @app.get("/user/calendar")
    async def get_user_calendar(user: User = Depends(get_current_user)):
        """Read back the caller's calendar config. The ICS URL is a secret — masked to its host
        + last 4 chars, enough to recognize WHICH feed is connected without disclosing it."""
        from urllib.parse import urlparse

        data = user.data if isinstance(user.data, dict) else {}
        url = data.get("calendar_ics_url")
        masked = None
        if url:
            host = urlparse(url).hostname or ""
            masked = f"{host}/…{url[-4:]}"
        return {
            "ics_url_set": bool(url),
            "ics_url_masked": masked,
            "auto_join_available": False,
            "auto_join": False,
        }

    # --- user tier: model + transcription self-serve prefs (users.data JSONB, like webhook) ---
    async def _put_user_prefs(update_fields: dict, data_key: str, user: User,
                              db: AsyncSession) -> dict:
        from sqlalchemy.orm import attributes
        cleaned = _validate_config_fields(update_fields, kind=data_key)
        data = dict(user.data or {})
        stored = data.get(data_key) or {}
        if (
            data_key == "transcription_prefs"
            and "url" in cleaned
            and cleaned["url"] != stored.get("url", "")
            and "token" not in cleaned
        ):
            cleaned["token"] = ""
        if (
            data_key == "model_prefs"
            and "base_url" in cleaned
            and cleaned["base_url"] != stored.get("base_url", "")
            and "api_key" not in cleaned
        ):
            cleaned["api_key"] = ""
        if (
            data_key == "model_prefs"
            and "mode" in cleaned
            and cleaned["mode"] != "custom"
        ):
            # Provider transitions are atomic. Deployment default/subscription cannot retain a
            # hidden custom origin or credential, even if supplied in the same partial update.
            cleaned["base_url"] = ""
            cleaned["api_key"] = ""
        data[data_key] = _apply_config_update(stored, cleaned)
        if not data[data_key]:
            data.pop(data_key, None)  # fully cleared → back to platform/env defaults
        user.data = data
        attributes.flag_modified(user, "data")
        db.add(user)
        await db.commit()
        return data.get(data_key) or {}

    @app.put("/user/models")
    async def set_user_models(update: ModelPrefsUpdate,
                              user: User = Depends(get_current_user),
                              db: AsyncSession = Depends(get_db)):
        """Set the caller's model config (partial; empty string clears a field). ``api_key``
        is a SECRET — stored, never echoed in the clear."""
        await _put_user_prefs(update.model_dump(exclude_unset=True), "model_prefs", user, db)
        return await get_user_models(user)

    @app.get("/user/models")
    async def get_user_models(user: User = Depends(get_current_user)):
        data = user.data if isinstance(user.data, dict) else {}
        prefs = data.get("model_prefs") or {}
        config_status = "valid"
        validation_error = None
        if prefs.get("mode") == "custom":
            if not prefs.get("base_url"):
                config_status = "incomplete"
                validation_error = "Personal model endpoint is incomplete; set an approved Base URL."
            else:
                try:
                    _validate_user_model_url(prefs["base_url"])
                except HTTPException:
                    config_status = "blocked"
                    validation_error = "Personal model endpoint is no longer operator-approved."
        return {
            "mode": prefs.get("mode"),
            "model": prefs.get("model"),
            "meeting_model": prefs.get("meeting_model"),
            "base_url": prefs.get("base_url"),
            "api_key_set": bool(prefs.get("api_key")),
            "api_key": _mask_secret(prefs.get("api_key")),
            "config_status": config_status,
            "validation_error": validation_error,
        }

    @app.put("/user/transcription")
    async def set_user_transcription(update: TranscriptionPrefsUpdate,
                                     user: User = Depends(get_current_user),
                                     db: AsyncSession = Depends(get_db)):
        """Set an operator-allowlisted HTTPS STT override. ``token`` is masked on read."""
        await _put_user_prefs(update.model_dump(exclude_unset=True), "transcription_prefs", user, db)
        return await get_user_transcription(user)

    @app.get("/user/transcription")
    async def get_user_transcription(user: User = Depends(get_current_user)):
        data = user.data if isinstance(user.data, dict) else {}
        prefs = data.get("transcription_prefs") or {}
        config_status = "valid"
        validation_error = None
        if prefs.get("url"):
            try:
                _validate_user_transcription_url(prefs["url"])
            except HTTPException:
                config_status = "blocked"
                validation_error = "Personal transcription endpoint is no longer operator-approved."
        return {
            "url": prefs.get("url"),
            "token_set": bool(prefs.get("token")),
            "token": _mask_secret(prefs.get("token")),
            "config_status": config_status,
            "validation_error": validation_error,
        }

    # --- internal tier: the gateway's authz oracle (FAIL-CLOSED) ---
    @app.post("/internal/validate", include_in_schema=False)
    async def validate_token(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
        secret = _internal_secret()
        # Fail closed: no secret configured → reject unless dev mode.
        if not _dev_mode() and not secret:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE,
                                detail="INTERNAL_API_SECRET not configured")
        if secret:
            provided = request.headers.get("X-Internal-Secret", "")
            if not hmac.compare_digest(provided, secret):
                raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Invalid internal secret")

        token = payload.get("token", "")
        if not token:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Missing token")

        row = (await db.execute(
            select(APIToken, User).join(User, APIToken.user_id == User.id)
            .where(APIToken.token == token)
        )).first()
        if not row:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
        api_token, user = row

        if api_token.expires_at is not None and api_token.expires_at < datetime.utcnow():
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Token expired")

        api_token.last_used_at = datetime.utcnow()
        await db.commit()

        scopes = list(api_token.scopes) if api_token.scopes else ["legacy"]
        resp = {
            "user_id": user.id,
            "scopes": scopes,
            "max_concurrent": user.max_concurrent_bots,
            "email": user.email,
            # DB-backed admin role (bootstrap-claimed on a fresh instance) — the terminal's
            # admin gate reads THIS, with its VEXA_ADMIN_EMAILS allowlist kept as an override.
            "is_admin": (user.data or {}).get("is_admin") is True if isinstance(user.data, dict) else False,
        }
        data_blob = user.data if isinstance(user.data, dict) else {}
        if data_blob.get("webhook_url"):
            resp["webhook_url"] = data_blob["webhook_url"]
            if data_blob.get("webhook_secret"):
                resp["webhook_secret"] = data_blob["webhook_secret"]
            if data_blob.get("webhook_events"):
                resp["webhook_events"] = data_blob["webhook_events"]
        # Lane A: the caller's shared-workspace membership ids (from the derived users.data.memberships[]),
        # so the gateway can inject x-user-workspaces → meeting-api authorizes a member's transcript subscribe.
        memberships = data_blob.get("memberships")
        if isinstance(memberships, list):
            resp["workspaces"] = [m["workspace_id"] for m in memberships
                                  if isinstance(m, dict) and m.get("workspace_id")]
        return resp

    # --- internal tier: workspace membership index (Lane M) — the DERIVED users.data.memberships[]
    #     mirror of the authoritative policy/members.json in each shared workspace's git repo. agent-api
    #     (no DB) POSTs mirror updates here over the same X-Internal-Secret internal edge as /internal/
    #     validate. The git file is the source of truth (Q6): this index is a rebuildable listing cache.
    def _check_internal(request: Request) -> None:
        secret = _internal_secret()
        if not _dev_mode() and not secret:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE,
                                detail="INTERNAL_API_SECRET not configured")
        if secret:
            provided = request.headers.get("X-Internal-Secret", "")
            if not hmac.compare_digest(provided, secret):
                raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Invalid internal secret")

    async def _load_user(user_id: str, db: AsyncSession) -> User:
        try:
            uid = int(user_id)
        except (TypeError, ValueError):
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Unknown user")
        user = (await db.execute(select(User).where(User.id == uid))).scalar_one_or_none()
        if user is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Unknown user")
        return user

    @app.get("/internal/users/{user_id}/minutes", include_in_schema=False)
    async def internal_user_minutes(user_id: str, request: Request, response: Response,
                                    db: AsyncSession = Depends(get_db)):
        """Identity's fail-closed authority view consumed by Minutes and Agent services."""
        _check_internal(request)
        user = await _load_user(user_id, db)
        response.headers["Cache-Control"] = "no-store"
        return _minutes_view(user)

    # --- internal tier: instance identity — admin existence + the first-sign-in admin claim.
    #     A fresh install has NO admin; the login surface (via the terminal, which fronts this
    #     edge) shows a one-time "set up your instance" claim screen, and the first successful
    #     sign-in becomes the admin. The claim is race-safe: a pg advisory xact lock serializes
    #     concurrent first sign-ins so exactly ONE claims the role. ---
    _BOOTSTRAP_ADMIN_LOCK = 0x5EC4_AD31  # arbitrary app-wide advisory-lock key for the claim

    async def _admin_exists(db: AsyncSession) -> bool:
        row = (await db.execute(
            select(User.id).where(User.data["is_admin"].astext == "true").limit(1)
        )).first()
        return row is not None

    @app.get("/internal/instance", include_in_schema=False)
    async def instance_status(request: Request, db: AsyncSession = Depends(get_db)):
        _check_internal(request)
        return {"admin_exists": await _admin_exists(db)}

    @app.post("/internal/bootstrap-admin", include_in_schema=False)
    async def bootstrap_admin(payload: dict, request: Request,
                              db: AsyncSession = Depends(get_db)):
        """Claim the admin role for `user_id` IF no admin exists yet. Idempotent and race-safe:
        under the advisory lock the first caller claims, every later caller gets claimed=False.
        A user who already IS the admin re-claims harmlessly (claimed=False, admin_exists=True)."""
        from sqlalchemy import text as sa_text
        from sqlalchemy.orm import attributes

        _check_internal(request)
        user = await _load_user(str(payload.get("user_id", "")), db)
        await db.execute(sa_text("SELECT pg_advisory_xact_lock(:key)"),
                         {"key": _BOOTSTRAP_ADMIN_LOCK})
        if await _admin_exists(db):
            return {"claimed": False, "admin_exists": True}
        data = dict(user.data or {})
        data["is_admin"] = True
        user.data = data
        attributes.flag_modified(user, "data")
        db.add(user)
        await db.commit()
        return {"claimed": True, "admin_exists": True}

    @app.get("/internal/users/{user_id}/memberships", include_in_schema=False)
    async def list_memberships(user_id: str, request: Request, db: AsyncSession = Depends(get_db)):
        _check_internal(request)
        user = await _load_user(user_id, db)
        data = user.data if isinstance(user.data, dict) else {}
        return {"memberships": data.get("memberships", [])}

    @app.post("/internal/users/{user_id}/memberships", include_in_schema=False)
    async def upsert_membership(user_id: str, payload: dict, request: Request,
                                db: AsyncSession = Depends(get_db)):
        """Upsert {workspace_id, role, added_at} into the user's memberships[] (idempotent per ws)."""
        _check_internal(request)
        from sqlalchemy.orm import attributes
        user = await _load_user(user_id, db)
        ws_id = payload.get("workspace_id")
        if not ws_id:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="workspace_id required")
        entry = {"workspace_id": ws_id, "role": payload.get("role", "viewer"),
                 "added_at": payload.get("added_at")}
        data = dict(user.data or {})
        memberships = [m for m in (data.get("memberships") or []) if m.get("workspace_id") != ws_id]
        memberships.append(entry)
        data["memberships"] = memberships
        user.data = data
        attributes.flag_modified(user, "data")
        db.add(user)
        await db.commit()
        return {"memberships": memberships}

    @app.delete("/internal/users/{user_id}/memberships/{workspace_id}", include_in_schema=False)
    async def remove_membership(user_id: str, workspace_id: str, request: Request,
                                db: AsyncSession = Depends(get_db)):
        _check_internal(request)
        from sqlalchemy.orm import attributes
        user = await _load_user(user_id, db)
        data = dict(user.data or {})
        memberships = [m for m in (data.get("memberships") or []) if m.get("workspace_id") != workspace_id]
        data["memberships"] = memberships
        user.data = data
        attributes.flag_modified(user, "data")
        db.add(user)
        await db.commit()
        return {"memberships": memberships}

    # --- internal tier: calendar-sync configs — meeting-api's ICS poller discovers every user
    #     with a connected feed over the same X-Internal-Secret edge as /internal/validate. The
    #     secret URL crosses ONLY this internal hop (never a user-facing response). ---
    @app.get("/internal/calendar-configs", include_in_schema=False)
    async def list_calendar_configs(request: Request, db: AsyncSession = Depends(get_db)):
        _check_internal(request)
        rows = (await db.execute(
            select(User).where(User.data["calendar_ics_url"].astext.isnot(None))
        )).scalars().all()
        configs = []
        for u in rows:
            data = u.data if isinstance(u.data, dict) else {}
            url = data.get("calendar_ics_url")
            if url:
                configs.append({
                    "user_id": u.id,
                    "ics_url": url,
                    "auto_join": False,
                })
        return {"configs": configs}

    # --- internal tier: per-user spawn context — the managed launch path's stand-in for the headers
    #     the gateway injects on POST /bots (X-User-Limits + webhook config from /internal/validate).
    #     Same shape /internal/validate returns for those fields, keyed by user id. ---
    async def _platform_setting(key: str, db: AsyncSession) -> dict:
        row = await db.get(PlatformSetting, key)
        return dict(row.value) if row is not None and isinstance(row.value, dict) else {}

    @app.get("/internal/users/{user_id}/bot-context", include_in_schema=False)
    async def get_bot_context(user_id: str, request: Request, db: AsyncSession = Depends(get_db)):
        _check_internal(request)
        user = await _load_user(user_id, db)
        data = user.data if isinstance(user.data, dict) else {}
        resp: dict = {"max_concurrent": user.max_concurrent_bots}
        if data.get("webhook_url"):
            resp["webhook_url"] = data["webhook_url"]
            if data.get("webhook_secret"):
                resp["webhook_secret"] = data["webhook_secret"]
            if data.get("webhook_events"):
                resp["webhook_events"] = data["webhook_events"]
        # The effective transcription backend (one complete user tier or one complete platform
        # tier) — bot_spawn overrides its env-derived backend with this when present. The token
        # crosses ONLY this internal hop and never crosses backend ownership tiers.
        transcription = _resolve_transcription_backend(
            data.get("transcription_prefs") or {},
            await _platform_setting("transcription", db),
        )
        # Provenance is metadata on the internal envelope, never part of the bot's credential
        # bundle. Agent's user-facing Test button may spend only a personal endpoint credential;
        # inherited platform/deployment tiers are reported generically without a live probe.
        resp["transcription_credential_owner"] = (
            "user" if (data.get("transcription_prefs") or {}).get("url") else "operator"
        )
        if transcription:
            resp["transcription"] = transcription
        return resp

    # --- internal tier: platform-wide settings (the DB layer under per-user prefs) — written by
    #     the terminal's ADMIN-GATED settings editor over this edge, read by agent-api/meeting-api.
    @app.get("/internal/settings/{key}", include_in_schema=False)
    async def get_platform_setting(key: str, request: Request, db: AsyncSession = Depends(get_db)):
        _check_internal(request)
        if key not in SETTING_KEYS:
            raise HTTPException(status.HTTP_404_NOT_FOUND,
                                detail=f"Unknown setting key. Known: {sorted(SETTING_KEYS)}")
        return {"key": key, "value": await _platform_setting(key, db)}

    @app.put("/internal/settings/{key}", include_in_schema=False)
    async def put_platform_setting(key: str, payload: dict, request: Request,
                                   db: AsyncSession = Depends(get_db)):
        """Partial update, same field rules + clear semantics as the user-tier writers."""
        _check_internal(request)
        fields = SETTING_KEYS.get(key)
        if fields is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND,
                                detail=f"Unknown setting key. Known: {sorted(SETTING_KEYS)}")
        update = {f: payload.get(f) for f in fields if f in payload}
        cleaned = _validate_config_fields(update, kind=key)
        row = await db.get(PlatformSetting, key)
        stored = dict(row.value) if row is not None else {}
        # A token is authority for one STT origin. Rotating the URL without supplying the new
        # origin's token must not silently carry the old credential across that boundary. An
        # unchanged URL remains a normal partial update; an explicit token="" remains a clear.
        if (
            key == "transcription"
            and "url" in cleaned
            and cleaned["url"] != stored.get("url", "")
            and "token" not in cleaned
        ):
            cleaned["token"] = ""
        if (
            key == "models"
            and "base_url" in cleaned
            and cleaned["base_url"] != stored.get("base_url", "")
            and "api_key" not in cleaned
        ):
            cleaned["api_key"] = ""
        if key == "models" and "mode" in cleaned and cleaned["mode"] != "custom":
            cleaned["base_url"] = ""
            cleaned["api_key"] = ""
        merged = _apply_config_update(stored, cleaned)
        if row is None:
            row = PlatformSetting(key=key, value=merged)
        else:
            row.value = merged
        db.add(row)
        await db.commit()
        return {"key": key, "value": merged}

    # --- internal tier: the dispatch-time model config — agent-api resolves the subject's
    #     effective model setup (user pref > platform setting) in ONE call. Secrets (api_key)
    #     cross ONLY this internal hop, straight into the worker's brokered env.
    @app.get("/internal/users/{user_id}/model-config", include_in_schema=False)
    async def get_model_config(user_id: str, request: Request, db: AsyncSession = Depends(get_db)):
        _check_internal(request)
        user = await _load_user(user_id, db)
        data = user.data if isinstance(user.data, dict) else {}
        prefs = data.get("model_prefs") or {}
        return {"models": _resolve_model_backend(
            prefs,
            await _platform_setting("models", db),
        ), "credential_owner": "user" if prefs.get("mode") == "custom" else "operator"}

    @app.get("/")
    async def root():
        return {"message": "Vexa Admin API (v0.12)"}

    return app
