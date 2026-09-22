"""Default-off production composition for the Agent-side Minutes read capability."""
from __future__ import annotations

from typing import Callable, Optional

from control_plane.minutes_ingest import MinutesAnswerAuthorizer, MinutesIngestor
from control_plane.minutes_ownership import RedisMinutesOwnershipRegistry
from llm.registry import completion_from_env
from shared.config import Settings
from shared.minutes_read import MinutesReadClient, validate_read_token
from shared.minutes_settings import IdentityMinutesSettingsClient


def _ownership_from_redis_url(redis_url: str) -> RedisMinutesOwnershipRegistry:
    import redis

    return RedisMinutesOwnershipRegistry(
        redis.from_url(redis_url, decode_responses=True),
    )


def build_minutes_ingestor(
    settings: Settings,
    *,
    authorizer: Optional[MinutesAnswerAuthorizer],
    completion_factory: Callable = completion_from_env,
    read_factory: Callable = MinutesReadClient,
    identity_factory: Callable = IdentityMinutesSettingsClient,
    ownership_factory: Callable = _ownership_from_redis_url,
) -> Optional[MinutesIngestor]:
    """Compose the capability only when the fleet gate is literally enabled.

    There is intentionally no fallback authorizer. Until canonical Nullalis exposes its
    tombstone-aware content-free answer gate in this process, enabling the feature is a boot error.
    Model-derived Minutes text remains ephemeral and is never passed to this boundary.
    """
    if settings.minutes_read_enabled is not True:
        return None
    if authorizer is None or not callable(
        getattr(authorizer, "authorize_answer_if_not_erased", None)
    ):
        raise RuntimeError(
            "ZAKI_MINUTES_READ_ENABLED requires a tombstone-aware Agent Brain answer authorizer"
        )

    base_url = settings.minutes_read_base_url.strip()
    token = settings.minutes_read_token.get_secret_value()
    identity_url = settings.admin_api_url.strip()
    internal_secret = settings.internal_api_secret.get_secret_value()
    missing: list[str] = []
    if not base_url:
        missing.append("ZAKI_MINUTES_READ_BASE_URL")
    if not token:
        missing.append("ZAKI_READ_TOKEN_MINUTES")
    if not identity_url:
        missing.append("VEXA_ADMIN_API_URL")
    if not internal_secret:
        missing.append("VEXA_INTERNAL_API_SECRET")
    if missing:
        raise RuntimeError(
            "ZAKI_MINUTES_READ_ENABLED requires configured " + ", ".join(missing)
        )
    try:
        token = validate_read_token(token)
    except ValueError:
        raise RuntimeError(
            "ZAKI_READ_TOKEN_MINUTES must be unpadded printable ASCII between "
            "32 and 512 characters"
        ) from None
    signing_secret = settings.agent_erasure_signing_secret.get_secret_value()
    if token in {internal_secret, signing_secret}:
        raise RuntimeError(
            "ZAKI_READ_TOKEN_MINUTES must be distinct from internal and erasure secrets"
        )

    completion = completion_factory()
    if getattr(completion, "supports_max_tokens", False) is not True:
        raise RuntimeError(
            "ZAKI_MINUTES_READ_ENABLED requires a completion adapter with a fixed token ceiling"
        )
    reads = read_factory(base_url, token)
    identity = identity_factory(identity_url, internal_secret)
    ownership = ownership_factory(settings.redis_url)
    return MinutesIngestor(
        operator_enabled=True,
        settings=identity,
        reads=reads,
        ownership=ownership,
        completion=completion,
        writer=authorizer,
        model=settings.llm_model or settings.agent_model or None,
    )
