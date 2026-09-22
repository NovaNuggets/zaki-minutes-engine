"""Agent-side implementation of Minutes' cross-spoke Redis retention fence contract."""
from __future__ import annotations


FENCE_PREFIX = "zaki:retention:meeting"
PROCESSED_SCOPE = "processed"
_DEADLINE_TOKEN_PREFIX = "v1:"


def bind_processing_deadline(generation: str, expires_at_ms: int) -> str:
    """Bind an opaque ON generation to one immutable processed-content cutoff."""
    if not isinstance(generation, str) or not generation or ":" in generation:
        raise ValueError("meeting processing generation token is invalid")
    if (
        isinstance(expires_at_ms, bool)
        or not isinstance(expires_at_ms, int)
        or expires_at_ms <= 0
    ):
        raise ValueError("meeting processing deadline is invalid")
    return f"{_DEADLINE_TOKEN_PREFIX}{expires_at_ms}:{generation}"


def processing_deadline_from_token(token: str) -> int | None:
    """Return a managed generation's bound cutoff; legacy/ordinary opaque tokens have none."""
    if not isinstance(token, str) or not token:
        raise ValueError("meeting processing generation token is empty")
    if not token.startswith(_DEADLINE_TOKEN_PREFIX):
        return None
    parts = token.split(":", 2)
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2]:
        raise ValueError("meeting processing generation token is invalid")
    deadline = int(parts[1])
    if deadline <= 0:
        raise ValueError("meeting processing generation token is invalid")
    return deadline


def _deadline_arg(expires_at_ms: int | None, *, token: str | None = None) -> str:
    if token is not None:
        bound = processing_deadline_from_token(token)
        if expires_at_ms is None:
            expires_at_ms = bound
        elif bound != expires_at_ms:
            raise ValueError("meeting processing deadline does not match its generation")
    if expires_at_ms is None:
        return ""
    if (
        isinstance(expires_at_ms, bool)
        or not isinstance(expires_at_ms, int)
        or expires_at_ms <= 0
    ):
        raise ValueError("meeting processing deadline is invalid")
    return str(expires_at_ms)

_XADD_IF_WRITABLE = """
if redis.call('HGET', KEYS[1], ARGV[1]) == '1' then
  return 0
end
local cutoff_ms = tonumber(ARGV[2])
if cutoff_ms ~= nil then
  local redis_time = redis.call('TIME')
  local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
  if now_ms >= cutoff_ms then return 0 end
end
redis.call('XADD', KEYS[2], '*', unpack(ARGV, 3))
return 1
"""

_SET_IF_WRITABLE = """
if redis.call('HGET', KEYS[1], ARGV[1]) == '1' then
  return 0
end
local cutoff_ms = tonumber(ARGV[2])
if cutoff_ms ~= nil then
  local redis_time = redis.call('TIME')
  local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
  if now_ms >= cutoff_ms then return 0 end
end
redis.call('SET', KEYS[2], ARGV[3])
return 1
"""

_CHECK_WRITABLE = """
if redis.call('HGET', KEYS[1], ARGV[1]) == '1' then
  return 0
end
local cutoff_ms = tonumber(ARGV[2])
if cutoff_ms ~= nil then
  local redis_time = redis.call('TIME')
  local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
  if now_ms >= cutoff_ms then return 0 end
end
return 1
"""

_ACTIVATE_PROCESSING_IF_WRITABLE = """
if redis.call('HGET', KEYS[1], ARGV[1]) == '1' then
  return {0, ''}
end
local cutoff_ms = tonumber(ARGV[4])
if cutoff_ms ~= nil then
  local redis_time = redis.call('TIME')
  local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
  if now_ms >= cutoff_ms then return {0, ''} end
end
local generation = redis.call('GET', KEYS[2])
local expected_prefix = nil
if cutoff_ms ~= nil then
  expected_prefix = 'v1:' .. ARGV[4] .. ':'
end
if not generation or (expected_prefix ~= nil and string.sub(generation, 1, string.len(expected_prefix)) ~= expected_prefix) then
  redis.call('SET', KEYS[2], ARGV[3], 'EX', ARGV[2])
else
  redis.call('EXPIRE', KEYS[2], ARGV[2])
end
local cursor = redis.call('GET', KEYS[3])
return {1, cursor or ''}
"""

_CLAIM_PROCESSING_IF_WRITABLE = """
if redis.call('HGET', KEYS[1], ARGV[1]) == '1' then
  return {0, ''}
end
if redis.call('GET', KEYS[2]) ~= ARGV[3] then
  return {0, ''}
end
local cutoff_ms = tonumber(ARGV[4])
if cutoff_ms ~= nil then
  local redis_time = redis.call('TIME')
  local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
  if now_ms >= cutoff_ms then return {0, ''} end
end
redis.call('EXPIRE', KEYS[2], ARGV[2])
local cursor = redis.call('GET', KEYS[3])
return {1, cursor or ''}
"""

_CHECK_WRITABLE_AND_CURRENT = """
if redis.call('HGET', KEYS[1], ARGV[1]) == '1' then
  return 0
end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then
  return 0
end
local cutoff_ms = tonumber(ARGV[3])
if cutoff_ms ~= nil then
  local redis_time = redis.call('TIME')
  local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
  if now_ms >= cutoff_ms then return 0 end
end
return 1
"""

_XADD_IF_WRITABLE_AND_CURRENT = """
if redis.call('HGET', KEYS[1], ARGV[1]) == '1' then
  return 0
end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then
  return 0
end
local cutoff_ms = tonumber(ARGV[3])
if cutoff_ms ~= nil then
  local redis_time = redis.call('TIME')
  local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
  if now_ms >= cutoff_ms then return 0 end
end
redis.call('XADD', KEYS[3], '*', unpack(ARGV, 4))
return 1
"""

_SET_IF_WRITABLE_AND_CURRENT = """
if redis.call('HGET', KEYS[1], ARGV[1]) == '1' then
  return 0
end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then
  return 0
end
local cutoff_ms = tonumber(ARGV[3])
if cutoff_ms ~= nil then
  local redis_time = redis.call('TIME')
  local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
  if now_ms >= cutoff_ms then return 0 end
end
redis.call('SET', KEYS[3], ARGV[4])
return 1
"""


def retention_fence_key(meeting_id: str | int) -> str:
    value = str(meeting_id).strip()
    if not value:
        raise ValueError("meeting retention fence identity is empty")
    return f"{FENCE_PREFIX}:{value}:fence"


def meeting_id_from_proc_stream(proc_stream: str) -> str:
    prefix = "proc:meeting:"
    if not isinstance(proc_stream, str) or not proc_stream.startswith(prefix):
        raise ValueError("processed stream identity is invalid")
    return proc_stream[len(prefix) :]


def carrier_is_writable(
    stream,
    fence_key: str,
    *,
    scope: str = PROCESSED_SCOPE,
    expires_at_ms: int | None = None,
) -> bool:
    deadline = _deadline_arg(expires_at_ms)
    custom = getattr(stream, "carrier_is_writable", None)
    if custom is not None:
        kwargs = {"fence_key": fence_key, "scope": scope}
        if expires_at_ms is not None:
            kwargs["expires_at_ms"] = expires_at_ms
        return bool(custom(**kwargs))
    evaluator = getattr(stream, "eval", None)
    if evaluator is None:
        raise RuntimeError("atomic meeting retention check is unavailable")
    return bool(int(evaluator(_CHECK_WRITABLE, 1, fence_key, scope, deadline)))


def xadd_if_writable(
    stream,
    name: str,
    fields: dict,
    *,
    fence_key: str,
    scope: str = PROCESSED_SCOPE,
    expires_at_ms: int | None = None,
) -> bool:
    deadline = _deadline_arg(expires_at_ms)
    custom = getattr(stream, "xadd_if_writable", None)
    if custom is not None:
        kwargs = {"fence_key": fence_key, "scope": scope}
        if expires_at_ms is not None:
            kwargs["expires_at_ms"] = expires_at_ms
        return bool(custom(name, fields, **kwargs))
    evaluator = getattr(stream, "eval", None)
    if evaluator is None:
        raise RuntimeError("atomic meeting retention append is unavailable")
    args: list[str] = [scope, deadline]
    for key, value in fields.items():
        args.extend((str(key), str(value)))
    return bool(int(evaluator(_XADD_IF_WRITABLE, 2, fence_key, name, *args)))


def set_if_writable(
    stream,
    key: str,
    value: str,
    *,
    fence_key: str,
    scope: str = PROCESSED_SCOPE,
    expires_at_ms: int | None = None,
) -> bool:
    deadline = _deadline_arg(expires_at_ms)
    custom = getattr(stream, "set_if_writable", None)
    if custom is not None:
        kwargs = {"fence_key": fence_key, "scope": scope}
        if expires_at_ms is not None:
            kwargs["expires_at_ms"] = expires_at_ms
        return bool(custom(key, value, **kwargs))
    evaluator = getattr(stream, "eval", None)
    if evaluator is None:
        raise RuntimeError("atomic meeting retention cursor write is unavailable")
    return bool(
        int(evaluator(_SET_IF_WRITABLE, 2, fence_key, key, scope, deadline, str(value)))
    )


def activate_processing_if_writable(
    redis_client,
    meeting_id: str | int,
    *,
    flag_key: str,
    cursor_key: str,
    ttl_seconds: int,
    token: str = "1",
    expires_at_ms: int | None = None,
) -> tuple[bool, str | None]:
    """Atomically fence-check an opt-in activation and return its frozen resume cursor.

    ``token`` is an opaque activation generation.  A worker must present the same generation on
    every read/write guard, so an old worker cannot resume when a user turns processing back on.
    """

    deadline = _deadline_arg(expires_at_ms, token=token)

    custom = getattr(redis_client, "activate_processing_if_writable", None)
    if custom is not None:
        kwargs = {
            "fence_key": retention_fence_key(meeting_id),
            "flag_key": flag_key,
            "cursor_key": cursor_key,
            "ttl_seconds": ttl_seconds,
            "scope": PROCESSED_SCOPE,
            "token": token,
        }
        if expires_at_ms is not None:
            kwargs["expires_at_ms"] = expires_at_ms
        return custom(**kwargs)
    evaluator = getattr(redis_client, "eval", None)
    if evaluator is None:
        raise RuntimeError("atomic meeting processing activation is unavailable")
    result = evaluator(
        _ACTIVATE_PROCESSING_IF_WRITABLE,
        3,
        retention_fence_key(meeting_id),
        flag_key,
        cursor_key,
        PROCESSED_SCOPE,
        str(ttl_seconds),
        token,
        deadline,
    )
    if not isinstance(result, (list, tuple)) or len(result) != 2:
        raise RuntimeError("meeting processing activation returned an invalid result")
    allowed = bool(int(result[0]))
    cursor = result[1]
    if isinstance(cursor, bytes):
        cursor = cursor.decode()
    return allowed, str(cursor) if cursor else None


def processing_is_current(
    stream,
    *,
    fence_key: str,
    flag_key: str,
    token: str,
    scope: str = PROCESSED_SCOPE,
    expires_at_ms: int | None = None,
) -> bool:
    """Atomically prove that retention permits processing and this worker owns the live generation."""

    deadline = _deadline_arg(expires_at_ms, token=token)
    custom = getattr(stream, "processing_is_current", None)
    if custom is not None:
        kwargs = {"flag_key": flag_key, "token": token}
        if expires_at_ms is not None:
            kwargs["expires_at_ms"] = expires_at_ms
        return bool(custom(**kwargs))
    evaluator = getattr(stream, "eval", None)
    if evaluator is None:
        raise RuntimeError("atomic meeting processing consent check is unavailable")
    return bool(
        int(
            evaluator(
                _CHECK_WRITABLE_AND_CURRENT,
                2,
                fence_key,
                flag_key,
                scope,
                token,
                deadline,
            )
        )
    )


def xadd_if_writable_and_current(
    stream,
    name: str,
    fields: dict,
    *,
    fence_key: str,
    flag_key: str,
    token: str,
    scope: str = PROCESSED_SCOPE,
    expires_at_ms: int | None = None,
) -> bool:
    """Append only while both the permanent retention fence and consent generation allow it."""

    deadline = _deadline_arg(expires_at_ms, token=token)
    custom = getattr(stream, "xadd_if_writable_and_current", None)
    if custom is not None:
        return bool(
            custom(name, fields, **{
                "fence_key": fence_key,
                "scope": scope,
                "flag_key": flag_key,
                "token": token,
                **({"expires_at_ms": expires_at_ms} if expires_at_ms is not None else {}),
            })
        )
    evaluator = getattr(stream, "eval", None)
    if evaluator is None:
        raise RuntimeError("atomic meeting processing append is unavailable")
    args: list[str] = [scope, token, deadline]
    for key, value in fields.items():
        args.extend((str(key), str(value)))
    return bool(
        int(
            evaluator(
                _XADD_IF_WRITABLE_AND_CURRENT,
                3,
                fence_key,
                flag_key,
                name,
                *args,
            )
        )
    )


def set_if_writable_and_current(
    stream,
    key: str,
    value: str,
    *,
    fence_key: str,
    flag_key: str,
    token: str,
    scope: str = PROCESSED_SCOPE,
    expires_at_ms: int | None = None,
) -> bool:
    """Set a mutable cursor only for the still-current processing generation."""

    deadline = _deadline_arg(expires_at_ms, token=token)
    custom = getattr(stream, "set_if_writable_and_current", None)
    if custom is not None:
        return bool(
            custom(key, value, **{
                "fence_key": fence_key,
                "scope": scope,
                "flag_key": flag_key,
                "token": token,
                **({"expires_at_ms": expires_at_ms} if expires_at_ms is not None else {}),
            })
        )
    evaluator = getattr(stream, "eval", None)
    if evaluator is None:
        raise RuntimeError("atomic meeting processing cursor write is unavailable")
    return bool(
        int(
            evaluator(
                _SET_IF_WRITABLE_AND_CURRENT,
                3,
                fence_key,
                flag_key,
                key,
                scope,
                token,
                deadline,
                str(value),
            )
        )
    )


def claim_processing_if_writable(
    redis_client,
    meeting_id: str | int,
    *,
    flag_key: str,
    cursor_key: str,
    ttl_seconds: int,
    token: str,
    expires_at_ms: int | None = None,
) -> tuple[bool, str | None]:
    """Atomically refuse fenced/stale desired state and refresh a live processing claim."""

    deadline = _deadline_arg(expires_at_ms, token=token)
    custom = getattr(redis_client, "claim_processing_if_writable", None)
    if custom is not None:
        kwargs = {
            "fence_key": retention_fence_key(meeting_id),
            "flag_key": flag_key,
            "cursor_key": cursor_key,
            "ttl_seconds": ttl_seconds,
            "scope": PROCESSED_SCOPE,
            "token": token,
        }
        if expires_at_ms is not None:
            kwargs["expires_at_ms"] = expires_at_ms
        return custom(**kwargs)
    evaluator = getattr(redis_client, "eval", None)
    if evaluator is None:
        raise RuntimeError("atomic meeting processing claim is unavailable")
    result = evaluator(
        _CLAIM_PROCESSING_IF_WRITABLE,
        3,
        retention_fence_key(meeting_id),
        flag_key,
        cursor_key,
        PROCESSED_SCOPE,
        str(ttl_seconds),
        token,
        deadline,
    )
    if not isinstance(result, (list, tuple)) or len(result) != 2:
        raise RuntimeError("meeting processing claim returned an invalid result")
    allowed = bool(int(result[0]))
    cursor = result[1]
    if isinstance(cursor, bytes):
        cursor = cursor.decode()
    return allowed, str(cursor) if cursor else None
