"""Durable Agent ownership binding and account-erasure gates for Minutes ingestion."""
from __future__ import annotations

import re
import secrets


_ROW_ID = re.compile(r"^[1-9][0-9]{0,18}$")
_MAX_DB_ID = 2**63 - 1
_WATCH_RETRIES = 8


class MinutesOwnershipDenied(PermissionError):
    """A content-free refusal to read or derive Minutes data for this owner."""


def _canonical_user(user_id: object) -> int:
    if type(user_id) is not int or not 0 < user_id <= _MAX_DB_ID:
        raise MinutesOwnershipDenied("Minutes ownership is unavailable")
    return user_id


def _canonical_meeting(meeting_id: object) -> str:
    if not isinstance(meeting_id, str) or not meeting_id.startswith("meeting:"):
        raise MinutesOwnershipDenied("Minutes ownership is unavailable")
    row_id = meeting_id.removeprefix("meeting:")
    if not _ROW_ID.fullmatch(row_id) or int(row_id) > _MAX_DB_ID:
        raise MinutesOwnershipDenied("Minutes ownership is unavailable")
    return row_id


def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            raise MinutesOwnershipDenied("Minutes ownership is unavailable") from None
    return str(value)


def _decoded_hash(value: dict) -> dict[str, str]:
    try:
        return {_text(key) or "": _text(item) or "" for key, item in value.items()}
    except MinutesOwnershipDenied:
        raise
    except Exception:
        raise MinutesOwnershipDenied("Minutes ownership is unavailable") from None


def processing_claim_key(user_id: int) -> str:
    """Return the one durable, account-scoped Minutes processing claim key."""

    return f"zaki:agent:minutes-processing:{_canonical_user(user_id)}"


class RedisMinutesProcessingClaim:
    """Exclusive fail-closed claim held from spoke read through quarantine write.

    The claim intentionally has no automatic TTL. A process crash therefore makes erasure retryable
    instead of letting a still-running or partitioned model call outlive an apparently completed
    erasure. Operators may inspect the token, but only its holder may release the row.
    """

    def __init__(self, redis_client, *, user_id: int, token: str) -> None:
        self._redis = redis_client
        self._user_id = _canonical_user(user_id)
        self._token = token
        self._key = processing_claim_key(self._user_id)

    def __enter__(self) -> "RedisMinutesProcessingClaim":
        return self

    def __exit__(self, *_args) -> None:
        self.release()

    def _validated_state(self, value: dict) -> dict[str, str]:
        stored = _decoded_hash(value)
        if (
            set(stored) != {"token", "user_id", "state", "meeting_id"}
            or stored.get("token") != self._token
            or stored.get("user_id") != str(self._user_id)
            or stored.get("state") not in {"active", "cancelled"}
        ):
            raise MinutesOwnershipDenied("Minutes ownership is unavailable")
        meeting_id = stored.get("meeting_id", "")
        if meeting_id:
            _canonical_meeting(f"meeting:{meeting_id}")
        return stored

    def checkpoint(self) -> None:
        try:
            stored = self._validated_state(self._redis.hgetall(self._key))
            if stored["state"] != "active":
                raise MinutesOwnershipDenied("Minutes ownership is unavailable")
        except MinutesOwnershipDenied:
            raise
        except Exception:
            raise MinutesOwnershipDenied("Minutes ownership is unavailable") from None

    def bind_meeting(self, meeting_id: str) -> None:
        """Bind the post-read claim to one row unless account/meeting erasure already won."""

        from redis.exceptions import WatchError

        row_id = _canonical_meeting(meeting_id)
        account_key = f"zaki:agent:minutes-account-erasure:{self._user_id}"
        meeting_key = f"zaki:agent:minutes-erasure:{row_id}"
        for _attempt in range(_WATCH_RETRIES):
            try:
                with self._redis.pipeline(transaction=True) as pipe:
                    pipe.watch(self._key, account_key, meeting_key)
                    stored = self._validated_state(pipe.hgetall(self._key))
                    if stored["state"] != "active" or bool(pipe.exists(account_key)):
                        pipe.unwatch()
                        raise MinutesOwnershipDenied("Minutes ownership is unavailable")
                    meeting_state = _decoded_hash(pipe.hgetall(meeting_key))
                    if meeting_state not in ({}, {"user_id": str(self._user_id)}):
                        pipe.unwatch()
                        raise MinutesOwnershipDenied("Minutes ownership is unavailable")
                    if stored["meeting_id"] not in {"", row_id}:
                        pipe.unwatch()
                        raise MinutesOwnershipDenied("Minutes ownership is unavailable")
                    pipe.multi()
                    pipe.hset(self._key, "meeting_id", row_id)
                    pipe.persist(self._key)
                    pipe.execute()
                    return
            except WatchError:
                continue
            except MinutesOwnershipDenied:
                raise
            except Exception:
                raise MinutesOwnershipDenied("Minutes ownership is unavailable") from None
        raise MinutesOwnershipDenied("Minutes ownership is unavailable")

    def release(self) -> None:
        """Delete only this holder's claim; cancellation never transfers release authority."""

        from redis.exceptions import WatchError

        for _attempt in range(_WATCH_RETRIES):
            try:
                with self._redis.pipeline(transaction=True) as pipe:
                    pipe.watch(self._key)
                    if not bool(pipe.exists(self._key)):
                        pipe.unwatch()
                        return
                    self._validated_state(pipe.hgetall(self._key))
                    pipe.multi()
                    pipe.delete(self._key)
                    pipe.execute()
                    return
            except WatchError:
                continue
            except MinutesOwnershipDenied:
                raise
            except Exception:
                raise MinutesOwnershipDenied("Minutes ownership is unavailable") from None
        raise MinutesOwnershipDenied("Minutes ownership is unavailable")


class RedisMinutesOwnershipRegistry:
    """Serialize owner binding with the permanent account-erasure tombstone.

    A meeting hash containing only ``user_id`` is an ownership binding. Once the erasure state adds
    any field, the row is a per-meeting tombstone and can no longer be registered. This Redis gate is
    not final Brain write authority: the writer must atomically check same-store Brain tombstones.
    """

    def __init__(self, redis_client) -> None:
        self._redis = redis_client

    def claim_processing(self, user_id: int) -> RedisMinutesProcessingClaim:
        """Atomically fence account erasure and acquire the owner's sole processing claim."""

        from redis.exceptions import WatchError

        uid = _canonical_user(user_id)
        account_key = f"zaki:agent:minutes-account-erasure:{uid}"
        claim_key = processing_claim_key(uid)
        for _attempt in range(_WATCH_RETRIES):
            token = secrets.token_urlsafe(32)
            try:
                with self._redis.pipeline(transaction=True) as pipe:
                    pipe.watch(account_key, claim_key)
                    if bool(pipe.exists(account_key)) or bool(pipe.exists(claim_key)):
                        pipe.unwatch()
                        raise MinutesOwnershipDenied("Minutes ownership is unavailable")
                    pipe.multi()
                    pipe.hset(claim_key, mapping={
                        "token": token,
                        "user_id": str(uid),
                        "state": "active",
                        "meeting_id": "",
                    })
                    pipe.persist(claim_key)
                    pipe.execute()
                    return RedisMinutesProcessingClaim(
                        self._redis,
                        user_id=uid,
                        token=token,
                    )
            except WatchError:
                continue
            except MinutesOwnershipDenied:
                raise
            except Exception:
                raise MinutesOwnershipDenied("Minutes ownership is unavailable") from None
        raise MinutesOwnershipDenied("Minutes ownership is unavailable")

    def assert_read_allowed(self, user_id: int) -> None:
        uid = _canonical_user(user_id)
        try:
            if bool(self._redis.exists(f"zaki:agent:minutes-account-erasure:{uid}")):
                raise MinutesOwnershipDenied("Minutes ownership is unavailable")
        except MinutesOwnershipDenied:
            raise
        except Exception:
            raise MinutesOwnershipDenied("Minutes ownership is unavailable") from None

    def register_before_write(self, *, user_id: int, meeting_id: str) -> None:
        """Bind and index a row; final insertion still requires the governed Brain transaction."""

        from redis.exceptions import WatchError

        uid = _canonical_user(user_id)
        row_id = _canonical_meeting(meeting_id)
        account_key = f"zaki:agent:minutes-account-erasure:{uid}"
        index_key = f"zaki:agent:minutes-owner-index:{uid}"
        meeting_key = f"zaki:agent:minutes-erasure:{row_id}"
        for _attempt in range(_WATCH_RETRIES):
            try:
                with self._redis.pipeline(transaction=True) as pipe:
                    pipe.watch(account_key, index_key, meeting_key)
                    if bool(pipe.exists(account_key)):
                        pipe.unwatch()
                        raise MinutesOwnershipDenied("Minutes ownership is unavailable")
                    meeting_state = _decoded_hash(pipe.hgetall(meeting_key))
                    if meeting_state not in ({}, {"user_id": str(uid)}):
                        pipe.unwatch()
                        raise MinutesOwnershipDenied("Minutes ownership is unavailable")

                    pipe.multi()
                    if not meeting_state:
                        pipe.hset(meeting_key, mapping={"user_id": str(uid)})
                    pipe.persist(meeting_key)
                    pipe.sadd(index_key, row_id)
                    pipe.persist(index_key)
                    pipe.execute()
                    return
            except WatchError:
                continue
            except MinutesOwnershipDenied:
                raise
            except Exception:
                raise MinutesOwnershipDenied("Minutes ownership is unavailable") from None
        raise MinutesOwnershipDenied("Minutes ownership is unavailable")
