"""Agent-owned canonical ``erasure.v1`` signing for stable Minutes deletion counts.

This module deliberately implements the sealed wire algorithm locally.  Agent does not import
Minutes runtime code or share write ownership; the two spokes interoperate only through the contract.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import hmac
import json
import re
from typing import Callable, Mapping


_ROW_ID = re.compile(r"^[1-9][0-9]{0,18}$")
_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_NONCE = re.compile(r"^[A-Za-z0-9_-]{16,128}$")
_MAX_DB_ID = 2**63 - 1
_MAX_COUNT = 2_147_483_647
_SOURCE_COUNTS = ("unit_streams", "workspace_documents", "brain_records")
_WIRE_COUNTS = {
    "unit_streams": "agent_unit_streams",
    "workspace_documents": "agent_workspace_documents",
    "brain_records": "agent_brain_records",
}
_SIGNED_FIELDS = {
    "version", "owner", "scope", "subject", "counts", "issued_at",
    "key_id", "nonce", "digest", "signature",
}
_SHA256 = re.compile(r"^sha256=[0-9a-f]{64}$")
_WATCH_RETRIES = 8


class AgentErasureSigningError(RuntimeError):
    """The unsigned proof or injected signing configuration could not be safely signed."""


def _row_id(value: object) -> str:
    if isinstance(value, bool):
        raise AgentErasureSigningError("Agent erasure receipt is invalid")
    normalized = str(value) if isinstance(value, int) else value
    if (
        not isinstance(normalized, str)
        or not _ROW_ID.fullmatch(normalized)
        or int(normalized) > _MAX_DB_ID
    ):
        raise AgentErasureSigningError("Agent erasure receipt is invalid")
    return normalized


def _secret_bytes(secret: str | bytes) -> bytes:
    if isinstance(secret, str):
        try:
            value = secret.encode("ascii")
        except UnicodeEncodeError:
            raise AgentErasureSigningError("Agent erasure signer is invalid") from None
    elif isinstance(secret, bytes):
        value = secret
    else:
        raise AgentErasureSigningError("Agent erasure signer is invalid")
    if (
        not 32 <= len(value) <= 512
        or value[:1] == b" "
        or value[-1:] == b" "
        or any(not 0x20 <= character <= 0x7E for character in value)
    ):
        raise AgentErasureSigningError("Agent erasure signer is invalid")
    return value


def _canonical(value: Mapping[str, object]) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise AgentErasureSigningError("Agent erasure receipt is invalid") from None


def _deleted_counts(receipt: object, *, identity_field: str, identity: str) -> dict[str, int]:
    expected = {identity_field, "tombstoned", "deleted"}
    if not isinstance(receipt, dict) or set(receipt) != expected:
        raise AgentErasureSigningError("Agent erasure receipt is invalid")
    if receipt.get(identity_field) != identity or receipt.get("tombstoned") is not True:
        raise AgentErasureSigningError("Agent erasure receipt is invalid")
    deleted = receipt.get("deleted")
    if not isinstance(deleted, dict) or set(deleted) != set(_SOURCE_COUNTS):
        raise AgentErasureSigningError("Agent erasure receipt is invalid")
    if any(
        type(deleted.get(field)) is not int or not 0 <= deleted[field] <= _MAX_COUNT
        for field in _SOURCE_COUNTS
    ):
        raise AgentErasureSigningError("Agent erasure receipt is invalid")
    return {_WIRE_COUNTS[field]: deleted[field] for field in _SOURCE_COUNTS}


class AgentErasureV1Signer:
    """Convert stable Agent deletion receipts to exact, content-free ``owner=agent`` proofs."""

    def __init__(
        self,
        *,
        key_id: str,
        secret: str | bytes,
        previous_key_id: str | None = None,
        previous_secret: str | bytes | None = None,
        clock: Callable[[], datetime],
        nonce: Callable[[], str],
    ) -> None:
        if not isinstance(key_id, str) or not _KEY_ID.fullmatch(key_id):
            raise AgentErasureSigningError("Agent erasure signer is invalid")
        if not callable(clock) or not callable(nonce):
            raise AgentErasureSigningError("Agent erasure signer is invalid")
        self._key_id = key_id
        self._secret = _secret_bytes(secret)
        has_previous_id = previous_key_id is not None
        has_previous_secret = previous_secret is not None
        if has_previous_id != has_previous_secret:
            raise AgentErasureSigningError("Agent erasure signer is invalid")
        self._verification_secrets = {key_id: self._secret}
        if has_previous_id:
            if (
                not isinstance(previous_key_id, str)
                or not _KEY_ID.fullmatch(previous_key_id)
                or previous_key_id == key_id
            ):
                raise AgentErasureSigningError("Agent erasure signer is invalid")
            if previous_secret is None:
                raise AgentErasureSigningError("Agent erasure signer is invalid")
            normalized_previous = _secret_bytes(previous_secret)
            if hmac.compare_digest(normalized_previous, self._secret):
                raise AgentErasureSigningError("Agent erasure signer is invalid")
            self._verification_secrets[previous_key_id] = normalized_previous
        self._clock = clock
        self._nonce = nonce

    def sign_meeting(
        self,
        *,
        user_id: str | int,
        meeting_id: str | int,
        receipt: dict,
    ) -> dict:
        uid = _row_id(user_id)
        mid = _row_id(meeting_id)
        counts = _deleted_counts(receipt, identity_field="meeting_id", identity=mid)
        return self._sign(
            scope="meeting",
            subject={"user_id": uid, "meeting_id": mid},
            counts=counts,
        )

    def sign_account(self, *, user_id: str | int, receipt: dict) -> dict:
        uid = _row_id(user_id)
        counts = _deleted_counts(receipt, identity_field="user_id", identity=uid)
        return self._sign(scope="account", subject={"user_id": uid}, counts=counts)

    def _sign(self, *, scope: str, subject: dict[str, str], counts: dict[str, int]) -> dict:
        issued: object = None
        nonce: object = None
        unavailable = False
        try:
            issued = self._clock()
            nonce = self._nonce()
        except Exception:
            unavailable = True
        if unavailable:
            raise AgentErasureSigningError("Agent erasure signer is unavailable") from None
        if (
            not isinstance(issued, datetime)
            or issued.tzinfo is None
            or issued.utcoffset() is None
            or not isinstance(nonce, str)
            or not _NONCE.fullmatch(nonce)
        ):
            raise AgentErasureSigningError("Agent erasure signer is invalid")
        issued_at = issued.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        base = {
            "version": "erasure.v1",
            "owner": "agent",
            "scope": scope,
            "subject": subject,
            "counts": counts,
            "issued_at": issued_at,
            "key_id": self._key_id,
            "nonce": nonce,
        }
        digest = "sha256=" + hashlib.sha256(_canonical(base)).hexdigest()
        signed = {**base, "digest": digest}
        signature = hmac.new(self._secret, _canonical(signed), hashlib.sha256).hexdigest()
        return {**signed, "signature": "sha256=" + signature}

    def _validate_persisted(
        self,
        receipt: object,
        *,
        scope: str,
        subject: dict[str, str],
        counts: dict[str, int],
    ) -> dict:
        if not isinstance(receipt, dict) or set(receipt) != _SIGNED_FIELDS:
            raise AgentErasureSigningError("Agent erasure signed receipt is invalid")
        base = {key: receipt[key] for key in (
            "version", "owner", "scope", "subject", "counts", "issued_at",
            "key_id", "nonce",
        )}
        if (
            base["version"] != "erasure.v1"
            or base["owner"] != "agent"
            or base["scope"] != scope
            or base["subject"] != subject
            or base["counts"] != counts
            or not isinstance(base["issued_at"], str)
            or not isinstance(base["nonce"], str)
            or not _NONCE.fullmatch(base["nonce"])
        ):
            raise AgentErasureSigningError("Agent erasure signed receipt is invalid")
        persisted_key_id = base["key_id"]
        if not isinstance(persisted_key_id, str) or not _KEY_ID.fullmatch(persisted_key_id):
            raise AgentErasureSigningError("Agent erasure signed receipt is invalid")
        verification_secret = self._verification_secrets.get(persisted_key_id)
        if verification_secret is None:
            raise AgentErasureSigningError("Agent erasure signed receipt is invalid")
        try:
            issued = datetime.fromisoformat(base["issued_at"].replace("Z", "+00:00"))
        except ValueError:
            raise AgentErasureSigningError("Agent erasure signed receipt is invalid") from None
        if issued.tzinfo is None or issued.utcoffset() is None:
            raise AgentErasureSigningError("Agent erasure signed receipt is invalid")
        canonical_issued = issued.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        if canonical_issued != base["issued_at"]:
            raise AgentErasureSigningError("Agent erasure signed receipt is invalid")
        digest = receipt["digest"]
        signature = receipt["signature"]
        if (
            not isinstance(digest, str)
            or not _SHA256.fullmatch(digest)
            or not isinstance(signature, str)
            or not _SHA256.fullmatch(signature)
        ):
            raise AgentErasureSigningError("Agent erasure signed receipt is invalid")
        expected_digest = "sha256=" + hashlib.sha256(_canonical(base)).hexdigest()
        if not hmac.compare_digest(digest, expected_digest):
            raise AgentErasureSigningError("Agent erasure signed receipt is invalid")
        expected_signature = "sha256=" + hmac.new(
            verification_secret,
            _canonical({**base, "digest": expected_digest}),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected_signature):
            raise AgentErasureSigningError("Agent erasure signed receipt is invalid")
        return dict(receipt)


def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            raise AgentErasureSigningError("Agent erasure state is invalid") from None
    return str(value)


def _decoded_hash(value: dict) -> dict[str, str]:
    return {_text(key) or "": _text(item) or "" for key, item in value.items()}


class RedisAgentErasureReceiptStore:
    """Persist the first signed proof on the already-complete Agent erasure state."""

    def __init__(self, redis_client, *, signer: AgentErasureV1Signer) -> None:
        if not isinstance(signer, AgentErasureV1Signer):
            raise AgentErasureSigningError("Agent erasure signer is invalid")
        self._redis = redis_client
        self._signer = signer

    def persist_meeting(
        self,
        *,
        user_id: str | int,
        meeting_id: str | int,
        receipt: dict,
    ) -> dict:
        uid = _row_id(user_id)
        mid = _row_id(meeting_id)
        counts = _deleted_counts(receipt, identity_field="meeting_id", identity=mid)
        return self._persist(
            key=f"zaki:agent:minutes-erasure:{mid}",
            scope="meeting",
            subject={"user_id": uid, "meeting_id": mid},
            counts=counts,
            validate_state=lambda stored: self._validate_meeting_state(
                stored, user_id=uid, counts=counts,
            ),
            create=lambda: self._signer.sign_meeting(
                user_id=uid, meeting_id=mid, receipt=receipt,
            ),
        )

    def persist_account(self, *, user_id: str | int, receipt: dict) -> dict:
        uid = _row_id(user_id)
        counts = _deleted_counts(receipt, identity_field="user_id", identity=uid)
        return self._persist(
            key=f"zaki:agent:minutes-account-erasure:{uid}",
            scope="account",
            subject={"user_id": uid},
            counts=counts,
            validate_state=lambda stored: self._validate_account_state(
                stored, user_id=uid, source=receipt,
            ),
            create=lambda: self._signer.sign_account(user_id=uid, receipt=receipt),
        )

    def _persist(
        self,
        *,
        key: str,
        scope: str,
        subject: dict[str, str],
        counts: dict[str, int],
        validate_state: Callable[[dict[str, str]], None],
        create: Callable[[], dict],
    ) -> dict:
        from redis.exceptions import WatchError

        for _attempt in range(_WATCH_RETRIES):
            unavailable = False
            try:
                with self._redis.pipeline(transaction=True) as pipe:
                    pipe.watch(key)
                    stored = _decoded_hash(pipe.hgetall(key))
                    validate_state(stored)
                    encoded = stored.get("signed_receipt")
                    if encoded is not None:
                        pipe.unwatch()
                        try:
                            persisted = json.loads(encoded)
                        except json.JSONDecodeError:
                            raise AgentErasureSigningError(
                                "Agent erasure signed receipt is invalid"
                            ) from None
                        return self._signer._validate_persisted(
                            persisted, scope=scope, subject=subject, counts=counts,
                        )
                    candidate = create()
                    encoded = _canonical(candidate).decode("utf-8")
                    pipe.multi()
                    pipe.hset(key, "signed_receipt", encoded)
                    pipe.persist(key)
                    pipe.execute()
                    return candidate
            except WatchError:
                continue
            except AgentErasureSigningError:
                raise
            except Exception:
                unavailable = True
            if unavailable:
                raise AgentErasureSigningError(
                    "Agent erasure signed receipt is unavailable"
                ) from None
        raise AgentErasureSigningError("Agent erasure signed receipt is unavailable")

    @staticmethod
    def _validate_meeting_state(
        stored: dict[str, str], *, user_id: str, counts: dict[str, int],
    ) -> None:
        allowed = {"user_id", "state", *_SOURCE_COUNTS, "signed_receipt"}
        if set(stored) not in (allowed - {"signed_receipt"}, allowed):
            raise AgentErasureSigningError("Agent erasure state is invalid")
        expected = {
            "user_id": user_id,
            "state": "complete",
            **{
                field: str(counts[_WIRE_COUNTS[field]])
                for field in _SOURCE_COUNTS
            },
        }
        if any(stored.get(field) != value for field, value in expected.items()):
            raise AgentErasureSigningError("Agent erasure state is invalid")

    @staticmethod
    def _validate_account_state(
        stored: dict[str, str], *, user_id: str, source: dict,
    ) -> None:
        allowed = {"user_id", "state", "snapshot", "receipt", "signed_receipt"}
        if set(stored) not in (allowed - {"signed_receipt"}, allowed):
            raise AgentErasureSigningError("Agent erasure state is invalid")
        if stored.get("user_id") != user_id or stored.get("state") != "complete":
            raise AgentErasureSigningError("Agent erasure state is invalid")
        try:
            stable_source = json.loads(stored["receipt"])
        except (KeyError, json.JSONDecodeError):
            raise AgentErasureSigningError("Agent erasure state is invalid") from None
        if stable_source != source or stored["receipt"] != _canonical(source).decode("utf-8"):
            raise AgentErasureSigningError("Agent erasure state is invalid")


class SignedAgentMinutesErasure:
    """Replay one persisted signed meeting proof around a stable unsigned eraser."""

    def __init__(self, *, eraser, receipts: RedisAgentErasureReceiptStore) -> None:
        self._eraser = eraser
        self._receipts = receipts

    def bind_live_registry(self, live_registry: object) -> None:
        bind = getattr(self._eraser, "bind_live_registry", None)
        if callable(bind):
            bind(live_registry)

    def erase(self, *, user_id: int, meeting_id: str) -> dict:
        raw = self._eraser.erase(user_id=user_id, meeting_id=meeting_id)
        return self._receipts.persist_meeting(
            user_id=user_id, meeting_id=meeting_id, receipt=raw,
        )


class SignedAgentMinutesAccountErasure:
    """Replay one persisted signed account proof around a stable unsigned eraser."""

    def __init__(self, *, eraser, receipts: RedisAgentErasureReceiptStore) -> None:
        self._eraser = eraser
        self._receipts = receipts

    def erase(self, *, user_id: int) -> dict:
        raw = self._eraser.erase(user_id=user_id)
        return self._receipts.persist_account(user_id=user_id, receipt=raw)
