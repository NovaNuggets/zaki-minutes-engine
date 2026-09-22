"""Production PostgreSQL/object-storage composition for bounded Minutes TTL batches."""
from __future__ import annotations

from datetime import datetime, timedelta
import json
import re

from ..collector import purge_meeting_redis_carriers
from ..meeting_writes import meeting_write_lock_key, purge_scope_data_carriers
from ..obs import log_event
from .adapters import recording_prefixes_for_meeting
from .ttl import DueScope, TtlBatchReceipt, run_ttl_batch


_CANONICAL_UTC_EXPIRY = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"([.][0-9]{1,6})?(Z|[+]00:00)$"
)


async def run_production_ttl_once(
    *,
    enabled: bool,
    now,
    limit: int,
    session_factory,
    object_storage,
    redis_client=None,
    statement_factory=None,
) -> TtlBatchReceipt:
    """Run one explicitly-enabled production batch; disabled means zero carrier I/O."""

    if not isinstance(enabled, bool):
        raise ValueError("TTL operator flag must be boolean")
    if not enabled:
        return TtlBatchReceipt(
            attempted=0,
            audio_expired=0,
            transcript_expired=0,
            summary_expired=0,
            failed=0,
        )
    store = SqlAlchemyTtlStore(
        session_factory,
        object_storage,
        redis_client=redis_client,
        statement_factory=statement_factory,
    )
    return await run_ttl_batch(store, now=now, limit=limit)


class SqlAlchemyTtlStore:
    """Select and expire already-materialized per-scope retention deadlines."""

    def __init__(
        self,
        session_factory,
        object_storage,
        *,
        redis_client=None,
        statement_factory=None,
    ):
        self._session_factory = session_factory
        self._object_storage = object_storage
        self._redis = redis_client
        self._statement_factory = statement_factory

    def _statement(self, sql: str):
        if self._statement_factory is not None:
            return self._statement_factory(sql)
        from sqlalchemy import text

        return text(sql)

    @staticmethod
    def _meeting_id(value: str) -> int | None:
        try:
            meeting_id = int(value)
        except (TypeError, ValueError):
            return None
        return meeting_id if meeting_id > 0 else None

    @staticmethod
    def _stored_expiry(data: dict, scope: str) -> datetime | None:
        retention = data.get("zaki_retention")
        expiries = retention.get("scope_expiries") if isinstance(retention, dict) else None
        value = expiries.get(scope) if isinstance(expiries, dict) else None
        if not isinstance(value, str) or _CANONICAL_UTC_EXPIRY.fullmatch(value) is None:
            return None
        try:
            expiry = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if expiry.tzinfo is None or expiry.utcoffset() != timedelta(0):
            return None
        return expiry

    @staticmethod
    def _retention_root_is_corrupt(data: dict) -> bool:
        if "zaki_retention" not in data:
            # ``zaki_capture`` is the managed-data marker.  A terminal managed row that lost its
            # retention authority must be selected for fail-safe content purge, not hidden from
            # reads forever while its PII remains at rest.
            return "zaki_capture" in data
        retention = data.get("zaki_retention")
        if not isinstance(retention, dict):
            return True
        if retention.get("state") == "erasing":
            return False
        expired = retention.get("expired_scopes", [])
        expiries = retention.get("scope_expiries")
        expired_is_valid = (
            isinstance(expired, list)
            and all(isinstance(scope, str) for scope in expired)
        )
        return (
            retention.get("state") not in (None, "open")
            or not expired_is_valid
            or (
                expired_is_valid
                and (
                    len(expired) != len(set(expired))
                    or any(
                        scope not in {"audio", "transcript", "summary"}
                        for scope in expired
                    )
                )
            )
            or not isinstance(expiries, dict)
        )

    async def _exclusive_lock(self, db, meeting_id: int) -> None:
        await db.execute(
            self._statement("SELECT pg_advisory_xact_lock(:meeting_lock_key)"),
            {"meeting_lock_key": meeting_write_lock_key(meeting_id)},
        )

    async def _candidate(self, db, item: DueScope, meeting_id: int) -> dict | None:
        result = await db.execute(
            self._statement(
                "SELECT id, user_id, status, data FROM meetings "
                "WHERE id = :meeting_id FOR UPDATE"
            ),
            {"meeting_id": meeting_id},
        )
        row = result.mappings().first()
        if (
            row is None
            or str(row["user_id"]) != item.user_id
            or row["status"] not in {"completed", "failed"}
        ):
            return None
        data = dict(row["data"]) if isinstance(row["data"], dict) else {}
        retention = data.get("zaki_retention")
        if isinstance(retention, dict) and retention.get("state") == "erasing":
            return None
        corrupt_root = self._retention_root_is_corrupt(data)
        if corrupt_root:
            return (
                {"owner_id": row["user_id"], "data": data, "corrupt_root": True}
                if item.expiry_invalid
                else None
            )
        if not isinstance(retention, dict):
            return None
        expired = retention.get("expired_scopes", [])
        if not isinstance(expired, list) or item.scope in expired:
            return None
        stored_expiry = self._stored_expiry(data, item.scope)
        if item.expiry_invalid:
            if stored_expiry is not None:
                return None
        elif stored_expiry != item.expires_at:
            return None
        return {"owner_id": row["user_id"], "data": data, "corrupt_root": False}

    async def _persist_data(self, db, meeting_id: int, owner_id, data: dict) -> None:
        result = await db.execute(
            self._statement(
                "UPDATE meetings SET data = CAST(:data AS jsonb) "
                "WHERE id = :meeting_id AND user_id = :user_id"
            ),
            {
                "meeting_id": meeting_id,
                "user_id": owner_id,
                "data": json.dumps(data, sort_keys=True),
            },
        )
        if int(result.rowcount or 0) != 1:
            raise RuntimeError("TTL meeting update lost its owner")

    @staticmethod
    def _mark_expired(data: dict, scope: str) -> None:
        retention = dict(data["zaki_retention"])
        expired = set(retention.get("expired_scopes", []))
        expired.add(scope)
        retention["expired_scopes"] = sorted(expired)
        retry_after = retention.get("ttl_retry_after")
        if isinstance(retry_after, dict):
            retry_after = dict(retry_after)
            retry_after.pop(scope, None)
            if retry_after:
                retention["ttl_retry_after"] = retry_after
            else:
                retention.pop("ttl_retry_after", None)
        data["zaki_retention"] = retention

    async def defer_scope(self, item: DueScope, *, retry_at: datetime) -> None:
        """Persist a bounded retry delay for one unchanged, still-owned candidate."""

        if retry_at.tzinfo is None or retry_at.utcoffset() != timedelta(0):
            raise ValueError("TTL retry must be an aware UTC instant")
        meeting_id = self._meeting_id(item.meeting_id)
        if meeting_id is None:
            return
        async with self._session_factory() as db:
            await self._exclusive_lock(db, meeting_id)
            candidate = await self._candidate(db, item, meeting_id)
            if candidate is None:
                return
            data = candidate["data"]
            if candidate.get("corrupt_root"):
                retry_after = data.get("zaki_ttl_retry_after")
                retry_after = dict(retry_after) if isinstance(retry_after, dict) else {}
                retry_after[item.scope] = retry_at.isoformat()
                data["zaki_ttl_retry_after"] = retry_after
                await self._persist_data(db, meeting_id, candidate["owner_id"], data)
                await db.commit()
                return
            retention = dict(data["zaki_retention"])
            retry_after = retention.get("ttl_retry_after")
            retry_after = dict(retry_after) if isinstance(retry_after, dict) else {}
            retry_after[item.scope] = retry_at.isoformat()
            retention["ttl_retry_after"] = retry_after
            data["zaki_retention"] = retention
            await self._persist_data(db, meeting_id, candidate["owner_id"], data)
            await db.commit()

    async def list_due_scopes(self, *, now, limit: int) -> tuple[DueScope, ...]:
        sql = """
            WITH parsed_due AS (
                SELECT m.user_id, m.id AS meeting_id, due.scope,
                       due.expires_text, due.retry_text,
                       CASE
                           WHEN due.expires_text ~
                                '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?(Z|[+]00:00)$'
                            AND pg_input_is_valid(
                                due.expires_text, 'timestamp with time zone'
                            )
                           THEN CAST(due.expires_text AS timestamptz)
                       END AS expires_at,
                       CASE
                           WHEN due.retry_text ~
                                '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?(Z|[+]00:00)$'
                            AND pg_input_is_valid(
                                due.retry_text, 'timestamp with time zone'
                            )
                           THEN CAST(due.retry_text AS timestamptz)
                       END AS retry_at,
                       COALESCE(
                           m.data #> '{zaki_retention,expired_scopes}', '[]'::jsonb
                       ) AS expired_scopes
                FROM meetings m
                CROSS JOIN LATERAL (
                    VALUES
                        (
                            'audio',
                            m.data #>> '{zaki_retention,scope_expiries,audio}',
                            COALESCE(
                                m.data #>> '{zaki_retention,ttl_retry_after,audio}',
                                m.data #>> '{zaki_ttl_retry_after,audio}'
                            )
                        ),
                        (
                            'transcript',
                            m.data #>> '{zaki_retention,scope_expiries,transcript}',
                            COALESCE(
                                m.data #>> '{zaki_retention,ttl_retry_after,transcript}',
                                m.data #>> '{zaki_ttl_retry_after,transcript}'
                            )
                        ),
                        (
                            'summary',
                            m.data #>> '{zaki_retention,scope_expiries,summary}',
                            COALESCE(
                                m.data #>> '{zaki_retention,ttl_retry_after,summary}',
                                m.data #>> '{zaki_ttl_retry_after,summary}'
                            )
                        )
                ) AS due(scope, expires_text, retry_text)
                WHERE m.status IN ('completed', 'failed')
                  AND COALESCE(m.data #>> '{zaki_retention,state}', 'open') <> 'erasing'
                  AND (
                      m.data #> '{zaki_retention}' IS NOT NULL
                      OR m.data ? 'zaki_capture'
                  )
            )
            SELECT user_id, meeting_id, scope,
                   COALESCE(expires_at, :now) AS expires_at,
                   expires_at IS NULL AS expiry_invalid
            FROM parsed_due
            WHERE (expires_at IS NULL OR expires_at <= :now)
              AND (retry_text IS NULL OR retry_at IS NULL OR retry_at <= :now)
              AND NOT (expired_scopes ? scope)
            ORDER BY (parsed_due.expires_at IS NULL) DESC,
                     (parsed_due.retry_text IS NOT NULL),
                     parsed_due.expires_at, meeting_id, scope
            LIMIT :limit
        """
        async with self._session_factory() as db:
            result = await db.execute(
                self._statement(sql), {"now": now, "limit": limit}
            )
            items = tuple(
                DueScope(
                    user_id=str(row["user_id"]),
                    meeting_id=str(row["meeting_id"]),
                    scope=row["scope"],
                    expires_at=row["expires_at"],
                    expiry_invalid=bool(row.get("expiry_invalid", False)),
                )
                for row in result.mappings().all()
            )
            invalid_count = sum(item.expiry_invalid for item in items)
            if invalid_count:
                log_event(
                    "ttl_invalid_retention_selected",
                    audience="system",
                    level="warning",
                    span="retention.ttl",
                    fields={"invalid_candidates": invalid_count},
                )
            return items

    async def expire_scope(self, item: DueScope) -> int:
        """Expire one still-owned, unchanged terminal-meeting scope idempotently."""

        meeting_id = self._meeting_id(item.meeting_id)
        if meeting_id is None:
            return 0
        corrupt_root = False
        if item.scope in {"transcript", "summary"} or item.expiry_invalid:
            # Validate under the exclusive barrier, then release PostgreSQL before the bounded
            # Redis scan. Writers independently enforce the materialized wall-clock deadline, so
            # no post-deadline content is authorized during this gap. Revalidate below before the
            # durable mutation in case ownership/state/deadline changed meanwhile.
            async with self._session_factory() as db:
                await self._exclusive_lock(db, meeting_id)
                candidate = await self._candidate(db, item, meeting_id)
                if candidate is None:
                    return 0
                corrupt_root = bool(candidate.get("corrupt_root"))
            if item.scope in {"transcript", "summary"} or corrupt_root:
                await purge_meeting_redis_carriers(
                    self._redis,
                    meeting_id,
                    raw=item.scope == "transcript" or corrupt_root,
                    processed=True,
                )
        async with self._session_factory() as db:
            await self._exclusive_lock(db, meeting_id)
            candidate = await self._candidate(db, item, meeting_id)
            if candidate is None:
                return 0
            data = candidate["data"]
            if candidate.get("corrupt_root"):
                prefixes = recording_prefixes_for_meeting(candidate["owner_id"], data)
                if prefixes and self._object_storage is None:
                    raise RuntimeError("audio TTL object storage is unavailable")
                deleted = 0
                for prefix in prefixes:
                    deleted += await self._object_storage.delete_prefix(prefix)
                if prefixes and any([
                    await self._object_storage.count_prefix(prefix) for prefix in prefixes
                ]):
                    raise RuntimeError("audio TTL object deletion was incomplete")
                result = await db.execute(
                    self._statement(
                        "DELETE FROM transcriptions WHERE meeting_id = :meeting_id"
                    ),
                    {"meeting_id": meeting_id},
                )
                deleted += int(result.rowcount or 0)
                deleted += purge_scope_data_carriers(
                    data, "audio", "transcript", "summary"
                )
                data.pop("zaki_ttl_retry_after", None)
                expiry = item.expires_at.isoformat()
                data["zaki_retention"] = {
                    "state": "open",
                    "scope_expiries": {
                        scope: expiry for scope in ("audio", "transcript", "summary")
                    },
                    "expired_scopes": ["audio", "summary", "transcript"],
                }
                await self._persist_data(db, meeting_id, candidate["owner_id"], data)
                await db.commit()
                return deleted
            if item.scope == "transcript":
                result = await db.execute(
                    self._statement(
                        "DELETE FROM transcriptions WHERE meeting_id = :meeting_id"
                    ),
                    {"meeting_id": meeting_id},
                )
                deleted = int(result.rowcount or 0) + purge_scope_data_carriers(
                    data, "transcript"
                )
            elif item.scope == "summary":
                deleted = purge_scope_data_carriers(data, "summary")
            elif item.scope == "audio":
                if self._object_storage is None:
                    raise RuntimeError("audio TTL object storage is unavailable")
                prefixes = recording_prefixes_for_meeting(candidate["owner_id"], data)
                deleted = 0
                for prefix in prefixes:
                    deleted += await self._object_storage.delete_prefix(prefix)
                if any(
                    [
                        await self._object_storage.count_prefix(prefix)
                        for prefix in prefixes
                    ]
                ):
                    raise RuntimeError("audio TTL object deletion was incomplete")
                purge_scope_data_carriers(data, "audio")
            else:
                raise RuntimeError("TTL scope is invalid")
            self._mark_expired(data, item.scope)
            await self._persist_data(db, meeting_id, candidate["owner_id"], data)
            await db.commit()
            return deleted
