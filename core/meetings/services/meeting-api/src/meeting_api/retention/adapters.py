"""Production PostgreSQL and object-storage adapters for Minutes erasure.

Heavy runtime dependencies stay lazy so the offline meeting-api suite does not need boto3,
SQLAlchemy or asyncpg.  Tests inject protocol-compatible clients/factories into these adapters.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace
import json
import re
from typing import Optional

from ..collector import purge_meeting_redis_carriers
from ..meeting_writes import meeting_write_lock_key
from ..webhooks.platform_finalized import RedisTranscriptFinalizedOutbox
from ..webhooks.retry import purge_meeting_webhook_state
from .ports import ErasurePlan


_KEY_SEGMENT = re.compile(r"^[A-Za-z0-9._:-]+$")
_RUNTIME_WORKLOAD_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")


def _runtime_workload_id(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _RUNTIME_WORKLOAD_ID.fullmatch(value):
        raise ValueError("runtime workload identity is invalid")
    return value


def recording_prefixes_for_meeting(user_id: int | str, data: dict) -> tuple[str, ...]:
    """Derive narrow recording/session prefixes from one owned meeting's JSONB.

    Stored paths are treated as integrity evidence, not as deletion instructions: every path must
    agree with the owner/recording/session identity before its derived prefix can reach storage.
    """

    prefixes: set[str] = set()
    owner = str(user_id)
    if not _KEY_SEGMENT.fullmatch(owner) or owner in {".", ".."}:
        raise ValueError("recording owner identity is invalid")
    intents = data.get("zaki_recording_prefixes", []) if isinstance(data, dict) else []
    if not isinstance(intents, list):
        raise ValueError("recording prefix intents are invalid")
    for prefix in intents:
        parts = prefix.split("/") if isinstance(prefix, str) else []
        if (
            len(parts) != 5
            or parts[0] != "recordings"
            or parts[1] != owner
            or parts[-1] != ""
            or any(
                not _KEY_SEGMENT.fullmatch(part) or part in {".", ".."}
                for part in parts[1:-1]
            )
        ):
            raise ValueError("recording prefix intent is invalid")
        prefixes.add(prefix)
    recordings = data.get("recordings", []) if isinstance(data, dict) else []
    if not isinstance(recordings, list):
        raise ValueError("recording metadata is invalid")
    for recording in recordings:
        if not isinstance(recording, dict):
            raise ValueError("recording metadata is invalid")
        media_files = recording.get("media_files", [])
        if not isinstance(media_files, list) or any(
            not isinstance(media_file, dict) for media_file in media_files
        ):
            raise ValueError("recording metadata is invalid")
        paths = [media_file.get("storage_path") for media_file in media_files]
        paths = [path for path in paths if path]
        if not paths:
            continue
        recording_id = str(recording.get("id", ""))
        session_uid = str(recording.get("session_uid", ""))
        if (
            not _KEY_SEGMENT.fullmatch(recording_id)
            or recording_id in {".", ".."}
            or not _KEY_SEGMENT.fullmatch(session_uid)
            or session_uid in {".", ".."}
        ):
            raise ValueError("recording storage identity is invalid")
        prefix = f"recordings/{owner}/{recording_id}/{session_uid}/"
        for path in paths:
            if not isinstance(path, str) or not path.startswith(prefix):
                raise ValueError("recording storage identity mismatch")
        prefixes.add(prefix)
    return tuple(sorted(prefixes))


def _summary_document_count(data: dict) -> int:
    count = 0
    summaries = data.get("summaries") if isinstance(data, dict) else None
    if isinstance(summaries, list):
        count += len(summaries)
    if isinstance(data, dict) and data.get("summary") is not None:
        count += 1
    processed = data.get("processed") if isinstance(data, dict) else None
    views = processed.get("views") if isinstance(processed, dict) else None
    if isinstance(views, list):
        count += len(views)
    return count


class SqlAlchemyRetentionRepo:
    """Owner-scoped erasure over the production ``meetings`` schema.

    The repository uses the same signed-bigint PostgreSQL advisory key as recording writers.
    Erasure takes the transaction-scoped exclusive lock, waits for shared writer locks to drain,
    stores ``data.zaki_retention.state=erasing``, then commits. Later writers acquire their shared
    lock and observe that durable state before touching object storage.
    """

    def __init__(self, session_factory, *, redis_client=None, statement_factory=None):
        self._session_factory = session_factory
        self._redis = redis_client
        self._statement_factory = statement_factory

    def _statement(self, sql: str):
        if self._statement_factory is not None:
            return self._statement_factory(sql)
        from sqlalchemy import text

        return text(sql)

    @staticmethod
    def _meeting_id(meeting_id: str | int) -> int | None:
        try:
            value = int(meeting_id)
        except (TypeError, ValueError):
            return None
        return value if value > 0 else None

    async def _exclusive_lock(self, db, meeting_id: int) -> None:
        await db.execute(
            self._statement("SELECT pg_advisory_xact_lock(:meeting_lock_key)"),
            {"meeting_lock_key": meeting_write_lock_key(meeting_id)},
        )

    async def _owned_meeting(self, db, meeting_id: int, user_id: str | int):
        result = await db.execute(
            self._statement(
                "SELECT id, user_id, status, data, bot_container_id FROM meetings "
                "WHERE id = :meeting_id FOR UPDATE"
            ),
            {"meeting_id": meeting_id},
        )
        row = result.mappings().first()
        if row is None or str(row["user_id"]) != str(user_id):
            return None
        return row

    async def _persist_retention(self, db, meeting_id: int, metadata: dict) -> None:
        await db.execute(
            self._statement(
                "UPDATE meetings SET data = jsonb_set(COALESCE(data, '{}'::jsonb), "
                "'{zaki_retention}', CAST(:retention AS jsonb), true) "
                "WHERE id = :meeting_id"
            ),
            {"meeting_id": meeting_id, "retention": json.dumps(metadata, sort_keys=True)},
        )

    @staticmethod
    def _agent_fields(
        value: object, *, user_id: str | int | None = None, meeting_id: str | int | None = None
    ) -> dict:
        if value is None:
            return {
                "agent_tombstoned": False,
                "agent_unit_streams": 0,
                "agent_workspace_documents": 0,
                "agent_brain_records": 0,
            }
        if (
            not isinstance(value, dict)
            or value.get("version") != "erasure.v1"
            or value.get("owner") != "agent"
            or value.get("scope") != "meeting"
            or not isinstance(value.get("subject"), dict)
            or (
                user_id is not None
                and value["subject"].get("user_id") != str(user_id)
            )
            or (
                meeting_id is not None
                and value["subject"].get("meeting_id") != str(meeting_id)
            )
            or not isinstance(value.get("counts"), dict)
            or set(value["counts"]) != {
                "agent_unit_streams",
                "agent_workspace_documents",
                "agent_brain_records",
            }
        ):
            raise RuntimeError("Agent erasure receipt is invalid")
        counts = {}
        for key in ("agent_unit_streams", "agent_workspace_documents", "agent_brain_records"):
            count = value["counts"].get(key)
            if type(count) is not int or count < 0 or count > 2_147_483_647:
                raise RuntimeError("Agent erasure receipt is invalid")
            counts[key] = count
        return {
            "agent_tombstoned": True,
            **counts,
        }

    async def completed_erasure(self, user_id: str, meeting_id: str) -> dict | None:
        mid = self._meeting_id(meeting_id)
        try:
            uid = int(user_id)
        except (TypeError, ValueError):
            return None
        if mid is None or uid <= 0:
            return None
        async with self._session_factory() as db:
            result = await db.execute(
                self._statement(
                    "SELECT receipt FROM minutes_erasure_receipts "
                    "WHERE user_id = :user_id AND meeting_id = :meeting_id"
                ),
                {"user_id": uid, "meeting_id": mid},
            )
            receipt = result.scalar_one_or_none()
            return dict(receipt) if isinstance(receipt, dict) else None

    async def begin_erasure(self, user_id: str, meeting_id: str) -> ErasurePlan | None:
        mid = self._meeting_id(meeting_id)
        if mid is None:
            return None
        async with self._session_factory() as db:
            await self._exclusive_lock(db, mid)
            row = await self._owned_meeting(db, mid, user_id)
            if row is None or row["status"] not in {"completed", "failed"}:
                return None
            data = dict(row["data"]) if isinstance(row["data"], dict) else {}
            metadata = data.get("zaki_retention")
            row_workload_id = _runtime_workload_id(row.get("bot_container_id"))
            if not isinstance(metadata, dict) or metadata.get("state") != "erasing":
                prefixes = recording_prefixes_for_meeting(row["user_id"], data)
                transcript_result = await db.execute(
                    self._statement(
                        "SELECT count(*) FROM transcriptions WHERE meeting_id = :meeting_id"
                    ),
                    {"meeting_id": mid},
                )
                transcript_rows = int(transcript_result.scalar_one())
                summary_documents = _summary_document_count(data)
                metadata = {
                    "state": "erasing",
                    "recording_prefixes": list(prefixes),
                    "recording_objects": None,
                    "transcript_rows": transcript_rows,
                    "summary_documents": summary_documents,
                    "runtime_workload_id": row_workload_id,
                }
                await self._persist_retention(db, mid, metadata)
                await db.commit()
            else:
                prefixes = recording_prefixes_for_meeting(
                    row["user_id"],
                    {
                        "zaki_recording_prefixes": metadata.get("recording_prefixes", []),
                        "recordings": [],
                    },
                )
                transcript_rows = int(metadata.get("transcript_rows") or 0)
                summary_documents = int(metadata.get("summary_documents") or 0)
                if "runtime_workload_id" not in metadata:
                    metadata = {
                        **metadata,
                        "runtime_workload_id": row_workload_id,
                    }
                    await self._persist_retention(db, mid, metadata)
                    await db.commit()
            workload_id = _runtime_workload_id(metadata.get("runtime_workload_id"))
            agent_fields = self._agent_fields(
                metadata.get("agent_erasure"), user_id=user_id, meeting_id=meeting_id
            )
            return ErasurePlan(
                user_id=str(user_id),
                meeting_id=str(meeting_id),
                transcript_rows=transcript_rows,
                summary_documents=summary_documents,
                recording_prefixes=tuple(prefixes),
                recording_objects=metadata.get("recording_objects"),
                runtime_workload_id=workload_id,
                agent_receipt=deepcopy(metadata.get("agent_erasure"))
                if isinstance(metadata.get("agent_erasure"), dict)
                else None,
                **agent_fields,
            )

    async def record_object_census(
        self, plan: ErasurePlan, recording_objects: int
    ) -> ErasurePlan:
        mid = self._meeting_id(plan.meeting_id)
        if mid is None or recording_objects < 0:
            raise RuntimeError("meeting erasure census is invalid")
        async with self._session_factory() as db:
            await self._exclusive_lock(db, mid)
            row = await self._owned_meeting(db, mid, plan.user_id)
            if row is None:
                raise RuntimeError("meeting erasure census lost its owner")
            if row["status"] not in {"completed", "failed"}:
                raise RuntimeError("meeting erasure census requires a terminal meeting")
            data = dict(row["data"]) if isinstance(row["data"], dict) else {}
            metadata = data.get("zaki_retention")
            if not isinstance(metadata, dict) or metadata.get("state") != "erasing":
                raise RuntimeError("meeting erasure census has no durable plan")
            stable_count = metadata.get("recording_objects")
            if stable_count is None:
                stable_count = int(recording_objects)
                metadata = {**metadata, "recording_objects": stable_count}
                await self._persist_retention(db, mid, metadata)
                await db.commit()
            return replace(plan, recording_objects=int(stable_count))

    async def record_agent_erasure(self, plan: ErasurePlan, receipt: dict) -> ErasurePlan:
        mid = self._meeting_id(plan.meeting_id)
        if mid is None:
            raise RuntimeError("Agent erasure receipt has an invalid meeting")
        fields = self._agent_fields(
            receipt, user_id=plan.user_id, meeting_id=plan.meeting_id
        )
        if not fields["agent_tombstoned"]:
            raise RuntimeError("Agent erasure receipt is invalid")
        async with self._session_factory() as db:
            await self._exclusive_lock(db, mid)
            row = await self._owned_meeting(db, mid, plan.user_id)
            if row is None or row["status"] not in {"completed", "failed"}:
                raise RuntimeError("Agent erasure receipt lost its owner")
            data = dict(row["data"]) if isinstance(row["data"], dict) else {}
            metadata = data.get("zaki_retention")
            if not isinstance(metadata, dict) or metadata.get("state") != "erasing":
                raise RuntimeError("Agent erasure receipt has no durable plan")
            current = metadata.get("agent_erasure")
            if current is None:
                metadata = {**metadata, "agent_erasure": deepcopy(receipt)}
                await self._persist_retention(db, mid, metadata)
                await db.commit()
                current = receipt
            stable = self._agent_fields(
                current, user_id=plan.user_id, meeting_id=plan.meeting_id
            )
            if not stable["agent_tombstoned"]:
                raise RuntimeError("Agent erasure receipt is invalid")
            return replace(plan, agent_receipt=deepcopy(current), **stable)

    async def record_erasure_receipt(self, plan: ErasurePlan, receipt: dict) -> dict:
        mid = self._meeting_id(plan.meeting_id)
        if mid is None:
            raise RuntimeError("Minutes erasure receipt has an invalid meeting")
        async with self._session_factory() as db:
            await self._exclusive_lock(db, mid)
            row = await self._owned_meeting(db, mid, plan.user_id)
            if row is None:
                completed = await db.execute(
                    self._statement(
                        "SELECT receipt FROM minutes_erasure_receipts "
                        "WHERE user_id = :user_id AND meeting_id = :meeting_id"
                    ),
                    {"user_id": int(plan.user_id), "meeting_id": mid},
                )
                stable = completed.scalar_one_or_none()
                if isinstance(stable, dict):
                    return deepcopy(stable)
                raise RuntimeError("Minutes erasure receipt lost its durable plan")
            data = dict(row["data"]) if isinstance(row["data"], dict) else {}
            metadata = data.get("zaki_retention")
            if not isinstance(metadata, dict) or metadata.get("state") != "erasing":
                raise RuntimeError("Minutes erasure receipt has no durable plan")
            current = metadata.get("minutes_erasure_receipt")
            if current is None:
                current = deepcopy(receipt)
                metadata = {**metadata, "minutes_erasure_receipt": current}
                await self._persist_retention(db, mid, metadata)
                await db.commit()
            if not isinstance(current, dict):
                raise RuntimeError("Minutes erasure receipt is invalid")
            return deepcopy(current)

    async def commit_erasure(
        self,
        plan: ErasurePlan,
        *,
        erased_at=None,
        policy_version: str | None = None,
        receipt: dict | None = None,
    ) -> dict:
        mid = self._meeting_id(plan.meeting_id)
        if mid is None:
            return {"meeting_rows": 0, "transcript_rows": 0, "summary_documents": 0}
        async with self._session_factory() as db:
            await self._exclusive_lock(db, mid)
            row = await self._owned_meeting(db, mid, plan.user_id)
            if row is None:
                if receipt is not None:
                    completed = await db.execute(
                        self._statement(
                            "SELECT receipt FROM minutes_erasure_receipts "
                            "WHERE user_id = :user_id AND meeting_id = :meeting_id"
                        ),
                        {"user_id": int(plan.user_id), "meeting_id": mid},
                    )
                    stable = completed.scalar_one_or_none()
                    if isinstance(stable, dict):
                        return deepcopy(stable)
                return {"meeting_rows": 0, "transcript_rows": 0, "summary_documents": 0}
            if row["status"] not in {"completed", "failed"}:
                raise RuntimeError("meeting erasure database commit requires a terminal meeting")
            data = row["data"] if isinstance(row["data"], dict) else {}
            metadata = data.get("zaki_retention")
            if not isinstance(metadata, dict) or metadata.get("state") != "erasing":
                raise RuntimeError("meeting erasure database commit has no durable plan")
            stable_agent = self._agent_fields(
                metadata.get("agent_erasure"),
                user_id=plan.user_id,
                meeting_id=plan.meeting_id,
            )
            if plan.agent_tombstoned and stable_agent != {
                "agent_tombstoned": plan.agent_tombstoned,
                "agent_unit_streams": plan.agent_unit_streams,
                "agent_workspace_documents": plan.agent_workspace_documents,
                "agent_brain_records": plan.agent_brain_records,
            }:
                raise RuntimeError("meeting erasure Agent receipt changed")
            transcript_result = await db.execute(
                self._statement(
                    "DELETE FROM transcriptions WHERE meeting_id = :meeting_id"
                ),
                {"meeting_id": mid},
            )
            await db.execute(
                self._statement(
                    "DELETE FROM meeting_sessions WHERE meeting_id = :meeting_id"
                ),
                {"meeting_id": mid},
            )
            meeting_result = await db.execute(
                self._statement(
                    "DELETE FROM meetings WHERE id = :meeting_id AND user_id = :user_id"
                ),
                {"meeting_id": mid, "user_id": int(row["user_id"])},
            )
            deleted = {
                "meeting_rows": int(meeting_result.rowcount or 0),
                "transcript_rows": int(transcript_result.rowcount or 0),
                "summary_documents": plan.summary_documents,
            }
            if receipt is not None:
                stable_receipt = metadata.get("minutes_erasure_receipt")
                expected_counts = {
                    **deleted,
                    "recording_objects": int(plan.recording_objects or 0),
                    "agent_unit_streams": plan.agent_unit_streams,
                    "agent_workspace_documents": plan.agent_workspace_documents,
                    "agent_brain_records": plan.agent_brain_records,
                }
                if stable_receipt != receipt or receipt.get("counts") != expected_counts:
                    raise RuntimeError("Minutes erasure receipt changed")
                if erased_at is None or not isinstance(policy_version, str) or not policy_version:
                    raise RuntimeError("meeting erasure completion metadata is missing")
                await db.execute(
                    self._statement(
                        "INSERT INTO minutes_erasure_receipts "
                        "(user_id, meeting_id, erased_at, policy_version, receipt) "
                        "VALUES (:user_id, :meeting_id, :erased_at, :policy_version, "
                        "CAST(:receipt AS jsonb)) "
                        "ON CONFLICT (user_id, meeting_id) DO NOTHING"
                    ),
                    {
                        "user_id": int(row["user_id"]),
                        "meeting_id": mid,
                        "erased_at": erased_at,
                        "policy_version": policy_version,
                        "receipt": json.dumps(receipt, sort_keys=True),
                    },
                )
                stored_result = await db.execute(
                    self._statement(
                        "SELECT receipt FROM minutes_erasure_receipts "
                        "WHERE user_id = :user_id AND meeting_id = :meeting_id"
                    ),
                    {"user_id": int(row["user_id"]), "meeting_id": mid},
                )
                stored_receipt = stored_result.scalar_one_or_none()
                if stored_receipt != receipt:
                    raise RuntimeError("Minutes erasure receipt conflict")
            elif plan.agent_tombstoned:
                if erased_at is None or not isinstance(policy_version, str) or not policy_version:
                    raise RuntimeError("meeting erasure completion metadata is missing")
                full_receipt = {
                    "user_id": plan.user_id,
                    "meeting_id": plan.meeting_id,
                    "erased_at": erased_at.isoformat(),
                    "policy_version": policy_version,
                    "agent_tombstoned": True,
                    "deleted": {
                        **deleted,
                        "recording_objects": int(plan.recording_objects or 0),
                        "agent_unit_streams": plan.agent_unit_streams,
                        "agent_workspace_documents": plan.agent_workspace_documents,
                        "agent_brain_records": plan.agent_brain_records,
                    },
                }
                await db.execute(
                    self._statement(
                        "INSERT INTO minutes_erasure_receipts "
                        "(user_id, meeting_id, erased_at, policy_version, receipt) "
                        "VALUES (:user_id, :meeting_id, :erased_at, :policy_version, "
                        "CAST(:receipt AS jsonb)) "
                        "ON CONFLICT (user_id, meeting_id) DO NOTHING"
                    ),
                    {
                        "user_id": int(row["user_id"]),
                        "meeting_id": mid,
                        "erased_at": erased_at,
                        "policy_version": policy_version,
                        "receipt": json.dumps(full_receipt, sort_keys=True),
                    },
                )
            await db.commit()
            return deepcopy(receipt) if receipt is not None else deleted

    async def purge_carriers(self, plan: ErasurePlan) -> None:
        """Validate the durable fence, then perform bounded Redis I/O outside PostgreSQL."""

        mid = self._meeting_id(plan.meeting_id)
        if mid is None:
            raise RuntimeError("meeting erasure carrier purge is invalid")
        async with self._session_factory() as db:
            await self._exclusive_lock(db, mid)
            row = await self._owned_meeting(db, mid, plan.user_id)
            if row is None:
                completed = await db.execute(
                    self._statement(
                        "SELECT receipt FROM minutes_erasure_receipts "
                        "WHERE user_id = :user_id AND meeting_id = :meeting_id"
                    ),
                    {"user_id": int(plan.user_id), "meeting_id": mid},
                )
                if isinstance(completed.scalar_one_or_none(), dict):
                    return
                raise RuntimeError("meeting erasure carrier purge lost its owner")
            if row["status"] not in {"completed", "failed"}:
                raise RuntimeError("meeting erasure carrier purge requires a terminal meeting")
            data = row["data"] if isinstance(row["data"], dict) else {}
            metadata = data.get("zaki_retention")
            if not isinstance(metadata, dict) or metadata.get("state") != "erasing":
                raise RuntimeError("meeting erasure carrier purge has no durable plan")
        # Fence outbound copies before deleting the source carriers. Both operations leave only
        # content-free tombstones, so a retry/recovery worker cannot resurrect or redeliver PII.
        await purge_meeting_webhook_state(self._redis, mid)
        await RedisTranscriptFinalizedOutbox(self._redis).cancel(mid)
        await purge_meeting_redis_carriers(
            self._redis, mid, raw=True, processed=True
        )


class S3RetentionStorage:
    """Prefix census/deletion over one S3 or MinIO bucket.

    Unversioned current objects, or every version and delete marker in a versioned/suspended bucket,
    are deleted in batches of at most 1,000. Object Lock/legal-hold policy remains an Infra launch
    gate: storage refusal fails erasure and preserves the durable retry plan.
    """

    def __init__(
        self,
        *,
        bucket: str,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        client=None,
    ):
        self._bucket = bucket
        self._endpoint = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._client = client

    def _c(self):
        if self._client is None:
            import boto3

            self._client = boto3.client(
                "s3",
                endpoint_url=self._endpoint,
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
            )
        return self._client

    async def _run(self, fn, *args, **kwargs):
        call = asyncio.create_task(asyncio.to_thread(fn, *args, **kwargs))
        try:
            return await asyncio.shield(call)
        except asyncio.CancelledError as cancelled:
            try:
                await call
            finally:
                raise cancelled

    async def _is_versioned(self) -> bool:
        response = await self._run(
            self._c().get_bucket_versioning,
            Bucket=self._bucket,
        )
        return response.get("Status") in {"Enabled", "Suspended"}

    async def _count_versions(self, prefix: str) -> int:
        count = 0
        key_marker: Optional[str] = None
        version_marker: Optional[str] = None
        while True:
            kwargs = {"Bucket": self._bucket, "Prefix": prefix, "MaxKeys": 1000}
            if key_marker is not None:
                kwargs["KeyMarker"] = key_marker
            if version_marker is not None:
                kwargs["VersionIdMarker"] = version_marker
            response = await self._run(self._c().list_object_versions, **kwargs)
            count += len(response.get("Versions", []))
            count += len(response.get("DeleteMarkers", []))
            if not response.get("IsTruncated"):
                return count
            key_marker = response.get("NextKeyMarker")
            version_marker = response.get("NextVersionIdMarker")
            if key_marker is None:
                raise RuntimeError(
                    "object-version census returned a truncated page without a cursor"
                )

    async def _delete_versions(self, prefix: str) -> int:
        deleted = 0
        while True:
            # Re-read the first remaining page after each delete; advancing a stale cursor can skip
            # versions on S3-compatible implementations with positional pagination.
            response = await self._run(
                self._c().list_object_versions,
                Bucket=self._bucket,
                Prefix=prefix,
                MaxKeys=1000,
            )
            objects = [
                {"Key": item["Key"], "VersionId": item["VersionId"]}
                for item in response.get("Versions", []) + response.get("DeleteMarkers", [])
            ]
            if not objects:
                return deleted
            result = await self._run(
                self._c().delete_objects,
                Bucket=self._bucket,
                Delete={"Objects": objects, "Quiet": True},
            )
            if result.get("Errors"):
                raise RuntimeError("object storage reported an incomplete version delete")
            deleted += len(objects)

    async def count_prefix(self, prefix: str) -> int:
        if await self._is_versioned():
            return await self._count_versions(prefix)
        count = 0
        token: Optional[str] = None
        while True:
            kwargs = {
                "Bucket": self._bucket,
                "Prefix": prefix,
                "MaxKeys": 1000,
            }
            if token:
                kwargs["ContinuationToken"] = token
            response = await self._run(self._c().list_objects_v2, **kwargs)
            count += len(response.get("Contents", []))
            if not response.get("IsTruncated"):
                return count
            token = response.get("NextContinuationToken")
            if not token:
                raise RuntimeError("object census returned a truncated page without a cursor")

    async def delete_prefix(self, prefix: str) -> int:
        if await self._is_versioned():
            return await self._delete_versions(prefix)
        deleted = 0
        while True:
            # Always read the first remaining page. Continuing from a token after deleting earlier
            # pages can skip keys on S3-compatible implementations whose cursor is positional.
            response = await self._run(
                self._c().list_objects_v2,
                Bucket=self._bucket,
                Prefix=prefix,
                MaxKeys=1000,
            )
            keys = [obj["Key"] for obj in response.get("Contents", [])]
            if not keys:
                return deleted
            result = await self._run(
                self._c().delete_objects,
                Bucket=self._bucket,
                Delete={"Objects": [{"Key": key} for key in keys], "Quiet": True},
            )
            if result.get("Errors"):
                raise RuntimeError("object storage reported an incomplete prefix delete")
            deleted += len(keys)
