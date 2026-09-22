"""Live-PostgreSQL regressions for retention selection SQL.

This optional backing-store test runs with the admin-api testcontainers environment and skips in
the offline meeting-api unit environment where SQLAlchemy/testcontainers are intentionally absent.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import shutil
import subprocess

import pytest


def _docker_ok() -> bool:
    return bool(shutil.which("docker")) and subprocess.run(
        ["docker", "info"], capture_output=True
    ).returncode == 0


@pytest.mark.skipif(not _docker_ok(), reason="docker daemon not available")
def test_postgres_ttl_selection_marks_malformed_expiry_due_and_returns_healthy_tenant():
    sqlalchemy = pytest.importorskip("sqlalchemy")
    pytest.importorskip("asyncpg")
    PostgresContainer = pytest.importorskip("testcontainers.postgres").PostgresContainer
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from meeting_api.retention.ttl_adapters import SqlAlchemyTtlStore

    now = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)

    async def exercise(async_url: str):
        engine = create_async_engine(async_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sqlalchemy.text(
                        "CREATE TABLE meetings ("
                        "id integer PRIMARY KEY, user_id integer NOT NULL, "
                        "status text NOT NULL, data jsonb NOT NULL)"
                    )
                )
                await connection.execute(
                    sqlalchemy.text(
                        "INSERT INTO meetings (id, user_id, status, data) "
                        "VALUES (:id, :user_id, 'completed', CAST(:data AS jsonb))"
                    ),
                    [
                        {
                            "id": 1,
                            "user_id": 11,
                            "data": json.dumps(
                                {
                                    "zaki_retention": {
                                        "scope_expiries": {
                                            "audio": "not-a-timestamp",
                                            "transcript": (now + timedelta(days=1)).isoformat(),
                                            "summary": (now + timedelta(days=1)).isoformat(),
                                        }
                                    }
                                }
                            ),
                        },
                        {
                            "id": 2,
                            "user_id": 22,
                            "data": json.dumps(
                                {
                                    "zaki_retention": {
                                        "scope_expiries": {
                                            "audio": (now - timedelta(minutes=1)).isoformat(),
                                            "transcript": (now + timedelta(days=1)).isoformat(),
                                            "summary": (now + timedelta(days=1)).isoformat(),
                                        }
                                    }
                                }
                            ),
                        },
                        {
                            "id": 3,
                            "user_id": 33,
                            "data": json.dumps(
                                {
                                    "zaki_retention": {
                                        "scope_expiries": {
                                            "audio": (now - timedelta(days=2)).isoformat(),
                                            "transcript": (now + timedelta(days=1)).isoformat(),
                                            "summary": (now + timedelta(days=1)).isoformat(),
                                        },
                                        "ttl_retry_after": {
                                            "audio": (now - timedelta(minutes=1)).isoformat()
                                        },
                                    }
                                }
                            ),
                        },
                        {
                            "id": 4,
                            "user_id": 44,
                            "data": json.dumps(
                                {
                                    "zaki_capture": {
                                        "state": "authorized",
                                        "bot_name": "ZAKI Notetaker",
                                    },
                                    "summary": {"text": "must be fail-safe purged"},
                                }
                            ),
                        },
                    ],
                )

            store = SqlAlchemyTtlStore(
                async_sessionmaker(engine, expire_on_commit=False),
                object_storage=None,
            )
            due = await store.list_due_scopes(now=now, limit=5)

            assert [
                (item.user_id, item.meeting_id, item.scope, item.expiry_invalid)
                for item in due
            ] == [
                ("11", "1", "audio", True),
                ("44", "4", "audio", True),
                ("44", "4", "summary", True),
                ("44", "4", "transcript", True),
                ("22", "2", "audio", False),
            ]
        finally:
            await engine.dispose()

    with PostgresContainer("postgres:16-alpine", driver="psycopg") as postgres:
        async_url = postgres.get_connection_url().replace(
            "postgresql+psycopg://", "postgresql+asyncpg://"
        )
        asyncio.run(exercise(async_url))
