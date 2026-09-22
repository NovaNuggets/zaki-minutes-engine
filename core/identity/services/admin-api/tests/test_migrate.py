"""One-shot production schema migration entrypoint."""

from __future__ import annotations

import asyncio


def test_migration_entrypoint_configures_the_database_and_converges_schema(monkeypatch):
    from admin_api import migrate

    engine = object()
    configured: list[str] = []
    converged: list[tuple[object, object]] = []

    monkeypatch.setattr(migrate, "_database_url", lambda: "postgresql+asyncpg://db/migration")
    monkeypatch.setattr(migrate.app_db, "configure", configured.append)
    monkeypatch.setattr(migrate.app_db, "get_engine", lambda: engine)

    async def converge(actual_engine, base):
        converged.append((actual_engine, base))

    monkeypatch.setattr(migrate, "ensure_schema", converge)

    asyncio.run(migrate.migrate_schema())

    assert configured == ["postgresql+asyncpg://db/migration"]
    assert converged == [(engine, migrate.Base)]
