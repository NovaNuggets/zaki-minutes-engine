"""One-shot idempotent schema convergence for deployment migration Jobs."""

from __future__ import annotations

import asyncio

from .__main__ import _database_url
from .app import db as app_db
from .schema.models import Base
from .schema.sync import ensure_schema


async def migrate_schema() -> None:
    """Bind the production database and run the same monotonic convergence used at startup."""

    app_db.configure(_database_url())
    await ensure_schema(app_db.get_engine(), Base)


def main() -> None:
    asyncio.run(migrate_schema())


if __name__ == "__main__":
    main()
