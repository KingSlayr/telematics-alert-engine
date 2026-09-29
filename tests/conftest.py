import os

# Must be set before any app module is imported.
# Tests run against their own database file.
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_telematics.db"

import pytest
from sqlalchemy import text

from app.db import Base, engine, init_db


@pytest.fixture(autouse=True)
async def clean_db():
    """Fresh tables for every test."""
    await init_db()

    yield

    async with engine.begin() as connection:
        # Drop everything so tests never see each other's data.
        for table in reversed(Base.metadata.sorted_tables):
            await connection.execute(text(f'DROP TABLE IF EXISTS "{table.name}"'))
