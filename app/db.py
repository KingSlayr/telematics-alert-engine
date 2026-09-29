from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import DATABASE_URL

engine = create_async_engine(DATABASE_URL)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine.sync_engine, "connect")
    def _sqlite_pragmas(dbapi_connection, record):
        # Wait instead of failing when another worker holds the write lock.
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.close()


class Base(DeclarativeBase):
    pass


def utcnow():
    """Naive UTC timestamp. SQLite stores naive datetimes, so we use UTC everywhere."""
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(tzinfo=None)


async def get_db():
    async with SessionLocal() as session:
        yield session


async def init_db():
    # Import so SQLAlchemy registers all models on Base.metadata.
    from app import models  # noqa: F401

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

        # SQLite only: readers don't block the writer while workers commit alerts.
        if DATABASE_URL.startswith("sqlite"):
            await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
