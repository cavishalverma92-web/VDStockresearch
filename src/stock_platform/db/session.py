"""SQLAlchemy engine/session helpers."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session

from stock_platform.config import get_settings
from stock_platform.db.migrations import (
    ensure_parent_directory_for_sqlite,
    is_in_memory_sqlite,
    repair_legacy_sqlite_schema,
    run_migrations,
    stamp_existing_database,
)
from stock_platform.db.models import Base


def get_engine(database_url: str | None = None) -> Engine:
    """Create a SQLAlchemy engine from settings or an override."""
    url = database_url or get_settings().database_url
    ensure_parent_directory_for_sqlite(url)
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    return create_engine(url, connect_args=connect_args)


def create_all_tables(engine: Engine | None = None) -> None:
    """Ensure the database schema exists.

    For real local databases this runs Alembic migrations. For in-memory test
    databases it keeps the fast SQLAlchemy ``create_all`` path.
    """
    active_engine = engine or get_engine()
    if is_in_memory_sqlite(active_engine):
        Base.metadata.create_all(active_engine)
        return

    repair_legacy_sqlite_schema(active_engine)
    stamp_existing_database(active_engine)
    run_migrations(str(active_engine.url))


@contextmanager
def get_session(engine: Engine | None = None) -> Iterator[Session]:
    """Yield a session and commit/rollback safely."""
    active_engine = engine or get_engine()
    with Session(active_engine) as session:
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
