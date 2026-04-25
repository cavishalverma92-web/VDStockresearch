"""SQLAlchemy engine/session helpers."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session

from stock_platform.config import get_settings
from stock_platform.db.models import Base


def get_engine(database_url: str | None = None) -> Engine:
    """Create a SQLAlchemy engine from settings or an override."""
    url = database_url or get_settings().database_url
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    return create_engine(url, connect_args=connect_args)


def create_all_tables(engine: Engine | None = None) -> None:
    """Create tables for local MVP use."""
    Base.metadata.create_all(engine or get_engine())


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
