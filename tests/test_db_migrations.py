from __future__ import annotations

from sqlalchemy import create_engine, inspect

from stock_platform.db import create_all_tables
from stock_platform.db.migrations import run_migrations
from stock_platform.db.models import Base


def test_create_all_tables_uses_alembic_for_file_database(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'stock_platform.db'}"
    engine = create_engine(database_url)

    create_all_tables(engine)

    tables = set(inspect(engine).get_table_names())
    assert "alembic_version" in tables
    assert "stock_universe" in tables
    assert "price_daily" in tables
    assert "composite_scores" in tables
    assert "index_membership_history" in tables


def test_create_all_tables_keeps_fast_in_memory_path() -> None:
    engine = create_engine("sqlite:///:memory:")

    create_all_tables(engine)

    tables = set(inspect(engine).get_table_names())
    assert "alembic_version" not in tables
    assert "stock_universe" in tables
    assert "index_membership_history" in tables


def test_run_migrations_stamps_existing_unversioned_database(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'legacy.db'}"
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)

    create_all_tables(engine)
    run_migrations(database_url)

    tables = set(inspect(engine).get_table_names())
    assert "alembic_version" in tables
    assert "daily_refresh_runs" in tables
