"""Alembic migration helpers.

Alembic is the reviewable database-change log. The app still supports
in-memory SQLite in tests, but real local databases should move through
migrations so schema changes are predictable.
"""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, inspect, text

from stock_platform.config import ROOT_DIR, get_settings
from stock_platform.db.models import Base

ALEMBIC_INI = ROOT_DIR / "alembic.ini"

LEGACY_SQLITE_COLUMNS: dict[str, tuple[tuple[str, str], ...]] = {
    "stock_universe": (
        ("market_cap", "FLOAT"),
        ("listing_date", "DATE"),
        ("delisting_date", "DATE"),
        ("index_membership", "TEXT"),
        ("index_entry_date", "DATE"),
        ("index_exit_date", "DATE"),
    ),
    "fundamentals_annual": (
        ("ebitda", "FLOAT"),
        ("eps", "FLOAT"),
        ("book_value", "FLOAT"),
        ("free_cash_flow", "FLOAT"),
        ("debt", "FLOAT"),
        ("net_debt", "FLOAT"),
        ("cash_and_equivalents", "FLOAT"),
        ("enterprise_value", "FLOAT"),
    ),
    "fundamentals_quarterly": (
        ("ebitda", "FLOAT"),
        ("eps", "FLOAT"),
        ("free_cash_flow", "FLOAT"),
    ),
}


def alembic_config(database_url: str | None = None) -> Config:
    """Build an Alembic config without exposing secrets in source files."""
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("script_location", str(ROOT_DIR / "alembic"))
    config.attributes["database_url"] = database_url or get_settings().database_url
    return config


def run_migrations(database_url: str | None = None) -> None:
    """Upgrade the configured database to the latest migration revision."""
    command.upgrade(alembic_config(database_url), "head")


def stamp_existing_database(engine: Engine, revision: str = "head") -> bool:
    """Mark an existing unversioned database as migrated.

    This is only for the first migration rollout. If a user already has a local
    SQLite DB created by ``Base.metadata.create_all()``, running the baseline
    migration directly would try to create tables that already exist. We first
    ensure any missing current tables exist, then stamp the DB at the baseline.

    Returns True when stamping happened.
    """
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    app_tables = table_names - {"alembic_version"}
    if not app_tables or "alembic_version" in table_names:
        return False

    Base.metadata.create_all(engine)
    command.stamp(alembic_config(str(engine.url)), revision)
    return True


def repair_legacy_sqlite_schema(engine: Engine) -> None:
    """Add missing nullable baseline columns to older local SQLite DBs.

    SQLite cannot easily alter existing constraints, but adding nullable columns
    is safe and keeps old local prototype databases usable after the baseline
    migration rollout.
    """
    if engine.url.get_backend_name() != "sqlite":
        return

    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())

    with engine.begin() as connection:
        for table_name, columns in LEGACY_SQLITE_COLUMNS.items():
            if table_name not in table_names:
                continue
            existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
            for column_name, column_type in columns:
                if column_name not in existing_columns:
                    connection.execute(
                        text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
                    )


def is_in_memory_sqlite(engine: Engine) -> bool:
    """Return True for the fast temporary DBs used heavily in tests."""
    return engine.url.get_backend_name() == "sqlite" and str(engine.url.database) in {
        "",
        ":memory:",
        "None",
    }


def ensure_parent_directory_for_sqlite(database_url: str) -> None:
    """Create the parent folder for a file-based SQLite database if needed."""
    if not database_url.startswith("sqlite:///"):
        return
    sqlite_path = database_url.replace("sqlite:///", "", 1)
    if sqlite_path in {"", ":memory:"}:
        return
    path = Path(sqlite_path)
    if not path.is_absolute():
        path = ROOT_DIR / path
    path.parent.mkdir(parents=True, exist_ok=True)
