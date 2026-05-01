"""Alembic environment for stock-platform database migrations."""

from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from stock_platform.config import get_settings
from stock_platform.db.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def include_object(object_, name, type_, reflected, compare_to):
    """Reduce noisy SQLite autogenerate diffs for legacy baseline checks."""
    if type_ in {"index", "unique_constraint"} and name and name.startswith("uq_"):
        return False
    return not (
        type_ == "column"
        and name == "updated_at"
        and getattr(object_, "table", None) is not None
        and object_.table.name == "signal_audit"
    )


def _database_url() -> str:
    """Return the DB URL passed by code or loaded from project settings."""
    override = config.attributes.get("database_url")
    if override:
        return str(override)
    return get_settings().database_url


def run_migrations_offline() -> None:
    """Run migrations without opening a DB connection."""
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations with a live DB connection."""
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = _database_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
