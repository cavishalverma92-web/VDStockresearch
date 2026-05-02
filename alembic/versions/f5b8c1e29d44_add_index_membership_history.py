"""add index membership history.

Revision ID: f5b8c1e29d44
Revises: b4f9d85b73e3
Create Date: 2026-05-03 01:08:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f5b8c1e29d44"
down_revision: str | None = "b4f9d85b73e3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "index_membership_history",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("index_name", sa.String(length=120), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("company_name", sa.String(length=255), nullable=True),
        sa.Column("industry", sa.String(length=160), nullable=True),
        sa.Column("isin", sa.String(length=32), nullable=True),
        sa.Column("from_date", sa.Date(), nullable=False),
        sa.Column("to_date", sa.Date(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("source", sa.String(length=80), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "index_name",
            "symbol",
            "from_date",
            "source",
            name="uq_index_membership_period",
        ),
    )
    op.create_index(
        op.f("ix_index_membership_history_from_date"),
        "index_membership_history",
        ["from_date"],
        unique=False,
    )
    op.create_index(
        op.f("ix_index_membership_history_index_name"),
        "index_membership_history",
        ["index_name"],
        unique=False,
    )
    op.create_index(
        op.f("ix_index_membership_history_symbol"),
        "index_membership_history",
        ["symbol"],
        unique=False,
    )
    op.create_index(
        op.f("ix_index_membership_history_to_date"),
        "index_membership_history",
        ["to_date"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_index_membership_history_to_date"),
        table_name="index_membership_history",
    )
    op.drop_index(
        op.f("ix_index_membership_history_symbol"),
        table_name="index_membership_history",
    )
    op.drop_index(
        op.f("ix_index_membership_history_index_name"),
        table_name="index_membership_history",
    )
    op.drop_index(
        op.f("ix_index_membership_history_from_date"),
        table_name="index_membership_history",
    )
    op.drop_table("index_membership_history")
