"""add market_flows_daily.

Revision ID: a7c2e914d6f2
Revises: f5b8c1e29d44
Create Date: 2026-05-04 00:30:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a7c2e914d6f2"
down_revision: str | None = "f5b8c1e29d44"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "market_flows_daily",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("participant", sa.String(length=16), nullable=False),
        sa.Column("buy_value_cr", sa.Float(), nullable=True),
        sa.Column("sell_value_cr", sa.Float(), nullable=True),
        sa.Column("net_value_cr", sa.Float(), nullable=True),
        sa.Column("source", sa.String(length=80), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "trade_date",
            "participant",
            "source",
            name="uq_market_flows_daily",
        ),
    )
    op.create_index(
        op.f("ix_market_flows_daily_trade_date"),
        "market_flows_daily",
        ["trade_date"],
        unique=False,
    )
    op.create_index(
        op.f("ix_market_flows_daily_participant"),
        "market_flows_daily",
        ["participant"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_market_flows_daily_participant"),
        table_name="market_flows_daily",
    )
    op.drop_index(
        op.f("ix_market_flows_daily_trade_date"),
        table_name="market_flows_daily",
    )
    op.drop_table("market_flows_daily")
