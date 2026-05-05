"""add strategy scan persistence.

Revision ID: c83e1f4d9a21
Revises: a7c2e914d6f2
Create Date: 2026-05-06 01:10:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c83e1f4d9a21"
down_revision: str | None = "a7c2e914d6f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "strategy_scan_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("universe_name", sa.String(length=120), nullable=False),
        sa.Column("requested_symbols", sa.Integer(), nullable=False),
        sa.Column("scanned_symbols", sa.Integer(), nullable=False),
        sa.Column("failed_symbols", sa.Integer(), nullable=False),
        sa.Column("result_count", sa.Integer(), nullable=False),
        sa.Column("min_confidence_filter", sa.Float(), nullable=True),
        sa.Column("min_rr_filter", sa.Float(), nullable=True),
        sa.Column("source", sa.String(length=80), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("errors_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_strategy_scan_runs_universe_name"),
        "strategy_scan_runs",
        ["universe_name"],
        unique=False,
    )
    op.create_table(
        "strategy_scan_results",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("company_name", sa.String(length=255), nullable=True),
        sa.Column("sector", sa.String(length=120), nullable=True),
        sa.Column("strategy", sa.String(length=120), nullable=False),
        sa.Column("setup_type", sa.String(length=40), nullable=False),
        sa.Column("signal_date", sa.Date(), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("entry_zone_low", sa.Float(), nullable=True),
        sa.Column("entry_zone_high", sa.Float(), nullable=True),
        sa.Column("stop_loss", sa.Float(), nullable=True),
        sa.Column("target_price", sa.Float(), nullable=True),
        sa.Column("risk_reward", sa.Float(), nullable=True),
        sa.Column("rsi", sa.Float(), nullable=True),
        sa.Column("trend_status", sa.String(length=40), nullable=True),
        sa.Column("relative_volume", sa.Float(), nullable=True),
        sa.Column("atr_pct", sa.Float(), nullable=True),
        sa.Column("liquidity_status", sa.String(length=40), nullable=True),
        sa.Column("data_source", sa.String(length=80), nullable=False),
        sa.Column("data_freshness", sa.String(length=40), nullable=True),
        sa.Column("confidence_score", sa.Float(), nullable=False),
        sa.Column("why_this_appeared", sa.Text(), nullable=False),
        sa.Column("key_risk", sa.Text(), nullable=False),
        sa.Column("data_trust", sa.String(length=40), nullable=False),
        sa.Column("market_cap_bucket", sa.String(length=32), nullable=True),
        sa.Column("ema_20", sa.Float(), nullable=True),
        sa.Column("ema_50", sa.Float(), nullable=True),
        sa.Column("ema_100", sa.Float(), nullable=True),
        sa.Column("ema_200", sa.Float(), nullable=True),
        sa.Column("breakout_level", sa.Float(), nullable=True),
        sa.Column("avg_traded_value_cr", sa.Float(), nullable=True),
        sa.Column("warnings_json", sa.Text(), nullable=False),
        sa.Column("provider_fallback_reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["strategy_scan_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id",
            "symbol",
            "strategy",
            "signal_date",
            name="uq_strategy_scan_result",
        ),
    )
    for column in (
        "run_id",
        "symbol",
        "sector",
        "strategy",
        "setup_type",
        "signal_date",
        "liquidity_status",
        "market_cap_bucket",
    ):
        op.create_index(
            op.f(f"ix_strategy_scan_results_{column}"),
            "strategy_scan_results",
            [column],
            unique=False,
        )


def downgrade() -> None:
    for column in (
        "market_cap_bucket",
        "liquidity_status",
        "signal_date",
        "setup_type",
        "strategy",
        "sector",
        "symbol",
        "run_id",
    ):
        op.drop_index(
            op.f(f"ix_strategy_scan_results_{column}"), table_name="strategy_scan_results"
        )
    op.drop_table("strategy_scan_results")
    op.drop_index(op.f("ix_strategy_scan_runs_universe_name"), table_name="strategy_scan_runs")
    op.drop_table("strategy_scan_runs")
