"""SQLAlchemy models for the platform database.

Phase 1 starts with the stock universe and fundamentals tables. The models are
source-aware so every displayed metric can later show where it came from and how
fresh it is.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp for provenance fields."""
    return datetime.now(UTC)


class Base(DeclarativeBase):
    """Base class for all ORM models."""


class StockUniverse(Base):
    """One tradable equity in the analysis universe."""

    __tablename__ = "stock_universe"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), unique=True, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    exchange: Mapped[str] = mapped_column(String(16), nullable=False, default="NSE")
    isin: Mapped[str | None] = mapped_column(String(32), unique=True)
    sector: Mapped[str | None] = mapped_column(String(120), index=True)
    industry: Mapped[str | None] = mapped_column(String(160))
    market_cap: Mapped[float | None] = mapped_column(Float)
    market_cap_bucket: Mapped[str | None] = mapped_column(String(32), index=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    listing_date: Mapped[date | None] = mapped_column(Date)
    delisting_date: Mapped[date | None] = mapped_column(Date)
    index_membership: Mapped[str | None] = mapped_column(Text)
    index_entry_date: Mapped[date | None] = mapped_column(Date)
    index_exit_date: Mapped[date | None] = mapped_column(Date)
    source: Mapped[str] = mapped_column(String(80), nullable=False, default="manual")
    source_url: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    annual_fundamentals: Mapped[list[FundamentalsAnnual]] = relationship(
        back_populates="stock",
        cascade="all, delete-orphan",
    )
    quarterly_fundamentals: Mapped[list[FundamentalsQuarterly]] = relationship(
        back_populates="stock",
        cascade="all, delete-orphan",
    )


class FundamentalsAnnual(Base):
    """Annual financial statement facts normalized to a common schema."""

    __tablename__ = "fundamentals_annual"
    __table_args__ = (
        UniqueConstraint("symbol", "fiscal_year", "source", name="uq_fundamentals_annual"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(
        ForeignKey("stock_universe.symbol"),
        index=True,
        nullable=False,
    )
    fiscal_year: Mapped[int] = mapped_column(Integer, nullable=False)
    period_end: Mapped[date | None] = mapped_column(Date)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="INR")

    revenue: Mapped[float | None] = mapped_column(Float)
    gross_profit: Mapped[float | None] = mapped_column(Float)
    ebitda: Mapped[float | None] = mapped_column(Float)
    ebit: Mapped[float | None] = mapped_column(Float)
    net_income: Mapped[float | None] = mapped_column(Float)
    eps: Mapped[float | None] = mapped_column(Float)
    book_value: Mapped[float | None] = mapped_column(Float)
    operating_cash_flow: Mapped[float | None] = mapped_column(Float)
    capital_expenditure: Mapped[float | None] = mapped_column(Float)
    free_cash_flow: Mapped[float | None] = mapped_column(Float)
    debt: Mapped[float | None] = mapped_column(Float)
    net_debt: Mapped[float | None] = mapped_column(Float)
    cash_and_equivalents: Mapped[float | None] = mapped_column(Float)
    total_assets: Mapped[float | None] = mapped_column(Float)
    total_liabilities: Mapped[float | None] = mapped_column(Float)
    current_assets: Mapped[float | None] = mapped_column(Float)
    current_liabilities: Mapped[float | None] = mapped_column(Float)
    retained_earnings: Mapped[float | None] = mapped_column(Float)
    shares_outstanding: Mapped[float | None] = mapped_column(Float)
    market_cap: Mapped[float | None] = mapped_column(Float)
    enterprise_value: Mapped[float | None] = mapped_column(Float)

    source: Mapped[str] = mapped_column(String(80), nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    stock: Mapped[StockUniverse] = relationship(back_populates="annual_fundamentals")


class FundamentalsQuarterly(Base):
    """Quarterly financial statement facts normalized to a common schema."""

    __tablename__ = "fundamentals_quarterly"
    __table_args__ = (
        UniqueConstraint(
            "symbol",
            "fiscal_year",
            "fiscal_quarter",
            "source",
            name="uq_fundamentals_quarterly",
        ),
        CheckConstraint("fiscal_quarter between 1 and 4", name="ck_fiscal_quarter_range"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(
        ForeignKey("stock_universe.symbol"),
        index=True,
        nullable=False,
    )
    fiscal_year: Mapped[int] = mapped_column(Integer, nullable=False)
    fiscal_quarter: Mapped[int] = mapped_column(Integer, nullable=False)
    period_end: Mapped[date | None] = mapped_column(Date)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="INR")

    revenue: Mapped[float | None] = mapped_column(Float)
    ebitda: Mapped[float | None] = mapped_column(Float)
    ebit: Mapped[float | None] = mapped_column(Float)
    net_income: Mapped[float | None] = mapped_column(Float)
    eps: Mapped[float | None] = mapped_column(Float)
    operating_cash_flow: Mapped[float | None] = mapped_column(Float)
    free_cash_flow: Mapped[float | None] = mapped_column(Float)
    total_assets: Mapped[float | None] = mapped_column(Float)
    total_liabilities: Mapped[float | None] = mapped_column(Float)
    shares_outstanding: Mapped[float | None] = mapped_column(Float)

    source: Mapped[str] = mapped_column(String(80), nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    stock: Mapped[StockUniverse] = relationship(back_populates="quarterly_fundamentals")


class DeliveryData(Base):
    """Daily NSE delivery percentage for one equity symbol."""

    __tablename__ = "delivery_data"
    __table_args__ = (UniqueConstraint("symbol", "trade_date", name="uq_delivery_data"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, index=True, nullable=False)
    series: Mapped[str | None] = mapped_column(String(8))
    traded_qty: Mapped[float | None] = mapped_column(Float)
    deliverable_qty: Mapped[float | None] = mapped_column(Float)
    delivery_pct: Mapped[float | None] = mapped_column(Float)
    turnover_lacs: Mapped[float | None] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String(80), nullable=False, default="nse_bhavcopy")
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class BulkBlockDeal(Base):
    """One bulk or block deal reported by NSE."""

    __tablename__ = "bulk_block_deals"
    __table_args__ = (
        UniqueConstraint(
            "symbol",
            "deal_date",
            "client_name",
            "deal_type",
            "quantity",
            name="uq_bulk_block_deal",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    deal_date: Mapped[date] = mapped_column(Date, index=True, nullable=False)
    client_name: Mapped[str | None] = mapped_column(String(255))
    buy_sell: Mapped[str | None] = mapped_column(String(8))
    quantity: Mapped[float | None] = mapped_column(Float)
    price: Mapped[float | None] = mapped_column(Float)
    deal_type: Mapped[str] = mapped_column(String(16), nullable=False)
    source: Mapped[str] = mapped_column(String(80), nullable=False, default="nse")
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class CorporateAction(Base):
    """Dividend, split, bonus, or earnings event for one symbol."""

    __tablename__ = "corporate_actions"
    __table_args__ = (
        UniqueConstraint("symbol", "ex_date", "action_type", name="uq_corporate_action"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    ex_date: Mapped[date] = mapped_column(Date, index=True, nullable=False)
    action_type: Mapped[str] = mapped_column(String(32), nullable=False)
    value: Mapped[float | None] = mapped_column(Float)
    detail: Mapped[str | None] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(80), nullable=False, default="yfinance")
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class SignalAudit(Base):
    """Audit trail for every observed technical signal scan."""

    __tablename__ = "signal_audit"
    __table_args__ = (
        UniqueConstraint(
            "symbol",
            "as_of_date",
            "signal_name",
            "source",
            name="uq_signal_audit_observation",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    as_of_date: Mapped[date] = mapped_column(Date, index=True, nullable=False)
    signal_name: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False)
    strength: Mapped[str] = mapped_column(String(40), nullable=False, default="info")
    detail: Mapped[str] = mapped_column(Text, nullable=False)
    trigger_price: Mapped[float | None] = mapped_column(Float)
    entry_zone_low: Mapped[float | None] = mapped_column(Float)
    entry_zone_high: Mapped[float | None] = mapped_column(Float)
    stop_loss: Mapped[float | None] = mapped_column(Float)
    target_price: Mapped[float | None] = mapped_column(Float)
    risk_reward: Mapped[float | None] = mapped_column(Float)
    confidence: Mapped[float | None] = mapped_column(Float)
    close: Mapped[float | None] = mapped_column(Float)
    rsi_14: Mapped[float | None] = mapped_column(Float)
    ema_20: Mapped[float | None] = mapped_column(Float)
    ema_50: Mapped[float | None] = mapped_column(Float)
    ema_200: Mapped[float | None] = mapped_column(Float)
    relative_volume: Mapped[float | None] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String(80), nullable=False, default="yfinance")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )
    scan_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class UniverseScanRun(Base):
    """One saved Phase 8 universe scanner run."""

    __tablename__ = "universe_scan_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    universe_name: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    requested_symbols: Mapped[int] = mapped_column(Integer, nullable=False)
    successful_symbols: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_symbols: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lookback_days: Mapped[int] = mapped_column(Integer, nullable=False)
    min_score_filter: Mapped[float | None] = mapped_column(Float)
    min_signals_filter: Mapped[int | None] = mapped_column(Integer)
    source: Mapped[str] = mapped_column(String(80), nullable=False, default="yfinance")
    note: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    results: Mapped[list[UniverseScanResult]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
    )


class UniverseScanResult(Base):
    """One symbol result from a saved universe scanner run."""

    __tablename__ = "universe_scan_results"
    __table_args__ = (UniqueConstraint("run_id", "symbol", name="uq_universe_scan_result_symbol"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("universe_scan_runs.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    composite_score: Mapped[float | None] = mapped_column(Float)
    band: Mapped[str | None] = mapped_column(String(40))
    fundamentals_score: Mapped[float | None] = mapped_column(Float)
    technicals_score: Mapped[float | None] = mapped_column(Float)
    flows_score: Mapped[float | None] = mapped_column(Float)
    events_quality_score: Mapped[float | None] = mapped_column(Float)
    macro_sector_score: Mapped[float | None] = mapped_column(Float)
    active_signal_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    active_signals_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    last_close: Mapped[float | None] = mapped_column(Float)
    rsi_14: Mapped[float | None] = mapped_column(Float)
    ma_stack: Mapped[str | None] = mapped_column(String(40))
    data_quality_warnings_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    run: Mapped[UniverseScanRun] = relationship(back_populates="results")


class ResearchWatchlistItem(Base):
    """One symbol saved by the user for follow-up research."""

    __tablename__ = "research_watchlist_items"
    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "watchlist_name",
            "symbol",
            name="uq_research_watchlist_item",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(80), nullable=False, default="local")
    watchlist_name: Mapped[str] = mapped_column(
        String(120),
        index=True,
        nullable=False,
        default="research_shortlist",
    )
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    source_universe: Mapped[str | None] = mapped_column(String(120))
    source_run_id: Mapped[int | None] = mapped_column(ForeignKey("universe_scan_runs.id"))
    reason: Mapped[str | None] = mapped_column(Text)
    review_status: Mapped[str] = mapped_column(String(40), nullable=False, default="watch")
    tags: Mapped[str] = mapped_column(String(240), nullable=False, default="")
    notes: Mapped[str] = mapped_column(Text, nullable=False, default="")
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )
