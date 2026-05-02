"""Tests for Phase 1 database models."""

from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from stock_platform.db.models import (
    Base,
    FundamentalsAnnual,
    FundamentalsQuarterly,
    IndexMembershipHistory,
    StockUniverse,
)


def test_stock_universe_and_fundamentals_tables_can_be_created() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        stock = StockUniverse(
            symbol="RELIANCE.NS",
            name="Reliance Industries",
            exchange="NSE",
            isin="INE002A01018",
            sector="Energy",
            industry="Oil & Gas",
            market_cap=2_100_000,
            market_cap_bucket="large_cap",
            listing_date=date(1977, 1, 1),
            index_membership="Nifty 50",
            index_entry_date=date(1995, 1, 1),
            source="manual",
        )
        stock.annual_fundamentals.append(
            FundamentalsAnnual(
                symbol="RELIANCE.NS",
                fiscal_year=2025,
                period_end=date(2025, 3, 31),
                revenue=1_000,
                ebitda=180,
                net_income=100,
                eps=10,
                book_value=1_100,
                free_cash_flow=80,
                debt=700,
                cash_and_equivalents=100,
                total_assets=2_000,
                total_liabilities=900,
                enterprise_value=2_600,
                source="sample",
            )
        )
        stock.quarterly_fundamentals.append(
            FundamentalsQuarterly(
                symbol="RELIANCE.NS",
                fiscal_year=2025,
                fiscal_quarter=4,
                period_end=date(2025, 3, 31),
                revenue=260,
                ebitda=50,
                net_income=28,
                eps=2.8,
                free_cash_flow=20,
                source="sample",
            )
        )
        session.add(stock)
        session.commit()

    with Session(engine) as session:
        stored = session.scalar(select(StockUniverse).where(StockUniverse.symbol == "RELIANCE.NS"))

        assert stored is not None
        assert stored.name == "Reliance Industries"
        assert stored.market_cap == 2_100_000
        assert stored.index_membership == "Nifty 50"
        assert len(stored.annual_fundamentals) == 1
        assert len(stored.quarterly_fundamentals) == 1
        assert stored.annual_fundamentals[0].source == "sample"
        assert stored.annual_fundamentals[0].enterprise_value == 2_600


def test_index_membership_history_table_can_be_created() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        row = IndexMembershipHistory(
            index_name="Nifty 50",
            symbol="RELIANCE.NS",
            company_name="Reliance Industries Ltd.",
            industry="Oil Gas & Consumable Fuels",
            isin="INE002A01018",
            from_date=date(2026, 5, 3),
            source="nse_index_csv",
            source_url="https://archives.nseindia.com/content/indices/ind_nifty50list.csv",
        )
        session.add(row)
        session.commit()

    with Session(engine) as session:
        stored = session.scalar(
            select(IndexMembershipHistory).where(IndexMembershipHistory.symbol == "RELIANCE.NS")
        )
        assert stored is not None
        assert stored.index_name == "Nifty 50"
        assert stored.active is True
        assert stored.to_date is None
