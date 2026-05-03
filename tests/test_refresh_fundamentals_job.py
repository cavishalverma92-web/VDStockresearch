"""Tests for refresh_fundamentals job."""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from stock_platform.data.providers.base import FundamentalsDataProvider
from stock_platform.db.models import Base, FundamentalsAnnual, FundamentalsQuarterly
from stock_platform.jobs.refresh_fundamentals import refresh_fundamentals


class _FakeProvider(FundamentalsDataProvider):
    name = "fake"

    def __init__(self, source_label: str = "fake") -> None:
        self._source = source_label

    def get_annual_fundamentals(self, symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "fiscal_year": [2024, 2025],
                "revenue": [1000.0, 1200.0],
                "net_income": [100.0, 150.0],
                "total_assets": [5000.0, 5500.0],
                "shares_outstanding": [10.0, 10.0],
                "source": [self._source, self._source],
            }
        )

    def get_quarterly_fundamentals(self, symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "fiscal_year": [2025, 2025],
                "fiscal_quarter": [3, 4],
                "revenue": [320.0, 340.0],
                "net_income": [32.0, 40.0],
                "source": [self._source, self._source],
            }
        )

    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        return pd.DataFrame()

    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        return pd.DataFrame()

    def get_cash_flow(self, symbol: str) -> pd.DataFrame:
        return pd.DataFrame()


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


def test_refresh_fundamentals_persists_one_source(engine) -> None:
    summary = refresh_fundamentals(
        universe=["RELIANCE.NS"],
        sources=["yfinance"],
        provider_factory=lambda src: _FakeProvider(source_label="yfinance"),
        engine=engine,
    )
    assert summary.successful == 1
    assert summary.failed == 0
    assert summary.annual_rows_upserted == 2
    assert summary.quarterly_rows_upserted == 2

    with Session(engine) as session:
        annual = session.query(FundamentalsAnnual).all()
        quarterly = session.query(FundamentalsQuarterly).all()
    assert len(annual) == 2
    assert len(quarterly) == 2


def test_refresh_fundamentals_two_sources_coexist(engine) -> None:
    summary = refresh_fundamentals(
        universe=["RELIANCE.NS"],
        sources=["yfinance", "screener"],
        provider_factory=lambda src: _FakeProvider(source_label=src),
        engine=engine,
    )
    assert summary.successful == 2  # one per (symbol, source)
    assert summary.annual_rows_upserted == 4  # 2 years × 2 sources

    with Session(engine) as session:
        sources = {row.source for row in session.query(FundamentalsAnnual).all()}
    assert sources == {"yfinance", "screener"}


def test_refresh_fundamentals_dry_run_does_not_write(engine) -> None:
    summary = refresh_fundamentals(
        universe=["RELIANCE.NS"],
        sources=["yfinance"],
        provider_factory=lambda src: _FakeProvider(),
        engine=engine,
        dry_run=True,
    )
    assert summary.dry_run is True
    with Session(engine) as session:
        assert session.query(FundamentalsAnnual).count() == 0


def test_refresh_fundamentals_isolates_provider_failures(engine) -> None:
    class _BoomProvider(FundamentalsDataProvider):
        name = "boom"

        def get_annual_fundamentals(self, symbol: str) -> pd.DataFrame:
            raise RuntimeError("upstream failure")

        def get_quarterly_fundamentals(self, symbol: str) -> pd.DataFrame:
            return pd.DataFrame()

        def get_income_statement(self, symbol):
            return pd.DataFrame()

        def get_balance_sheet(self, symbol):
            return pd.DataFrame()

        def get_cash_flow(self, symbol):
            return pd.DataFrame()

    summary = refresh_fundamentals(
        universe=["BAD.NS", "GOOD.NS"],
        sources=["yfinance"],
        provider_factory=lambda src: _BoomProvider() if src == "yfinance" else _FakeProvider(),
        engine=engine,
    )
    # Both symbols use the boom provider here, both should fail but the run completes
    assert summary.failed == 2
    assert summary.successful == 0
