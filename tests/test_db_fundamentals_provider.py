"""Tests for DbFundamentalsProvider — DB-backed read with live fallback."""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from stock_platform.data.providers.base import FundamentalsDataProvider
from stock_platform.data.providers.db_fundamentals import DbFundamentalsProvider
from stock_platform.data.repositories.fundamentals import upsert_fundamentals_annual
from stock_platform.db.models import Base, FundamentalsAnnual


class _StubLive(FundamentalsDataProvider):
    name = "yfinance"

    def __init__(self) -> None:
        self.annual_calls = 0
        self.quarterly_calls = 0

    def get_annual_fundamentals(self, symbol: str) -> pd.DataFrame:
        self.annual_calls += 1
        return pd.DataFrame(
            {
                "fiscal_year": [2025],
                "revenue": [1200.0],
                "net_income": [150.0],
                "total_assets": [5500.0],
                "shares_outstanding": [10.0],
                "source": ["yfinance"],
            }
        )

    def get_quarterly_fundamentals(self, symbol: str) -> pd.DataFrame:
        self.quarterly_calls += 1
        return pd.DataFrame()

    def get_income_statement(self, symbol):
        return pd.DataFrame()

    def get_balance_sheet(self, symbol):
        return pd.DataFrame()

    def get_cash_flow(self, symbol):
        return pd.DataFrame()


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


def test_db_provider_returns_persisted_data_without_fallback(engine) -> None:
    with Session(engine) as session:
        upsert_fundamentals_annual(
            session,
            "RELIANCE",
            pd.DataFrame({"fiscal_year": [2024], "revenue": [1000.0]}),
            source="yfinance",
        )
        session.commit()

    live = _StubLive()
    provider = DbFundamentalsProvider(fallback=live, engine=engine)
    frame = provider.get_annual_fundamentals("RELIANCE")
    assert not frame.empty
    assert live.annual_calls == 0  # served from DB


def test_db_provider_falls_back_and_writes_through(engine) -> None:
    live = _StubLive()
    provider = DbFundamentalsProvider(fallback=live, engine=engine)

    frame = provider.get_annual_fundamentals("RELIANCE")
    assert not frame.empty
    assert live.annual_calls == 1

    with Session(engine) as session:
        rows = session.query(FundamentalsAnnual).all()
    assert len(rows) == 1
    assert rows[0].source == "yfinance"


def test_db_provider_no_fallback_returns_empty(engine) -> None:
    provider = DbFundamentalsProvider(fallback=None, engine=engine)
    assert provider.get_annual_fundamentals("MISSING").empty
