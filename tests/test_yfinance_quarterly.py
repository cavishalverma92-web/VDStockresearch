"""Tests for quarterly fundamentals support in the yfinance provider."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from stock_platform.data.providers.yfinance_fundamentals import (
    YFinanceFundamentalsProvider,
    _fiscal_quarter,
)


def test_fiscal_quarter_mapping():
    assert _fiscal_quarter(pd.Timestamp("2025-06-30")) == 1  # Apr–Jun
    assert _fiscal_quarter(pd.Timestamp("2025-09-30")) == 2  # Jul–Sep
    assert _fiscal_quarter(pd.Timestamp("2025-12-31")) == 3  # Oct–Dec
    assert _fiscal_quarter(pd.Timestamp("2025-03-31")) == 4  # Jan–Mar
    assert _fiscal_quarter(pd.Timestamp("2025-01-31")) == 4


def _make_quarterly_income(dates: list[str]) -> pd.DataFrame:
    cols = pd.to_datetime(dates)
    return pd.DataFrame(
        {
            "Total Revenue": [400_000, 380_000, 360_000, 340_000],
            "EBITDA": [80_000, 75_000, 70_000, 65_000],
            "Net Income": [40_000, 38_000, 36_000, 34_000],
            "Diluted EPS": [4.0, 3.8, 3.6, 3.4],
        },
        index=cols[:4],
    ).T


def _mock_quarterly_ticker() -> MagicMock:
    dates = ["2025-06-30", "2025-03-31", "2024-12-31", "2024-09-30"]
    ticker = MagicMock()
    # Annual statements (used by other tests; provide minimal values)
    ticker.income_stmt = pd.DataFrame()
    ticker.balance_sheet = pd.DataFrame()
    ticker.cashflow = pd.DataFrame()
    # Quarterly statements
    ticker.quarterly_income_stmt = _make_quarterly_income(dates)
    ticker.quarterly_balance_sheet = pd.DataFrame()
    ticker.quarterly_cashflow = pd.DataFrame()
    ticker.info = {"sector": "IT", "industry": "Software"}
    return ticker


@patch("stock_platform.data.providers.yfinance_fundamentals.yf.Ticker")
def test_get_quarterly_fundamentals_returns_rows(mock_cls):
    mock_cls.return_value = _mock_quarterly_ticker()
    provider = YFinanceFundamentalsProvider()
    frame = provider.get_quarterly_fundamentals("TCS.NS")
    assert not frame.empty
    assert "fiscal_quarter" in frame.columns
    assert "revenue" in frame.columns
    # Sorted ascending
    pairs = list(zip(frame["fiscal_year"], frame["fiscal_quarter"], strict=False))
    assert pairs == sorted(pairs)


@patch("stock_platform.data.providers.yfinance_fundamentals.yf.Ticker")
def test_get_quarterly_fundamentals_fiscal_year_q4_jan_mar(mock_cls):
    mock_cls.return_value = _mock_quarterly_ticker()
    provider = YFinanceFundamentalsProvider()
    frame = provider.get_quarterly_fundamentals("TCS.NS")
    # Mar 2025 should be FY2025 / Q4
    march = frame[(frame["fiscal_year"] == 2025) & (frame["fiscal_quarter"] == 4)]
    assert not march.empty


@patch("stock_platform.data.providers.yfinance_fundamentals.yf.Ticker")
def test_get_quarterly_snapshots_typed(mock_cls):
    mock_cls.return_value = _mock_quarterly_ticker()
    provider = YFinanceFundamentalsProvider()
    snaps = provider.get_quarterly_snapshots("TCS.NS")
    assert len(snaps) == 4
    assert snaps[0].symbol == "TCS.NS"
    assert all(s.fiscal_quarter in (1, 2, 3, 4) for s in snaps)


@patch("stock_platform.data.providers.yfinance_fundamentals.yf.Ticker")
def test_get_quarterly_fundamentals_empty_when_no_data(mock_cls):
    ticker = MagicMock()
    ticker.income_stmt = pd.DataFrame()
    ticker.balance_sheet = pd.DataFrame()
    ticker.cashflow = pd.DataFrame()
    ticker.quarterly_income_stmt = pd.DataFrame()
    ticker.quarterly_balance_sheet = pd.DataFrame()
    ticker.quarterly_cashflow = pd.DataFrame()
    ticker.info = {}
    mock_cls.return_value = ticker
    provider = YFinanceFundamentalsProvider()
    assert provider.get_quarterly_fundamentals("X.NS").empty
