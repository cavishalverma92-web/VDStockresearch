"""Tests for the yfinance fundamentals provider."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from stock_platform.data.providers.yfinance_fundamentals import (
    YFinanceFundamentalsProvider,
    _fiscal_year,
    _market_cap_bucket,
)

# ---------------------------------------------------------------------------
# Helpers to build fake yfinance statement DataFrames
# ---------------------------------------------------------------------------


def _make_income(dates: list[str]) -> pd.DataFrame:
    """Minimal income statement with dates as columns (yfinance format)."""
    cols = pd.to_datetime(dates)
    return pd.DataFrame(
        {
            "Total Revenue": [1_000_000, 900_000],
            "Gross Profit": [400_000, 350_000],
            "EBITDA": [200_000, 180_000],
            "Operating Income": [150_000, 130_000],
            "Net Income": [100_000, 90_000],
            "Diluted EPS": [10.0, 9.0],
        },
        index=cols[:2],
    ).T


def _make_balance(dates: list[str]) -> pd.DataFrame:
    cols = pd.to_datetime(dates)
    return pd.DataFrame(
        {
            "Total Assets": [5_000_000, 4_500_000],
            "Total Liabilities Net Minority Interest": [2_000_000, 1_800_000],
            "Current Assets": [1_500_000, 1_300_000],
            "Current Liabilities": [700_000, 600_000],
            "Total Debt": [800_000, 700_000],
            "Cash And Cash Equivalents": [300_000, 250_000],
            "Common Stock Equity": [3_000_000, 2_700_000],
        },
        index=cols[:2],
    ).T


def _make_cashflow(dates: list[str]) -> pd.DataFrame:
    cols = pd.to_datetime(dates)
    return pd.DataFrame(
        {
            "Operating Cash Flow": [150_000, 130_000],
            "Capital Expenditure": [-40_000, -35_000],
            "Free Cash Flow": [110_000, 95_000],
        },
        index=cols[:2],
    ).T


def _mock_ticker(symbol: str = "RELIANCE.NS") -> MagicMock:
    dates = ["2025-03-31", "2024-03-31"]
    ticker = MagicMock()
    ticker.income_stmt = _make_income(dates)
    ticker.balance_sheet = _make_balance(dates)
    ticker.cashflow = _make_cashflow(dates)
    ticker.info = {
        "marketCap": 20_000_000_000_000,  # 20 lakh crore INR → large_cap
        "sector": "Energy",
        "industry": "Oil & Gas Refining",
    }
    return ticker


# ---------------------------------------------------------------------------
# _fiscal_year helper
# ---------------------------------------------------------------------------


def test_fiscal_year_march_end():
    ts = pd.Timestamp("2025-03-31")
    assert _fiscal_year(ts) == 2025


def test_fiscal_year_december_end():
    ts = pd.Timestamp("2024-12-31")
    assert _fiscal_year(ts) == 2025


def test_fiscal_year_april_start():
    ts = pd.Timestamp("2025-04-30")
    assert _fiscal_year(ts) == 2026


# ---------------------------------------------------------------------------
# _market_cap_bucket
# ---------------------------------------------------------------------------


def test_market_cap_bucket_large():
    mc_inr = 20_000 * 1e7  # 20000 crore
    assert _market_cap_bucket(mc_inr) == "large_cap"


def test_market_cap_bucket_mid():
    mc_inr = 7_000 * 1e7
    assert _market_cap_bucket(mc_inr) == "mid_cap"


def test_market_cap_bucket_small():
    mc_inr = 2_000 * 1e7
    assert _market_cap_bucket(mc_inr) == "small_cap"


def test_market_cap_bucket_none():
    assert _market_cap_bucket(None) is None


# ---------------------------------------------------------------------------
# YFinanceFundamentalsProvider.get_annual_fundamentals
# ---------------------------------------------------------------------------


@patch("stock_platform.data.providers.yfinance_fundamentals.yf.Ticker")
def test_get_annual_fundamentals_returns_rows(mock_ticker_cls):
    mock_ticker_cls.return_value = _mock_ticker()
    provider = YFinanceFundamentalsProvider()
    frame = provider.get_annual_fundamentals("RELIANCE.NS")

    assert not frame.empty
    assert "fiscal_year" in frame.columns
    assert "revenue" in frame.columns
    assert "net_income" in frame.columns
    assert frame["symbol"].iloc[0] == "RELIANCE.NS"
    assert frame["source"].iloc[0] == "yfinance"


@patch("stock_platform.data.providers.yfinance_fundamentals.yf.Ticker")
def test_get_annual_fundamentals_sorted_by_year(mock_ticker_cls):
    mock_ticker_cls.return_value = _mock_ticker()
    provider = YFinanceFundamentalsProvider()
    frame = provider.get_annual_fundamentals("RELIANCE.NS")
    years = frame["fiscal_year"].tolist()
    assert years == sorted(years)


@patch("stock_platform.data.providers.yfinance_fundamentals.yf.Ticker")
def test_get_annual_fundamentals_net_debt_derived(mock_ticker_cls):
    mock_ticker_cls.return_value = _mock_ticker()
    provider = YFinanceFundamentalsProvider()
    frame = provider.get_annual_fundamentals("RELIANCE.NS")
    # net_debt = debt - cash; most recent row
    latest = frame.iloc[-1]
    expected_net_debt = latest["debt"] - latest["cash_and_equivalents"]
    assert abs(latest["net_debt"] - expected_net_debt) < 1


@patch("stock_platform.data.providers.yfinance_fundamentals.yf.Ticker")
def test_get_annual_fundamentals_empty_on_exception(mock_ticker_cls):
    ticker = MagicMock()
    ticker.income_stmt = pd.DataFrame()
    ticker.balance_sheet = pd.DataFrame()
    ticker.cashflow = pd.DataFrame()
    ticker.info = {}
    mock_ticker_cls.return_value = ticker

    provider = YFinanceFundamentalsProvider()
    frame = provider.get_annual_fundamentals("UNKNOWN.NS")
    assert frame.empty


@patch("stock_platform.data.providers.yfinance_fundamentals.yf.Ticker")
def test_get_annual_fundamentals_sector_and_industry(mock_ticker_cls):
    mock_ticker_cls.return_value = _mock_ticker()
    provider = YFinanceFundamentalsProvider()
    frame = provider.get_annual_fundamentals("RELIANCE.NS")
    assert frame.iloc[-1]["sector"] == "Energy"
    assert frame.iloc[-1]["industry"] == "Oil & Gas Refining"


# ---------------------------------------------------------------------------
# get_snapshots
# ---------------------------------------------------------------------------


@patch("stock_platform.data.providers.yfinance_fundamentals.yf.Ticker")
def test_get_snapshots_returns_typed_list(mock_ticker_cls):
    mock_ticker_cls.return_value = _mock_ticker()
    provider = YFinanceFundamentalsProvider()
    snapshots = provider.get_snapshots("RELIANCE.NS")
    assert len(snapshots) == 2
    s = snapshots[-1]
    assert s.symbol == "RELIANCE.NS"
    assert isinstance(s.fiscal_year, int)
    assert s.revenue is not None
    assert s.net_income is not None


# ---------------------------------------------------------------------------
# sub-statement helpers
# ---------------------------------------------------------------------------


@patch("stock_platform.data.providers.yfinance_fundamentals.yf.Ticker")
def test_get_income_statement(mock_ticker_cls):
    mock_ticker_cls.return_value = _mock_ticker()
    provider = YFinanceFundamentalsProvider()
    inc = provider.get_income_statement("RELIANCE.NS")
    assert "revenue" in inc.columns
    assert "net_income" in inc.columns


@patch("stock_platform.data.providers.yfinance_fundamentals.yf.Ticker")
def test_get_cash_flow(mock_ticker_cls):
    mock_ticker_cls.return_value = _mock_ticker()
    provider = YFinanceFundamentalsProvider()
    cf = provider.get_cash_flow("RELIANCE.NS")
    assert "operating_cash_flow" in cf.columns
    assert "free_cash_flow" in cf.columns
