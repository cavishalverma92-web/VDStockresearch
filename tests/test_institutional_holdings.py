"""Tests for institutional and MF holdings provider."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from stock_platform.data.providers.institutional_holdings import (
    get_institutional_holders,
    get_major_holders,
    get_mutualfund_holders,
    holdings_summary,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_major_holders() -> pd.DataFrame:
    """Simulate yfinance major_holders output (old column format 0/1)."""
    return pd.DataFrame(
        {
            0: ["0.4523", "0.6312", "0.5881", "1024"],
            1: [
                "% of Shares Held by All Insider",
                "% of Shares Held by Institutions",
                "% of Float Held by Institutions",
                "Number of Institutions Holding Shares",
            ],
        }
    )


def _make_inst_holders() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Holder": ["Fund A", "Fund B"],
            "Shares": [1_000_000, 500_000],
            "Date Reported": ["2025-03-31", "2025-03-31"],
            "% Out": [0.05, 0.025],
            "Value": [5_000_000, 2_500_000],
        }
    )


def _make_mf_holders() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Holder": ["MF Scheme X"],
            "Shares": [200_000],
            "Date Reported": ["2025-03-31"],
            "% Out": [0.01],
            "Value": [1_000_000],
        }
    )


# ---------------------------------------------------------------------------
# get_major_holders
# ---------------------------------------------------------------------------


@patch("stock_platform.data.providers.institutional_holdings.yf.Ticker")
def test_get_major_holders_returns_frame(mock_cls):
    ticker = MagicMock()
    ticker.major_holders = _make_major_holders()
    mock_cls.return_value = ticker

    result = get_major_holders("RELIANCE.NS")
    assert not result.empty
    assert "category" in result.columns
    assert "value" in result.columns


@patch("stock_platform.data.providers.institutional_holdings.yf.Ticker")
def test_get_major_holders_empty_on_none(mock_cls):
    ticker = MagicMock()
    ticker.major_holders = None
    mock_cls.return_value = ticker

    result = get_major_holders("RELIANCE.NS")
    assert result.empty


@patch("stock_platform.data.providers.institutional_holdings.yf.Ticker")
def test_get_major_holders_empty_on_exception(mock_cls):
    mock_cls.side_effect = RuntimeError("network error")
    result = get_major_holders("RELIANCE.NS")
    assert result.empty


# ---------------------------------------------------------------------------
# get_institutional_holders
# ---------------------------------------------------------------------------


@patch("stock_platform.data.providers.institutional_holdings.yf.Ticker")
def test_get_institutional_holders_returns_frame(mock_cls):
    ticker = MagicMock()
    ticker.institutional_holders = _make_inst_holders()
    mock_cls.return_value = ticker

    result = get_institutional_holders("RELIANCE.NS")
    assert not result.empty
    assert "holder" in result.columns
    assert "pct_held" in result.columns


@patch("stock_platform.data.providers.institutional_holdings.yf.Ticker")
def test_get_institutional_holders_empty_on_none(mock_cls):
    ticker = MagicMock()
    ticker.institutional_holders = None
    mock_cls.return_value = ticker

    result = get_institutional_holders("RELIANCE.NS")
    assert result.empty


# ---------------------------------------------------------------------------
# get_mutualfund_holders
# ---------------------------------------------------------------------------


@patch("stock_platform.data.providers.institutional_holdings.yf.Ticker")
def test_get_mutualfund_holders_returns_frame(mock_cls):
    ticker = MagicMock()
    ticker.mutualfund_holders = _make_mf_holders()
    mock_cls.return_value = ticker

    result = get_mutualfund_holders("RELIANCE.NS")
    assert not result.empty
    assert "holder" in result.columns


@patch("stock_platform.data.providers.institutional_holdings.yf.Ticker")
def test_get_mutualfund_holders_empty_on_none(mock_cls):
    ticker = MagicMock()
    ticker.mutualfund_holders = None
    mock_cls.return_value = ticker

    result = get_mutualfund_holders("RELIANCE.NS")
    assert result.empty


# ---------------------------------------------------------------------------
# holdings_summary
# ---------------------------------------------------------------------------


@patch("stock_platform.data.providers.institutional_holdings.get_institutional_holders")
@patch("stock_platform.data.providers.institutional_holdings.get_major_holders")
def test_holdings_summary_data_available(mock_major, mock_inst):
    mock_major.return_value = pd.DataFrame(
        {
            "category": [
                "% of Shares Held by All Insider",
                "% of Shares Held by Institutions",
                "% of Float Held by Institutions",
            ],
            "value": ["45.23", "63.12", "58.81"],
        }
    )
    mock_inst.return_value = pd.DataFrame(
        {
            "holder": ["Fund A"],
            "shares": [1_000_000],
            "date_reported": ["2025-03-31"],
            "pct_held": [5.0],
            "value": [5_000_000],
        }
    )

    summary = holdings_summary("RELIANCE.NS")
    assert summary["data_available"] is True
    assert summary["insider_pct"] is not None
    assert summary["top_holder"] == "Fund A"


@patch("stock_platform.data.providers.institutional_holdings.get_institutional_holders")
@patch("stock_platform.data.providers.institutional_holdings.get_major_holders")
def test_holdings_summary_not_available(mock_major, mock_inst):
    mock_major.return_value = pd.DataFrame(columns=["category", "value"])
    mock_inst.return_value = pd.DataFrame(
        columns=["holder", "shares", "date_reported", "pct_held", "value"]
    )

    summary = holdings_summary("UNKNOWN.NS")
    assert summary["data_available"] is False
    assert summary["insider_pct"] is None
