"""Tests for fundamentals data quality checks."""

from __future__ import annotations

import pandas as pd

from stock_platform.data.validators import validate_annual_fundamentals


def _valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "TEST.NS",
                "fiscal_year": 2024,
                "revenue": 100,
                "gross_profit": 40,
                "ebit": 15,
                "net_income": 9,
                "operating_cash_flow": 11,
                "total_assets": 180,
                "total_liabilities": 85,
                "current_assets": 42,
                "current_liabilities": 31,
                "retained_earnings": 52,
                "shares_outstanding": 10,
                "market_cap": 190,
                "source": "manual",
                "source_url": "",
            },
            {
                "symbol": "TEST.NS",
                "fiscal_year": 2025,
                "revenue": 112,
                "gross_profit": 43,
                "ebit": 17,
                "net_income": 10,
                "operating_cash_flow": 13,
                "total_assets": 195,
                "total_liabilities": 87,
                "current_assets": 46,
                "current_liabilities": 32,
                "retained_earnings": 59,
                "shares_outstanding": 10,
                "market_cap": 210,
                "source": "manual",
                "source_url": "",
            },
        ]
    )


def test_valid_annual_fundamentals_pass() -> None:
    report = validate_annual_fundamentals(_valid_frame(), "TEST.NS", raise_on_error=False)

    assert report.ok
    assert report.errors == []


def test_duplicate_fiscal_year_fails() -> None:
    frame = _valid_frame()
    frame.loc[1, "fiscal_year"] = 2024

    report = validate_annual_fundamentals(frame, "TEST.NS", raise_on_error=False)

    assert not report.ok
    assert "duplicate_fiscal_year" in report.errors


def test_sample_source_warns() -> None:
    frame = _valid_frame()
    frame["source"] = "sample_placeholder"

    report = validate_annual_fundamentals(frame, "TEST.NS", raise_on_error=False)

    assert report.ok
    assert "sample_data_source" in report.warnings


def test_non_positive_revenue_fails() -> None:
    frame = _valid_frame()
    frame.loc[1, "revenue"] = 0

    report = validate_annual_fundamentals(frame, "TEST.NS", raise_on_error=False)

    assert not report.ok
    assert "non_positive_revenue" in report.errors
