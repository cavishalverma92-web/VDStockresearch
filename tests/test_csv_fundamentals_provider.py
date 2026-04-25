"""Tests for the local CSV fundamentals provider."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from stock_platform.data.providers import CsvFundamentalsProvider


def test_csv_provider_filters_symbol_and_sorts_years(tmp_path: Path) -> None:
    path = tmp_path / "fundamentals.csv"
    pd.DataFrame(
        [
            {"symbol": "TEST.NS", "fiscal_year": 2025, "revenue": 120, "source": "sample"},
            {"symbol": "OTHER.NS", "fiscal_year": 2025, "revenue": 999, "source": "sample"},
            {"symbol": "TEST.NS", "fiscal_year": 2024, "revenue": 100, "source": "sample"},
        ]
    ).to_csv(path, index=False)

    provider = CsvFundamentalsProvider(path)
    frame = provider.get_annual_fundamentals("test.ns")

    assert frame["symbol"].tolist() == ["TEST.NS", "TEST.NS"]
    assert frame["fiscal_year"].tolist() == [2024, 2025]


def test_csv_provider_returns_typed_snapshots(tmp_path: Path) -> None:
    path = tmp_path / "fundamentals.csv"
    pd.DataFrame(
        [
            {
                "symbol": "TEST.NS",
                "fiscal_year": 2025,
                "revenue": 120,
                "gross_profit": 50,
                "ebitda": 25,
                "ebit": 20,
                "net_income": 12,
                "eps": 1.2,
                "book_value": 120,
                "operating_cash_flow": 14,
                "capital_expenditure": 4,
                "free_cash_flow": 10,
                "debt": 70,
                "net_debt": 55,
                "cash_and_equivalents": 15,
                "total_assets": 200,
                "total_liabilities": 80,
                "current_assets": 70,
                "current_liabilities": 30,
                "retained_earnings": 60,
                "shares_outstanding": 10,
                "market_cap": 300,
                "enterprise_value": 355,
                "source": "sample",
                "source_url": "",
            }
        ]
    ).to_csv(path, index=False)

    snapshots = CsvFundamentalsProvider(path).get_snapshots("TEST.NS")

    assert len(snapshots) == 1
    assert snapshots[0].symbol == "TEST.NS"
    assert snapshots[0].revenue == 120
    assert snapshots[0].ebitda == 25
    assert snapshots[0].free_cash_flow == 10
    assert snapshots[0].enterprise_value == 355
    assert snapshots[0].total_assets == 200


def test_csv_provider_handles_missing_file(tmp_path: Path) -> None:
    provider = CsvFundamentalsProvider(tmp_path / "missing.csv")

    assert provider.get_annual_fundamentals("TEST.NS").empty
    assert provider.get_snapshots("TEST.NS") == []
