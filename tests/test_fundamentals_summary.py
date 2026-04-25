"""Tests for UI-ready fundamentals summaries."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from stock_platform.analytics.fundamentals.summary import build_fundamentals_summary
from stock_platform.data.providers import CsvFundamentalsProvider


def test_summary_builds_latest_rows_for_symbols(tmp_path: Path) -> None:
    path = tmp_path / "fundamentals.csv"
    pd.DataFrame(
        [
            _row("TEST.NS", 2024, revenue=100, net_income=10, source="manual"),
            _row("TEST.NS", 2025, revenue=125, net_income=15, source="manual"),
            _row("OTHER.NS", 2025, revenue=90, net_income=8, source="sample_placeholder"),
        ]
    ).to_csv(path, index=False)

    summary = build_fundamentals_summary(
        CsvFundamentalsProvider(path),
        ["TEST.NS", "OTHER.NS", "MISSING.NS"],
    )

    test_row = summary.loc[summary["symbol"] == "TEST.NS"].iloc[0]
    other_row = summary.loc[summary["symbol"] == "OTHER.NS"].iloc[0]
    missing_row = summary.loc[summary["symbol"] == "MISSING.NS"].iloc[0]

    assert test_row["fiscal_year"] == 2025
    assert test_row["revenue_growth_pct"] == 25
    assert test_row["status"] == "ok"
    assert other_row["status"] == "sample"
    assert missing_row["status"] == "no_data"


def _row(symbol: str, fiscal_year: int, revenue: float, net_income: float, source: str) -> dict:
    return {
        "symbol": symbol,
        "fiscal_year": fiscal_year,
        "revenue": revenue,
        "gross_profit": revenue * 0.4,
        "ebit": revenue * 0.2,
        "net_income": net_income,
        "operating_cash_flow": net_income + 2,
        "total_assets": revenue * 2,
        "total_liabilities": revenue * 0.8,
        "current_assets": revenue * 0.7,
        "current_liabilities": revenue * 0.3,
        "retained_earnings": revenue * 0.5,
        "shares_outstanding": 10,
        "market_cap": revenue * 3,
        "source": source,
        "source_url": "",
    }
