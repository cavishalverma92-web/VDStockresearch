"""Tests for manual fundamentals CSV/Excel import helpers."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from stock_platform.analytics.fundamentals.importer import (
    import_manual_fundamentals,
    normalize_manual_fundamentals_frame,
    preview_manual_fundamentals_import,
)
from stock_platform.data.repositories import fetch_fundamentals_annual
from stock_platform.db.models import Base, StockUniverse


def _annual_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Ticker": "RELIANCE",
                "Year": 2025,
                "Sales": "10,000",
                "Gross Profit": "7,000",
                "Operating Profit": "2,500",
                "Profit Before Tax": "2,000",
                "Net Profit": "1,500",
                "EPS": "110",
                "Cash From Operating Activity": "2,100",
                "Capex": "-500",
                "Borrowings": "3,000",
                "Total Assets": "20,000",
                "Total Liabilities": "8,000",
                "Current Assets": "5,000",
                "Current Liabilities": "2,000",
                "Reserves": "9,000",
                "Shares Outstanding": "676",
                "Market Cap": "150,000",
            }
        ]
    )


def test_normalize_manual_fundamentals_aliases_and_crore_scaling() -> None:
    normalized = normalize_manual_fundamentals_frame(
        _annual_frame(),
        statement_type="annual",
        source="manual_screener_export",
        values_in_crores=True,
    )

    assert normalized.loc[0, "symbol"] == "RELIANCE.NS"
    assert normalized.loc[0, "fiscal_year"] == 2025
    assert normalized.loc[0, "revenue"] == 100_000_000_000
    assert normalized.loc[0, "eps"] == 110
    assert normalized.loc[0, "source"] == "manual_screener_export"


def test_preview_manual_fundamentals_reports_missing_required_columns() -> None:
    preview = preview_manual_fundamentals_import(
        pd.DataFrame([{"symbol": "TCS", "fiscal_year": 2025, "net_profit": 1000}]),
        statement_type="annual",
        source="manual_screener_export",
    )

    assert not preview.ok
    assert "TCS.NS" in preview.errors


def test_import_manual_fundamentals_writes_annual_rows() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    result = import_manual_fundamentals(
        _annual_frame(),
        statement_type="annual",
        source="manual_screener_export",
        values_in_crores=True,
        dry_run=False,
        engine=engine,
    )

    with Session(engine) as session:
        annual = fetch_fundamentals_annual(
            session,
            "RELIANCE.NS",
            source="manual_screener_export",
        )
        stock = session.scalar(select(StockUniverse).where(StockUniverse.symbol == "RELIANCE.NS"))

    assert result.inserted == 1
    assert result.updated == 0
    assert list(annual["symbol"]) == ["RELIANCE.NS"]
    assert annual.loc[0, "revenue"] == 100_000_000_000
    assert stock is not None
