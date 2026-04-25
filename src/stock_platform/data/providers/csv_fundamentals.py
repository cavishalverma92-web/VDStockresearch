"""Local CSV fundamentals provider.

This is the safe Phase 1 provider: it reads a local CSV file rather than
scraping any website. It lets the fundamentals engine and UI mature while data
source terms are reviewed.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from stock_platform.analytics.fundamentals.schema import FundamentalSnapshot
from stock_platform.config import ROOT_DIR
from stock_platform.data.providers.base import FundamentalsDataProvider

ANNUAL_COLUMNS = [
    "symbol",
    "fiscal_year",
    "revenue",
    "gross_profit",
    "ebitda",
    "ebit",
    "net_income",
    "eps",
    "book_value",
    "operating_cash_flow",
    "capital_expenditure",
    "free_cash_flow",
    "debt",
    "net_debt",
    "cash_and_equivalents",
    "total_assets",
    "total_liabilities",
    "current_assets",
    "current_liabilities",
    "retained_earnings",
    "shares_outstanding",
    "market_cap",
    "enterprise_value",
    "sector",
    "industry",
    "market_cap_bucket",
    "source",
    "source_url",
]


class CsvFundamentalsProvider(FundamentalsDataProvider):
    """Provider for annual fundamentals stored in a local CSV file."""

    name = "local_csv"

    def __init__(self, annual_path: str | Path | None = None) -> None:
        self.annual_path = Path(
            annual_path or ROOT_DIR / "data/sample/fundamentals_annual_sample.csv"
        )

    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        frame = self.get_annual_fundamentals(symbol)
        return frame[
            [
                "symbol",
                "fiscal_year",
                "revenue",
                "gross_profit",
                "ebitda",
                "ebit",
                "net_income",
                "eps",
            ]
        ]

    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        frame = self.get_annual_fundamentals(symbol)
        return frame[
            [
                "symbol",
                "fiscal_year",
                "total_assets",
                "total_liabilities",
                "current_assets",
                "current_liabilities",
                "retained_earnings",
                "book_value",
                "debt",
                "net_debt",
                "cash_and_equivalents",
                "shares_outstanding",
                "market_cap",
                "enterprise_value",
            ]
        ]

    def get_cash_flow(self, symbol: str) -> pd.DataFrame:
        frame = self.get_annual_fundamentals(symbol)
        return frame[
            [
                "symbol",
                "fiscal_year",
                "operating_cash_flow",
                "capital_expenditure",
                "free_cash_flow",
            ]
        ]

    def get_annual_fundamentals(self, symbol: str) -> pd.DataFrame:
        """Return annual fundamentals rows for one symbol, sorted by fiscal year."""
        frame = self.get_all_annual_fundamentals()
        if frame.empty or "symbol" not in frame.columns:
            return pd.DataFrame(columns=ANNUAL_COLUMNS)

        filtered = frame[frame["symbol"].astype(str).str.upper() == symbol.upper()].copy()
        if filtered.empty:
            return pd.DataFrame(columns=ANNUAL_COLUMNS)

        return _sort_by_fiscal_year(filtered)

    def get_all_annual_fundamentals(self) -> pd.DataFrame:
        """Return all annual fundamentals rows from the local CSV."""
        if not self.annual_path.exists():
            return pd.DataFrame(columns=ANNUAL_COLUMNS)

        frame = pd.read_csv(self.annual_path)
        if "symbol" not in frame.columns:
            return pd.DataFrame(columns=ANNUAL_COLUMNS)
        return frame

    def get_snapshots(self, symbol: str) -> list[FundamentalSnapshot]:
        """Return annual fundamentals as typed snapshots."""
        frame = self.get_annual_fundamentals(symbol)
        snapshots: list[FundamentalSnapshot] = []
        for row in frame.to_dict(orient="records"):
            snapshots.append(
                FundamentalSnapshot(
                    symbol=str(row["symbol"]),
                    fiscal_year=int(row["fiscal_year"]),
                    revenue=_optional_float(row.get("revenue")),
                    gross_profit=_optional_float(row.get("gross_profit")),
                    ebitda=_optional_float(row.get("ebitda")),
                    ebit=_optional_float(row.get("ebit")),
                    net_income=_optional_float(row.get("net_income")),
                    eps=_optional_float(row.get("eps")),
                    book_value=_optional_float(row.get("book_value")),
                    operating_cash_flow=_optional_float(row.get("operating_cash_flow")),
                    capital_expenditure=_optional_float(row.get("capital_expenditure")),
                    free_cash_flow=_optional_float(row.get("free_cash_flow")),
                    debt=_optional_float(row.get("debt")),
                    net_debt=_optional_float(row.get("net_debt")),
                    cash_and_equivalents=_optional_float(row.get("cash_and_equivalents")),
                    total_assets=_optional_float(row.get("total_assets")),
                    total_liabilities=_optional_float(row.get("total_liabilities")),
                    current_assets=_optional_float(row.get("current_assets")),
                    current_liabilities=_optional_float(row.get("current_liabilities")),
                    retained_earnings=_optional_float(row.get("retained_earnings")),
                    shares_outstanding=_optional_float(row.get("shares_outstanding")),
                    market_cap=_optional_float(row.get("market_cap")),
                    enterprise_value=_optional_float(row.get("enterprise_value")),
                )
            )
        return snapshots


def _optional_float(value: object) -> float | None:
    if value is None or pd.isna(value) or value == "":
        return None
    return float(value)


def _sort_by_fiscal_year(frame: pd.DataFrame) -> pd.DataFrame:
    frame["fiscal_year"] = pd.to_numeric(frame["fiscal_year"], errors="coerce")
    frame = frame.dropna(subset=["fiscal_year"])
    frame["fiscal_year"] = frame["fiscal_year"].astype(int)
    return frame.sort_values("fiscal_year").reset_index(drop=True)
