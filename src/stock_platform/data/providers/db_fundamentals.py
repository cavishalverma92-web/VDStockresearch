"""Read-only fundamentals provider backed by the local DB.

Use this in the UI / summary path so renders don't hit yfinance live. If the
DB has no rows for a symbol, the wrapper falls back to a configured live
provider (and optionally upserts what it fetched, so the next render is fast).
"""

from __future__ import annotations

from typing import Protocol

import pandas as pd
from sqlalchemy import Engine

from stock_platform.analytics.fundamentals.schema import (
    FundamentalSnapshot,
    QuarterlyFundamentalSnapshot,
)
from stock_platform.data.providers.base import FundamentalsDataProvider
from stock_platform.data.repositories import (
    fetch_fundamentals_annual,
    fetch_fundamentals_quarterly,
    upsert_fundamentals_annual,
    upsert_fundamentals_quarterly,
)
from stock_platform.db import get_engine, get_session
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


class _LiveProvider(Protocol):
    def get_annual_fundamentals(self, symbol: str) -> pd.DataFrame: ...
    def get_quarterly_fundamentals(self, symbol: str) -> pd.DataFrame: ...


class DbFundamentalsProvider(FundamentalsDataProvider):
    """Reads persisted fundamentals; falls back to a live provider on cache miss."""

    name = "db"

    def __init__(
        self,
        *,
        fallback: _LiveProvider | None = None,
        engine: Engine | None = None,
        preferred_source: str | None = None,
        write_through: bool = True,
    ) -> None:
        self._fallback = fallback
        self._engine = engine or get_engine()
        self._preferred_source = preferred_source
        self._write_through = write_through

    # ------------------------------------------------------------------
    # FundamentalsDataProvider interface
    # ------------------------------------------------------------------

    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        frame = self.get_annual_fundamentals(symbol)
        cols = [
            c
            for c in ("symbol", "fiscal_year", "revenue", "ebitda", "net_income", "eps")
            if c in frame.columns
        ]
        return frame[cols] if cols else frame

    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        frame = self.get_annual_fundamentals(symbol)
        cols = [
            c
            for c in ("symbol", "fiscal_year", "total_assets", "total_liabilities", "debt")
            if c in frame.columns
        ]
        return frame[cols] if cols else frame

    def get_cash_flow(self, symbol: str) -> pd.DataFrame:
        frame = self.get_annual_fundamentals(symbol)
        cols = [
            c
            for c in ("symbol", "fiscal_year", "operating_cash_flow", "free_cash_flow")
            if c in frame.columns
        ]
        return frame[cols] if cols else frame

    # ------------------------------------------------------------------
    # Annual + quarterly
    # ------------------------------------------------------------------

    def get_annual_fundamentals(self, symbol: str) -> pd.DataFrame:
        with get_session(self._engine) as session:
            frame = fetch_fundamentals_annual(session, symbol, source=self._preferred_source)

        if not frame.empty:
            return _pick_one_per_year(frame, self._preferred_source)

        if self._fallback is None:
            return frame
        live = self._fallback.get_annual_fundamentals(symbol)
        if not live.empty and self._write_through:
            source = str(live.iloc[0].get("source") or self._fallback_source())
            with get_session(self._engine) as session:
                upsert_fundamentals_annual(session, symbol, live, source=source)
        return live

    def get_quarterly_fundamentals(self, symbol: str) -> pd.DataFrame:
        with get_session(self._engine) as session:
            frame = fetch_fundamentals_quarterly(session, symbol, source=self._preferred_source)
        if not frame.empty:
            return _pick_one_per_quarter(frame, self._preferred_source)

        if self._fallback is None:
            return frame
        live = self._fallback.get_quarterly_fundamentals(symbol)
        if not live.empty and self._write_through:
            source = str(live.iloc[0].get("source") or self._fallback_source())
            with get_session(self._engine) as session:
                upsert_fundamentals_quarterly(session, symbol, live, source=source)
        return live

    def get_snapshots(self, symbol: str) -> list[FundamentalSnapshot]:
        frame = self.get_annual_fundamentals(symbol)
        out: list[FundamentalSnapshot] = []
        for row in frame.to_dict(orient="records"):
            out.append(
                FundamentalSnapshot(
                    symbol=str(row.get("symbol") or symbol).upper(),
                    fiscal_year=int(row["fiscal_year"]),
                    **{
                        k: _f(row.get(k))
                        for k in (
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
                        )
                    },
                )
            )
        return out

    def get_quarterly_snapshots(self, symbol: str) -> list[QuarterlyFundamentalSnapshot]:
        frame = self.get_quarterly_fundamentals(symbol)
        out: list[QuarterlyFundamentalSnapshot] = []
        for row in frame.to_dict(orient="records"):
            out.append(
                QuarterlyFundamentalSnapshot(
                    symbol=str(row.get("symbol") or symbol).upper(),
                    fiscal_year=int(row["fiscal_year"]),
                    fiscal_quarter=int(row["fiscal_quarter"]),
                    **{
                        k: _f(row.get(k))
                        for k in (
                            "revenue",
                            "ebitda",
                            "ebit",
                            "net_income",
                            "eps",
                            "operating_cash_flow",
                            "free_cash_flow",
                            "total_assets",
                            "total_liabilities",
                            "shares_outstanding",
                        )
                    },
                )
            )
        return out

    # ------------------------------------------------------------------

    def _fallback_source(self) -> str:
        return getattr(self._fallback, "name", "unknown") if self._fallback else "unknown"


def _pick_one_per_year(frame: pd.DataFrame, preferred_source: str | None) -> pd.DataFrame:
    """If multiple sources exist for a year, keep the preferred (else first)."""
    if "source" not in frame.columns or "fiscal_year" not in frame.columns:
        return frame
    if preferred_source is not None:
        match = frame[frame["source"] == preferred_source]
        if not match.empty:
            return match.sort_values("fiscal_year").reset_index(drop=True)
    return (
        frame.sort_values(["fiscal_year", "source"])
        .drop_duplicates("fiscal_year", keep="first")
        .reset_index(drop=True)
    )


def _pick_one_per_quarter(frame: pd.DataFrame, preferred_source: str | None) -> pd.DataFrame:
    if "source" not in frame.columns:
        return frame
    if preferred_source is not None:
        match = frame[frame["source"] == preferred_source]
        if not match.empty:
            return match.sort_values(["fiscal_year", "fiscal_quarter"]).reset_index(drop=True)
    return (
        frame.sort_values(["fiscal_year", "fiscal_quarter", "source"])
        .drop_duplicates(["fiscal_year", "fiscal_quarter"], keep="first")
        .reset_index(drop=True)
    )


def _f(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
