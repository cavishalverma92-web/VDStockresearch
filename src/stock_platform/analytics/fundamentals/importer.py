"""Manual fundamentals import helpers.

This module supports user-provided CSV/Excel exports. It does not scrape any
website and does not transmit data anywhere. Rows are normalized into the same
annual/quarterly fundamentals tables used by provider refresh jobs.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from stock_platform.data.repositories import (
    FundamentalsUpsertSummary,
    upsert_fundamentals_annual,
    upsert_fundamentals_quarterly,
)
from stock_platform.data.validators import (
    validate_annual_fundamentals,
    validate_quarterly_fundamentals,
)
from stock_platform.db import create_all_tables, get_engine, get_session
from stock_platform.db.models import StockUniverse

ANNUAL = "annual"
QUARTERLY = "quarterly"

_COLUMN_ALIASES = {
    "symbol": "symbol",
    "ticker": "symbol",
    "nse_symbol": "symbol",
    "fiscal_year": "fiscal_year",
    "year": "fiscal_year",
    "fy": "fiscal_year",
    "financial_year": "fiscal_year",
    "fiscal_quarter": "fiscal_quarter",
    "quarter": "fiscal_quarter",
    "fq": "fiscal_quarter",
    "period_end": "period_end",
    "period_ending": "period_end",
    "currency": "currency",
    "source": "source",
    "source_url": "source_url",
    "sales": "revenue",
    "revenue": "revenue",
    "gross_profit": "gross_profit",
    "operating_profit": "ebitda",
    "ebitda": "ebitda",
    "profit_before_tax": "ebit",
    "ebit": "ebit",
    "net_profit": "net_income",
    "pat": "net_income",
    "net_income": "net_income",
    "eps": "eps",
    "book_value": "book_value",
    "cash_from_operating_activity": "operating_cash_flow",
    "operating_cash_flow": "operating_cash_flow",
    "capex": "capital_expenditure",
    "capital_expenditure": "capital_expenditure",
    "free_cash_flow": "free_cash_flow",
    "borrowings": "debt",
    "debt": "debt",
    "net_debt": "net_debt",
    "cash": "cash_and_equivalents",
    "cash_and_equivalents": "cash_and_equivalents",
    "total_assets": "total_assets",
    "total_liabilities": "total_liabilities",
    "current_assets": "current_assets",
    "current_liabilities": "current_liabilities",
    "reserves": "retained_earnings",
    "retained_earnings": "retained_earnings",
    "shares_outstanding": "shares_outstanding",
    "equity_capital": "shares_outstanding",
    "market_cap": "market_cap",
    "enterprise_value": "enterprise_value",
}

_NUMERIC_COLUMNS = {
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
}

_CRORE_AMOUNT_COLUMNS = _NUMERIC_COLUMNS - {"eps", "shares_outstanding"}


@dataclass(frozen=True)
class ManualFundamentalsImportPreview:
    statement_type: str
    source: str
    rows: int
    symbols: int
    normalized_frame: pd.DataFrame
    errors: dict[str, list[str]] = field(default_factory=dict)
    warnings: dict[str, list[str]] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass(frozen=True)
class ManualFundamentalsImportResult:
    preview: ManualFundamentalsImportPreview
    inserted: int
    updated: int
    skipped: int
    dry_run: bool


def normalize_manual_fundamentals_frame(
    frame: pd.DataFrame,
    *,
    statement_type: str,
    source: str,
    values_in_crores: bool = True,
) -> pd.DataFrame:
    """Normalize a user-provided fundamentals table into repository columns."""
    if frame is None or frame.empty:
        return pd.DataFrame()

    normalized_type = _normalize_statement_type(statement_type)
    working = frame.copy()
    working.columns = [_normalize_column_name(column) for column in working.columns]
    working = working.rename(columns={c: _COLUMN_ALIASES.get(c, c) for c in working.columns})
    working = working.loc[:, ~working.columns.duplicated()].copy()

    if "symbol" in working.columns:
        working["symbol"] = working["symbol"].map(_normalize_symbol)
    if "currency" not in working.columns:
        working["currency"] = "INR"
    working["source"] = source

    if "fiscal_year" in working.columns:
        working["fiscal_year"] = pd.to_numeric(working["fiscal_year"], errors="coerce").astype(
            "Int64"
        )
    if normalized_type == QUARTERLY and "fiscal_quarter" in working.columns:
        working["fiscal_quarter"] = pd.to_numeric(
            working["fiscal_quarter"], errors="coerce"
        ).astype("Int64")
    if "period_end" in working.columns:
        working["period_end"] = pd.to_datetime(working["period_end"], errors="coerce").dt.date

    for column in _NUMERIC_COLUMNS.intersection(working.columns):
        numeric = working[column].map(_clean_number)
        if values_in_crores and column in _CRORE_AMOUNT_COLUMNS:
            numeric = numeric * 10_000_000
        working[column] = numeric

    required = ["symbol", "fiscal_year", "source"]
    if normalized_type == QUARTERLY:
        required.append("fiscal_quarter")
    for column in required:
        if column not in working.columns:
            working[column] = pd.NA

    return working.reset_index(drop=True)


def preview_manual_fundamentals_import(
    frame: pd.DataFrame,
    *,
    statement_type: str,
    source: str,
    values_in_crores: bool = True,
) -> ManualFundamentalsImportPreview:
    """Normalize and validate a user-provided fundamentals frame."""
    normalized_type = _normalize_statement_type(statement_type)
    normalized = normalize_manual_fundamentals_frame(
        frame,
        statement_type=normalized_type,
        source=source,
        values_in_crores=values_in_crores,
    )
    errors: dict[str, list[str]] = {}
    warnings: dict[str, list[str]] = {}
    if normalized.empty:
        errors["file"] = ["No rows found in uploaded fundamentals file."]
    elif (
        normalized["symbol"].isna().any()
        or normalized["symbol"].astype(str).str.strip().eq("").any()
    ):
        errors["file"] = ["One or more rows are missing a symbol."]

    for symbol, group in _groups_by_symbol(normalized):
        report = (
            validate_annual_fundamentals(group, symbol, raise_on_error=False)
            if normalized_type == ANNUAL
            else validate_quarterly_fundamentals(group, symbol, raise_on_error=False)
        )
        if report.errors:
            errors[symbol] = report.errors
        if report.warnings:
            warnings[symbol] = report.warnings

    return ManualFundamentalsImportPreview(
        statement_type=normalized_type,
        source=source,
        rows=len(normalized),
        symbols=0 if normalized.empty else int(normalized["symbol"].dropna().nunique()),
        normalized_frame=normalized,
        errors=errors,
        warnings=warnings,
    )


def import_manual_fundamentals(
    frame: pd.DataFrame,
    *,
    statement_type: str,
    source: str = "manual_screener_export",
    values_in_crores: bool = True,
    allow_partial: bool = False,
    dry_run: bool = True,
    engine: Engine | None = None,
) -> ManualFundamentalsImportResult:
    """Validate and optionally write manual fundamentals rows to the database."""
    active_engine = engine or get_engine()
    create_all_tables(active_engine)
    preview = preview_manual_fundamentals_import(
        frame,
        statement_type=statement_type,
        source=source,
        values_in_crores=values_in_crores,
    )
    if preview.errors and not allow_partial:
        return ManualFundamentalsImportResult(preview, 0, 0, preview.rows, dry_run=dry_run)
    if dry_run:
        return ManualFundamentalsImportResult(preview, 0, 0, 0, dry_run=True)

    inserted = updated = skipped = 0
    with get_session(active_engine) as session:
        for symbol, group in _groups_by_symbol(preview.normalized_frame):
            if symbol in preview.errors and not allow_partial:
                skipped += len(group)
                continue
            _ensure_stock_universe_symbol(session, symbol)
            summary = _upsert_group(
                session,
                symbol,
                group,
                statement_type=preview.statement_type,
                source=source,
            )
            inserted += summary.inserted
            updated += summary.updated
            skipped += summary.skipped

    return ManualFundamentalsImportResult(
        preview=preview,
        inserted=inserted,
        updated=updated,
        skipped=skipped,
        dry_run=False,
    )


def _upsert_group(
    session: Session,
    symbol: str,
    group: pd.DataFrame,
    *,
    statement_type: str,
    source: str,
) -> FundamentalsUpsertSummary:
    if statement_type == ANNUAL:
        return upsert_fundamentals_annual(session, symbol, group, source=source)
    return upsert_fundamentals_quarterly(session, symbol, group, source=source)


def _ensure_stock_universe_symbol(session: Session, symbol: str) -> None:
    exists = session.scalar(select(StockUniverse).where(StockUniverse.symbol == symbol))
    if exists is not None:
        return
    session.add(
        StockUniverse(
            symbol=symbol,
            name=symbol,
            exchange="NSE",
            source="manual_fundamentals_import",
        )
    )


def _groups_by_symbol(frame: pd.DataFrame):
    if frame.empty or "symbol" not in frame.columns:
        return []
    clean = frame[frame["symbol"].notna()].copy()
    return [(str(symbol), group.copy()) for symbol, group in clean.groupby("symbol", sort=True)]


def _normalize_statement_type(value: str) -> str:
    cleaned = str(value).strip().lower()
    if cleaned not in {ANNUAL, QUARTERLY}:
        raise ValueError("statement_type must be 'annual' or 'quarterly'")
    return cleaned


def _normalize_symbol(value: object) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        return ""
    return symbol if "." in symbol else f"{symbol}.NS"


def _normalize_column_name(value: object) -> str:
    return (
        str(value)
        .strip()
        .lower()
        .replace("\ufeff", "")
        .replace("(rs)", "")
        .replace("in rs", "")
        .replace("%", "pct")
        .replace("/", "_")
        .replace("-", "_")
        .replace(" ", "_")
        .replace("__", "_")
        .strip("_")
    )


def _clean_number(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text in {"-", "--"}:
        return None
    multiplier = -1 if text.startswith("(") and text.endswith(")") else 1
    text = (
        text.strip("()")
        .replace(",", "")
        .replace("INR", "")
        .replace("Rs.", "")
        .replace("Rs", "")
        .replace("Cr.", "")
        .replace("%", "")
        .strip()
    )
    try:
        return float(text) * multiplier
    except ValueError:
        return None
