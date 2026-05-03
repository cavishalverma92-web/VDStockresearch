"""Persistence helpers for annual and quarterly fundamentals.

Source-aware UPSERT keyed on the table-level UniqueConstraints:
- annual: ``(symbol, fiscal_year, source)``
- quarterly: ``(symbol, fiscal_year, fiscal_quarter, source)``

Columns absent from the incoming frame are left ``NULL``; columns present
overwrite existing values for the same key. This lets a second provider
(e.g. Screener) coexist with yfinance — both rows live in the table and
the summary layer can compare or pick a preferred source.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from stock_platform.db.models import FundamentalsAnnual, FundamentalsQuarterly, utc_now
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class FundamentalsUpsertSummary:
    inserted: int
    updated: int
    skipped: int


_ANNUAL_NUMERIC_COLUMNS = (
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

_QUARTERLY_NUMERIC_COLUMNS = (
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


def upsert_fundamentals_annual(
    session: Session,
    symbol: str,
    frame: pd.DataFrame,
    *,
    source: str,
) -> FundamentalsUpsertSummary:
    """Insert or update annual fundamentals rows for one symbol from one source."""
    cleaned = _clean_symbol(symbol)
    if frame is None or frame.empty:
        return FundamentalsUpsertSummary(0, 0, 0)
    if "fiscal_year" not in frame.columns:
        raise KeyError("upsert_fundamentals_annual requires a 'fiscal_year' column")

    incoming_years = [int(y) for y in frame["fiscal_year"].dropna().unique()]
    existing = {
        row.fiscal_year: row
        for row in session.scalars(
            select(FundamentalsAnnual).where(
                FundamentalsAnnual.symbol == cleaned,
                FundamentalsAnnual.source == source,
                FundamentalsAnnual.fiscal_year.in_(incoming_years),
            )
        ).all()
    }

    inserted = 0
    updated = 0
    skipped = 0
    now = utc_now()
    source_url = _first_str(frame, "source_url")

    for raw in frame.to_dict(orient="records"):
        fy = _safe_int(raw.get("fiscal_year"))
        if fy is None:
            skipped += 1
            continue
        values = {col: _safe_float(raw.get(col)) for col in _ANNUAL_NUMERIC_COLUMNS}
        period_end = _safe_date(raw.get("period_end"))
        currency = str(raw.get("currency") or "INR")

        if fy in existing:
            session.execute(
                update(FundamentalsAnnual)
                .where(
                    FundamentalsAnnual.symbol == cleaned,
                    FundamentalsAnnual.source == source,
                    FundamentalsAnnual.fiscal_year == fy,
                )
                .values(
                    period_end=period_end,
                    currency=currency,
                    source_url=source_url,
                    fetched_at=now,
                    **values,
                )
            )
            updated += 1
        else:
            session.add(
                FundamentalsAnnual(
                    symbol=cleaned,
                    fiscal_year=fy,
                    period_end=period_end,
                    currency=currency,
                    source=source,
                    source_url=source_url,
                    fetched_at=now,
                    **values,
                )
            )
            inserted += 1

    log.info(
        "upsert_fundamentals_annual symbol={} source={} inserted={} updated={} skipped={}",
        cleaned,
        source,
        inserted,
        updated,
        skipped,
    )
    return FundamentalsUpsertSummary(inserted=inserted, updated=updated, skipped=skipped)


def upsert_fundamentals_quarterly(
    session: Session,
    symbol: str,
    frame: pd.DataFrame,
    *,
    source: str,
) -> FundamentalsUpsertSummary:
    """Insert or update quarterly fundamentals rows for one symbol from one source."""
    cleaned = _clean_symbol(symbol)
    if frame is None or frame.empty:
        return FundamentalsUpsertSummary(0, 0, 0)
    if "fiscal_year" not in frame.columns or "fiscal_quarter" not in frame.columns:
        raise KeyError("upsert_fundamentals_quarterly requires fiscal_year + fiscal_quarter")

    pairs = [
        (int(y), int(q))
        for y, q in zip(frame["fiscal_year"], frame["fiscal_quarter"], strict=False)
        if pd.notna(y) and pd.notna(q)
    ]
    existing_rows = session.scalars(
        select(FundamentalsQuarterly).where(
            FundamentalsQuarterly.symbol == cleaned,
            FundamentalsQuarterly.source == source,
        )
    ).all()
    existing = {(r.fiscal_year, r.fiscal_quarter): r for r in existing_rows}

    inserted = 0
    updated = 0
    skipped = 0
    now = utc_now()
    source_url = _first_str(frame, "source_url")

    for raw in frame.to_dict(orient="records"):
        fy = _safe_int(raw.get("fiscal_year"))
        fq = _safe_int(raw.get("fiscal_quarter"))
        if fy is None or fq is None or not (1 <= fq <= 4):
            skipped += 1
            continue
        values = {col: _safe_float(raw.get(col)) for col in _QUARTERLY_NUMERIC_COLUMNS}
        period_end = _safe_date(raw.get("period_end"))
        currency = str(raw.get("currency") or "INR")

        if (fy, fq) in existing:
            session.execute(
                update(FundamentalsQuarterly)
                .where(
                    FundamentalsQuarterly.symbol == cleaned,
                    FundamentalsQuarterly.source == source,
                    FundamentalsQuarterly.fiscal_year == fy,
                    FundamentalsQuarterly.fiscal_quarter == fq,
                )
                .values(
                    period_end=period_end,
                    currency=currency,
                    source_url=source_url,
                    fetched_at=now,
                    **values,
                )
            )
            updated += 1
        else:
            session.add(
                FundamentalsQuarterly(
                    symbol=cleaned,
                    fiscal_year=fy,
                    fiscal_quarter=fq,
                    period_end=period_end,
                    currency=currency,
                    source=source,
                    source_url=source_url,
                    fetched_at=now,
                    **values,
                )
            )
            inserted += 1

    # silence unused pairs (kept for clarity that we considered constraint scope)
    del pairs

    log.info(
        "upsert_fundamentals_quarterly symbol={} source={} inserted={} updated={} skipped={}",
        cleaned,
        source,
        inserted,
        updated,
        skipped,
    )
    return FundamentalsUpsertSummary(inserted=inserted, updated=updated, skipped=skipped)


def fetch_fundamentals_annual(
    session: Session,
    symbol: str,
    *,
    source: str | None = None,
) -> pd.DataFrame:
    """Return persisted annual fundamentals as a DataFrame, ascending by fiscal_year.

    When ``source`` is None and multiple sources exist, all rows are returned —
    the caller can pick or compare. With one source it behaves like a normal fetch.
    """
    cleaned = _clean_symbol(symbol)
    statement = select(FundamentalsAnnual).where(FundamentalsAnnual.symbol == cleaned)
    if source is not None:
        statement = statement.where(FundamentalsAnnual.source == source)
    statement = statement.order_by(
        FundamentalsAnnual.fiscal_year.asc(), FundamentalsAnnual.source.asc()
    )
    rows = session.scalars(statement).all()
    if not rows:
        return pd.DataFrame()

    records = []
    for r in rows:
        record: dict[str, object] = {
            "symbol": r.symbol,
            "fiscal_year": r.fiscal_year,
            "period_end": r.period_end,
            "currency": r.currency,
            "source": r.source,
            "source_url": r.source_url,
            "fetched_at": r.fetched_at,
        }
        for col in _ANNUAL_NUMERIC_COLUMNS:
            record[col] = getattr(r, col)
        records.append(record)
    return pd.DataFrame(records)


def fetch_fundamentals_quarterly(
    session: Session,
    symbol: str,
    *,
    source: str | None = None,
) -> pd.DataFrame:
    """Return persisted quarterly fundamentals, ascending by (fiscal_year, fiscal_quarter)."""
    cleaned = _clean_symbol(symbol)
    statement = select(FundamentalsQuarterly).where(FundamentalsQuarterly.symbol == cleaned)
    if source is not None:
        statement = statement.where(FundamentalsQuarterly.source == source)
    statement = statement.order_by(
        FundamentalsQuarterly.fiscal_year.asc(),
        FundamentalsQuarterly.fiscal_quarter.asc(),
        FundamentalsQuarterly.source.asc(),
    )
    rows = session.scalars(statement).all()
    if not rows:
        return pd.DataFrame()

    records = []
    for r in rows:
        record: dict[str, object] = {
            "symbol": r.symbol,
            "fiscal_year": r.fiscal_year,
            "fiscal_quarter": r.fiscal_quarter,
            "period_end": r.period_end,
            "currency": r.currency,
            "source": r.source,
            "source_url": r.source_url,
            "fetched_at": r.fetched_at,
        }
        for col in _QUARTERLY_NUMERIC_COLUMNS:
            record[col] = getattr(r, col)
        records.append(record)
    return pd.DataFrame(records)


def latest_fundamentals_period(
    session: Session,
    symbol: str,
    *,
    source: str | None = None,
) -> tuple[int, int] | None:
    """Return ``(fiscal_year, fiscal_quarter)`` for the latest persisted quarter, or None."""
    cleaned = _clean_symbol(symbol)
    statement = select(
        FundamentalsQuarterly.fiscal_year,
        FundamentalsQuarterly.fiscal_quarter,
    ).where(FundamentalsQuarterly.symbol == cleaned)
    if source is not None:
        statement = statement.where(FundamentalsQuarterly.source == source)
    statement = statement.order_by(
        FundamentalsQuarterly.fiscal_year.desc(),
        FundamentalsQuarterly.fiscal_quarter.desc(),
    ).limit(1)
    row = session.execute(statement).first()
    if row is None:
        return None
    return int(row[0]), int(row[1])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean_symbol(symbol: str) -> str:
    cleaned = str(symbol or "").strip().upper()
    if not cleaned:
        raise ValueError("symbol is required")
    return cleaned


def _safe_float(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _safe_int(value: object) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _safe_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        ts = pd.to_datetime(value, errors="coerce")
    except (TypeError, ValueError):
        return None
    if ts is pd.NaT or ts is None or pd.isna(ts):
        return None
    return ts.date()


def _first_str(frame: pd.DataFrame, column: str) -> str | None:
    if column not in frame.columns:
        return None
    values = frame[column].dropna()
    if values.empty:
        return None
    return str(values.iloc[0])
