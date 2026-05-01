"""Persisted corporate actions (splits, dividends, bonuses).

Splits and bonuses drive the price-adjustment pipeline. Dividends are stored
for audit and future total-return backtests but are not applied to the price
series used by trend indicators.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from stock_platform.db.models import CorporateAction, utc_now
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class CorporateActionUpsertSummary:
    """Counts returned by :func:`upsert_corporate_actions`."""

    inserted: int
    updated: int
    skipped: int


def upsert_corporate_actions(
    session: Session,
    symbol: str,
    frame: pd.DataFrame,
    *,
    action_type: str,
    source: str,
    value_column: str = "value",
    date_column: str = "ex_date",
    detail_column: str | None = None,
) -> CorporateActionUpsertSummary:
    """Upsert one row per ``(symbol, ex_date, action_type)``.

    The unique key matches the ``CorporateAction`` table. Rows missing a date
    or value are skipped — they would violate the table constraint anyway and
    can't be reconciled silently.
    """
    cleaned_symbol = str(symbol or "").strip().upper()
    if not cleaned_symbol:
        raise ValueError("symbol is required")
    if frame is None or frame.empty:
        return CorporateActionUpsertSummary(0, 0, 0)
    if date_column not in frame.columns or value_column not in frame.columns:
        raise KeyError(
            f"upsert_corporate_actions missing columns: "
            f"{[c for c in (date_column, value_column) if c not in frame.columns]}"
        )

    inserted = 0
    updated = 0
    skipped = 0
    now = utc_now()

    for record in frame.to_dict(orient="records"):
        ex_date = _coerce_date(record.get(date_column))
        if ex_date is None:
            skipped += 1
            continue

        value = _safe_float(record.get(value_column))
        if value is None:
            skipped += 1
            continue

        detail = (
            _clean_optional(record.get(detail_column))
            if detail_column and detail_column in frame.columns
            else None
        )

        existing_id = session.scalar(
            select(CorporateAction.id).where(
                CorporateAction.symbol == cleaned_symbol,
                CorporateAction.ex_date == ex_date,
                CorporateAction.action_type == action_type,
            )
        )

        values = {
            "symbol": cleaned_symbol,
            "ex_date": ex_date,
            "action_type": action_type,
            "value": value,
            "detail": detail,
            "source": source,
            "fetched_at": now,
        }

        if existing_id is None:
            session.add(CorporateAction(**values))
            inserted += 1
        else:
            session.execute(
                update(CorporateAction).where(CorporateAction.id == existing_id).values(**values)
            )
            updated += 1

    log.info(
        "upsert_corporate_actions symbol={} action_type={} source={} "
        "inserted={} updated={} skipped={}",
        cleaned_symbol,
        action_type,
        source,
        inserted,
        updated,
        skipped,
    )
    return CorporateActionUpsertSummary(inserted=inserted, updated=updated, skipped=skipped)


def fetch_corporate_actions(
    session: Session,
    symbol: str,
    *,
    action_type: str | None = None,
) -> pd.DataFrame:
    """Return persisted corporate actions for a symbol as a DataFrame.

    Columns: ``symbol, ex_date, action_type, value, detail, source``.
    """
    cleaned = str(symbol or "").strip().upper()
    columns = ["symbol", "ex_date", "action_type", "value", "detail", "source"]
    if not cleaned:
        return pd.DataFrame(columns=columns)

    statement = select(CorporateAction).where(CorporateAction.symbol == cleaned)
    if action_type is not None:
        statement = statement.where(CorporateAction.action_type == action_type)
    statement = statement.order_by(CorporateAction.ex_date.asc())

    rows = session.scalars(statement).all()
    if not rows:
        return pd.DataFrame(columns=columns)

    return pd.DataFrame(
        [
            {
                "symbol": row.symbol,
                "ex_date": row.ex_date,
                "action_type": row.action_type,
                "value": row.value,
                "detail": row.detail,
                "source": row.source,
            }
            for row in rows
        ]
    )


def _coerce_date(value: object):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        timestamp = pd.to_datetime(value, errors="coerce")
    except (TypeError, ValueError):
        return None
    if timestamp is pd.NaT or timestamp is None:
        return None
    try:
        return timestamp.date()
    except AttributeError:
        return None


def _safe_float(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _clean_optional(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    cleaned = str(value).strip()
    return cleaned or None
