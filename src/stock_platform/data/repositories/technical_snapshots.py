"""Persisted daily technical-indicator snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from stock_platform.db.models import TechnicalSnapshot, utc_now
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class TechnicalUpsertSummary:
    """Counts returned by :func:`upsert_technical_snapshots`."""

    inserted: int
    updated: int
    skipped: int


_INDICATOR_COLUMNS = (
    "close",
    "rsi_14",
    "macd",
    "macd_signal",
    "macd_hist",
    "atr_14",
    "atr_pct",
    "relative_volume",
    "sma_20",
    "sma_50",
    "sma_100",
    "sma_200",
    "ema_20",
    "ema_50",
    "ema_100",
    "ema_200",
)


def upsert_technical_snapshots(
    session: Session,
    symbol: str,
    enriched_frame: pd.DataFrame,
    *,
    source: str,
    only_after: date | None = None,
) -> TechnicalUpsertSummary:
    """Persist indicator rows from a frame produced by ``add_technical_indicators``.

    Args:
        session: active SQLAlchemy session.
        symbol: app symbol (e.g. ``RELIANCE.NS``).
        enriched_frame: DataFrame with a date index and indicator columns. Rows
            where every indicator is missing are skipped — early bars in a
            history won't have 200-EMA values yet, and storing them adds noise.
        source: provenance tag, propagated to the row.
        only_after: when provided, restricts the upsert to rows with
            ``as_of_date >= only_after``. Use this to limit work during
            incremental refresh.
    """
    cleaned_symbol = str(symbol or "").strip().upper()
    if not cleaned_symbol:
        raise ValueError("symbol is required")
    if enriched_frame is None or enriched_frame.empty:
        return TechnicalUpsertSummary(0, 0, 0)

    indicator_columns = [c for c in _INDICATOR_COLUMNS if c in enriched_frame.columns]
    if not indicator_columns:
        return TechnicalUpsertSummary(0, 0, 0)

    working = enriched_frame.copy()
    if not isinstance(working.index, pd.DatetimeIndex):
        working.index = pd.to_datetime(working.index, errors="coerce")
    working = working[~working.index.isna()].copy()
    working.index = pd.DatetimeIndex(working.index).tz_localize(None).normalize()
    working.index = pd.Index([ts.date() for ts in working.index], name="as_of_date")
    working = working[~working.index.duplicated(keep="last")]

    if only_after is not None:
        working = working[working.index >= only_after]
    if working.empty:
        return TechnicalUpsertSummary(0, 0, 0)

    incoming_dates = list(working.index.unique())
    existing_dates = set(
        session.scalars(
            select(TechnicalSnapshot.as_of_date).where(
                TechnicalSnapshot.symbol == cleaned_symbol,
                TechnicalSnapshot.source == source,
                TechnicalSnapshot.as_of_date.in_(incoming_dates),
            )
        ).all()
    )

    inserted = 0
    updated = 0
    skipped = 0
    now = utc_now()

    for as_of_date, row in working.iterrows():
        values = _row_to_values(row, indicator_columns)
        if not _has_any_indicator(values):
            skipped += 1
            continue

        if as_of_date in existing_dates:
            session.execute(
                update(TechnicalSnapshot)
                .where(
                    TechnicalSnapshot.symbol == cleaned_symbol,
                    TechnicalSnapshot.as_of_date == as_of_date,
                    TechnicalSnapshot.source == source,
                )
                .values(
                    **values,
                    ma_stack_status=_clean_optional(row.get("ma_stack_status")),
                    created_at=now,
                )
            )
            updated += 1
        else:
            session.add(
                TechnicalSnapshot(
                    symbol=cleaned_symbol,
                    as_of_date=as_of_date,
                    source=source,
                    ma_stack_status=_clean_optional(row.get("ma_stack_status")),
                    created_at=now,
                    **values,
                )
            )
            inserted += 1

    log.info(
        "upsert_technical_snapshots symbol={} source={} inserted={} updated={} skipped={}",
        cleaned_symbol,
        source,
        inserted,
        updated,
        skipped,
    )
    return TechnicalUpsertSummary(inserted=inserted, updated=updated, skipped=skipped)


def _row_to_values(row: pd.Series, columns: list[str]) -> dict[str, float | None]:
    values: dict[str, float | None] = {}
    for column in columns:
        raw = row.get(column)
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            values[column] = None
        else:
            try:
                values[column] = float(raw)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                values[column] = None
    return values


def _has_any_indicator(values: dict[str, float | None]) -> bool:
    return any(v is not None for k, v in values.items() if k != "close")


def _clean_optional(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    cleaned = str(value).strip()
    return cleaned or None
