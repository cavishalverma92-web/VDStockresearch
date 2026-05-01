"""Persisted daily OHLCV bars used by indicators, scanner, and backtests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from stock_platform.db.models import PriceDaily, utc_now
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class PriceUpsertSummary:
    """Counts returned by :func:`upsert_price_daily`."""

    inserted: int
    updated: int
    skipped: int


_PRICE_COLUMNS = ("open", "high", "low", "close")


def upsert_price_daily(
    session: Session,
    symbol: str,
    frame: pd.DataFrame,
    *,
    source: str,
) -> PriceUpsertSummary:
    """Insert or update OHLCV rows for one symbol from a single source.

    The incoming frame must be indexed by date (DatetimeIndex or date objects)
    and have ``open/high/low/close`` columns; ``volume`` defaults to 0 when
    absent. Rows with any missing OHLC value are skipped and logged.
    """
    cleaned_symbol = str(symbol or "").strip().upper()
    if not cleaned_symbol:
        raise ValueError("symbol is required")
    if frame is None or frame.empty:
        return PriceUpsertSummary(0, 0, 0)

    missing = [column for column in _PRICE_COLUMNS if column not in frame.columns]
    if missing:
        raise KeyError(f"upsert_price_daily missing columns for {cleaned_symbol}: {missing}")

    incoming = _prepare_frame(frame)
    if incoming.empty:
        return PriceUpsertSummary(0, 0, 0)

    incoming_dates = list(incoming.index.unique())
    existing_dates = set(
        session.scalars(
            select(PriceDaily.trade_date).where(
                PriceDaily.symbol == cleaned_symbol,
                PriceDaily.source == source,
                PriceDaily.trade_date.in_(incoming_dates),
            )
        ).all()
    )

    inserted = 0
    updated = 0
    skipped = 0
    now = utc_now()

    for trade_date, row in incoming.iterrows():
        ohlc = _ohlc_values(row)
        if ohlc is None:
            skipped += 1
            continue
        volume = _safe_float(row.get("volume"), default=0.0)

        if trade_date in existing_dates:
            session.execute(
                update(PriceDaily)
                .where(
                    PriceDaily.symbol == cleaned_symbol,
                    PriceDaily.trade_date == trade_date,
                    PriceDaily.source == source,
                )
                .values(
                    open=ohlc["open"],
                    high=ohlc["high"],
                    low=ohlc["low"],
                    close=ohlc["close"],
                    volume=volume,
                    fetched_at=now,
                )
            )
            updated += 1
        else:
            session.add(
                PriceDaily(
                    symbol=cleaned_symbol,
                    trade_date=trade_date,
                    open=ohlc["open"],
                    high=ohlc["high"],
                    low=ohlc["low"],
                    close=ohlc["close"],
                    volume=volume,
                    source=source,
                    fetched_at=now,
                )
            )
            inserted += 1

    log.info(
        "upsert_price_daily symbol={} source={} inserted={} updated={} skipped={}",
        cleaned_symbol,
        source,
        inserted,
        updated,
        skipped,
    )
    return PriceUpsertSummary(inserted=inserted, updated=updated, skipped=skipped)


def latest_trade_date(
    session: Session,
    symbol: str,
    *,
    source: str | None = None,
) -> date | None:
    """Return the latest stored ``trade_date`` for a symbol, optionally per source."""
    cleaned = str(symbol or "").strip().upper()
    if not cleaned:
        return None

    statement = (
        select(PriceDaily.trade_date)
        .where(PriceDaily.symbol == cleaned)
        .order_by(PriceDaily.trade_date.desc())
        .limit(1)
    )
    if source is not None:
        statement = statement.where(PriceDaily.source == source)

    return session.scalar(statement)


def fetch_price_daily(
    session: Session,
    symbol: str,
    *,
    start: date | None = None,
    end: date | None = None,
    source: str | None = None,
) -> pd.DataFrame:
    """Return persisted OHLCV rows as a DataFrame indexed by trade_date.

    The frame matches the shape produced by the live providers so it is a
    drop-in replacement for ``provider.get_ohlcv()``: columns are
    ``open/high/low/close/volume`` plus ``source`` and ``symbol`` metadata.
    """
    cleaned = str(symbol or "").strip().upper()
    if not cleaned:
        return _empty_price_frame()

    statement = select(PriceDaily).where(PriceDaily.symbol == cleaned)
    if start is not None:
        statement = statement.where(PriceDaily.trade_date >= start)
    if end is not None:
        statement = statement.where(PriceDaily.trade_date <= end)
    if source is not None:
        statement = statement.where(PriceDaily.source == source)
    statement = statement.order_by(PriceDaily.trade_date.asc())

    rows = session.scalars(statement).all()
    if not rows:
        return _empty_price_frame()

    frame = pd.DataFrame(
        {
            "open": [row.open for row in rows],
            "high": [row.high for row in rows],
            "low": [row.low for row in rows],
            "close": [row.close for row in rows],
            "volume": [row.volume for row in rows],
            "source": [row.source for row in rows],
            "symbol": [row.symbol for row in rows],
        },
        index=pd.to_datetime([row.trade_date for row in rows]),
    )
    frame.index.name = "date"
    frame["adj_close"] = frame["close"]
    return frame


def _prepare_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize the incoming frame's index to ``date`` and drop duplicates."""
    working = frame.copy()
    if not isinstance(working.index, pd.DatetimeIndex):
        working.index = pd.to_datetime(working.index, errors="coerce")
    working = working[~working.index.isna()].copy()
    if working.empty:
        return working
    working.index = pd.DatetimeIndex(working.index).tz_localize(None).normalize()
    working = working[~working.index.duplicated(keep="last")]
    working.index = pd.Index([ts.date() for ts in working.index], name="trade_date")
    return working.sort_index()


def _ohlc_values(row: pd.Series) -> dict[str, float] | None:
    values: dict[str, float] = {}
    for column in _PRICE_COLUMNS:
        raw = row.get(column)
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            return None
        try:
            values[column] = float(raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None
    return values


def _safe_float(value: object, *, default: float = 0.0) -> float:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _empty_price_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        columns=["open", "high", "low", "close", "volume", "source", "symbol", "adj_close"]
    )
    frame.index = pd.DatetimeIndex([], name="date")
    return frame
