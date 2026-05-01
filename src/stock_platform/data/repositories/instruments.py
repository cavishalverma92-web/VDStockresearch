"""Persisted Kite/exchange instrument metadata.

The platform never places orders; ``instrument_master`` exists only to map app
symbols (e.g. ``RELIANCE.NS``) to broker/exchange instrument tokens used by the
historical-candle and quote endpoints.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from stock_platform.db.models import InstrumentMaster, utc_now
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class InstrumentUpsertSummary:
    """Counts returned by :func:`upsert_instruments`."""

    inserted: int
    updated: int
    skipped: int


_REQUIRED_COLUMNS = ("instrument_token", "tradingsymbol", "exchange")


def upsert_instruments(
    session: Session,
    frame: pd.DataFrame,
    *,
    source: str = "kite",
) -> InstrumentUpsertSummary:
    """Upsert one row per ``(exchange, tradingsymbol, segment, instrument_type)``.

    Rows missing an ``instrument_token`` or ``tradingsymbol`` are skipped — the
    unique constraint requires both. The function never raises on a single bad
    row; it skips and logs.
    """
    if frame is None or frame.empty:
        return InstrumentUpsertSummary(0, 0, 0)

    missing = [column for column in _REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise KeyError(f"upsert_instruments missing columns: {missing}")

    inserted = 0
    updated = 0
    skipped = 0
    now = utc_now()

    for record in frame.to_dict(orient="records"):
        token = _safe_int(record.get("instrument_token"))
        tradingsymbol = str(record.get("tradingsymbol") or "").strip()
        exchange = str(record.get("exchange") or "").strip().upper()
        segment = _clean_optional(record.get("segment"))
        instrument_type = _clean_optional(record.get("instrument_type"))

        if not token or not tradingsymbol or not exchange:
            skipped += 1
            continue

        existing_id = session.scalar(
            select(InstrumentMaster.id).where(
                InstrumentMaster.exchange == exchange,
                InstrumentMaster.tradingsymbol == tradingsymbol,
                _eq_or_is_null(InstrumentMaster.segment, segment),
                _eq_or_is_null(InstrumentMaster.instrument_type, instrument_type),
            )
        )

        values = {
            "instrument_token": token,
            "exchange_token": _safe_int(record.get("exchange_token")),
            "tradingsymbol": tradingsymbol,
            "name": _clean_optional(record.get("name")),
            "exchange": exchange,
            "segment": segment,
            "instrument_type": instrument_type,
            "tick_size": _safe_float(record.get("tick_size")),
            "lot_size": _safe_int(record.get("lot_size")),
            "expiry": _safe_date(record.get("expiry")),
            "strike": _safe_float(record.get("strike")),
            "source": source,
            "fetched_at": now,
        }

        if existing_id is None:
            session.add(InstrumentMaster(**values))
            inserted += 1
        else:
            session.execute(
                update(InstrumentMaster).where(InstrumentMaster.id == existing_id).values(**values)
            )
            updated += 1

    log.info(
        "upsert_instruments source={} inserted={} updated={} skipped={}",
        source,
        inserted,
        updated,
        skipped,
    )
    return InstrumentUpsertSummary(inserted=inserted, updated=updated, skipped=skipped)


def find_instrument_token(
    session: Session,
    tradingsymbol: str,
    *,
    exchange: str = "NSE",
    segment: str | None = None,
    instrument_type: str | None = None,
) -> int | None:
    """Return the persisted Kite instrument token for a trading symbol."""
    cleaned = str(tradingsymbol or "").strip().upper()
    if not cleaned:
        return None

    statement = select(InstrumentMaster.instrument_token).where(
        InstrumentMaster.exchange == exchange.upper(),
        InstrumentMaster.tradingsymbol == cleaned,
    )
    if segment is not None:
        statement = statement.where(_eq_or_is_null(InstrumentMaster.segment, segment))
    if instrument_type is not None:
        statement = statement.where(
            _eq_or_is_null(InstrumentMaster.instrument_type, instrument_type)
        )

    token = session.scalar(statement)
    return int(token) if token is not None else None


def count_instruments(session: Session, *, exchange: str | None = None) -> int:
    """Return the count of stored instruments, optionally filtered by exchange."""
    statement = select(InstrumentMaster.id)
    if exchange is not None:
        statement = statement.where(InstrumentMaster.exchange == exchange.upper())
    return len(session.scalars(statement).all())


def _eq_or_is_null(column, value):
    return column.is_(None) if value in (None, "") else column == value


def _clean_optional(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _safe_int(value: object) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _safe_float(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _safe_date(value: object):
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
