"""Persistence helpers for daily FII/DII market flows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from stock_platform.db.models import MarketFlowDaily, utc_now
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class MarketFlowsUpsertSummary:
    inserted: int
    updated: int
    skipped: int


def upsert_market_flows(
    session: Session,
    frame: pd.DataFrame,
    *,
    source: str = "nse",
) -> MarketFlowsUpsertSummary:
    """Insert or update FII/DII rows keyed on (trade_date, participant, source)."""
    if frame is None or frame.empty:
        return MarketFlowsUpsertSummary(0, 0, 0)
    required = {"trade_date", "participant"}
    missing = required - set(frame.columns)
    if missing:
        raise KeyError(f"upsert_market_flows missing columns: {sorted(missing)}")

    pairs = [
        (_as_date(row.get("trade_date")), str(row.get("participant") or "").strip().upper())
        for row in frame.to_dict(orient="records")
    ]
    valid_pairs = {(d, p) for d, p in pairs if d is not None and p}
    if not valid_pairs:
        # All rows malformed — count them as skipped so callers can detect it.
        return MarketFlowsUpsertSummary(0, 0, len(pairs))

    existing_rows = session.scalars(
        select(MarketFlowDaily).where(
            MarketFlowDaily.source == source,
            MarketFlowDaily.trade_date.in_({d for d, _ in valid_pairs}),
        )
    ).all()
    existing = {(r.trade_date, r.participant): r for r in existing_rows}

    inserted = 0
    updated = 0
    skipped = 0
    now = utc_now()

    for record in frame.to_dict(orient="records"):
        trade_date = _as_date(record.get("trade_date"))
        participant = str(record.get("participant") or "").strip().upper()
        if trade_date is None or not participant:
            skipped += 1
            continue
        values = {
            "buy_value_cr": _as_float(record.get("buy_value_cr")),
            "sell_value_cr": _as_float(record.get("sell_value_cr")),
            "net_value_cr": _as_float(record.get("net_value_cr")),
            "source_url": record.get("source_url") or None,
            "fetched_at": now,
        }
        if (trade_date, participant) in existing:
            session.execute(
                update(MarketFlowDaily)
                .where(
                    MarketFlowDaily.trade_date == trade_date,
                    MarketFlowDaily.participant == participant,
                    MarketFlowDaily.source == source,
                )
                .values(**values)
            )
            updated += 1
        else:
            session.add(
                MarketFlowDaily(
                    trade_date=trade_date,
                    participant=participant,
                    source=source,
                    **values,
                )
            )
            inserted += 1

    log.info(
        "upsert_market_flows source={} inserted={} updated={} skipped={}",
        source,
        inserted,
        updated,
        skipped,
    )
    return MarketFlowsUpsertSummary(inserted=inserted, updated=updated, skipped=skipped)


def fetch_market_flows(
    session: Session,
    *,
    start: date | None = None,
    end: date | None = None,
    participant: str | None = None,
    source: str | None = None,
) -> pd.DataFrame:
    """Return persisted flows as a DataFrame, ascending by trade_date."""
    statement = select(MarketFlowDaily)
    if start is not None:
        statement = statement.where(MarketFlowDaily.trade_date >= start)
    if end is not None:
        statement = statement.where(MarketFlowDaily.trade_date <= end)
    if participant is not None:
        statement = statement.where(MarketFlowDaily.participant == participant.upper())
    if source is not None:
        statement = statement.where(MarketFlowDaily.source == source)
    statement = statement.order_by(
        MarketFlowDaily.trade_date.asc(),
        MarketFlowDaily.participant.asc(),
    )
    rows = session.scalars(statement).all()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(
        [
            {
                "trade_date": r.trade_date,
                "participant": r.participant,
                "buy_value_cr": r.buy_value_cr,
                "sell_value_cr": r.sell_value_cr,
                "net_value_cr": r.net_value_cr,
                "source": r.source,
                "source_url": r.source_url,
                "fetched_at": r.fetched_at,
            }
            for r in rows
        ]
    )


def latest_market_flow_date(
    session: Session,
    *,
    source: str | None = None,
) -> date | None:
    """Return the latest persisted ``trade_date`` across all participants."""
    statement = (
        select(MarketFlowDaily.trade_date).order_by(MarketFlowDaily.trade_date.desc()).limit(1)
    )
    if source is not None:
        statement = statement.where(MarketFlowDaily.source == source)
    return session.scalar(statement)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _as_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        ts = pd.to_datetime(value, errors="coerce")
    except (TypeError, ValueError):
        return None
    if ts is pd.NaT or ts is None or pd.isna(ts):
        return None
    return ts.date()


def _as_float(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if value is pd.NA:
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
