"""Persisted daily composite-score snapshots."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

import pandas as pd
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from stock_platform.analytics.signals import SignalResult
from stock_platform.db.models import CompositeScoreSnapshot, utc_now
from stock_platform.scoring import CompositeScore
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class CompositeScoreUpsertSummary:
    """Counts returned by :func:`upsert_composite_score`."""

    inserted: int
    updated: int


def upsert_composite_score(
    session: Session,
    *,
    symbol: str,
    as_of_date: date,
    composite: CompositeScore,
    signals: Iterable[SignalResult] | None = None,
    source: str,
) -> CompositeScoreUpsertSummary:
    """Insert or update one ``(symbol, as_of_date, source)`` composite-score row."""
    cleaned_symbol = str(symbol or "").strip().upper()
    if not cleaned_symbol:
        raise ValueError("symbol is required")

    active_signal_names = (
        [s.name for s in signals if getattr(s, "active", False)] if signals else []
    )

    sub = composite.sub_scores or {}
    values = {
        "symbol": cleaned_symbol,
        "as_of_date": as_of_date,
        "score": _safe_float(composite.score),
        "band": composite.band or None,
        "fundamentals_score": _safe_float(sub.get("fundamentals")),
        "technicals_score": _safe_float(sub.get("technicals")),
        "flows_score": _safe_float(sub.get("flows")),
        "events_quality_score": _safe_float(sub.get("events_quality")),
        "macro_sector_score": _safe_float(sub.get("macro_sector")),
        "active_signal_count": len(active_signal_names),
        "active_signals_json": json.dumps(active_signal_names),
        "reasons_json": json.dumps(list(composite.reasons or [])),
        "risks_json": json.dumps(list(composite.risks or [])),
        "missing_data_json": json.dumps(list(composite.missing_data or [])),
        "source": source,
    }

    existing_id = session.scalar(
        select(CompositeScoreSnapshot.id).where(
            CompositeScoreSnapshot.symbol == cleaned_symbol,
            CompositeScoreSnapshot.as_of_date == as_of_date,
            CompositeScoreSnapshot.source == source,
        )
    )

    if existing_id is None:
        session.add(CompositeScoreSnapshot(**values))
        log.info(
            "composite score saved: symbol={} as_of_date={} score={} source={}",
            cleaned_symbol,
            as_of_date,
            values["score"],
            source,
        )
        return CompositeScoreUpsertSummary(inserted=1, updated=0)

    session.execute(
        update(CompositeScoreSnapshot)
        .where(CompositeScoreSnapshot.id == existing_id)
        .values(**values, updated_at=utc_now())
    )
    log.info(
        "composite score updated: symbol={} as_of_date={} score={} source={}",
        cleaned_symbol,
        as_of_date,
        values["score"],
        source,
    )
    return CompositeScoreUpsertSummary(inserted=0, updated=1)


def latest_composite_score(
    session: Session,
    symbol: str,
    *,
    source: str | None = None,
) -> CompositeScoreSnapshot | None:
    """Return the most recent persisted score row for a symbol."""
    cleaned = str(symbol or "").strip().upper()
    if not cleaned:
        return None

    statement = (
        select(CompositeScoreSnapshot)
        .where(CompositeScoreSnapshot.symbol == cleaned)
        .order_by(
            CompositeScoreSnapshot.as_of_date.desc(),
            CompositeScoreSnapshot.id.desc(),
        )
        .limit(1)
    )
    if source is not None:
        statement = statement.where(CompositeScoreSnapshot.source == source)
    return session.scalar(statement)


def fetch_composite_scores(
    session: Session,
    symbol: str,
    *,
    start: date | None = None,
    end: date | None = None,
    source: str | None = None,
) -> pd.DataFrame:
    """Return persisted composite-score rows as a DataFrame indexed by date."""
    columns = [
        "as_of_date",
        "score",
        "band",
        "fundamentals_score",
        "technicals_score",
        "flows_score",
        "events_quality_score",
        "macro_sector_score",
        "active_signal_count",
        "active_signals",
        "source",
    ]
    cleaned = str(symbol or "").strip().upper()
    if not cleaned:
        empty = pd.DataFrame(columns=columns)
        empty.index = pd.DatetimeIndex([], name="as_of_date")
        return empty

    statement = select(CompositeScoreSnapshot).where(CompositeScoreSnapshot.symbol == cleaned)
    if start is not None:
        statement = statement.where(CompositeScoreSnapshot.as_of_date >= start)
    if end is not None:
        statement = statement.where(CompositeScoreSnapshot.as_of_date <= end)
    if source is not None:
        statement = statement.where(CompositeScoreSnapshot.source == source)
    statement = statement.order_by(CompositeScoreSnapshot.as_of_date.asc())

    rows = session.scalars(statement).all()
    if not rows:
        empty = pd.DataFrame(columns=columns)
        empty.index = pd.DatetimeIndex([], name="as_of_date")
        return empty

    frame = pd.DataFrame(
        [
            {
                "as_of_date": row.as_of_date,
                "score": row.score,
                "band": row.band,
                "fundamentals_score": row.fundamentals_score,
                "technicals_score": row.technicals_score,
                "flows_score": row.flows_score,
                "events_quality_score": row.events_quality_score,
                "macro_sector_score": row.macro_sector_score,
                "active_signal_count": row.active_signal_count,
                "active_signals": ", ".join(_json_list(row.active_signals_json)),
                "source": row.source,
            }
            for row in rows
        ]
    )
    frame.index = pd.to_datetime(frame["as_of_date"])
    frame.index.name = "as_of_date"
    return frame


def _safe_float(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]
