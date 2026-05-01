"""Market Today dashboard assembly.

This module reads persisted local tables instead of making fresh network calls.
The Streamlit homepage should be fast, calm, and useful even when Kite tokens
are missing or market-data providers are temporarily down.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from stock_platform.auth import kite_token_path
from stock_platform.config import get_settings
from stock_platform.db import create_all_tables, get_engine
from stock_platform.db.models import (
    CompositeScoreSnapshot,
    CorporateAction,
    PriceDaily,
)
from stock_platform.ops.data_health import DataHealthReport, build_data_health_report

IST = ZoneInfo("Asia/Kolkata")


@dataclass(frozen=True)
class ProviderHealth:
    """Simple red/amber/green provider status for the homepage."""

    label: str
    color: str
    detail: str


@dataclass(frozen=True)
class MarketBreadth:
    """Advance/decline summary from persisted daily prices."""

    latest_trade_date: date | None
    compared_symbols: int
    advances: int
    declines: int
    unchanged: int
    advance_decline_ratio: float | None


@dataclass(frozen=True)
class KiteTokenCountdown:
    """Token freshness without exposing the token itself."""

    configured: bool
    source: str
    saved_at: datetime | None
    expires_at: datetime | None
    hours_remaining: float | None
    status: str
    message: str


@dataclass(frozen=True)
class MarketTodaySummary:
    """Single bundle consumed by the Streamlit homepage."""

    provider_health: ProviderHealth
    breadth: MarketBreadth
    kite_token: KiteTokenCountdown
    score_movers: pd.DataFrame
    top_attention: pd.DataFrame
    upcoming_events: pd.DataFrame
    stale_symbols: pd.DataFrame
    health: DataHealthReport
    generated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


def build_market_today_summary(
    *,
    engine: Engine | None = None,
    today: date | None = None,
    now: datetime | None = None,
    score_limit: int = 5,
    stale_limit: int = 10,
    event_trading_days: int = 5,
) -> MarketTodaySummary:
    """Build the persisted-data homepage summary."""
    active_engine = engine or get_engine()
    create_all_tables(active_engine)
    snapshot_today = today or date.today()
    snapshot_now = now or datetime.now(UTC)

    health = build_data_health_report(engine=active_engine, today=snapshot_today)
    latest_run = health.recent_refresh_runs[0] if health.recent_refresh_runs else None

    with Session(active_engine) as session:
        breadth = _market_breadth(session)
        score_movers = _score_movers(session, limit=score_limit)
        top_attention = _top_attention(session, limit=score_limit)
        upcoming_events = _upcoming_events(
            session,
            today=snapshot_today,
            trading_days=event_trading_days,
        )

    return MarketTodaySummary(
        provider_health=_provider_health(latest_run),
        breadth=breadth,
        kite_token=_kite_token_countdown(now=snapshot_now),
        score_movers=score_movers,
        top_attention=top_attention,
        upcoming_events=upcoming_events,
        stale_symbols=_stale_symbols_frame(health, limit=stale_limit),
        health=health,
        generated_at=snapshot_now,
    )


def _provider_health(latest_run) -> ProviderHealth:
    if latest_run is None:
        return ProviderHealth(
            label="No refresh yet",
            color="amber",
            detail="Run the EOD refresh before treating saved data as current.",
        )
    status = str(latest_run.status or "").lower()
    if status == "completed" and latest_run.failed_symbols == 0:
        return ProviderHealth(
            label="Healthy",
            color="green",
            detail=f"Last refresh #{latest_run.run_id} completed without failed symbols.",
        )
    if status in {"completed", "completed_with_errors"}:
        return ProviderHealth(
            label="Partial",
            color="amber",
            detail=(
                f"Last refresh #{latest_run.run_id} had "
                f"{latest_run.failed_symbols} failed symbol(s)."
            ),
        )
    return ProviderHealth(
        label="Needs attention",
        color="red",
        detail=f"Last refresh #{latest_run.run_id} status: {latest_run.status}.",
    )


def _market_breadth(session: Session) -> MarketBreadth:
    rows = session.execute(
        select(PriceDaily.symbol, PriceDaily.trade_date, PriceDaily.close).order_by(
            PriceDaily.symbol.asc(),
            PriceDaily.trade_date.desc(),
            PriceDaily.id.desc(),
        )
    ).all()
    if not rows:
        return MarketBreadth(None, 0, 0, 0, 0, None)

    by_symbol: dict[str, list[tuple[date, float]]] = {}
    latest_trade_date: date | None = None
    for symbol, trade_date, close in rows:
        cleaned = str(symbol)
        by_symbol.setdefault(cleaned, [])
        if len(by_symbol[cleaned]) < 2:
            by_symbol[cleaned].append((trade_date, float(close)))
        if latest_trade_date is None or trade_date > latest_trade_date:
            latest_trade_date = trade_date

    advances = declines = unchanged = compared = 0
    for points in by_symbol.values():
        if len(points) < 2:
            continue
        latest_close = points[0][1]
        previous_close = points[1][1]
        compared += 1
        if latest_close > previous_close:
            advances += 1
        elif latest_close < previous_close:
            declines += 1
        else:
            unchanged += 1

    ratio = round(advances / declines, 2) if declines else (float(advances) if advances else None)
    return MarketBreadth(latest_trade_date, compared, advances, declines, unchanged, ratio)


def _score_movers(session: Session, *, limit: int) -> pd.DataFrame:
    rows = session.execute(
        select(
            CompositeScoreSnapshot.symbol,
            CompositeScoreSnapshot.as_of_date,
            CompositeScoreSnapshot.score,
            CompositeScoreSnapshot.band,
            CompositeScoreSnapshot.active_signal_count,
            CompositeScoreSnapshot.source,
        ).order_by(
            CompositeScoreSnapshot.symbol.asc(),
            CompositeScoreSnapshot.as_of_date.desc(),
            CompositeScoreSnapshot.id.desc(),
        )
    ).all()
    by_symbol: dict[str, list[object]] = {}
    for row in rows:
        by_symbol.setdefault(str(row.symbol), [])
        if len(by_symbol[str(row.symbol)]) < 2 and row.score is not None:
            by_symbol[str(row.symbol)].append(row)

    records: list[dict[str, object]] = []
    for symbol, points in by_symbol.items():
        if len(points) < 2:
            continue
        latest, previous = points[0], points[1]
        change = float(latest.score) - float(previous.score)
        records.append(
            {
                "symbol": symbol,
                "latest_date": latest.as_of_date,
                "previous_date": previous.as_of_date,
                "score": round(float(latest.score), 1),
                "previous_score": round(float(previous.score), 1),
                "score_change": round(change, 1),
                "band": latest.band,
                "signals": int(latest.active_signal_count or 0),
                "source": latest.source,
            }
        )

    frame = pd.DataFrame(records)
    if frame.empty:
        return _empty_score_movers_frame()
    frame["abs_change"] = frame["score_change"].abs()
    return (
        frame.sort_values(["abs_change", "score"], ascending=[False, False])
        .drop(columns=["abs_change"])
        .head(max(1, int(limit)))
        .reset_index(drop=True)
    )


def _top_attention(session: Session, *, limit: int) -> pd.DataFrame:
    latest_date = session.scalar(
        select(CompositeScoreSnapshot.as_of_date)
        .order_by(CompositeScoreSnapshot.as_of_date.desc())
        .limit(1)
    )
    if latest_date is None:
        return _empty_attention_frame()

    rows = session.scalars(
        select(CompositeScoreSnapshot)
        .where(CompositeScoreSnapshot.as_of_date == latest_date)
        .order_by(
            CompositeScoreSnapshot.score.desc().nullslast(),
            CompositeScoreSnapshot.active_signal_count.desc(),
        )
        .limit(max(1, int(limit)))
    ).all()
    if not rows:
        return _empty_attention_frame()

    records = []
    for row in rows:
        risks = _json_list(row.risks_json)
        missing = _json_list(row.missing_data_json)
        records.append(
            {
                "symbol": row.symbol,
                "as_of_date": row.as_of_date,
                "score": None if row.score is None else round(float(row.score), 1),
                "band": row.band,
                "signals": row.active_signal_count,
                "risk_callout": risks[0] if risks else (missing[0] if missing else "Review chart"),
                "source": row.source,
            }
        )
    return pd.DataFrame(records)


def _upcoming_events(session: Session, *, today: date, trading_days: int) -> pd.DataFrame:
    end_date = _business_day_offset(today, max(1, int(trading_days)))
    rows = session.scalars(
        select(CorporateAction)
        .where(CorporateAction.ex_date >= today, CorporateAction.ex_date <= end_date)
        .order_by(CorporateAction.ex_date.asc(), CorporateAction.symbol.asc())
        .limit(20)
    ).all()
    if not rows:
        return pd.DataFrame(columns=["symbol", "date", "event", "detail", "source", "days_until"])
    return pd.DataFrame(
        [
            {
                "symbol": row.symbol,
                "date": row.ex_date,
                "event": row.action_type,
                "detail": row.detail or row.value,
                "source": row.source,
                "days_until": (row.ex_date - today).days,
            }
            for row in rows
        ]
    )


def _stale_symbols_frame(health: DataHealthReport, *, limit: int) -> pd.DataFrame:
    if not health.stale_symbols:
        return pd.DataFrame(columns=["symbol", "latest_trade_date", "days_stale"])
    return pd.DataFrame(
        [
            {
                "symbol": row.symbol,
                "latest_trade_date": row.latest_trade_date,
                "days_stale": row.days_stale,
            }
            for row in health.stale_symbols[: max(1, int(limit))]
        ]
    )


def _kite_token_countdown(*, now: datetime) -> KiteTokenCountdown:
    settings = get_settings()
    token_configured = False
    source = "missing"
    saved_at = _read_token_saved_at()

    if kite_token_path().exists():
        token_configured = True
        source = "store"
    elif (settings.kite_access_token or "").strip():
        token_configured = True
        source = "env"

    if not token_configured:
        return KiteTokenCountdown(
            configured=False,
            source="missing",
            saved_at=None,
            expires_at=None,
            hours_remaining=None,
            status="missing",
            message="Kite token missing; app will use yfinance fallback where possible.",
        )

    if saved_at is None:
        return KiteTokenCountdown(
            configured=True,
            source=source,
            saved_at=None,
            expires_at=None,
            hours_remaining=None,
            status="unknown",
            message="Token is configured, but saved time is unknown. Test Kite in Settings.",
        )

    now_ist = _as_ist(now)
    expiry = _kite_expiry_for_saved_at(saved_at)
    hours_remaining = round((expiry - now_ist).total_seconds() / 3600, 2)
    if hours_remaining <= 0:
        status = "expired"
        message = "Kite token likely expired; refresh it in Settings."
    elif hours_remaining < 2:
        status = "warning"
        message = f"Kite token may expire soon: about {hours_remaining:.1f} hour(s) left."
    else:
        status = "ok"
        message = f"Kite token window: about {hours_remaining:.1f} hour(s) left."

    return KiteTokenCountdown(
        configured=True,
        source=source,
        saved_at=saved_at,
        expires_at=expiry,
        hours_remaining=hours_remaining,
        status=status,
        message=message,
    )


def _read_token_saved_at() -> datetime | None:
    path = kite_token_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or not payload.get("saved_at"):
        return None
    try:
        saved_at = datetime.fromisoformat(str(payload["saved_at"]))
    except ValueError:
        return None
    return _as_ist(saved_at)


def _kite_expiry_for_saved_at(saved_at: datetime) -> datetime:
    saved_ist = _as_ist(saved_at)
    same_day_expiry = datetime.combine(saved_ist.date(), time(hour=6), tzinfo=IST)
    if saved_ist < same_day_expiry:
        return same_day_expiry
    return same_day_expiry + timedelta(days=1)


def _as_ist(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(IST)


def _business_day_offset(start: date, business_days: int) -> date:
    current = start
    remaining = business_days
    while remaining > 0:
        current += timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


def _json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _empty_score_movers_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol",
            "latest_date",
            "previous_date",
            "score",
            "previous_score",
            "score_change",
            "band",
            "signals",
            "source",
        ]
    )


def _empty_attention_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["symbol", "as_of_date", "score", "band", "signals", "risk_callout", "source"]
    )
