"""Data Health report — single-pane view of platform freshness and trust.

Reads only the persisted tables built by ``jobs/refresh_eod_candles`` and
``jobs/sync_instruments``. Pure data assembly; the Streamlit UI consumes the
returned dataclasses without doing any of its own SQL.

Trading and portfolio paths are not touched.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import Engine, desc, func, select
from sqlalchemy.orm import Session

from stock_platform.auth import kite_token_path
from stock_platform.config import get_settings
from stock_platform.db import create_all_tables, get_engine
from stock_platform.db.models import (
    CompositeScoreSnapshot,
    DailyRefreshRun,
    IndexMembershipHistory,
    InstrumentMaster,
    PriceDaily,
)


@dataclass(frozen=True)
class KiteTokenStatus:
    """Where the Kite access token currently lives, without revealing it."""

    configured: bool
    source: str  # "store" / "env" / "missing"
    store_path: str | None
    api_key_configured: bool
    api_secret_configured: bool


@dataclass(frozen=True)
class RefreshRunSummary:
    """One persisted ``daily_refresh_runs`` row."""

    run_id: int
    universe: str
    status: str
    started_at: datetime | None
    finished_at: datetime | None
    successful_symbols: int
    failed_symbols: int
    price_rows_upserted: int
    technical_rows_upserted: int
    age_seconds: float | None


@dataclass(frozen=True)
class PriceCoverageSummary:
    """How much OHLCV history the platform actually owns."""

    distinct_symbols: int
    total_rows: int
    oldest_trade_date: date | None
    newest_trade_date: date | None
    by_source: dict[str, int]


@dataclass(frozen=True)
class CompositeScoreCoverage:
    """Persisted composite-score coverage."""

    distinct_symbols: int
    total_rows: int
    rows_last_7_days: int
    latest_as_of_date: date | None


@dataclass(frozen=True)
class InstrumentCoverage:
    """Instrument master coverage by exchange."""

    by_exchange: dict[str, int]
    total: int


@dataclass(frozen=True)
class IndexMembershipCoverage:
    """Current point-in-time index membership coverage."""

    index_name: str
    active_members: int
    total_periods: int
    earliest_from_date: date | None
    latest_from_date: date | None
    latest_observed_at: datetime | None
    source_url: str | None
    historical_backfill_ready: bool
    warning: str | None


@dataclass(frozen=True)
class StaleSymbol:
    """One symbol whose latest persisted bar is older than the threshold."""

    symbol: str
    latest_trade_date: date
    days_stale: int


@dataclass(frozen=True)
class DataHealthReport:
    """Top-level Data Health bundle."""

    generated_at: datetime
    today: date
    stale_threshold_days: int
    kite_token: KiteTokenStatus
    recent_refresh_runs: list[RefreshRunSummary] = field(default_factory=list)
    price_coverage: PriceCoverageSummary | None = None
    composite_score_coverage: CompositeScoreCoverage | None = None
    instrument_coverage: InstrumentCoverage | None = None
    index_membership_coverage: IndexMembershipCoverage | None = None
    stale_symbols: list[StaleSymbol] = field(default_factory=list)


def build_data_health_report(
    *,
    engine: Engine | None = None,
    today: date | None = None,
    stale_threshold_days: int = 5,
    refresh_run_limit: int = 5,
    stale_symbol_limit: int = 20,
) -> DataHealthReport:
    """Assemble a :class:`DataHealthReport` from persisted tables."""
    active_engine = engine or get_engine()
    create_all_tables(active_engine)
    snapshot_today = today or date.today()
    generated_at = datetime.now(UTC)

    token_status = _kite_token_status()

    with Session(active_engine) as session:
        recent_runs = _recent_refresh_runs(session, refresh_run_limit, generated_at)
        price_coverage = _price_coverage(session)
        composite_coverage = _composite_score_coverage(session, snapshot_today)
        instrument_coverage = _instrument_coverage(session)
        index_membership_coverage = _index_membership_coverage(session, index_name="Nifty 50")
        stale = _stale_symbols(
            session,
            today=snapshot_today,
            threshold_days=stale_threshold_days,
            limit=stale_symbol_limit,
        )

    return DataHealthReport(
        generated_at=generated_at,
        today=snapshot_today,
        stale_threshold_days=stale_threshold_days,
        kite_token=token_status,
        recent_refresh_runs=recent_runs,
        price_coverage=price_coverage,
        composite_score_coverage=composite_coverage,
        instrument_coverage=instrument_coverage,
        index_membership_coverage=index_membership_coverage,
        stale_symbols=stale,
    )


# ---------------------------------------------------------------------------
# Token presence (no token value ever read here — only existence)
# ---------------------------------------------------------------------------


def _kite_token_status() -> KiteTokenStatus:
    settings = get_settings()
    api_key = bool((settings.kite_api_key or "").strip())
    api_secret = bool((settings.kite_api_secret or "").strip())

    path = kite_token_path()
    store_present = path.exists()
    env_present = bool((settings.kite_access_token or "").strip())

    if store_present:
        source = "store"
        configured = True
    elif env_present:
        source = "env"
        configured = True
    else:
        source = "missing"
        configured = False

    return KiteTokenStatus(
        configured=configured,
        source=source,
        store_path=str(path) if store_present else None,
        api_key_configured=api_key,
        api_secret_configured=api_secret,
    )


# ---------------------------------------------------------------------------
# Internal queries
# ---------------------------------------------------------------------------


def _recent_refresh_runs(
    session: Session,
    limit: int,
    generated_at: datetime,
) -> list[RefreshRunSummary]:
    statement = (
        select(DailyRefreshRun)
        .order_by(desc(DailyRefreshRun.started_at), desc(DailyRefreshRun.id))
        .limit(max(1, int(limit)))
    )
    rows = session.scalars(statement).all()
    summaries: list[RefreshRunSummary] = []
    for row in rows:
        finished = row.finished_at
        age_seconds: float | None = None
        if finished is not None:
            ref = finished if finished.tzinfo else finished.replace(tzinfo=UTC)
            age_seconds = max(0.0, (generated_at - ref).total_seconds())
        summaries.append(
            RefreshRunSummary(
                run_id=int(row.id),
                universe=row.universe_name,
                status=row.status,
                started_at=row.started_at,
                finished_at=finished,
                successful_symbols=int(row.successful_symbols or 0),
                failed_symbols=int(row.failed_symbols or 0),
                price_rows_upserted=int(row.price_rows_upserted or 0),
                technical_rows_upserted=int(row.technical_rows_upserted or 0),
                age_seconds=age_seconds,
            )
        )
    return summaries


def _price_coverage(session: Session) -> PriceCoverageSummary:
    distinct_symbols = session.scalar(select(func.count(func.distinct(PriceDaily.symbol)))) or 0
    total_rows = session.scalar(select(func.count(PriceDaily.id))) or 0
    oldest, newest = session.execute(
        select(func.min(PriceDaily.trade_date), func.max(PriceDaily.trade_date))
    ).first() or (None, None)
    by_source_rows = session.execute(
        select(PriceDaily.source, func.count(PriceDaily.id)).group_by(PriceDaily.source)
    ).all()
    by_source = {str(row[0]): int(row[1]) for row in by_source_rows}
    return PriceCoverageSummary(
        distinct_symbols=int(distinct_symbols),
        total_rows=int(total_rows),
        oldest_trade_date=_as_date(oldest),
        newest_trade_date=_as_date(newest),
        by_source=by_source,
    )


def _composite_score_coverage(session: Session, today: date) -> CompositeScoreCoverage:
    distinct_symbols = (
        session.scalar(select(func.count(func.distinct(CompositeScoreSnapshot.symbol)))) or 0
    )
    total_rows = session.scalar(select(func.count(CompositeScoreSnapshot.id))) or 0
    latest = session.scalar(select(func.max(CompositeScoreSnapshot.as_of_date)))
    cutoff = today.fromordinal(today.toordinal() - 7)
    rows_last_7 = (
        session.scalar(
            select(func.count(CompositeScoreSnapshot.id)).where(
                CompositeScoreSnapshot.as_of_date >= cutoff
            )
        )
        or 0
    )
    return CompositeScoreCoverage(
        distinct_symbols=int(distinct_symbols),
        total_rows=int(total_rows),
        rows_last_7_days=int(rows_last_7),
        latest_as_of_date=_as_date(latest),
    )


def _instrument_coverage(session: Session) -> InstrumentCoverage:
    rows = session.execute(
        select(InstrumentMaster.exchange, func.count(InstrumentMaster.id)).group_by(
            InstrumentMaster.exchange
        )
    ).all()
    by_exchange = {str(row[0]): int(row[1]) for row in rows}
    total = sum(by_exchange.values())
    return InstrumentCoverage(by_exchange=by_exchange, total=total)


def _index_membership_coverage(
    session: Session,
    *,
    index_name: str,
) -> IndexMembershipCoverage:
    active_members = (
        session.scalar(
            select(func.count(IndexMembershipHistory.id)).where(
                IndexMembershipHistory.index_name == index_name,
                IndexMembershipHistory.active.is_(True),
                IndexMembershipHistory.to_date.is_(None),
            )
        )
        or 0
    )
    total_periods = (
        session.scalar(
            select(func.count(IndexMembershipHistory.id)).where(
                IndexMembershipHistory.index_name == index_name
            )
        )
        or 0
    )
    earliest_from, latest_from, latest_observed = session.execute(
        select(
            func.min(IndexMembershipHistory.from_date),
            func.max(IndexMembershipHistory.from_date),
            func.max(IndexMembershipHistory.observed_at),
        ).where(IndexMembershipHistory.index_name == index_name)
    ).first() or (None, None, None)
    source_url = session.scalar(
        select(IndexMembershipHistory.source_url)
        .where(IndexMembershipHistory.index_name == index_name)
        .order_by(desc(IndexMembershipHistory.observed_at), desc(IndexMembershipHistory.id))
        .limit(1)
    )

    historical_backfill_ready = bool(total_periods > active_members)
    warning: str | None = None
    if active_members == 0:
        warning = "No active index membership snapshot has been recorded yet."
    elif not historical_backfill_ready:
        warning = (
            "Only the current snapshot is available. Historical membership backfill is still "
            "pending, so old backtests should be labelled as limited."
        )

    return IndexMembershipCoverage(
        index_name=index_name,
        active_members=int(active_members),
        total_periods=int(total_periods),
        earliest_from_date=_as_date(earliest_from),
        latest_from_date=_as_date(latest_from),
        latest_observed_at=latest_observed,
        source_url=str(source_url) if source_url else None,
        historical_backfill_ready=historical_backfill_ready,
        warning=warning,
    )


def _stale_symbols(
    session: Session,
    *,
    today: date,
    threshold_days: int,
    limit: int,
) -> list[StaleSymbol]:
    cutoff = today.fromordinal(today.toordinal() - max(1, int(threshold_days)))
    rows = session.execute(
        select(PriceDaily.symbol, func.max(PriceDaily.trade_date).label("latest_date"))
        .group_by(PriceDaily.symbol)
        .having(func.max(PriceDaily.trade_date) < cutoff)
        .order_by(func.max(PriceDaily.trade_date).asc())
        .limit(max(1, int(limit)))
    ).all()
    return [
        StaleSymbol(
            symbol=str(row[0]),
            latest_trade_date=_as_date(row[1]) or today,
            days_stale=(today - (_as_date(row[1]) or today)).days,
        )
        for row in rows
    ]


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
