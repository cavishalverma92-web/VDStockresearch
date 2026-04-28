"""Local research shortlist helpers for Phase 8.2."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import Engine, inspect, select, text

from stock_platform.db import create_all_tables, get_engine, get_session
from stock_platform.db.models import (
    ResearchWatchlistItem,
    UniverseScanResult,
    UniverseScanRun,
    utc_now,
)

DEFAULT_WATCHLIST = "research_shortlist"
DEFAULT_USER_ID = "local"
REVIEW_STATUSES = ("watch", "deep_dive", "avoid", "done")


def add_symbols_to_watchlist(
    symbols: list[str],
    *,
    watchlist_name: str = DEFAULT_WATCHLIST,
    user_id: str = DEFAULT_USER_ID,
    source_universe: str | None = None,
    source_run_id: int | None = None,
    reason: str | None = None,
    engine: Engine | None = None,
) -> int:
    """Add or reactivate symbols in the local research shortlist.

    Returns the number of unique symbols processed. Existing symbols are updated
    instead of duplicated.
    """
    active_engine = engine or get_engine()
    create_all_tables(active_engine)
    ensure_watchlist_review_columns(active_engine)
    normalized = _normalize_symbols(symbols)
    if not normalized:
        return 0

    with get_session(active_engine) as session:
        for symbol in normalized:
            existing = session.scalar(
                select(ResearchWatchlistItem).where(
                    ResearchWatchlistItem.user_id == user_id,
                    ResearchWatchlistItem.watchlist_name == watchlist_name,
                    ResearchWatchlistItem.symbol == symbol,
                )
            )
            if existing is None:
                session.add(
                    ResearchWatchlistItem(
                        user_id=user_id,
                        watchlist_name=watchlist_name,
                        symbol=symbol,
                        source_universe=source_universe,
                        source_run_id=source_run_id,
                        reason=reason,
                        review_status="watch",
                        active=True,
                    )
                )
                continue

            existing.source_universe = source_universe or existing.source_universe
            existing.source_run_id = source_run_id or existing.source_run_id
            existing.reason = reason or existing.reason
            existing.active = True
            existing.updated_at = utc_now()

    return len(normalized)


def fetch_watchlist_items(
    *,
    watchlist_name: str = DEFAULT_WATCHLIST,
    user_id: str = DEFAULT_USER_ID,
    active_only: bool = True,
    engine: Engine | None = None,
) -> list[ResearchWatchlistItem]:
    """Return saved shortlist rows ordered by most recently updated."""
    active_engine = engine or get_engine()
    create_all_tables(active_engine)
    ensure_watchlist_review_columns(active_engine)

    statement = select(ResearchWatchlistItem).where(
        ResearchWatchlistItem.user_id == user_id,
        ResearchWatchlistItem.watchlist_name == watchlist_name,
    )
    if active_only:
        statement = statement.where(ResearchWatchlistItem.active.is_(True))
    statement = statement.order_by(ResearchWatchlistItem.updated_at.desc())

    with get_session(active_engine) as session:
        rows = list(session.scalars(statement).all())
        session.expunge_all()
        return rows


def watchlist_to_frame(rows: list[ResearchWatchlistItem]) -> pd.DataFrame:
    """Convert shortlist rows to a stable UI/export DataFrame."""
    columns = [
        "symbol",
        "watchlist_name",
        "source_universe",
        "source_run_id",
        "reason",
        "review_status",
        "tags",
        "notes",
        "active",
        "created_at",
        "updated_at",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)

    return pd.DataFrame(
        [
            {
                "symbol": row.symbol,
                "watchlist_name": row.watchlist_name,
                "source_universe": row.source_universe,
                "source_run_id": row.source_run_id,
                "reason": row.reason,
                "review_status": _safe_text(row.review_status) or "watch",
                "tags": _safe_text(row.tags),
                "notes": _safe_text(row.notes),
                "active": row.active,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }
            for row in rows
        ],
        columns=columns,
    )


def enrich_watchlist_with_latest_scores(
    frame: pd.DataFrame,
    *,
    engine: Engine | None = None,
) -> pd.DataFrame:
    """Add latest saved scan score/signals to shortlist rows."""
    if frame.empty:
        return frame.assign(
            latest_score=pd.NA,
            latest_band=pd.NA,
            latest_run_id=pd.NA,
            latest_run_at=pd.NA,
            latest_close=pd.NA,
            latest_active_signals=pd.NA,
        )

    active_engine = engine or get_engine()
    create_all_tables(active_engine)
    symbols = _normalize_symbols([str(symbol) for symbol in frame["symbol"].tolist()])
    latest_by_symbol: dict[str, dict[str, object]] = {}

    with get_session(active_engine) as session:
        statement = (
            select(UniverseScanResult, UniverseScanRun)
            .join(UniverseScanRun, UniverseScanResult.run_id == UniverseScanRun.id)
            .where(UniverseScanResult.symbol.in_(symbols))
            .order_by(
                UniverseScanResult.symbol.asc(),
                UniverseScanRun.created_at.desc(),
                UniverseScanRun.id.desc(),
            )
        )
        for result, run in session.execute(statement).all():
            if result.symbol in latest_by_symbol:
                continue
            latest_by_symbol[result.symbol] = {
                "latest_score": result.composite_score,
                "latest_band": result.band,
                "latest_run_id": run.id,
                "latest_run_at": run.created_at,
                "latest_close": result.last_close,
                "latest_active_signals": result.active_signal_count,
            }

    enriched = frame.copy()
    for column in (
        "latest_score",
        "latest_band",
        "latest_run_id",
        "latest_run_at",
        "latest_close",
        "latest_active_signals",
    ):
        enriched[column] = enriched["symbol"].map(
            lambda symbol, col=column: latest_by_symbol.get(str(symbol), {}).get(col, pd.NA)
        )

    return enriched


def update_watchlist_reviews(
    updates: list[dict[str, object]],
    *,
    watchlist_name: str = DEFAULT_WATCHLIST,
    user_id: str = DEFAULT_USER_ID,
    engine: Engine | None = None,
) -> int:
    """Update review fields for existing shortlist rows."""
    active_engine = engine or get_engine()
    create_all_tables(active_engine)
    ensure_watchlist_review_columns(active_engine)
    if not updates:
        return 0

    changed = 0
    with get_session(active_engine) as session:
        for raw in updates:
            symbol = str(raw.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            row = session.scalar(
                select(ResearchWatchlistItem).where(
                    ResearchWatchlistItem.user_id == user_id,
                    ResearchWatchlistItem.watchlist_name == watchlist_name,
                    ResearchWatchlistItem.symbol == symbol,
                )
            )
            if row is None:
                continue

            row.review_status = _normalize_review_status(raw.get("review_status"))
            row.tags = _safe_text(raw.get("tags"))[:240]
            row.notes = _safe_text(raw.get("notes"))
            row.active = bool(raw.get("active", True))
            row.updated_at = utc_now()
            changed += 1

    return changed


def ensure_watchlist_review_columns(engine: Engine | None = None) -> None:
    """Add Phase 8.3 review columns to older local SQLite/Postgres databases."""
    active_engine = engine or get_engine()
    inspector = inspect(active_engine)
    if "research_watchlist_items" not in inspector.get_table_names():
        return

    existing = {column["name"] for column in inspector.get_columns("research_watchlist_items")}
    additions = {
        "review_status": "VARCHAR(40) NOT NULL DEFAULT 'watch'",
        "tags": "VARCHAR(240) NOT NULL DEFAULT ''",
        "notes": "TEXT NOT NULL DEFAULT ''",
    }

    with active_engine.begin() as connection:
        for column, ddl in additions.items():
            if column not in existing:
                connection.execute(
                    text(f"ALTER TABLE research_watchlist_items ADD COLUMN {column} {ddl}")
                )


def _normalize_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for raw_symbol in symbols:
        symbol = str(raw_symbol).strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


def _normalize_review_status(value: object) -> str:
    status = _safe_text(value).lower().replace(" ", "_")
    return status if status in REVIEW_STATUSES else "watch"


def _safe_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()
