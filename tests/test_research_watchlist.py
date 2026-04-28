"""Tests for the local Phase 8.2 research shortlist."""

from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from stock_platform.analytics.scanner import (
    add_symbols_to_watchlist,
    enrich_watchlist_with_latest_scores,
    fetch_watchlist_items,
    update_watchlist_reviews,
    watchlist_to_frame,
)
from stock_platform.analytics.scanner.watchlist import ensure_watchlist_review_columns
from stock_platform.db.models import (
    Base,
    ResearchWatchlistItem,
    UniverseScanResult,
    UniverseScanRun,
)


def test_add_symbols_to_watchlist_creates_unique_rows() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    count = add_symbols_to_watchlist(
        ["reliance.ns", "RELIANCE.NS", "TCS.NS"],
        source_universe="nifty_50",
        source_run_id=10,
        reason="Strong scan candidates",
        engine=engine,
    )

    with Session(engine) as session:
        rows = list(session.scalars(select(ResearchWatchlistItem)).all())

    assert count == 2
    assert {row.symbol for row in rows} == {"RELIANCE.NS", "TCS.NS"}
    assert all(row.active for row in rows)


def test_add_symbols_to_watchlist_updates_existing_row() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    add_symbols_to_watchlist(["RELIANCE.NS"], source_run_id=1, reason="First", engine=engine)
    add_symbols_to_watchlist(["RELIANCE.NS"], source_run_id=2, reason="Second", engine=engine)

    rows = fetch_watchlist_items(engine=engine)

    assert len(rows) == 1
    assert rows[0].source_run_id == 2
    assert rows[0].reason == "Second"


def test_watchlist_to_frame_has_stable_columns() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    add_symbols_to_watchlist(["RELIANCE.NS"], source_universe="nifty_50", engine=engine)

    frame = watchlist_to_frame(fetch_watchlist_items(engine=engine))

    assert list(frame["symbol"]) == ["RELIANCE.NS"]
    assert "source_universe" in frame.columns
    assert "review_status" in frame.columns
    assert "tags" in frame.columns
    assert "notes" in frame.columns
    assert watchlist_to_frame([]).empty


def test_update_watchlist_reviews_edits_status_notes_tags_and_active() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    add_symbols_to_watchlist(["RELIANCE.NS"], source_universe="nifty_50", engine=engine)

    updated = update_watchlist_reviews(
        [
            {
                "symbol": "RELIANCE.NS",
                "review_status": "deep_dive",
                "tags": "energy, results",
                "notes": "Check refining margins before next review.",
                "active": False,
            }
        ],
        engine=engine,
    )

    all_rows = fetch_watchlist_items(active_only=False, engine=engine)
    active_rows = fetch_watchlist_items(active_only=True, engine=engine)

    assert updated == 1
    assert len(active_rows) == 0
    assert all_rows[0].review_status == "deep_dive"
    assert all_rows[0].tags == "energy, results"
    assert all_rows[0].notes == "Check refining margins before next review."
    assert all_rows[0].active is False


def test_enrich_watchlist_with_latest_scores_uses_newest_scan_result() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    add_symbols_to_watchlist(["RELIANCE.NS"], source_universe="nifty_50", engine=engine)

    with Session(engine) as session:
        old_run = UniverseScanRun(
            universe_name="nifty_50",
            requested_symbols=1,
            successful_symbols=1,
            failed_symbols=0,
            lookback_days=400,
        )
        new_run = UniverseScanRun(
            universe_name="nifty_50",
            requested_symbols=1,
            successful_symbols=1,
            failed_symbols=0,
            lookback_days=400,
        )
        session.add_all([old_run, new_run])
        session.flush()
        session.add_all(
            [
                UniverseScanResult(
                    run_id=old_run.id,
                    symbol="RELIANCE.NS",
                    composite_score=55.0,
                    band="watchlist",
                    active_signal_count=1,
                    active_signals_json='["old"]',
                    data_quality_warnings_json="[]",
                ),
                UniverseScanResult(
                    run_id=new_run.id,
                    symbol="RELIANCE.NS",
                    composite_score=72.5,
                    band="strong",
                    active_signal_count=3,
                    active_signals_json='["breakout"]',
                    data_quality_warnings_json="[]",
                    last_close=1327.8,
                ),
            ]
        )
        session.commit()

    frame = watchlist_to_frame(fetch_watchlist_items(engine=engine))
    enriched = enrich_watchlist_with_latest_scores(frame, engine=engine)

    assert enriched.loc[0, "latest_score"] == 72.5
    assert enriched.loc[0, "latest_band"] == "strong"
    assert enriched.loc[0, "latest_active_signals"] == 3
    assert enriched.loc[0, "latest_close"] == 1327.8


def test_ensure_watchlist_review_columns_upgrades_legacy_sqlite_table() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE research_watchlist_items (
                id INTEGER PRIMARY KEY,
                user_id VARCHAR(80) NOT NULL,
                watchlist_name VARCHAR(120) NOT NULL,
                symbol VARCHAR(32) NOT NULL,
                source_universe VARCHAR(120),
                source_run_id INTEGER,
                reason TEXT,
                active BOOLEAN NOT NULL DEFAULT 1,
                created_at DATETIME,
                updated_at DATETIME
            )
            """
        )

    ensure_watchlist_review_columns(engine)

    with engine.connect() as connection:
        columns = {
            row[1]
            for row in connection.exec_driver_sql(
                "PRAGMA table_info(research_watchlist_items)"
            ).fetchall()
        }

    assert {"review_status", "tags", "notes"}.issubset(columns)
