from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine

from stock_platform.analytics.scanner import (
    ScanResult,
    add_symbols_to_watchlist,
    build_daily_research_brief,
    daily_brief_table,
    save_universe_scan,
)
from stock_platform.db.models import Base


def _result(symbol: str, score: float, signals: list[str], *, warnings: list[str] | None = None):
    return ScanResult(
        symbol=symbol,
        composite_score=score,
        band="watch",
        sub_scores={"fundamentals": 50.0, "technicals": score, "flows": 0.0},
        active_signal_count=len(signals),
        active_signals=signals,
        last_close=100.0,
        rsi_14=58.0,
        ma_stack="bullish",
        data_quality_warnings=warnings or [],
        error=None,
    )


def _error(symbol: str, error: str):
    return ScanResult(
        symbol=symbol,
        composite_score=None,
        band=None,
        sub_scores={},
        active_signal_count=0,
        active_signals=[],
        last_close=None,
        rsi_14=None,
        ma_stack=None,
        data_quality_warnings=[],
        error=error,
    )


def test_daily_brief_summarizes_latest_scan_changes() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    save_universe_scan(
        universe_name="nifty_50",
        results=[
            _result("RELIANCE.NS", 60.0, ["Moving Average Stack"]),
            _result("TCS.NS", 80.0, ["Breakout with Volume"]),
        ],
        lookback_days=300,
        engine=engine,
    )
    latest_id = save_universe_scan(
        universe_name="nifty_50",
        results=[
            _result(
                "RELIANCE.NS",
                72.0,
                ["Moving Average Stack", "RSI 60 Momentum Continuation"],
            ),
            _result("TCS.NS", 70.0, []),
            _result("INFY.NS", 66.0, ["Golden Cross / Death Cross"]),
            _error("BAD.NS", "no price data"),
        ],
        lookback_days=300,
        engine=engine,
    )

    brief = build_daily_research_brief("nifty_50", engine=engine)

    assert brief.latest_run_id == latest_id
    assert brief.previous_run_id is not None
    assert brief.requested_symbols == 4
    assert brief.successful_symbols == 3
    assert brief.failed_symbols == 1
    assert brief.average_score == 69.3
    assert list(brief.improved["symbol"]) == ["RELIANCE.NS"]
    assert list(brief.weakened["symbol"]) == ["TCS.NS"]
    assert "RELIANCE.NS" in brief.new_signals["symbol"].tolist()
    assert "INFY.NS" in brief.new_opportunities["symbol"].tolist()
    assert "BAD.NS" in brief.data_quality_actions["symbol"].tolist()


def test_daily_brief_includes_shortlist_actions() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    save_universe_scan(
        universe_name="nifty_50",
        results=[_result("RELIANCE.NS", 70.0, ["Moving Average Stack"])],
        lookback_days=300,
        engine=engine,
    )
    add_symbols_to_watchlist(["RELIANCE.NS"], engine=engine)

    brief = build_daily_research_brief("nifty_50", engine=engine)

    assert list(brief.shortlist_actions["symbol"]) == ["RELIANCE.NS"]
    assert brief.shortlist_actions.iloc[0]["review_status"] == "watch"


def test_daily_brief_table_has_stable_empty_columns() -> None:
    frame = daily_brief_table(pd.DataFrame())

    assert list(frame.columns) == [
        "symbol",
        "composite_score",
        "previous_score",
        "score_change",
        "comparison_status",
        "active_signal_count",
        "new_active_signals",
        "active_signals",
        "data_quality_warnings",
        "error",
    ]
