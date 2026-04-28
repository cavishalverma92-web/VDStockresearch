from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine

from stock_platform.analytics.scanner import (
    DailyResearchBrief,
    ScanResult,
    add_symbols_to_watchlist,
    build_daily_research_brief,
    daily_brief_freshness,
    daily_brief_headline,
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


def test_daily_brief_handles_scan_with_only_error_rows() -> None:
    """Regression: a saved scan whose only result errored must not crash the brief.

    Reproduces the KeyError: 'comparison_status' that surfaced in the UI when
    the comparison frame had columns but zero successful rows.
    """
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    save_universe_scan(
        universe_name="nifty_50",
        results=[_error("BAD.NS", "no price data")],
        lookback_days=300,
        engine=engine,
    )

    brief = build_daily_research_brief("nifty_50", engine=engine)

    assert brief.has_latest_scan
    assert brief.successful_symbols == 0
    assert brief.failed_symbols == 1
    assert brief.improved.empty
    assert brief.weakened.empty
    assert brief.new_opportunities.empty
    assert brief.new_signals.empty
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


# --------------------------------------------------------------------------- #
# daily_brief_headline
# --------------------------------------------------------------------------- #


def _make_brief(
    *,
    has_scan: bool = True,
    new_opp: int = 0,
    improved: int = 0,
    weakened: int = 0,
    new_signals: int = 0,
    dq_actions: int = 0,
    shortlist_actions: int = 0,
    universe_name: str = "nifty_50",
    latest_run_id: int | None = 5,
    latest_run_at: object | None = None,
) -> DailyResearchBrief:
    def _df(n: int) -> pd.DataFrame:
        return pd.DataFrame({"symbol": [f"X{i}.NS" for i in range(n)]})

    return DailyResearchBrief(
        universe_name=universe_name,
        latest_run_id=latest_run_id if has_scan else None,
        previous_run_id=latest_run_id - 1 if has_scan and latest_run_id else None,
        latest_run_at=latest_run_at,
        requested_symbols=10 if has_scan else 0,
        successful_symbols=8 if has_scan else 0,
        failed_symbols=2 if has_scan else 0,
        average_score=70.0 if has_scan else None,
        top_score=90.0 if has_scan else None,
        improved=_df(improved),
        weakened=_df(weakened),
        new_opportunities=_df(new_opp),
        new_signals=_df(new_signals),
        data_quality_actions=_df(dq_actions),
        shortlist_actions=_df(shortlist_actions),
    )


def test_headline_no_scan() -> None:
    brief = _make_brief(has_scan=False)
    assert daily_brief_headline(brief) == "No saved scan yet for nifty_50."


def test_headline_no_changes() -> None:
    brief = _make_brief()
    headline = daily_brief_headline(brief)
    assert "no notable changes" in headline
    assert "Nifty 50 scan #5" in headline


def test_headline_combines_counts_with_correct_pluralization() -> None:
    brief = _make_brief(new_opp=3, improved=1, new_signals=2, dq_actions=1)
    headline = daily_brief_headline(brief)
    assert "3 new opportunities" in headline
    assert "1 score improver" in headline
    assert "2 newly active signals" in headline
    assert "1 data-quality action" in headline


def test_headline_singular_opportunity() -> None:
    brief = _make_brief(new_opp=1)
    headline = daily_brief_headline(brief)
    assert "1 new opportunity" in headline


# --------------------------------------------------------------------------- #
# daily_brief_freshness
# --------------------------------------------------------------------------- #


def test_freshness_unknown_when_no_timestamp() -> None:
    status, age = daily_brief_freshness(None)
    assert status == "unknown"
    assert "no saved scan" in age


def test_freshness_fresh_within_24h() -> None:
    now = datetime(2026, 4, 29, 12, 0, tzinfo=UTC)
    run_at = now - timedelta(hours=2)
    status, age = daily_brief_freshness(run_at, now=now)
    assert status == "fresh"
    assert "2 hours ago" in age


def test_freshness_aging_between_24_and_72_hours() -> None:
    now = datetime(2026, 4, 29, 12, 0, tzinfo=UTC)
    run_at = now - timedelta(hours=36)
    status, age = daily_brief_freshness(run_at, now=now)
    assert status == "aging"
    assert "36 hours ago" in age


def test_freshness_stale_past_72_hours() -> None:
    now = datetime(2026, 4, 29, 12, 0, tzinfo=UTC)
    run_at = now - timedelta(days=5)
    status, age = daily_brief_freshness(run_at, now=now)
    assert status == "stale"
    assert "5 days ago" in age


def test_freshness_handles_iso_string() -> None:
    now = datetime(2026, 4, 29, 12, 0, tzinfo=UTC)
    run_at_str = (now - timedelta(hours=1)).isoformat()
    status, _ = daily_brief_freshness(run_at_str, now=now)
    assert status == "fresh"


def test_freshness_naive_datetime_treated_as_utc() -> None:
    now = datetime(2026, 4, 29, 12, 0, tzinfo=UTC)
    run_at = (now - timedelta(hours=2)).replace(tzinfo=None)
    status, age = daily_brief_freshness(run_at, now=now)
    assert status == "fresh"
    assert "2 hours ago" in age


def test_freshness_minutes_when_under_an_hour() -> None:
    now = datetime(2026, 4, 29, 12, 0, tzinfo=UTC)
    run_at = now - timedelta(minutes=30)
    status, age = daily_brief_freshness(run_at, now=now)
    assert status == "fresh"
    assert "30 minutes ago" in age
