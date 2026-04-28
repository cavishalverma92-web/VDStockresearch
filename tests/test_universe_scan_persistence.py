"""Tests for saved Phase 8 universe scanner results."""

from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from stock_platform.analytics.scanner import (
    ScanResult,
    compare_latest_universe_scans,
    fetch_latest_universe_scan,
    fetch_recent_universe_scans,
    save_universe_scan,
    scan_storage_to_frame,
)
from stock_platform.db.models import Base, UniverseScanResult, UniverseScanRun


def _result(symbol: str, score: float | None, *, error: str | None = None) -> ScanResult:
    return ScanResult(
        symbol=symbol,
        composite_score=score,
        band="watch" if score is not None else None,
        sub_scores={"fundamentals": 10.0, "technicals": 80.0, "flows": 0.0},
        active_signal_count=1 if score is not None else 0,
        active_signals=["Moving Average Stack"] if score is not None else [],
        last_close=2500.0 if score is not None else None,
        rsi_14=58.0 if score is not None else None,
        ma_stack="bullish" if score is not None else None,
        data_quality_warnings=["zero_volume_rows: 1"] if score is not None else [],
        error=error,
    )


def _result_with_signals(symbol: str, score: float, signals: list[str]) -> ScanResult:
    return ScanResult(
        symbol=symbol,
        composite_score=score,
        band="watch",
        sub_scores={"fundamentals": 10.0, "technicals": score, "flows": 0.0},
        active_signal_count=len(signals),
        active_signals=signals,
        last_close=2500.0,
        rsi_14=58.0,
        ma_stack="bullish",
        data_quality_warnings=[],
        error=None,
    )


def test_save_universe_scan_creates_run_and_results() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    run_id = save_universe_scan(
        universe_name="nifty_50",
        results=[
            _result("RELIANCE.NS", 72.5),
            _result("BAD.NS", None, error="insufficient price history"),
        ],
        lookback_days=400,
        min_score_filter=60,
        min_signals_filter=1,
        engine=engine,
    )

    with Session(engine) as session:
        run = session.scalar(select(UniverseScanRun).where(UniverseScanRun.id == run_id))
        rows = list(
            session.scalars(
                select(UniverseScanResult).where(UniverseScanResult.run_id == run_id)
            ).all()
        )

    assert run is not None
    assert run.universe_name == "nifty_50"
    assert run.requested_symbols == 2
    assert run.successful_symbols == 1
    assert run.failed_symbols == 1
    assert len(rows) == 2


def test_fetch_latest_universe_scan_and_frame_round_trip() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    save_universe_scan(
        universe_name="nifty_50",
        results=[_result("A.NS", 60.0)],
        lookback_days=300,
        engine=engine,
    )
    latest_id = save_universe_scan(
        universe_name="nifty_50",
        results=[_result("B.NS", 82.0), _result("C.NS", 74.0)],
        lookback_days=400,
        engine=engine,
    )

    latest = fetch_latest_universe_scan("nifty_50", engine=engine)
    frame = scan_storage_to_frame(latest)

    assert latest is not None
    assert latest.id == latest_id
    assert list(frame["symbol"]) == ["B.NS", "C.NS"]
    assert frame.iloc[0]["active_signals"] == "Moving Average Stack"
    assert frame.iloc[0]["data_quality_warnings"] == "zero_volume_rows: 1"


def test_fetch_latest_universe_scan_returns_none_for_empty_db() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    latest = fetch_latest_universe_scan("nifty_50", engine=engine)

    assert latest is None
    assert scan_storage_to_frame(latest).empty


def test_fetch_recent_universe_scans_returns_newest_first() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    first_id = save_universe_scan(
        universe_name="nifty_50",
        results=[_result("A.NS", 60.0)],
        lookback_days=300,
        engine=engine,
    )
    second_id = save_universe_scan(
        universe_name="nifty_50",
        results=[_result("B.NS", 70.0)],
        lookback_days=300,
        engine=engine,
    )

    runs = fetch_recent_universe_scans("nifty_50", engine=engine)

    assert [run.id for run in runs] == [second_id, first_id]


def test_compare_latest_universe_scans_marks_score_and_signal_changes() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    save_universe_scan(
        universe_name="nifty_50",
        results=[
            _result_with_signals("RELIANCE.NS", 60.0, ["Moving Average Stack"]),
            _result_with_signals("TCS.NS", 80.0, ["Breakout with Volume"]),
        ],
        lookback_days=300,
        engine=engine,
    )
    latest_id = save_universe_scan(
        universe_name="nifty_50",
        results=[
            _result_with_signals(
                "RELIANCE.NS",
                72.0,
                ["Moving Average Stack", "RSI 60 Momentum Continuation"],
            ),
            _result_with_signals("INFY.NS", 65.0, ["Golden Cross / Death Cross"]),
        ],
        lookback_days=300,
        engine=engine,
    )

    latest, previous, comparison = compare_latest_universe_scans("nifty_50", engine=engine)

    assert latest is not None
    assert previous is not None
    assert latest.id == latest_id
    reliance = comparison[comparison["symbol"] == "RELIANCE.NS"].iloc[0]
    assert reliance["previous_score"] == 60.0
    assert reliance["score_change"] == 12.0
    assert reliance["signal_count_change"] == 1.0
    assert reliance["new_active_signals"] == "RSI 60 Momentum Continuation"
    assert reliance["comparison_status"] == "improved"
    infy = comparison[comparison["symbol"] == "INFY.NS"].iloc[0]
    assert infy["comparison_status"] == "new symbol"
