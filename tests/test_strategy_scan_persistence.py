"""Tests for saved strategy scanner results."""

from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from stock_platform.analytics.scanner import (
    StrategyScanResult,
    StrategyScanSummary,
    fetch_latest_strategy_scan,
    save_strategy_scan,
    strategy_scan_errors,
    strategy_scan_storage_to_frame,
    top_clean_strategy_hits,
)
from stock_platform.db.models import Base, StrategyScanRun
from stock_platform.db.models import StrategyScanResult as StrategyScanResultModel


def _strategy_result(
    symbol: str = "RELIANCE.NS",
    *,
    strategy: str = "EMA Stack Trend Filter",
    confidence_score: float = 82.0,
    data_trust: str = "Good data",
    liquidity_status: str = "Pass",
) -> StrategyScanResult:
    return StrategyScanResult(
        symbol=symbol,
        strategy=strategy,
        setup_type="Trend",
        signal_date=date(2026, 5, 1),
        close=1400.0,
        entry_zone_low=1390.0,
        entry_zone_high=1405.0,
        stop_loss=1360.0,
        target_price=1500.0,
        risk_reward=2.5,
        rsi=62.0,
        trend_status="bullish",
        relative_volume=1.2,
        atr_pct=2.0,
        liquidity_status=liquidity_status,
        data_source="kite",
        data_freshness="2026-05-01",
        confidence_score=confidence_score,
        why_this_appeared="Price is above a fully aligned EMA stack.",
        key_risk="Trend filters can appear late.",
        data_trust=data_trust,
        ema_20=1380.0,
        ema_50=1320.0,
        ema_100=1250.0,
        ema_200=1150.0,
        avg_traded_value_cr=120.0,
        warnings=("Mixed persisted price sources were deduplicated.",),
    )


def test_save_strategy_scan_creates_run_and_results() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    summary = StrategyScanSummary(
        requested_symbols=2,
        scanned_symbols=1,
        failed_symbols=1,
        results=[_strategy_result()],
        errors={"BAD.NS": "insufficient persisted price history"},
    )

    run_id = save_strategy_scan(
        universe_name="nifty_50",
        summary=summary,
        min_confidence_filter=60,
        min_rr_filter=1.5,
        engine=engine,
    )

    with Session(engine) as session:
        run = session.scalar(select(StrategyScanRun).where(StrategyScanRun.id == run_id))
        rows = list(
            session.scalars(
                select(StrategyScanResultModel).where(StrategyScanResultModel.run_id == run_id)
            ).all()
        )

    assert run is not None
    assert run.universe_name == "nifty_50"
    assert run.result_count == 1
    assert run.failed_symbols == 1
    assert len(rows) == 1
    assert rows[0].strategy == "EMA Stack Trend Filter"


def test_fetch_latest_strategy_scan_round_trips_to_frame_and_errors() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    first = StrategyScanSummary(
        requested_symbols=1,
        scanned_symbols=1,
        failed_symbols=0,
        results=[_strategy_result("A.NS")],
        errors={},
    )
    latest = StrategyScanSummary(
        requested_symbols=2,
        scanned_symbols=1,
        failed_symbols=1,
        results=[_strategy_result("B.NS")],
        errors={"BAD.NS": "data quality failure"},
    )
    save_strategy_scan(universe_name="nifty_50", summary=first, engine=engine)
    latest_id = save_strategy_scan(universe_name="nifty_50", summary=latest, engine=engine)

    run = fetch_latest_strategy_scan("nifty_50", engine=engine)
    frame = strategy_scan_storage_to_frame(run)
    errors = strategy_scan_errors(run)

    assert run is not None
    assert run.id == latest_id
    assert list(frame["symbol"]) == ["B.NS"]
    assert frame.iloc[0]["warnings"] == "Mixed persisted price sources were deduplicated."
    assert errors == {"BAD.NS": "data quality failure"}


def test_fetch_latest_strategy_scan_returns_none_for_empty_db() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    assert fetch_latest_strategy_scan("nifty_50", engine=engine) is None
    assert strategy_scan_storage_to_frame(None).empty


def test_top_clean_strategy_hits_filters_and_sorts_attention_rows() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    summary = StrategyScanSummary(
        requested_symbols=4,
        scanned_symbols=4,
        failed_symbols=0,
        results=[
            _strategy_result("LOW.NS", confidence_score=70),
            _strategy_result("WARN.NS", confidence_score=90, data_trust="Warning"),
            _strategy_result("THIN.NS", confidence_score=88, liquidity_status="Low"),
            _strategy_result(
                "BEST.NS",
                strategy="Breakout With Relative Volume",
                confidence_score=86,
            ),
        ],
        errors={},
    )
    save_strategy_scan(universe_name="nifty_50", summary=summary, engine=engine)
    run = fetch_latest_strategy_scan("nifty_50", engine=engine)

    frame = top_clean_strategy_hits(run, limit=5)

    assert list(frame["symbol"]) == ["BEST.NS", "LOW.NS"]
    assert "WARN.NS" not in set(frame["symbol"])
    assert "THIN.NS" not in set(frame["symbol"])
