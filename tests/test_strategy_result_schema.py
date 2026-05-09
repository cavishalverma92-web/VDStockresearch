"""Tests for strategy scanner result schema and first strategy runner."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import numpy as np
import pandas as pd

from stock_platform.analytics.scanner import (
    StrategyScanResult,
    scan_persisted_strategy_universe,
    strategy_results_to_frame,
    summarize_strategy_scan_frame,
)
from stock_platform.data.repositories import upsert_price_daily
from stock_platform.db import create_all_tables, get_engine, get_session


def _trend_frame(days: int = 260) -> pd.DataFrame:
    idx = pd.date_range(end=date.today(), periods=days, freq="B")
    close = np.linspace(100, 200, days)
    return pd.DataFrame(
        {
            "open": close - 0.5,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "adj_close": close,
            "volume": [1_000_000] * days,
        },
        index=idx,
    )


def _low_liquidity_trend_frame(days: int = 260) -> pd.DataFrame:
    frame = _trend_frame(days)
    frame["volume"] = 1_000
    return frame


def _breakout_frame(days: int = 260) -> pd.DataFrame:
    idx = pd.date_range(end=date.today(), periods=days, freq="B")
    close = np.linspace(100, 130, days)
    high = close + 1.0
    close[-1] = 136.0
    high[-1] = 138.0
    return pd.DataFrame(
        {
            "open": close - 0.75,
            "high": high,
            "low": close - 1.5,
            "close": close,
            "adj_close": close,
            "volume": [1_000_000] * (days - 1) + [2_800_000],
        },
        index=idx,
    )


def _moderate_high_breakout_frame(days: int = 260) -> pd.DataFrame:
    idx = pd.date_range(end=date.today(), periods=days, freq="B")
    close = np.linspace(100, 130, days)
    high = close + 1.0
    close[-1] = 135.0
    high[-1] = 136.0
    return pd.DataFrame(
        {
            "open": close - 0.75,
            "high": high,
            "low": close - 1.5,
            "close": close,
            "adj_close": close,
            "volume": [1_000_000] * (days - 1) + [1_600_000],
        },
        index=idx,
    )


def test_strategy_results_to_frame_has_default_and_advanced_columns():
    result = StrategyScanResult(
        symbol="RELIANCE.NS",
        strategy="EMA Stack Trend Filter",
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
        liquidity_status="Pass",
        data_source="kite",
        data_freshness="2026-05-01",
        confidence_score=82.0,
        why_this_appeared="EMA stack is aligned.",
        key_risk="Trend setups can appear late.",
        ema_20=1380.0,
        ema_50=1320.0,
        ema_100=1250.0,
        ema_200=1150.0,
    )

    frame = strategy_results_to_frame([result])

    assert frame.iloc[0]["symbol"] == "RELIANCE.NS"
    assert frame.iloc[0]["entry_zone"] == "INR 1390.00 - 1405.00"
    assert "why_this_appeared" in frame.columns
    assert "data_source" in frame.columns[:20]
    assert "ema_200" in frame.columns


def test_summarize_strategy_scan_frame_counts_attention_buckets():
    frame = pd.DataFrame(
        [
            {
                "symbol": "A.NS",
                "strategy": "Breakout With Relative Volume",
                "setup_type": "Breakout",
                "data_trust": "Good data",
            },
            {
                "symbol": "B.NS",
                "strategy": "EMA Stack Trend Filter",
                "setup_type": "Trend",
                "data_trust": "Warning",
            },
            {
                "symbol": "B.NS",
                "strategy": "EMA Stack Trend Filter",
                "setup_type": "Trend",
                "data_trust": "Do not trust signal",
            },
        ]
    )

    summary = summarize_strategy_scan_frame(frame)

    assert summary.total_setups == 3
    assert summary.unique_symbols == 2
    assert summary.clean_setups == 1
    assert summary.warning_setups == 1
    assert summary.untrusted_setups == 1
    assert summary.breakout_setups == 1
    assert summary.top_strategy == "EMA Stack Trend Filter"
    assert summary.top_strategy_count == 2


def test_scan_persisted_strategy_universe_finds_ema_stack_from_local_db():
    engine = get_engine("sqlite:///:memory:")
    create_all_tables(engine)
    with get_session(engine) as session:
        upsert_price_daily(session, "TREND.NS", _trend_frame(), source="kite")

    with patch(
        "stock_platform.analytics.scanner.strategy_scanner.load_universe",
        return_value=["TREND.NS"],
    ):
        summary = scan_persisted_strategy_universe("nifty_50", engine=engine)

    strategies = {result.strategy for result in summary.results}
    assert summary.scanned_symbols == 1
    assert summary.failed_symbols == 0
    assert "EMA Stack Trend Filter" in strategies


def test_scan_persisted_strategy_universe_flags_low_liquidity_as_untrusted():
    engine = get_engine("sqlite:///:memory:")
    create_all_tables(engine)
    with get_session(engine) as session:
        upsert_price_daily(session, "THIN.NS", _low_liquidity_trend_frame(), source="kite")

    with patch(
        "stock_platform.analytics.scanner.strategy_scanner.load_universe",
        return_value=["THIN.NS"],
    ):
        summary = scan_persisted_strategy_universe("nifty_50", engine=engine)

    assert summary.results
    assert {result.liquidity_status for result in summary.results} == {"Low"}
    assert {result.data_trust for result in summary.results} == {"Do not trust signal"}
    assert all(result.confidence_score < 76 for result in summary.results)


def test_scan_persisted_strategy_universe_finds_breakout_with_volume():
    engine = get_engine("sqlite:///:memory:")
    create_all_tables(engine)
    with get_session(engine) as session:
        upsert_price_daily(session, "BREAKOUT.NS", _breakout_frame(), source="kite")

    with patch(
        "stock_platform.analytics.scanner.strategy_scanner.load_universe",
        return_value=["BREAKOUT.NS"],
    ):
        summary = scan_persisted_strategy_universe("nifty_50", engine=engine)

    breakout = next(
        result for result in summary.results if result.strategy == "Breakout With Relative Volume"
    )
    assert breakout.setup_type == "Breakout"
    assert breakout.breakout_level is not None
    assert breakout.relative_volume is not None
    assert breakout.relative_volume >= 2.0
    assert breakout.data_trust in {"Good data", "Warning"}


def test_scan_persisted_strategy_universe_finds_high_breakout_without_strict_volume():
    engine = get_engine("sqlite:///:memory:")
    create_all_tables(engine)
    with get_session(engine) as session:
        upsert_price_daily(session, "HIGHBRK.NS", _moderate_high_breakout_frame(), source="kite")

    with patch(
        "stock_platform.analytics.scanner.strategy_scanner.load_universe",
        return_value=["HIGHBRK.NS"],
    ):
        summary = scan_persisted_strategy_universe("nifty_50", engine=engine)

    strategies = {result.strategy for result in summary.results}
    assert "52W / 120D High Breakout" in strategies
    assert "Breakout With Relative Volume" not in strategies
