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
    assert "ema_200" in frame.columns


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
