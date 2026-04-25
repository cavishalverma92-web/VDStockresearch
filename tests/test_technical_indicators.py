"""Tests for Phase 2 technical indicators."""

from __future__ import annotations

import pandas as pd

from stock_platform.analytics.technicals import add_technical_indicators, calculate_rsi


def _price_frame(n: int = 260) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    close = pd.Series(range(100, 100 + n), index=idx, dtype=float)
    return pd.DataFrame(
        {
            "open": close - 0.5,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "adj_close": close,
            "volume": 100_000,
        },
        index=idx,
    )


def test_add_technical_indicators_adds_expected_columns() -> None:
    result = add_technical_indicators(_price_frame())

    for column in (
        "sma_20",
        "sma_50",
        "sma_100",
        "sma_200",
        "ema_20",
        "ema_50",
        "ema_100",
        "ema_200",
        "rsi_14",
        "macd",
        "macd_signal",
        "bb_upper",
        "bb_lower",
        "atr_14",
        "atr_pct",
        "historical_volatility_20",
        "relative_volume",
        "low_52w",
        "distance_from_52w_high_pct",
        "distance_from_52w_low_pct",
        "distance_from_all_time_high_pct",
        "ma_stack_status",
    ):
        assert column in result.columns

    assert result["sma_20"].iloc[-1] == result["close"].iloc[-20:].mean()
    assert result["relative_volume"].iloc[-1] == 1
    assert result["ma_stack_status"].iloc[-1] == "bullish"


def test_rsi_is_high_for_persistent_uptrend() -> None:
    close = _price_frame()["close"]
    rsi = calculate_rsi(close)

    assert rsi.iloc[-1] > 95
