"""Tests for educational technical signal scanner."""

from __future__ import annotations

import pandas as pd

from stock_platform.analytics.signals import scan_technical_signals


def _breakout_frame() -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=260, freq="B")
    close = pd.Series(100.0, index=idx)
    close.iloc[-1] = 120.0
    high = close + 1
    high.iloc[:-1] = 110.0
    volume = pd.Series(100_000, index=idx)
    volume.iloc[-1] = 300_000
    return pd.DataFrame(
        {
            "open": close - 1,
            "high": high,
            "low": close - 2,
            "close": close,
            "adj_close": close,
            "volume": volume,
        },
        index=idx,
    )


def test_breakout_with_volume_can_be_active() -> None:
    signals = scan_technical_signals(
        _breakout_frame(),
        thresholds={
            "breakout_with_volume": {"lookback_days_for_high": 252, "volume_multiple": 2.0},
            "darvas_base_breakout": {"min_consolidation_days": 20, "max_range_pct": 15.0},
        },
    )

    active = {signal.name for signal in signals if signal.active}
    breakout = next(signal for signal in signals if signal.name == "Breakout With Volume")

    assert "Breakout With Volume" in active
    assert "Darvas Base Breakout" in active
    assert breakout.trigger_price == 120
    assert breakout.entry_zone_low is not None
    assert breakout.stop_loss is not None
    assert breakout.target_price is not None
    assert breakout.risk_reward == 2.5
    assert breakout.confidence is not None
    assert "atr_14" in breakout.data_used


def test_scanner_returns_named_results_for_short_history() -> None:
    idx = pd.date_range("2025-01-01", periods=5, freq="B")
    frame = pd.DataFrame(
        {
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100,
            "adj_close": 100,
            "volume": 1_000,
        },
        index=idx,
    )

    signals = scan_technical_signals(frame)

    assert {signal.name for signal in signals} == {
        "200 EMA Pullback",
        "RSI 60 Momentum",
        "Breakout With Volume",
        "Darvas Base Breakout",
        "Mean-Reversion Oversold",
        "MA Stack",
        "Golden/Death Cross",
    }
