"""Tests for swing pivot detection and S/R zone clustering."""

from __future__ import annotations

import numpy as np
import pandas as pd

from stock_platform.analytics.technicals.structure import (
    detect_swing_pivots,
    find_support_resistance_zones,
    latest_swing_levels,
)


def _make_ohlc(closes: list[float]) -> pd.DataFrame:
    """Build a DataFrame where high/low equal close±1, volume constant."""
    idx = pd.date_range("2024-01-01", periods=len(closes), freq="B")
    return pd.DataFrame(
        {
            "open": closes,
            "high": [c + 1 for c in closes],
            "low": [c - 1 for c in closes],
            "close": closes,
            "volume": [100_000] * len(closes),
        },
        index=idx,
    )


def test_detect_swing_pivots_finds_obvious_high():
    # Single peak in the middle: index 5 should be a swing high
    closes = [100, 101, 102, 103, 104, 110, 104, 103, 102, 101, 100]
    df = _make_ohlc(closes)
    sh, _ = detect_swing_pivots(df, window=3)
    assert sh.iloc[5] is True or bool(sh.iloc[5]) is True


def test_detect_swing_pivots_finds_obvious_low():
    closes = [110, 109, 108, 107, 106, 100, 106, 107, 108, 109, 110]
    df = _make_ohlc(closes)
    _, sl = detect_swing_pivots(df, window=3)
    assert bool(sl.iloc[5]) is True


def test_detect_swing_pivots_empty_frame():
    df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    sh, sl = detect_swing_pivots(df, window=5)
    assert sh.empty and sl.empty


def test_detect_swing_pivots_edges_are_false():
    closes = list(np.arange(100, 120, dtype=float))
    df = _make_ohlc(closes)
    sh, sl = detect_swing_pivots(df, window=3)
    # First 3 and last 3 cannot be confirmed
    assert not bool(sh.iloc[0])
    assert not bool(sh.iloc[-1])
    assert not bool(sl.iloc[0])
    assert not bool(sl.iloc[-1])


def test_find_support_resistance_zones_basic():
    # Build a price series with clear pivots above and below current close
    closes = (
        [100, 105, 110, 105, 100]  # pivot high near 110
        + [95, 90, 85, 90, 95]  # pivot low near 85
        + [100, 105, 110, 115, 120, 115, 110]  # pivot high near 120
        + [115, 110, 105, 100, 95]  # currently trending down to 95
    )
    df = _make_ohlc(closes)
    zones = find_support_resistance_zones(df, window=3, lookback=100, max_zones=3)
    # Last close is 95; resistance levels should be > 95
    assert all(r > 95 for r in zones["resistance"])
    # Support levels should be < 95
    assert all(s < 95 for s in zones["support"])


def test_find_support_resistance_zones_empty_returns_empty_lists():
    df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    result = find_support_resistance_zones(df)
    assert result == {"support": [], "resistance": []}


def test_latest_swing_levels_returns_floats():
    closes = [100, 101, 102, 105, 102, 101, 100, 99, 98, 99, 100]
    df = _make_ohlc(closes)
    levels = latest_swing_levels(df, window=2)
    assert "last_swing_high" in levels and "last_swing_low" in levels
    # Either both floats or both None depending on detection
    high_v, low_v = levels["last_swing_high"], levels["last_swing_low"]
    assert (high_v is None) or isinstance(high_v, float)
    assert (low_v is None) or isinstance(low_v, float)


def test_latest_swing_levels_empty_frame():
    df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    levels = latest_swing_levels(df)
    assert levels == {"last_swing_high": None, "last_swing_low": None}
