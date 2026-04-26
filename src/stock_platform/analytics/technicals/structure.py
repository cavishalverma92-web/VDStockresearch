"""Price-structure helpers: swing highs/lows and support/resistance zones.

Master prompt §4.2 lists "swing highs/lows, 52-week high/low distance,
all-time high distance, S/R zones, base detection".  52-week and all-time
distances live in indicators.py.  This module covers the pivot-based pieces.

A *swing high* at index ``i`` is a bar whose high is strictly greater than
every high in the surrounding ±N window.  Swing low is the mirror.  N is the
fractal window (default 5 — 5 bars on each side).

Support/resistance zones are derived by clustering recent swing pivots into
price bands (default tolerance: 1.5% of current close).
"""

from __future__ import annotations

import pandas as pd


def detect_swing_pivots(frame: pd.DataFrame, window: int = 5) -> tuple[pd.Series, pd.Series]:
    """Return (swing_high_mask, swing_low_mask) boolean Series.

    A bar is a swing high if its high is strictly greater than every high
    within ±``window`` bars (excluding itself).  Mirror for swing lows.
    The first/last ``window`` bars cannot be confirmed and are False.
    """
    if frame.empty or window < 1:
        empty = pd.Series(False, index=frame.index)
        return empty, empty.copy()

    highs = frame["high"]
    lows = frame["low"]
    n = len(frame)

    swing_high = pd.Series(False, index=frame.index)
    swing_low = pd.Series(False, index=frame.index)

    for i in range(window, n - window):
        h = highs.iloc[i]
        left_h = highs.iloc[i - window : i]
        right_h = highs.iloc[i + 1 : i + 1 + window]
        if h > left_h.max() and h > right_h.max():
            swing_high.iloc[i] = True

        lv = lows.iloc[i]
        left_l = lows.iloc[i - window : i]
        right_l = lows.iloc[i + 1 : i + 1 + window]
        if lv < left_l.min() and lv < right_l.min():
            swing_low.iloc[i] = True

    return swing_high, swing_low


def find_support_resistance_zones(
    frame: pd.DataFrame,
    *,
    window: int = 5,
    lookback: int = 252,
    tolerance_pct: float = 1.5,
    max_zones: int = 5,
) -> dict[str, list[float]]:
    """Cluster recent swing pivots into support / resistance zones.

    Args:
        frame: OHLCV DataFrame, sorted ascending by date.
        window: pivot detection window (see ``detect_swing_pivots``).
        lookback: how many bars back to scan.
        tolerance_pct: cluster two pivots into the same zone if they are
            within this percent of each other.
        max_zones: maximum zones to return on each side (closest to current
            price first).

    Returns:
        ``{"support": [..price..], "resistance": [..price..]}`` — both lists
        are sorted ascending by absolute distance from the most recent close.
    """
    if frame.empty:
        return {"support": [], "resistance": []}

    df = frame.tail(lookback)
    swing_high, swing_low = detect_swing_pivots(df, window=window)

    high_pivots = df.loc[swing_high, "high"].tolist()
    low_pivots = df.loc[swing_low, "low"].tolist()
    last_close = float(df["close"].iloc[-1])

    resistance = _cluster_levels(high_pivots, tolerance_pct)
    support = _cluster_levels(low_pivots, tolerance_pct)

    # Filter: resistance is above current price; support is below
    resistance = [r for r in resistance if r > last_close]
    support = [s for s in support if s < last_close]

    # Closest first
    resistance.sort(key=lambda x: x - last_close)
    support.sort(key=lambda x: last_close - x)

    return {
        "support": support[:max_zones],
        "resistance": resistance[:max_zones],
    }


def _cluster_levels(prices: list[float], tolerance_pct: float) -> list[float]:
    """Cluster nearby prices into a single zone (mean of cluster)."""
    if not prices:
        return []
    sorted_prices = sorted(prices)
    clusters: list[list[float]] = [[sorted_prices[0]]]
    for price in sorted_prices[1:]:
        last_cluster_mean = sum(clusters[-1]) / len(clusters[-1])
        if abs(price - last_cluster_mean) / last_cluster_mean * 100 <= tolerance_pct:
            clusters[-1].append(price)
        else:
            clusters.append([price])
    return [sum(c) / len(c) for c in clusters]


def latest_swing_levels(frame: pd.DataFrame, *, window: int = 5) -> dict[str, float | None]:
    """Return the most recent confirmed swing high and swing low prices."""
    if frame.empty:
        return {"last_swing_high": None, "last_swing_low": None}

    swing_high, swing_low = detect_swing_pivots(frame, window=window)
    last_high = float(frame.loc[swing_high, "high"].iloc[-1]) if swing_high.any() else None
    last_low = float(frame.loc[swing_low, "low"].iloc[-1]) if swing_low.any() else None
    return {"last_swing_high": last_high, "last_swing_low": last_low}
