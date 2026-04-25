"""Result-date volatility analysis.

Compares ATR (Average True Range) during a ±N-day window around earnings
dates against the baseline ATR outside those windows.  A high volatility
multiple indicates the stock is significantly more volatile around results —
useful for position sizing and options premium context.

All functions are pure DataFrame transformations — no I/O.
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

_WINDOW_DAYS = 5  # bars on each side of the earnings date
_MIN_BASELINE_BARS = 10  # need at least this many non-event bars


def compute_result_volatility(
    ohlcv: pd.DataFrame,
    earnings_dates: list[date],
    *,
    window: int = _WINDOW_DAYS,
) -> dict:
    """Measure ATR expansion around earnings events.

    Args:
        ohlcv: DataFrame with DatetimeIndex and columns high, low, close.
        earnings_dates: List of past earnings / result announcement dates.
        window: Number of bars on each side of an event to include.

    Returns:
        dict with keys:
        - event_atr: mean ATR during result windows
        - baseline_atr: mean ATR outside result windows
        - volatility_multiple: event_atr / baseline_atr (None if insufficient data)
        - events_found: how many earnings dates fell within the OHLCV range
        - result_windows: list of (start, end) date tuples used
    """
    result: dict = {
        "event_atr": None,
        "baseline_atr": None,
        "volatility_multiple": None,
        "events_found": 0,
        "result_windows": [],
    }

    if ohlcv is None or ohlcv.empty or not earnings_dates:
        return result

    if not {"high", "low", "close"}.issubset(ohlcv.columns):
        return result

    df = ohlcv.copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df["tr"] = _true_range(df)

    ohlcv_dates = {idx.date() for idx in df.index}
    min_date = min(ohlcv_dates)
    max_date = max(ohlcv_dates)

    event_mask = pd.Series(False, index=df.index)
    windows_used: list[tuple[date, date]] = []

    for earnings_dt in earnings_dates:
        if earnings_dt < min_date or earnings_dt > max_date:
            continue
        win_start = earnings_dt - timedelta(days=window * 2)
        win_end = earnings_dt + timedelta(days=window * 2)
        mask = (df.index.date >= win_start) & (df.index.date <= win_end)
        event_mask = event_mask | pd.Series(mask, index=df.index)
        windows_used.append((win_start, win_end))

    result["events_found"] = len(windows_used)
    result["result_windows"] = windows_used

    event_tr = df.loc[event_mask, "tr"].dropna()
    baseline_tr = df.loc[~event_mask, "tr"].dropna()

    if len(event_tr) < 2 or len(baseline_tr) < _MIN_BASELINE_BARS:
        return result

    event_atr = float(event_tr.mean())
    baseline_atr = float(baseline_tr.mean())

    result["event_atr"] = round(event_atr, 2)
    result["baseline_atr"] = round(baseline_atr, 2)
    if baseline_atr > 0:
        result["volatility_multiple"] = round(event_atr / baseline_atr, 2)

    return result


def _true_range(df: pd.DataFrame) -> pd.Series:
    """Compute True Range from high, low, close columns."""
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr
