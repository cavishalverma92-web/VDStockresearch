"""Delivery percentage analytics for NSE equity data.

High delivery % indicates institutional/long-term conviction; low delivery %
suggests intraday or speculative activity.  This module computes rolling
averages and flags statistically unusual delivery spikes.

All functions are pure transformations on DataFrames — no I/O.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

_MA_WINDOW = 20  # trading days for rolling average
_ZSCORE_WINDOW = 20
_UNUSUAL_MULTIPLIER = 1.5  # flag when delivery_pct > 1.5× its 20-day average
_MIN_DELIVERY_FOR_FLAG = 40.0  # only flag if absolute delivery % ≥ 40


def compute_delivery_analytics(delivery_df: pd.DataFrame) -> pd.DataFrame:
    """Enrich a delivery DataFrame with rolling statistics and anomaly flags.

    Input must have columns: trade_date (date), delivery_pct (float).
    Sorts by trade_date before computing.

    Added columns:
    - delivery_pct_ma20: 20-day simple moving average of delivery_pct
    - delivery_pct_zscore: rolling z-score (delivery_pct vs. 20-day window)
    - unusual_delivery: True when pct > MULTIPLIER × ma20 AND pct ≥ MIN_THRESHOLD
    """
    if delivery_df is None or delivery_df.empty:
        return delivery_df if delivery_df is not None else pd.DataFrame()

    df = delivery_df.copy().sort_values("trade_date").reset_index(drop=True)

    if "delivery_pct" not in df.columns:
        return df

    pct = pd.to_numeric(df["delivery_pct"], errors="coerce")

    df["delivery_pct_ma20"] = pct.rolling(_MA_WINDOW, min_periods=5).mean().round(2)

    rolling_std = pct.rolling(_ZSCORE_WINDOW, min_periods=5).std()
    rolling_mean = pct.rolling(_ZSCORE_WINDOW, min_periods=5).mean()
    df["delivery_pct_zscore"] = ((pct - rolling_mean) / rolling_std.replace(0, np.nan)).round(2)

    df["unusual_delivery"] = (pct >= _MIN_DELIVERY_FOR_FLAG) & (
        pct > _UNUSUAL_MULTIPLIER * df["delivery_pct_ma20"]
    )

    return df


def delivery_stats(delivery_df: pd.DataFrame) -> dict:
    """Summarise delivery data into scalar metrics for UI display.

    Returns a dict with keys:
    - latest_pct: most recent delivery_pct
    - ma20_pct: most recent 20-day average
    - avg_90d_pct: mean over the whole DataFrame
    - trend: 'rising' | 'falling' | 'flat' | None
    - unusual_today: bool — is the latest bar flagged as unusual?
    - days_with_data: number of rows with non-null delivery_pct
    """
    empty: dict = {
        "latest_pct": None,
        "ma20_pct": None,
        "avg_90d_pct": None,
        "trend": None,
        "unusual_today": False,
        "days_with_data": 0,
    }
    if delivery_df is None or delivery_df.empty:
        return empty

    enriched = compute_delivery_analytics(delivery_df)
    pct = pd.to_numeric(enriched.get("delivery_pct"), errors="coerce").dropna()

    if pct.empty:
        return empty

    latest_idx = pct.last_valid_index()
    latest_pct = float(pct.iloc[-1]) if latest_idx is not None else None
    ma20_col = enriched.get("delivery_pct_ma20")
    ma20_pct = float(ma20_col.iloc[-1]) if ma20_col is not None and not ma20_col.empty else None

    # Trend: compare last 5 days average vs. previous 5 days average
    trend: str | None = None
    if len(pct) >= 10:
        recent = pct.iloc[-5:].mean()
        prior = pct.iloc[-10:-5].mean()
        if recent > prior * 1.05:
            trend = "rising"
        elif recent < prior * 0.95:
            trend = "falling"
        else:
            trend = "flat"

    unusual_col = enriched.get("unusual_delivery")
    unusual_today = bool(unusual_col.iloc[-1]) if unusual_col is not None else False

    return {
        "latest_pct": round(latest_pct, 1) if latest_pct is not None else None,
        "ma20_pct": round(ma20_pct, 1) if ma20_pct is not None else None,
        "avg_90d_pct": round(float(pct.mean()), 1),
        "trend": trend,
        "unusual_today": unusual_today,
        "days_with_data": int(pct.notna().sum()),
    }
