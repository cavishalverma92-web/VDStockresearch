"""Aggregate FII/DII flow analytics.

Takes the long-format frame produced by ``fetch_market_flows`` (one row per
``(trade_date, participant)``) and returns rolling sums, latest snapshots,
and trend classifications used by the UI and composite scoring.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

Trend = Literal["bullish", "bearish", "neutral", "unknown"]


@dataclass(frozen=True)
class InstitutionalFlowSnapshot:
    """One participant's latest flow snapshot + rolling-window context."""

    participant: str
    latest_date: pd.Timestamp | None
    latest_net_cr: float | None
    rolling_5d_net_cr: float | None
    rolling_20d_net_cr: float | None
    trend: Trend


def compute_institutional_flow_snapshots(
    frame: pd.DataFrame,
    *,
    short_window: int = 5,
    long_window: int = 20,
) -> dict[str, InstitutionalFlowSnapshot]:
    """Return one snapshot per participant present in the frame.

    Trend classification:
    - **bullish** when both rolling sums are positive *and* the short window
      is at least as positive as the long window (i.e. recent flow is
      accelerating or holding strong).
    - **bearish** when both rolling sums are negative and the short window
      is at least as negative as the long window.
    - **neutral** when rolling windows disagree in sign or are flat.
    - **unknown** when there isn't enough history to form a 5-day sum.
    """
    if frame is None or frame.empty:
        return {}
    if "participant" not in frame.columns or "trade_date" not in frame.columns:
        return {}

    result: dict[str, InstitutionalFlowSnapshot] = {}
    for participant, group in frame.groupby("participant"):
        ordered = group.sort_values("trade_date").reset_index(drop=True)
        latest = ordered.iloc[-1]
        latest_net = _as_float(latest.get("net_value_cr"))
        latest_date = pd.to_datetime(latest.get("trade_date"), errors="coerce")

        short_sum = _rolling_sum(ordered["net_value_cr"], short_window)
        long_sum = _rolling_sum(ordered["net_value_cr"], long_window)
        trend = _classify_trend(short_sum, long_sum, short_window, long_window)

        result[str(participant)] = InstitutionalFlowSnapshot(
            participant=str(participant),
            latest_date=latest_date if not pd.isna(latest_date) else None,
            latest_net_cr=latest_net,
            rolling_5d_net_cr=short_sum,
            rolling_20d_net_cr=long_sum,
            trend=trend,
        )
    return result


def institutional_flow_score(
    snapshots: dict[str, InstitutionalFlowSnapshot],
) -> float | None:
    """Map FII + DII flow context to a 0–100 score for composite scoring.

    Heuristic:
    - FII bullish + DII bullish → 80
    - One bullish, one neutral → 65
    - Mixed (one bullish, one bearish) → 50
    - One bearish, one neutral → 40
    - Both bearish → 25
    - Insufficient data → None
    """
    fii = snapshots.get("FII")
    dii = snapshots.get("DII")
    if fii is None and dii is None:
        return None
    trends = [s.trend for s in (fii, dii) if s is not None]
    if all(t == "unknown" for t in trends):
        return None
    bullish = sum(1 for t in trends if t == "bullish")
    bearish = sum(1 for t in trends if t == "bearish")
    if bullish == 2:
        return 80.0
    if bullish == 1 and bearish == 0:
        return 65.0
    if bullish == 1 and bearish == 1:
        return 50.0
    if bearish == 1 and bullish == 0:
        return 40.0
    if bearish == 2:
        return 25.0
    return 50.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _rolling_sum(series: pd.Series, window: int) -> float | None:
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    if cleaned.empty:
        return None
    if len(cleaned) < window:
        return None
    return float(cleaned.tail(window).sum())


def _classify_trend(
    short_sum: float | None,
    long_sum: float | None,
    short_window: int,
    long_window: int,
) -> Trend:
    if short_sum is None or long_sum is None or short_window <= 0 or long_window <= 0:
        return "unknown"
    # Compare per-day averages so the window scales cancel: bullish when
    # both windows have a positive daily average and the recent average
    # isn't decaying (≥ half the long-run average).
    short_avg = short_sum / short_window
    long_avg = long_sum / long_window
    if short_avg > 0 and long_avg > 0 and short_avg >= long_avg * 0.5:
        return "bullish"
    if short_avg < 0 and long_avg < 0 and short_avg <= long_avg * 0.5:
        return "bearish"
    return "neutral"


def _as_float(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
