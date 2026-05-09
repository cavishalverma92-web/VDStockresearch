"""Relative-volume breakout strategy."""

from __future__ import annotations

import pandas as pd

from stock_platform.analytics.scanner.result_schema import StrategyScanResult
from stock_platform.analytics.strategies.base import (
    StrategyContext,
    all_present,
    common_result_kwargs,
    confidence_from_context,
    safe_float,
)
from stock_platform.config import get_thresholds_config


class BreakoutWithVolumeStrategy:
    """Close above prior multi-month/52-week high with volume confirmation."""

    name = "Breakout With Relative Volume"
    setup_type = "Breakout"

    def evaluate(self, context: StrategyContext) -> StrategyScanResult | None:
        frame = context.technical_frame
        row = context.latest
        required = ("close", "high", "ema_50", "ema_200", "relative_volume", "atr_pct")
        if len(frame) < 140 or not all_present(row, required):
            return None

        thresholds = get_thresholds_config().get("signals", {}).get("breakout_with_volume", {})
        primary_lookback = int(thresholds.get("lookback_days_for_high", 252))
        shorter_lookback = int(thresholds.get("shorter_lookback_days", 120))
        volume_multiple = float(thresholds.get("volume_multiple", 2.0))
        max_extension_pct = float(thresholds.get("max_extension_pct", 6.0))
        lookback = primary_lookback if len(frame) >= primary_lookback + 1 else shorter_lookback
        if len(frame) < lookback + 1:
            return None

        prior_high = _prior_high(frame, lookback)
        close = safe_float(row.get("close"))
        ema_50 = safe_float(row.get("ema_50"))
        ema_200 = safe_float(row.get("ema_200"))
        relative_volume = safe_float(row.get("relative_volume"))
        atr_pct = safe_float(row.get("atr_pct"))
        if None in (prior_high, close, ema_50, ema_200, relative_volume, atr_pct):
            return None

        extension_pct = ((close - prior_high) / prior_high) * 100  # type: ignore[operator]
        triggered = (
            close > prior_high  # type: ignore[operator]
            and extension_pct <= max_extension_pct
            and relative_volume >= volume_multiple  # type: ignore[operator]
            and close > ema_50 > ema_200  # type: ignore[operator]
        )
        if not triggered:
            return None

        label = "52-week high" if lookback >= 240 else f"{lookback}-day high"
        kwargs = common_result_kwargs(context)
        kwargs["breakout_level"] = round(float(prior_high), 2)
        return StrategyScanResult(
            strategy=self.name,
            setup_type=self.setup_type,
            confidence_score=confidence_from_context(context, 78),
            why_this_appeared=(
                f"Close broke above the prior {label} with relative volume of "
                f"{relative_volume:.2f}x and price remains above the 50/200 EMA trend filter."
            ),
            key_risk=(
                "Breakouts can fail quickly if volume fades or the move is event-driven; "
                "watch for a close back below the breakout level."
            ),
            **kwargs,
        )


class HighBreakoutStrategy:
    """Close at a 52-week or 120-day high with moderate participation."""

    name = "52W / 120D High Breakout"
    setup_type = "Breakout"

    def evaluate(self, context: StrategyContext) -> StrategyScanResult | None:
        frame = context.technical_frame
        row = context.latest
        required = ("close", "high", "ema_50", "ema_200", "relative_volume", "atr_pct")
        if len(frame) < 140 or not all_present(row, required):
            return None

        thresholds = get_thresholds_config().get("signals", {}).get("high_breakout", {})
        primary_lookback = int(thresholds.get("lookback_days_for_high", 252))
        shorter_lookback = int(thresholds.get("shorter_lookback_days", 120))
        volume_floor = float(thresholds.get("relative_volume_floor", 1.2))
        max_extension_pct = float(thresholds.get("max_extension_pct", 4.0))
        lookback = primary_lookback if len(frame) >= primary_lookback + 1 else shorter_lookback
        if len(frame) < lookback + 1:
            return None

        prior_high = _prior_high(frame, lookback)
        close = safe_float(row.get("close"))
        ema_50 = safe_float(row.get("ema_50"))
        ema_200 = safe_float(row.get("ema_200"))
        relative_volume = safe_float(row.get("relative_volume"))
        if None in (prior_high, close, ema_50, ema_200, relative_volume):
            return None

        extension_pct = ((close - prior_high) / prior_high) * 100  # type: ignore[operator]
        triggered = (
            close >= prior_high  # type: ignore[operator]
            and extension_pct <= max_extension_pct
            and relative_volume >= volume_floor  # type: ignore[operator]
            and close > ema_50 > ema_200  # type: ignore[operator]
        )
        if not triggered:
            return None

        label = "52-week high" if lookback >= 240 else f"{lookback}-day high"
        kwargs = common_result_kwargs(context)
        kwargs["breakout_level"] = round(float(prior_high), 2)
        return StrategyScanResult(
            strategy=self.name,
            setup_type=self.setup_type,
            confidence_score=confidence_from_context(context, 72),
            why_this_appeared=(
                f"Close is at or above the prior {label}, relative volume is "
                f"{relative_volume:.2f}x, and price remains above the 50/200 EMA trend filter."
            ),
            key_risk=(
                "New-high breakouts can become extended quickly; verify sector strength, "
                "event risk, and whether price is still near the breakout level."
            ),
            **kwargs,
        )


def _prior_high(frame: pd.DataFrame, lookback: int) -> float | None:
    highs = pd.to_numeric(frame["high"], errors="coerce").shift(1)
    value = highs.rolling(window=lookback, min_periods=lookback).max().iloc[-1]
    if pd.isna(value):
        return None
    return float(value)
