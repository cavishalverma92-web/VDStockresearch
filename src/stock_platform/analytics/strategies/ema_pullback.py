"""20/50 EMA pullback strategy."""

from __future__ import annotations

from stock_platform.analytics.scanner.result_schema import StrategyScanResult
from stock_platform.analytics.strategies.base import (
    StrategyContext,
    all_present,
    common_result_kwargs,
    confidence_from_context,
    safe_float,
)


class EmaPullbackStrategy:
    """Pullback setup in an established uptrend."""

    name = "20/50 EMA Pullback In Uptrend"
    setup_type = "Pullback"

    def evaluate(self, context: StrategyContext) -> StrategyScanResult | None:
        row = context.latest
        required = ("close", "low", "ema_20", "ema_50", "ema_200", "rsi_14")
        if len(context.technical_frame) < 220 or not all_present(row, required):
            return None

        close = safe_float(row.get("close"))
        low = safe_float(row.get("low"))
        ema_20 = safe_float(row.get("ema_20"))
        ema_50 = safe_float(row.get("ema_50"))
        ema_200 = safe_float(row.get("ema_200"))
        rsi = safe_float(row.get("rsi_14"))
        if None in (close, low, ema_20, ema_50, ema_200, rsi):
            return None

        near_20 = min(abs(close - ema_20), abs(low - ema_20)) / ema_20 * 100  # type: ignore[operator]
        near_50 = min(abs(close - ema_50), abs(low - ema_50)) / ema_50 * 100  # type: ignore[operator]
        triggered = (
            close > ema_200  # type: ignore[operator]
            and ema_20 > ema_50 > ema_200  # type: ignore[operator]
            and min(near_20, near_50) <= 3.0
            and 40 <= rsi <= 60  # type: ignore[operator]
        )
        if not triggered:
            return None

        touched = "20 EMA" if near_20 <= near_50 else "50 EMA"
        return StrategyScanResult(
            strategy=self.name,
            setup_type=self.setup_type,
            confidence_score=confidence_from_context(context, 74),
            why_this_appeared=(
                f"Price is in an uptrend and has pulled back near the {touched} "
                "while RSI remains in a constructive 40-60 zone."
            ),
            key_risk="Pullbacks can turn into trend breaks; confirm support holds before treating it as useful.",
            **common_result_kwargs(context),
        )
