"""EMA stack trend strategy."""

from __future__ import annotations

from stock_platform.analytics.scanner.result_schema import StrategyScanResult
from stock_platform.analytics.strategies.base import (
    StrategyContext,
    all_present,
    common_result_kwargs,
    confidence_from_context,
)


class EmaStackStrategy:
    """Trend filter: close > EMA20 > EMA50 > EMA100 > EMA200."""

    name = "EMA Stack Trend Filter"
    setup_type = "Trend"

    def evaluate(self, context: StrategyContext) -> StrategyScanResult | None:
        if len(context.technical_frame) < 220:
            return None
        row = context.latest
        required = ("close", "ema_20", "ema_50", "ema_100", "ema_200")
        if not all_present(row, required):
            return None
        triggered = row["close"] > row["ema_20"] > row["ema_50"] > row["ema_100"] > row["ema_200"]
        if not triggered:
            return None
        return StrategyScanResult(
            strategy=self.name,
            setup_type=self.setup_type,
            confidence_score=confidence_from_context(context, 76),
            why_this_appeared=(
                "Price is above a fully aligned 20/50/100/200 EMA stack, "
                "which indicates a clean medium-term uptrend."
            ),
            key_risk="This is a trend filter, so it can appear late after a large rally.",
            **common_result_kwargs(context),
        )
