"""RSI 60 momentum continuation strategy."""

from __future__ import annotations

from stock_platform.analytics.scanner.result_schema import StrategyScanResult
from stock_platform.analytics.strategies.base import (
    StrategyContext,
    all_present,
    common_result_kwargs,
    confidence_from_context,
    safe_float,
)


class RsiMomentumStrategy:
    """Momentum setup: RSI 60-75 with price above rising medium-term trend."""

    name = "RSI 60 Momentum Continuation"
    setup_type = "Momentum"

    def evaluate(self, context: StrategyContext) -> StrategyScanResult | None:
        row = context.latest
        required = ("close", "ema_50", "ema_200", "rsi_14", "relative_volume")
        if len(context.technical_frame) < 220 or not all_present(row, required):
            return None
        rsi = safe_float(row.get("rsi_14"))
        relative_volume = safe_float(row.get("relative_volume"))
        triggered = (
            row["close"] > row["ema_50"] > row["ema_200"]
            and rsi is not None
            and 60 <= rsi <= 75
            and relative_volume is not None
            and relative_volume >= 1.0
        )
        if not triggered:
            return None
        return StrategyScanResult(
            strategy=self.name,
            setup_type=self.setup_type,
            confidence_score=confidence_from_context(context, 72),
            why_this_appeared=(
                "RSI is in the 60-75 momentum zone, price is above the 50 EMA, "
                "and the 50 EMA is above the 200 EMA."
            ),
            key_risk="Momentum setups can chase extended moves; verify nearby resistance and event risk.",
            **common_result_kwargs(context),
        )
