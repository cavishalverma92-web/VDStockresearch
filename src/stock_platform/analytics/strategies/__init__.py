"""Strategy scanner modules for research-only swing-trading setups."""

from stock_platform.analytics.strategies.base import (
    StrategyContext,
    StrategyDefinition,
    evaluate_default_strategies,
)
from stock_platform.analytics.strategies.breakout import (
    BreakoutWithVolumeStrategy,
    HighBreakoutStrategy,
)
from stock_platform.analytics.strategies.ema_pullback import EmaPullbackStrategy
from stock_platform.analytics.strategies.ema_stack import EmaStackStrategy
from stock_platform.analytics.strategies.rsi_momentum import RsiMomentumStrategy

__all__ = [
    "BreakoutWithVolumeStrategy",
    "EmaPullbackStrategy",
    "EmaStackStrategy",
    "HighBreakoutStrategy",
    "RsiMomentumStrategy",
    "StrategyContext",
    "StrategyDefinition",
    "evaluate_default_strategies",
]
