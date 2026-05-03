"""Signal backtest engine (Phase 2).

Evaluates forward returns for saved signal observations stored in
the signal_audit table. This is an educational tool — not a
recommendation engine and not investment advice.
"""

from stock_platform.analytics.backtest.signal_backtest import (
    BacktestSummary,
    PortfolioBacktestMetrics,
    TradeResult,
    compute_portfolio_metrics,
    filter_events_by_index_membership,
    portfolio_metrics_to_frame,
    run_signal_backtest,
    run_walk_forward_validation,
    summaries_to_frame,
    trades_to_frame,
)

__all__ = [
    "BacktestSummary",
    "PortfolioBacktestMetrics",
    "TradeResult",
    "compute_portfolio_metrics",
    "filter_events_by_index_membership",
    "portfolio_metrics_to_frame",
    "run_signal_backtest",
    "run_walk_forward_validation",
    "summaries_to_frame",
    "trades_to_frame",
]
