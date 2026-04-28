"""Fundamental analysis helpers for Phase 1."""

from stock_platform.analytics.fundamentals.cagr import (
    cagr_summary_for_metric,
    calculate_cagr,
    compute_multi_year_cagr,
)
from stock_platform.analytics.fundamentals.extended_ratios import (
    cash_conversion_cycle,
    compute_extended_health,
    interest_coverage,
    working_capital_trend,
)
from stock_platform.analytics.fundamentals.quality_scores import (
    calculate_altman_z_score,
    calculate_piotroski_f_score,
)
from stock_platform.analytics.fundamentals.ratios import calculate_basic_ratios, calculate_growth
from stock_platform.analytics.fundamentals.schema import (
    BankingFundamentalSnapshot,
    FundamentalSnapshot,
    ScoreResult,
)
from stock_platform.analytics.fundamentals.sector_policy import (
    fundamentals_required_columns_for,
    fundamentals_score_inputs_for,
    is_financial_sector,
    is_industrial_metric_applicable,
)

__all__ = [
    "FundamentalSnapshot",
    "BankingFundamentalSnapshot",
    "ScoreResult",
    "cagr_summary_for_metric",
    "calculate_altman_z_score",
    "calculate_basic_ratios",
    "calculate_cagr",
    "calculate_growth",
    "calculate_piotroski_f_score",
    "cash_conversion_cycle",
    "compute_extended_health",
    "compute_multi_year_cagr",
    "fundamentals_required_columns_for",
    "fundamentals_score_inputs_for",
    "interest_coverage",
    "is_financial_sector",
    "is_industrial_metric_applicable",
    "working_capital_trend",
]
