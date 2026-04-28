"""Sector-aware fundamentals rules.

The MVP started with industrial-company metrics. Banks and financial services
need a gentler rule set because their statements do not naturally expose
inventory, gross profit, working capital, or Altman-style distress metrics.
"""

from __future__ import annotations

from collections.abc import Mapping

_FINANCIAL_KEYWORDS = (
    "bank",
    "banks",
    "financial",
    "finance",
    "nbfc",
    "insurance",
    "asset management",
    "capital markets",
    "broking",
    "housing finance",
)


def is_financial_sector(
    *,
    symbol: str | None = None,
    sector: object = None,
    industry: object = None,
    row: Mapping[str, object] | None = None,
) -> bool:
    """Return True when a stock should use financial-sector fundamentals rules."""
    row = row or {}
    symbol_text = str(symbol or row.get("symbol") or "").upper()
    sector_text = str(sector or row.get("sector") or "").lower()
    industry_text = str(industry or row.get("industry") or "").lower()
    combined = f"{symbol_text.lower()} {sector_text} {industry_text}"
    return any(keyword in combined for keyword in _FINANCIAL_KEYWORDS)


def fundamentals_required_columns_for(
    *,
    symbol: str,
    sector: object = None,
    industry: object = None,
) -> list[str]:
    """Return required annual-fundamentals columns for a symbol's sector."""
    common = [
        "symbol",
        "fiscal_year",
        "revenue",
        "net_income",
        "total_assets",
        "total_liabilities",
        "shares_outstanding",
        "market_cap",
        "source",
    ]
    if is_financial_sector(symbol=symbol, sector=sector, industry=industry):
        return common
    return [
        *common,
        "gross_profit",
        "ebit",
        "operating_cash_flow",
        "current_assets",
        "current_liabilities",
        "retained_earnings",
    ]


def fundamentals_score_inputs_for(
    *,
    symbol: str,
    sector: object = None,
    industry: object = None,
) -> set[str]:
    """Return score-input columns that are meaningful for the sector."""
    financial_inputs = {
        "revenue",
        "net_income",
        "eps",
        "book_value",
        "total_assets",
        "total_liabilities",
        "shares_outstanding",
        "market_cap",
    }
    if is_financial_sector(symbol=symbol, sector=sector, industry=industry):
        return financial_inputs
    return {
        *financial_inputs,
        "gross_profit",
        "ebitda",
        "ebit",
        "operating_cash_flow",
        "capital_expenditure",
        "free_cash_flow",
        "debt",
        "net_debt",
        "cash_and_equivalents",
        "current_assets",
        "current_liabilities",
        "retained_earnings",
        "enterprise_value",
    }


def is_industrial_metric_applicable(metric_name: str, *, is_financial: bool) -> bool:
    """Return False for industrial metrics that should be hidden for financials."""
    if not is_financial:
        return True
    return metric_name not in {
        "altman_z_score",
        "cash_conversion_cycle",
        "working_capital",
        "interest_coverage",
        "debt_to_equity",
        "roce",
    }
