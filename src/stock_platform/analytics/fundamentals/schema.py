"""Typed fundamentals records used by Phase 1 calculations."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FundamentalSnapshot:
    """One annual fundamentals snapshot for a company.

    Values are expected to be in the same currency/unit for a given company.
    Missing values are represented as `None` so calculations can report which
    criteria were skipped instead of silently treating missing data as zero.
    """

    symbol: str
    fiscal_year: int
    revenue: float | None = None
    gross_profit: float | None = None
    ebitda: float | None = None
    ebit: float | None = None
    net_income: float | None = None
    eps: float | None = None
    book_value: float | None = None
    operating_cash_flow: float | None = None
    capital_expenditure: float | None = None
    free_cash_flow: float | None = None
    debt: float | None = None
    net_debt: float | None = None
    cash_and_equivalents: float | None = None
    total_assets: float | None = None
    total_liabilities: float | None = None
    current_assets: float | None = None
    current_liabilities: float | None = None
    retained_earnings: float | None = None
    shares_outstanding: float | None = None
    market_cap: float | None = None
    enterprise_value: float | None = None

    # Working-capital components (added Phase 7+ gap fill)
    accounts_receivable: float | None = None
    inventory: float | None = None
    accounts_payable: float | None = None
    interest_expense: float | None = None
    cost_of_revenue: float | None = None


@dataclass(frozen=True)
class ScoreResult:
    """A score plus the criteria that could not be computed."""

    score: float | None
    max_score: float
    missing_criteria: tuple[str, ...] = ()
