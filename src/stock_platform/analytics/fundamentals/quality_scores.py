"""Fundamentals quality score calculations.

These are pure calculations over already-clean fundamentals data. They do not
fetch, scrape, or store source data.
"""

from __future__ import annotations

from collections.abc import Callable

from stock_platform.analytics.fundamentals.ratios import (
    asset_turnover,
    calculate_basic_ratios,
    safe_divide,
    working_capital,
)
from stock_platform.analytics.fundamentals.schema import FundamentalSnapshot, ScoreResult


def _criterion(name: str, predicate: Callable[[], bool | None]) -> tuple[int, str | None]:
    value = predicate()
    if value is None:
        return 0, name
    return (1 if value else 0), None


def calculate_piotroski_f_score(
    current: FundamentalSnapshot,
    previous: FundamentalSnapshot,
) -> ScoreResult:
    """Calculate the 0-9 Piotroski F-Score.

    Missing inputs do not become false signals. They are reported in
    `missing_criteria` so the UI can later warn the user.
    """
    current_ratios = calculate_basic_ratios(current)
    previous_ratios = calculate_basic_ratios(previous)

    checks: list[tuple[str, Callable[[], bool | None]]] = [
        (
            "positive_net_income",
            lambda: None if current.net_income is None else current.net_income > 0,
        ),
        (
            "positive_operating_cash_flow",
            lambda: (
                None if current.operating_cash_flow is None else current.operating_cash_flow > 0
            ),
        ),
        (
            "return_on_assets_improved",
            lambda: _greater(
                current_ratios["return_on_assets"],
                previous_ratios["return_on_assets"],
            ),
        ),
        (
            "cash_flow_exceeds_net_income",
            lambda: _greater(current.operating_cash_flow, current.net_income),
        ),
        (
            "lower_leverage",
            lambda: _less(
                safe_divide(current.total_liabilities, current.total_assets),
                safe_divide(previous.total_liabilities, previous.total_assets),
            ),
        ),
        (
            "current_ratio_improved",
            lambda: _greater(current_ratios["current_ratio"], previous_ratios["current_ratio"]),
        ),
        (
            "no_share_dilution",
            lambda: _less_or_equal(current.shares_outstanding, previous.shares_outstanding),
        ),
        (
            "gross_margin_improved",
            lambda: _greater(current_ratios["gross_margin"], previous_ratios["gross_margin"]),
        ),
        (
            "asset_turnover_improved",
            lambda: _greater(asset_turnover(current), asset_turnover(previous)),
        ),
    ]

    score = 0
    missing: list[str] = []
    for name, predicate in checks:
        point, missing_name = _criterion(name, predicate)
        score += point
        if missing_name is not None:
            missing.append(missing_name)

    return ScoreResult(score=float(score), max_score=9.0, missing_criteria=tuple(missing))


def calculate_altman_z_score(snapshot: FundamentalSnapshot) -> ScoreResult:
    """Calculate the classic Altman Z-Score input set.

    Formula:
    1.2 * working capital / total assets
    + 1.4 * retained earnings / total assets
    + 3.3 * EBIT / total assets
    + 0.6 * market cap / total liabilities
    + 1.0 * revenue / total assets
    """
    components = {
        "working_capital_to_assets": safe_divide(working_capital(snapshot), snapshot.total_assets),
        "retained_earnings_to_assets": safe_divide(
            snapshot.retained_earnings,
            snapshot.total_assets,
        ),
        "ebit_to_assets": safe_divide(snapshot.ebit, snapshot.total_assets),
        "market_cap_to_liabilities": safe_divide(snapshot.market_cap, snapshot.total_liabilities),
        "revenue_to_assets": safe_divide(snapshot.revenue, snapshot.total_assets),
    }

    missing = tuple(name for name, value in components.items() if value is None)
    if missing:
        return ScoreResult(score=None, max_score=10.0, missing_criteria=missing)

    score = (
        1.2 * components["working_capital_to_assets"]
        + 1.4 * components["retained_earnings_to_assets"]
        + 3.3 * components["ebit_to_assets"]
        + 0.6 * components["market_cap_to_liabilities"]
        + components["revenue_to_assets"]
    )
    return ScoreResult(score=score, max_score=10.0, missing_criteria=())


def _greater(left: float | None, right: float | None) -> bool | None:
    if left is None or right is None:
        return None
    return left > right


def _less(left: float | None, right: float | None) -> bool | None:
    if left is None or right is None:
        return None
    return left < right


def _less_or_equal(left: float | None, right: float | None) -> bool | None:
    if left is None or right is None:
        return None
    return left <= right
