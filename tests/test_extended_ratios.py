"""Tests for extended balance-sheet health ratios."""

from __future__ import annotations

from stock_platform.analytics.fundamentals.extended_ratios import (
    cash_conversion_cycle,
    compute_extended_health,
    interest_coverage,
    working_capital_trend,
)
from stock_platform.analytics.fundamentals.schema import FundamentalSnapshot


def _snap(year: int, **kwargs: float) -> FundamentalSnapshot:
    return FundamentalSnapshot(symbol="X.NS", fiscal_year=year, **kwargs)


# ---------------------------------------------------------------------------
# interest_coverage
# ---------------------------------------------------------------------------


def test_interest_coverage_basic():
    s = _snap(2025, ebit=1000, interest_expense=200)
    assert interest_coverage(s) == 5.0


def test_interest_coverage_handles_negative_interest():
    # yfinance reports interest expense as negative
    s = _snap(2025, ebit=1000, interest_expense=-200)
    assert interest_coverage(s) == 5.0


def test_interest_coverage_none_when_inputs_missing():
    assert interest_coverage(_snap(2025)) is None
    assert interest_coverage(_snap(2025, ebit=1000)) is None


def test_interest_coverage_none_on_zero_interest():
    s = _snap(2025, ebit=1000, interest_expense=0)
    assert interest_coverage(s) is None


# ---------------------------------------------------------------------------
# cash_conversion_cycle
# ---------------------------------------------------------------------------


def test_cash_conversion_cycle_basic():
    s = _snap(
        2025,
        revenue=10_000,
        cost_of_revenue=6_000,
        accounts_receivable=1_000,
        inventory=600,
        accounts_payable=500,
    )
    result = cash_conversion_cycle(s)
    # DSO = 1000/10000*365 = 36.5
    # DIO = 600/6000*365   = 36.5
    # DPO = 500/6000*365   ≈ 30.4
    # CCC ≈ 42.6
    assert abs(result["dso_days"] - 36.5) < 0.1
    assert abs(result["dio_days"] - 36.5) < 0.1
    assert result["ccc_days"] is not None
    assert abs(result["ccc_days"] - 42.6) < 0.5


def test_cash_conversion_cycle_partial_inputs():
    s = _snap(2025, revenue=10_000, accounts_receivable=1_000)
    result = cash_conversion_cycle(s)
    assert result["dso_days"] is not None
    assert result["dio_days"] is None
    assert result["ccc_days"] is None  # CCC needs all three


# ---------------------------------------------------------------------------
# working_capital_trend
# ---------------------------------------------------------------------------


def test_working_capital_trend_full_history():
    snapshots = [
        _snap(2022, current_assets=1_000, current_liabilities=400),  # WC 600
        _snap(2023, current_assets=1_100, current_liabilities=450),  # WC 650
        _snap(2024, current_assets=1_200, current_liabilities=500),  # WC 700
        _snap(2025, current_assets=1_400, current_liabilities=560),  # WC 840
    ]
    result = working_capital_trend(snapshots)
    assert result["latest"] == 840
    assert result["prior_year"] == 700
    # YoY change = (840 - 700) / 700 = 0.20
    assert abs(result["yoy_change"] - 0.20) < 0.01
    # 3Y slope = (840 - 600) / 3 = 80
    assert result["slope_3y"] == 80


def test_working_capital_trend_empty_snapshots():
    result = working_capital_trend([])
    assert result["latest"] is None
    assert result["yoy_change"] is None


def test_working_capital_trend_only_latest():
    snapshots = [_snap(2025, current_assets=1_000, current_liabilities=400)]
    result = working_capital_trend(snapshots)
    assert result["latest"] == 600
    assert result["prior_year"] is None
    assert result["yoy_change"] is None
    assert result["slope_3y"] is None


# ---------------------------------------------------------------------------
# compute_extended_health composite
# ---------------------------------------------------------------------------


def test_compute_extended_health_keys():
    snapshots = [
        _snap(
            2025,
            ebit=1_000,
            interest_expense=200,
            revenue=10_000,
            cost_of_revenue=6_000,
            accounts_receivable=1_000,
            inventory=600,
            accounts_payable=500,
            current_assets=2_000,
            current_liabilities=800,
        )
    ]
    result = compute_extended_health(snapshots)
    expected_keys = {
        "interest_coverage",
        "dso_days",
        "dio_days",
        "dpo_days",
        "ccc_days",
        "working_capital_latest",
        "working_capital_yoy_change",
        "working_capital_3y_slope",
    }
    assert expected_keys.issubset(result.keys())
    assert result["interest_coverage"] == 5.0
    assert result["working_capital_latest"] == 1_200
