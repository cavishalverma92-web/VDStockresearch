"""Tests for Phase 1 fundamentals calculations."""

from __future__ import annotations

import pytest

from stock_platform.analytics.fundamentals import (
    FundamentalSnapshot,
    calculate_altman_z_score,
    calculate_basic_ratios,
    calculate_growth,
    calculate_piotroski_f_score,
)


def _previous_snapshot() -> FundamentalSnapshot:
    return FundamentalSnapshot(
        symbol="TEST.NS",
        fiscal_year=2024,
        revenue=1_000,
        gross_profit=400,
        ebitda=180,
        ebit=140,
        net_income=90,
        eps=9,
        book_value=450,
        operating_cash_flow=95,
        capital_expenditure=25,
        free_cash_flow=70,
        debt=430,
        net_debt=360,
        cash_and_equivalents=70,
        total_assets=900,
        total_liabilities=450,
        current_assets=300,
        current_liabilities=180,
        retained_earnings=250,
        shares_outstanding=100,
        market_cap=1_500,
        enterprise_value=1_860,
    )


def _current_snapshot() -> FundamentalSnapshot:
    return FundamentalSnapshot(
        symbol="TEST.NS",
        fiscal_year=2025,
        revenue=1_200,
        gross_profit=510,
        ebitda=230,
        ebit=180,
        net_income=130,
        eps=13,
        book_value=580,
        operating_cash_flow=160,
        capital_expenditure=40,
        free_cash_flow=120,
        debt=400,
        net_debt=330,
        cash_and_equivalents=70,
        total_assets=1_000,
        total_liabilities=420,
        current_assets=360,
        current_liabilities=180,
        retained_earnings=320,
        shares_outstanding=100,
        market_cap=1_800,
        enterprise_value=2_130,
    )


def test_basic_ratios_are_calculated_safely() -> None:
    ratios = calculate_basic_ratios(_current_snapshot())

    assert ratios["return_on_assets"] == pytest.approx(0.13)
    assert ratios["return_on_equity"] == pytest.approx(130 / 580)
    assert ratios["return_on_capital_employed"] == pytest.approx(180 / 820)
    assert ratios["current_ratio"] == pytest.approx(2.0)
    assert ratios["gross_margin"] == pytest.approx(0.425)
    assert ratios["ebitda_margin"] == pytest.approx(230 / 1_200)
    assert ratios["pat_margin"] == pytest.approx(130 / 1_200)
    assert ratios["free_cash_flow_yield"] == pytest.approx(120 / 1_800)
    assert ratios["price_to_earnings"] == pytest.approx(1_800 / 130)
    assert ratios["ev_to_ebitda"] == pytest.approx(2_130 / 230)


def test_growth_compares_current_to_previous_year() -> None:
    growth = calculate_growth(_current_snapshot(), _previous_snapshot())

    assert growth["revenue_growth"] == pytest.approx(0.20)
    assert growth["net_income_growth"] == pytest.approx(40 / 90)
    assert growth["eps_growth"] == pytest.approx(4 / 9)
    assert growth["free_cash_flow_growth"] == pytest.approx(50 / 70)
    assert growth["book_value_growth"] == pytest.approx(130 / 450)


def test_piotroski_score_rewards_improving_company() -> None:
    result = calculate_piotroski_f_score(_current_snapshot(), _previous_snapshot())

    assert result.score == 9
    assert result.max_score == 9
    assert result.missing_criteria == ()


def test_piotroski_reports_missing_inputs() -> None:
    current = FundamentalSnapshot(symbol="TEST.NS", fiscal_year=2025, net_income=10)
    previous = FundamentalSnapshot(symbol="TEST.NS", fiscal_year=2024)

    result = calculate_piotroski_f_score(current, previous)

    assert result.score == 1
    assert "positive_operating_cash_flow" in result.missing_criteria
    assert "asset_turnover_improved" in result.missing_criteria


def test_altman_z_score_returns_missing_criteria_when_inputs_absent() -> None:
    result = calculate_altman_z_score(
        FundamentalSnapshot(symbol="TEST.NS", fiscal_year=2025, revenue=1_000)
    )

    assert result.score is None
    assert "working_capital_to_assets" in result.missing_criteria
    assert "ebit_to_assets" in result.missing_criteria


def test_altman_z_score_calculates_when_inputs_exist() -> None:
    result = calculate_altman_z_score(_current_snapshot())

    expected = (
        1.2 * (180 / 1_000)
        + 1.4 * (320 / 1_000)
        + 3.3 * (180 / 1_000)
        + 0.6 * (1_800 / 420)
        + (1_200 / 1_000)
    )
    assert result.score == pytest.approx(expected)
    assert result.missing_criteria == ()
