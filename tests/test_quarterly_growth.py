"""Tests for QoQ / YoY quarterly growth helper."""

from __future__ import annotations

from stock_platform.analytics.fundamentals.ratios import calculate_quarterly_growth
from stock_platform.analytics.fundamentals.schema import QuarterlyFundamentalSnapshot


def _q(year: int, qtr: int, revenue: float, net_income: float) -> QuarterlyFundamentalSnapshot:
    return QuarterlyFundamentalSnapshot(
        symbol="X",
        fiscal_year=year,
        fiscal_quarter=qtr,
        revenue=revenue,
        net_income=net_income,
        ebitda=net_income * 1.5,
        eps=net_income / 10.0,
    )


def test_quarterly_growth_qoq_and_yoy():
    snaps = [
        _q(2024, 4, revenue=100, net_income=10),
        _q(2025, 1, revenue=110, net_income=11),
        _q(2025, 2, revenue=120, net_income=12),
        _q(2025, 3, revenue=130, net_income=13),
        _q(2025, 4, revenue=140, net_income=14),  # latest
    ]
    out = calculate_quarterly_growth(snaps)
    # QoQ vs Q3 2025 (130 → 140)
    assert out["revenue_qoq"] == (140 - 130) / 130
    # YoY vs Q4 2024 (100 → 140)
    assert out["revenue_yoy"] == (140 - 100) / 100
    assert out["net_income_yoy"] == (14 - 10) / 10


def test_quarterly_growth_no_year_ago():
    snaps = [
        _q(2025, 1, revenue=100, net_income=10),
        _q(2025, 2, revenue=110, net_income=11),
    ]
    out = calculate_quarterly_growth(snaps)
    assert out["revenue_qoq"] == (110 - 100) / 100
    assert out["revenue_yoy"] is None


def test_quarterly_growth_empty():
    out = calculate_quarterly_growth([])
    assert all(v is None for v in out.values())


def test_quarterly_growth_handles_zero_denominator():
    snaps = [
        _q(2024, 4, revenue=0, net_income=0),
        _q(2025, 4, revenue=100, net_income=10),
    ]
    out = calculate_quarterly_growth(snaps)
    assert out["revenue_yoy"] is None  # zero denominator → None
