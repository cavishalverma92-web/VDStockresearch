"""Tests for multi-year CAGR helpers."""

from __future__ import annotations

from stock_platform.analytics.fundamentals.cagr import (
    cagr_summary_for_metric,
    calculate_cagr,
    compute_multi_year_cagr,
)
from stock_platform.analytics.fundamentals.schema import FundamentalSnapshot


def _snap(year: int, **kwargs: float) -> FundamentalSnapshot:
    return FundamentalSnapshot(symbol="X.NS", fiscal_year=year, **kwargs)


def test_calculate_cagr_basic():
    # 100 → 161.05 over 5 years = 10% CAGR
    cagr = calculate_cagr(100, 161.051, 5)
    assert cagr is not None
    assert abs(cagr - 0.10) < 0.001


def test_calculate_cagr_returns_none_for_missing_inputs():
    assert calculate_cagr(None, 100, 3) is None
    assert calculate_cagr(100, None, 3) is None


def test_calculate_cagr_returns_none_for_zero_or_negative_start():
    assert calculate_cagr(0, 100, 3) is None
    assert calculate_cagr(-50, 100, 3) is None


def test_calculate_cagr_returns_none_for_zero_or_negative_years():
    assert calculate_cagr(100, 200, 0) is None
    assert calculate_cagr(100, 200, -1) is None


def test_compute_multi_year_cagr_with_full_history():
    # 11 years of 10% revenue growth: 100 → ~259
    snapshots = [_snap(2015 + i, revenue=100 * (1.10**i)) for i in range(11)]
    result = compute_multi_year_cagr(snapshots)
    assert result["revenue_cagr_3y"] is not None
    assert abs(result["revenue_cagr_3y"] - 0.10) < 0.001
    assert abs(result["revenue_cagr_5y"] - 0.10) < 0.001
    assert abs(result["revenue_cagr_10y"] - 0.10) < 0.001


def test_compute_multi_year_cagr_partial_history():
    # Only 4 years — 3Y should compute, 5Y and 10Y should be None
    snapshots = [_snap(2022 + i, revenue=100 * (1.10**i)) for i in range(4)]
    result = compute_multi_year_cagr(snapshots)
    assert result["revenue_cagr_3y"] is not None
    assert result["revenue_cagr_5y"] is None
    assert result["revenue_cagr_10y"] is None


def test_compute_multi_year_cagr_empty_returns_empty():
    assert compute_multi_year_cagr([]) == {}


def test_cagr_summary_for_metric():
    snapshots = [_snap(2020 + i, net_income=50 * (1.15**i)) for i in range(6)]
    summary = cagr_summary_for_metric(snapshots, "net_income")
    assert "3y" in summary and "5y" in summary and "10y" in summary
    assert abs(summary["3y"] - 0.15) < 0.001
    assert summary["10y"] is None


def test_cagr_summary_for_unknown_metric_raises():
    import pytest

    with pytest.raises(ValueError):
        cagr_summary_for_metric([], "not_a_metric")
