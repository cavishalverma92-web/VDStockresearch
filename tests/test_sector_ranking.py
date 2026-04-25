"""Tests for sector-relative percentile ranking."""

from __future__ import annotations

import pandas as pd
import pytest

from stock_platform.analytics.fundamentals.sector_ranking import (
    compute_sector_percentile_ranks,
    sector_rank_summary,
)


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Basic column creation
# ---------------------------------------------------------------------------


def test_adds_rank_columns_for_high_is_better_metric():
    df = _frame(
        [
            {"symbol": "A", "sector": "IT", "roe_pct": 30.0},
            {"symbol": "B", "sector": "IT", "roe_pct": 20.0},
        ]
    )
    result = compute_sector_percentile_ranks(df)
    assert "roe_pct_sector_rank" in result.columns


def test_adds_rank_columns_for_low_is_better_metric():
    df = _frame(
        [
            {"symbol": "A", "sector": "FIN", "debt_to_equity": 0.2},
            {"symbol": "B", "sector": "FIN", "debt_to_equity": 1.0},
        ]
    )
    result = compute_sector_percentile_ranks(df)
    assert "debt_to_equity_sector_rank" in result.columns


# ---------------------------------------------------------------------------
# Ordering checks
# ---------------------------------------------------------------------------


def test_higher_roe_gets_higher_sector_rank():
    df = _frame(
        [
            {"symbol": "A", "sector": "IT", "roe_pct": 30.0},
            {"symbol": "B", "sector": "IT", "roe_pct": 20.0},
            {"symbol": "C", "sector": "IT", "roe_pct": 10.0},
        ]
    )
    result = compute_sector_percentile_ranks(df).set_index("symbol")
    ranks = result["roe_pct_sector_rank"]
    assert ranks["A"] > ranks["B"] > ranks["C"]


def test_lower_debt_to_equity_gets_higher_sector_rank():
    df = _frame(
        [
            {"symbol": "A", "sector": "FIN", "debt_to_equity": 0.1},
            {"symbol": "B", "sector": "FIN", "debt_to_equity": 0.5},
            {"symbol": "C", "sector": "FIN", "debt_to_equity": 1.5},
        ]
    )
    result = compute_sector_percentile_ranks(df).set_index("symbol")
    ranks = result["debt_to_equity_sector_rank"]
    assert ranks["A"] > ranks["B"] > ranks["C"]


# ---------------------------------------------------------------------------
# Missing data
# ---------------------------------------------------------------------------


def test_missing_metric_gives_nan_rank():
    df = _frame(
        [
            {"symbol": "A", "sector": "IT", "roe_pct": 30.0},
            {"symbol": "B", "sector": "IT", "roe_pct": None},
        ]
    )
    result = compute_sector_percentile_ranks(df).set_index("symbol")
    assert pd.isna(result.loc["B", "roe_pct_sector_rank"])


# ---------------------------------------------------------------------------
# Group isolation
# ---------------------------------------------------------------------------


def test_different_sectors_are_ranked_independently():
    df = _frame(
        [
            {"symbol": "A", "sector": "IT", "roe_pct": 5.0},
            {"symbol": "B", "sector": "FIN", "roe_pct": 40.0},
        ]
    )
    result = compute_sector_percentile_ranks(df).set_index("symbol")
    # Each is the sole member of its sector → rank = 100 for both.
    assert result.loc["A", "roe_pct_sector_rank"] == pytest.approx(100.0)
    assert result.loc["B", "roe_pct_sector_rank"] == pytest.approx(100.0)


def test_industry_ranks_use_industry_column():
    df = _frame(
        [
            {"symbol": "A", "sector": "IT", "industry": "IT Services", "roe_pct": 35.0},
            {"symbol": "B", "sector": "IT", "industry": "IT Services", "roe_pct": 20.0},
            {"symbol": "C", "sector": "IT", "industry": "Software", "roe_pct": 10.0},
        ]
    )
    result = compute_sector_percentile_ranks(df).set_index("symbol")
    # A and B are in the same industry; C is alone.
    assert result.loc["A", "roe_pct_industry_rank"] > result.loc["B", "roe_pct_industry_rank"]
    assert result.loc["C", "roe_pct_industry_rank"] == pytest.approx(100.0)


def test_market_cap_bucket_ranks_are_independent_of_sector():
    df = _frame(
        [
            {"symbol": "A", "market_cap_bucket": "Large Cap", "roe_pct": 35.0},
            {"symbol": "B", "market_cap_bucket": "Large Cap", "roe_pct": 20.0},
            {"symbol": "C", "market_cap_bucket": "Mid Cap", "roe_pct": 10.0},
        ]
    )
    result = compute_sector_percentile_ranks(df).set_index("symbol")
    assert result.loc["A", "roe_pct_mkt_cap_rank"] > result.loc["B", "roe_pct_mkt_cap_rank"]
    assert result.loc["C", "roe_pct_mkt_cap_rank"] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_dataframe_returns_empty():
    result = compute_sector_percentile_ranks(pd.DataFrame())
    assert result.empty


def test_missing_group_column_skipped_gracefully():
    df = _frame([{"symbol": "A", "roe_pct": 30.0}])
    result = compute_sector_percentile_ranks(df)
    # No sector column → no sector rank column added, no error.
    assert "roe_pct_sector_rank" not in result.columns


def test_sector_rank_summary_returns_dict():
    df = _frame(
        [
            {"symbol": "A", "sector": "IT", "roe_pct": 35.0},
            {"symbol": "B", "sector": "IT", "roe_pct": 20.0},
        ]
    )
    ranked = compute_sector_percentile_ranks(df)
    summary = sector_rank_summary(ranked, "A")
    assert isinstance(summary, dict)
    assert "roe_pct_sector_rank" in summary
    assert summary["roe_pct_sector_rank"] == pytest.approx(100.0)
