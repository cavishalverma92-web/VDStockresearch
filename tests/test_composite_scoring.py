"""Tests for the Phase 4 composite scoring engine."""

from __future__ import annotations

import pandas as pd

from stock_platform.analytics.signals import SignalResult
from stock_platform.scoring import composite_scores_to_frame, score_stock


def _weights() -> dict:
    return {
        "buckets": {
            "fundamentals": 0.35,
            "technicals": 0.30,
            "flows": 0.17,
            "events_quality": 0.12,
            "macro_sector": 0.06,
        },
        "score_bands": {
            "strong_candidate": 80,
            "watchlist": 60,
            "neutral": 40,
        },
    }


def test_score_stock_returns_explainable_score() -> None:
    result = score_stock(
        symbol="test.ns",
        fundamentals={
            "status": "ok",
            "piotroski_f_score": 8,
            "altman_z_score": 3.2,
            "roe_pct_sector_rank": 90,
            "revenue_growth_pct_sector_rank": 80,
            "debt_to_equity_sector_rank": 70,
            "piotroski_f_score_sector_rank": 85,
        },
        technicals=pd.Series(
            {
                "close": 120.0,
                "ema_50": 110.0,
                "ema_200": 95.0,
                "rsi_14": 58.0,
                "relative_volume": 1.4,
            }
        ),
        signals=[SignalResult("Breakout With Volume", True, "test", "breakout")],
        delivery={"latest_pct": 55.0, "ma20_pct": 48.0, "trend": "rising"},
        result_volatility={"volatility_multiple": 1.0},
        weights=_weights(),
    )

    assert result.symbol == "TEST.NS"
    assert result.score >= 70
    assert result.sub_scores["fundamentals"] > 70
    assert result.sub_scores["technicals"] > 60
    assert result.reasons


def test_score_stock_records_missing_data_without_failing() -> None:
    result = score_stock(
        symbol="MISSING.NS",
        fundamentals=None,
        technicals=None,
        signals=[],
        delivery=None,
        result_volatility=None,
        weights=_weights(),
    )

    assert 0 <= result.score <= 100
    assert "fundamentals" in result.missing_data
    assert "delivery and institutional flow data" in result.missing_data
    assert result.risks


def test_composite_scores_to_frame_has_expected_columns() -> None:
    result = score_stock(
        symbol="TEST.NS",
        fundamentals=None,
        technicals=None,
        signals=[],
        weights=_weights(),
    )

    frame = composite_scores_to_frame([result])

    assert list(frame.columns) == [
        "symbol",
        "composite_score",
        "band",
        "fundamentals",
        "technicals",
        "flows",
        "events_quality",
        "macro_sector",
        "missing_data",
    ]
    assert frame.loc[0, "symbol"] == "TEST.NS"
