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


def test_score_stock_uses_financial_sector_rules() -> None:
    result = score_stock(
        symbol="HDFCBANK.NS",
        fundamentals={
            "symbol": "HDFCBANK.NS",
            "sector": "Financial Services",
            "industry": "Banks - Private Sector",
            "status": "ok",
            "piotroski_f_score": 6,
            "roe_pct": 16,
            "roa_pct": 1.8,
            "revenue_growth_pct": 18,
        },
        technicals=pd.Series({"close": 100.0, "ema_50": 95.0, "ema_200": 80.0, "rsi_14": 55.0}),
        signals=[],
        delivery=None,
        result_volatility=None,
        weights=_weights(),
    )

    assert result.sub_scores["fundamentals"] > 0
    assert "Using financial-sector fundamental scoring." in result.reasons
    assert not any("Altman" in risk for risk in result.risks)
    assert "manual banking metrics" in result.missing_data


def test_score_stock_uses_manual_banking_metrics_when_available() -> None:
    without_bank_metrics = score_stock(
        symbol="HDFCBANK.NS",
        fundamentals={
            "symbol": "HDFCBANK.NS",
            "sector": "Financial Services",
            "industry": "Banks - Private Sector",
            "status": "ok",
            "piotroski_f_score": 6,
            "roe_pct": 16,
            "roa_pct": 1.8,
            "revenue_growth_pct": 18,
        },
        technicals=pd.Series({"close": 100.0, "ema_50": 95.0, "ema_200": 80.0, "rsi_14": 55.0}),
        signals=[],
        delivery=None,
        result_volatility=None,
        weights=_weights(),
    )

    with_bank_metrics = score_stock(
        symbol="HDFCBANK.NS",
        fundamentals={
            "symbol": "HDFCBANK.NS",
            "sector": "Financial Services",
            "industry": "Banks - Private Sector",
            "status": "ok",
            "piotroski_f_score": 6,
            "roe_pct": 16,
            "roa_pct": 1.8,
            "revenue_growth_pct": 18,
        },
        banking_fundamentals={
            "nim_pct": 4.0,
            "gnpa_pct": 1.2,
            "nnpa_pct": 0.3,
            "casa_pct": 40.0,
            "credit_growth_pct": 12.0,
            "deposit_growth_pct": 11.0,
            "capital_adequacy_pct": 18.0,
            "source": "annual_report",
            "last_updated": "2026-04-28",
        },
        technicals=pd.Series({"close": 100.0, "ema_50": 95.0, "ema_200": 80.0, "rsi_14": 55.0}),
        signals=[],
        delivery=None,
        result_volatility=None,
        weights=_weights(),
    )

    assert (
        with_bank_metrics.sub_scores["fundamentals"]
        > without_bank_metrics.sub_scores["fundamentals"]
    )
    assert "manual banking metrics" not in with_bank_metrics.missing_data
    assert any("Banking NIM is healthy" in reason for reason in with_bank_metrics.reasons)
    assert any("Manual banking metrics source" in reason for reason in with_bank_metrics.reasons)


def test_score_stock_flags_weak_manual_banking_metrics() -> None:
    result = score_stock(
        symbol="HDFCBANK.NS",
        fundamentals={
            "symbol": "HDFCBANK.NS",
            "sector": "Financial Services",
            "industry": "Banks - Private Sector",
            "status": "ok",
            "piotroski_f_score": 6,
            "roe_pct": 16,
            "roa_pct": 1.8,
            "revenue_growth_pct": 18,
        },
        banking_fundamentals={
            "nim_pct": 2.4,
            "gnpa_pct": 6.0,
            "nnpa_pct": 2.5,
            "casa_pct": 25.0,
            "credit_growth_pct": 25.0,
            "deposit_growth_pct": 5.0,
            "capital_adequacy_pct": 12.5,
            "source": "annual_report",
        },
        technicals=pd.Series({"close": 100.0, "ema_50": 95.0, "ema_200": 80.0, "rsi_14": 55.0}),
        signals=[],
        delivery=None,
        result_volatility=None,
        weights=_weights(),
    )

    assert any("GNPA is elevated" in risk for risk in result.risks)
    assert any("NNPA is elevated" in risk for risk in result.risks)
    assert any("Credit growth is materially faster" in risk for risk in result.risks)


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
