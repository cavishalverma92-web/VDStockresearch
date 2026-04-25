"""Tests for result-date volatility analysis (Phase 3)."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from stock_platform.analytics.flows.result_volatility import compute_result_volatility


def _make_ohlcv(
    n: int = 200,
    base_close: float = 100.0,
    *,
    start: date = date(2023, 1, 2),
    spike_indices: list[int] | None = None,
    spike_multiplier: float = 3.0,
) -> pd.DataFrame:
    """Build a minimal OHLCV DataFrame with DatetimeIndex."""
    dates = pd.date_range(start=str(start), periods=n, freq="B")
    closes = [base_close] * n
    highs = [base_close * 1.01] * n
    lows = [base_close * 0.99] * n

    if spike_indices:
        for idx in spike_indices:
            highs[idx] = base_close * spike_multiplier
            lows[idx] = base_close * (1 / spike_multiplier)

    return pd.DataFrame(
        {"open": closes, "high": highs, "low": lows, "close": closes, "volume": [1_000_000] * n},
        index=dates,
    )


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_ohlcv_returns_none_multiple():
    result = compute_result_volatility(pd.DataFrame(), [date(2024, 1, 1)])
    assert result["volatility_multiple"] is None


def test_no_earnings_dates_returns_none_multiple():
    df = _make_ohlcv()
    result = compute_result_volatility(df, [])
    assert result["volatility_multiple"] is None


def test_earnings_outside_ohlcv_range_counts_zero():
    df = _make_ohlcv(n=50, start=date(2024, 1, 2))
    far_past = date(2020, 1, 1)
    result = compute_result_volatility(df, [far_past])
    assert result["events_found"] == 0


# ---------------------------------------------------------------------------
# Volatility multiple
# ---------------------------------------------------------------------------


def test_higher_volatility_around_earnings():
    """Spike on day 100 should yield a volatility_multiple > 1."""
    # Build 200-day OHLCV with a spike cluster around day 100
    spike_days = list(range(95, 106))
    df = _make_ohlcv(n=200, spike_indices=spike_days, spike_multiplier=4.0)
    earnings_date = df.index[100].date()

    result = compute_result_volatility(df, [earnings_date])
    assert result["events_found"] == 1
    assert result["volatility_multiple"] is not None
    assert result["volatility_multiple"] > 1.0


def test_flat_volatility_yields_multiple_near_one():
    """No spikes anywhere → event ATR ≈ baseline ATR → multiple ≈ 1."""
    df = _make_ohlcv(n=200)
    earnings_date = df.index[100].date()
    result = compute_result_volatility(df, [earnings_date])
    if result["volatility_multiple"] is not None:
        assert result["volatility_multiple"] == pytest.approx(1.0, abs=0.3)


def test_result_windows_populated():
    df = _make_ohlcv(n=200)
    e1 = df.index[50].date()
    e2 = df.index[150].date()
    result = compute_result_volatility(df, [e1, e2])
    assert len(result["result_windows"]) == 2


def test_atrs_are_positive():
    df = _make_ohlcv(n=200, spike_indices=[100], spike_multiplier=3.0)
    earnings_date = df.index[100].date()
    result = compute_result_volatility(df, [earnings_date])
    if result["event_atr"] is not None:
        assert result["event_atr"] > 0
    if result["baseline_atr"] is not None:
        assert result["baseline_atr"] > 0
