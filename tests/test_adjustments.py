"""Tests for split-aware price adjustment."""

from __future__ import annotations

from datetime import date

import pandas as pd

from stock_platform.analytics.adjustments import (
    apply_split_adjustment,
    compute_split_adjustment_factors,
)


def _ohlcv() -> pd.DataFrame:
    idx = pd.to_datetime(
        [
            "2026-01-01",
            "2026-01-02",
            "2026-01-05",
            "2026-01-06",  # split happens on 2026-01-06
            "2026-01-07",
        ]
    )
    return pd.DataFrame(
        {
            "open": [200.0, 200.0, 200.0, 100.0, 100.0],
            "high": [202.0, 202.0, 202.0, 101.0, 101.0],
            "low": [198.0, 198.0, 198.0, 99.0, 99.0],
            "close": [200.0, 200.0, 200.0, 100.0, 100.0],
            "volume": [1000.0, 1000.0, 1000.0, 2000.0, 2000.0],
            "adj_close": [200.0, 200.0, 200.0, 100.0, 100.0],
        },
        index=idx,
    )


def test_no_splits_returns_unchanged_factors() -> None:
    frame = _ohlcv()
    factors = compute_split_adjustment_factors(frame.index, pd.DataFrame())
    assert (factors == 1.0).all()


def test_single_2_for_1_split_halves_pre_split_close() -> None:
    frame = _ohlcv()
    splits = pd.DataFrame(
        {
            "ex_date": [date(2026, 1, 6)],
            "value": [2.0],
        }
    )

    adjusted = apply_split_adjustment(frame, splits)

    # Pre-split rows (Jan 1, 2, 5) should be halved.
    assert list(round(adjusted["close"], 2))[:3] == [100.0, 100.0, 100.0]
    # Post-split rows (Jan 6, 7) should be unchanged.
    assert list(round(adjusted["close"], 2))[3:] == [100.0, 100.0]
    # Volume should double on pre-split rows, unchanged on post-split rows.
    assert list(adjusted["volume"])[:3] == [2000.0, 2000.0, 2000.0]
    assert list(adjusted["volume"])[3:] == [2000.0, 2000.0]
    # adjustment_factor column should be exposed.
    assert list(adjusted["adjustment_factor"]) == [2.0, 2.0, 2.0, 1.0, 1.0]


def test_apply_split_adjustment_overwrites_yfinance_adj_close() -> None:
    frame = _ohlcv()
    splits = pd.DataFrame({"ex_date": [date(2026, 1, 6)], "value": [2.0]})

    adjusted = apply_split_adjustment(frame, splits)

    # adj_close should now equal split-adjusted close, not yfinance's
    # split+dividend adjusted column (which we don't trust as-is for
    # technicals).
    assert (adjusted["adj_close"] == adjusted["close"]).all()


def test_multiple_splits_compose_multiplicatively() -> None:
    frame = _ohlcv()
    # Two splits: 2:1 on Jan 5, then 5:1 on Jan 7. Pre-Jan-5 factor = 10.
    splits = pd.DataFrame(
        {
            "ex_date": [date(2026, 1, 5), date(2026, 1, 7)],
            "value": [2.0, 5.0],
        }
    )

    adjusted = apply_split_adjustment(frame, splits)
    factors = list(adjusted["adjustment_factor"])

    # Jan 1, 2: before both splits → factor 2 * 5 = 10
    # Jan 5, 6: between the splits (>= Jan 5, < Jan 7) → factor 5
    # Jan 7: on/after the second split → factor 1
    assert factors == [10.0, 10.0, 5.0, 5.0, 1.0]


def test_splits_with_zero_or_missing_ratio_are_ignored() -> None:
    frame = _ohlcv()
    splits = pd.DataFrame(
        {
            "ex_date": [date(2026, 1, 5), date(2026, 1, 6)],
            "value": [0.0, None],
        }
    )

    adjusted = apply_split_adjustment(frame, splits)
    assert (adjusted["adjustment_factor"] == 1.0).all()


def test_apply_split_adjustment_handles_empty_frame() -> None:
    empty = pd.DataFrame()
    out = apply_split_adjustment(empty, pd.DataFrame({"ex_date": [], "value": []}))
    assert out.empty


def test_compute_split_adjustment_accepts_ratio_column_name() -> None:
    """yfinance's get_splits returns a 'ratio' column; the helper should accept it."""
    idx = pd.to_datetime(["2026-01-01", "2026-01-05", "2026-01-06"])
    splits = pd.DataFrame({"ex_date": [date(2026, 1, 5)], "ratio": [2.0]})

    factors = compute_split_adjustment_factors(idx, splits)

    assert list(factors) == [2.0, 1.0, 1.0]
