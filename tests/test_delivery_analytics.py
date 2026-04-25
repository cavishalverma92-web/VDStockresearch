"""Tests for delivery percentage analytics (Phase 3)."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from stock_platform.analytics.flows.delivery import (
    compute_delivery_analytics,
    delivery_stats,
)


def _make_delivery(n: int, base_pct: float = 55.0) -> pd.DataFrame:
    """Build a synthetic delivery DataFrame with *n* trading-day rows."""
    start = date(2024, 1, 2)
    dates = [start + timedelta(days=i) for i in range(n)]
    return pd.DataFrame(
        {
            "symbol": "TEST.NS",
            "trade_date": dates,
            "series": "EQ",
            "traded_qty": [1_000_000] * n,
            "deliverable_qty": [int(1_000_000 * base_pct / 100)] * n,
            "delivery_pct": [base_pct] * n,
            "turnover_lacs": [50_000.0] * n,
            "source": "nse_bhavcopy",
        }
    )


# ---------------------------------------------------------------------------
# compute_delivery_analytics
# ---------------------------------------------------------------------------


def test_empty_dataframe_returns_empty():
    result = compute_delivery_analytics(pd.DataFrame())
    assert result.empty


def test_none_returns_empty():
    result = compute_delivery_analytics(None)  # type: ignore[arg-type]
    assert result.empty


def test_ma20_added_for_sufficient_rows():
    df = _make_delivery(30)
    enriched = compute_delivery_analytics(df)
    assert "delivery_pct_ma20" in enriched.columns
    # With constant pct, MA should equal pct after warmup period
    last = enriched.iloc[-1]
    assert last["delivery_pct_ma20"] == pytest.approx(55.0, abs=0.5)


def test_zscore_added():
    df = _make_delivery(30)
    enriched = compute_delivery_analytics(df)
    assert "delivery_pct_zscore" in enriched.columns


def test_unusual_delivery_flag_set_on_spike():
    df = _make_delivery(30, base_pct=40.0)
    # Force last row to a very high delivery % (spike)
    df.at[df.index[-1], "delivery_pct"] = 80.0
    enriched = compute_delivery_analytics(df)
    # The spike row should be flagged; non-spike rows should not
    assert enriched.iloc[-1]["unusual_delivery"] is True or enriched.iloc[-1]["unusual_delivery"]


def test_no_unusual_flag_on_flat_data():
    df = _make_delivery(30, base_pct=55.0)
    enriched = compute_delivery_analytics(df)
    # All rows constant → no row should be flagged
    assert not enriched["unusual_delivery"].any()


def test_sorts_by_trade_date():
    df = _make_delivery(10)
    # Shuffle rows
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    enriched = compute_delivery_analytics(df)
    dates = enriched["trade_date"].tolist()
    assert dates == sorted(dates)


# ---------------------------------------------------------------------------
# delivery_stats
# ---------------------------------------------------------------------------


def test_stats_on_empty_returns_none_values():
    stats = delivery_stats(pd.DataFrame())
    assert stats["latest_pct"] is None
    assert stats["days_with_data"] == 0


def test_stats_latest_pct_is_last_row():
    df = _make_delivery(25, base_pct=60.0)
    df.at[df.index[-1], "delivery_pct"] = 72.0
    stats = delivery_stats(df)
    assert stats["latest_pct"] == pytest.approx(72.0, abs=0.5)


def test_stats_trend_rising():
    # 15 bars at 40 then 5 bars at 65 → iloc[-5:] = 65, iloc[-10:-5] = 40
    pcts = [40.0] * 15 + [65.0] * 5
    df = _make_delivery(20)
    df["delivery_pct"] = pcts
    stats = delivery_stats(df)
    assert stats["trend"] == "rising"


def test_stats_trend_falling():
    # 15 bars at 70 then 5 bars at 40 → iloc[-5:] = 40, iloc[-10:-5] = 70
    pcts = [70.0] * 15 + [40.0] * 5
    df = _make_delivery(20)
    df["delivery_pct"] = pcts
    stats = delivery_stats(df)
    assert stats["trend"] == "falling"


def test_stats_avg_90d_computed():
    df = _make_delivery(30, base_pct=55.0)
    stats = delivery_stats(df)
    assert stats["avg_90d_pct"] == pytest.approx(55.0, abs=1.0)
