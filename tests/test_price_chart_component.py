from __future__ import annotations

import pandas as pd

from stock_platform.ui.components.price_chart import build_price_chart


class _Signal:
    name = "Breakout"
    entry_zone_low = 101
    entry_zone_high = 104
    stop_loss = 96
    target_price = 116


def _sample_price_frame() -> pd.DataFrame:
    dates = pd.date_range("2026-04-01", periods=5, freq="D")
    return pd.DataFrame(
        {
            "open": [100, 102, 101, 104, 106],
            "high": [103, 104, 105, 108, 109],
            "low": [99, 100, 100, 103, 105],
            "close": [102, 101, 104, 106, 108],
            "volume": [1000, 1300, 1250, 1600, 1500],
        },
        index=dates,
    )


def _sample_technical_frame() -> pd.DataFrame:
    dates = pd.date_range("2026-04-01", periods=5, freq="D")
    return pd.DataFrame(
        {
            "ema_20": [101, 101.5, 102, 103, 104],
            "ema_50": [100, 100.5, 101, 102, 103],
            "ema_200": [98, 98.5, 99, 99.5, 100],
            "bb_upper": [110, 111, 112, 113, 114],
            "bb_lower": [90, 91, 92, 93, 94],
            "high_52w": [120, 120, 120, 120, 120],
            "low_52w": [80, 80, 80, 80, 80],
        },
        index=dates,
    )


def test_price_chart_defaults_are_readable() -> None:
    fig = build_price_chart(
        _sample_price_frame(),
        _sample_technical_frame(),
        symbol="RELIANCE.NS",
        source_label="Zerodha Kite",
    )

    trace_names = [trace.name for trace in fig.data]
    assert "RELIANCE.NS" in trace_names
    assert "50 EMA" in trace_names
    assert "Volume" in trace_names
    assert "20 EMA" not in trace_names
    assert "200 EMA" not in trace_names
    assert "Bollinger upper" not in trace_names
    assert fig.layout.xaxis.rangeslider.visible is False
    assert "Zerodha Kite | last bar 2026-04-05 | 1d" in fig.layout.title.text


def test_price_chart_adds_optional_context_only_when_requested() -> None:
    fig = build_price_chart(
        _sample_price_frame(),
        _sample_technical_frame(),
        symbol="RELIANCE.NS",
        source_label="yfinance fallback",
        show_20_ema=True,
        show_200_ema=True,
        show_bollinger=True,
        show_52w=True,
        active_signals=[_Signal()],
        event_markers=[{"date": "2026-04-03", "label": "Result"}],
        freshness_note="fallback used",
    )

    trace_names = [trace.name for trace in fig.data]
    assert "20 EMA" in trace_names
    assert "200 EMA" in trace_names
    assert "Bollinger upper" in trace_names
    assert "52W high" in trace_names
    assert len(fig.layout.shapes) >= 4
    assert "fallback used" in fig.layout.title.text
