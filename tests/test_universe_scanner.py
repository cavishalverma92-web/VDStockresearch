"""Tests for the Phase 8 universe scanner."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from stock_platform.analytics.scanner import (
    ScanResult,
    list_available_universes,
    load_universe,
    scan_results_to_frame,
    scan_universe,
)

# ---------------------------------------------------------------------------
# Universe loading
# ---------------------------------------------------------------------------


def test_list_available_universes_returns_named_lists():
    names = list_available_universes()
    assert "nifty_50" in names
    assert "nifty_next_50" in names
    assert "all_nse_listed" in names


def test_load_universe_nifty_50():
    tickers = load_universe("nifty_50")
    assert len(tickers) >= 40  # always at least 40 in any reasonable Nifty 50
    assert "RELIANCE.NS" in tickers
    # All should be uppercase and stripped
    for t in tickers:
        assert t == t.upper().strip()


def test_load_universe_unknown_raises():
    with pytest.raises(KeyError):
        load_universe("nifty_9999")


def test_load_csv_universe_filters_eq_series(tmp_path):
    csv_path = tmp_path / "nse_equity_list.csv"
    csv_path.write_text(
        "SYMBOL,SERIES,NAME OF COMPANY\n"
        "ABC,EQ,ABC Limited\n"
        "DEF,BE,DEF Limited\n"
        "XYZ.NS,EQ,XYZ Limited\n",
        encoding="utf-8",
    )

    config = {
        "csv_universes": {
            "test_all": {
                "path": str(csv_path),
                "symbol_column": "SYMBOL",
                "series_column": "SERIES",
                "series_value": "EQ",
            }
        }
    }
    with patch(
        "stock_platform.analytics.scanner.universe_scanner.get_universes_config",
        return_value=config,
    ):
        symbols = load_universe("test_all")

    assert symbols == ["ABC.NS", "XYZ.NS"]


def test_load_csv_universe_missing_file_explains_next_step(tmp_path):
    config = {
        "csv_universes": {
            "missing_all": {
                "path": str(tmp_path / "missing.csv"),
                "symbol_column": "SYMBOL",
            }
        }
    }
    with (
        patch(
            "stock_platform.analytics.scanner.universe_scanner.get_universes_config",
            return_value=config,
        ),
        pytest.raises(FileNotFoundError, match="update_nse_universe"),
    ):
        load_universe("missing_all")


# ---------------------------------------------------------------------------
# scan_results_to_frame
# ---------------------------------------------------------------------------


def test_scan_results_to_frame_empty():
    df = scan_results_to_frame([])
    assert df.empty
    assert "symbol" in df.columns
    assert "composite_score" in df.columns


def test_scan_results_to_frame_with_rows():
    results = [
        ScanResult(
            symbol="A.NS",
            composite_score=80.0,
            band="strong",
            sub_scores={"fundamentals": 70, "technicals": 90},
            active_signal_count=2,
            active_signals=["sig_1", "sig_2"],
            last_close=100.0,
            rsi_14=55.0,
            ma_stack="bullish",
            data_quality_warnings=[],
        ),
        ScanResult(
            symbol="B.NS",
            composite_score=None,
            band=None,
            sub_scores={},
            active_signal_count=0,
            active_signals=[],
            last_close=None,
            rsi_14=None,
            ma_stack=None,
            data_quality_warnings=[],
            error="insufficient price history",
        ),
    ]
    df = scan_results_to_frame(results)
    assert len(df) == 2
    assert df.iloc[0]["composite_score"] == 80.0
    assert df.iloc[0]["fundamentals"] == 70
    assert df.iloc[0]["active_signals"] == "sig_1, sig_2"
    assert df.iloc[0]["data_quality_warnings"] == ""
    assert df.iloc[1]["error"] == "insufficient price history"


# ---------------------------------------------------------------------------
# scan_universe — mocked yfinance fetch + composite score
# ---------------------------------------------------------------------------


def _make_synthetic_ohlcv(days: int = 300, base: float = 100.0) -> pd.DataFrame:
    """Build a synthetic OHLCV frame long enough for 200 EMA & 52W metrics."""
    rng = np.random.default_rng(42)
    idx = pd.date_range(end=date.today(), periods=days, freq="B")
    drift = np.linspace(0, 20, days)
    noise = rng.normal(0, 1.5, days)
    close = base + drift + noise.cumsum() * 0.05
    high = close + rng.uniform(0.5, 2.0, days)
    low = close - rng.uniform(0.5, 2.0, days)
    return pd.DataFrame(
        {
            "open": close - 0.5,
            "high": high,
            "low": low,
            "close": close,
            "adj_close": close,
            "volume": rng.integers(100_000, 500_000, days),
        },
        index=idx,
    )


@patch("stock_platform.analytics.scanner.universe_scanner.YahooFinanceProvider")
def test_scan_universe_returns_one_row_per_symbol(mock_provider_cls):
    instance = mock_provider_cls.return_value
    instance.get_ohlcv.return_value = _make_synthetic_ohlcv()

    results = scan_universe(
        ["A.NS", "B.NS", "C.NS"],
        lookback_days=300,
        max_workers=2,
    )

    assert len(results) == 3
    assert {r.symbol for r in results} == {"A.NS", "B.NS", "C.NS"}
    # Synthetic data should produce composite scores
    assert all(r.composite_score is not None for r in results)


@patch("stock_platform.analytics.scanner.universe_scanner.YahooFinanceProvider")
def test_scan_universe_sorts_errors_to_bottom(mock_provider_cls):
    instance = mock_provider_cls.return_value

    def _fetch(symbol, start, end):
        if symbol == "BAD.NS":
            return pd.DataFrame()  # empty → error
        return _make_synthetic_ohlcv()

    instance.get_ohlcv.side_effect = _fetch

    results = scan_universe(["A.NS", "BAD.NS", "C.NS"], lookback_days=300, max_workers=2)
    # The error row should be at the bottom
    assert results[-1].symbol == "BAD.NS"
    assert results[-1].error is not None


@patch("stock_platform.analytics.scanner.universe_scanner.YahooFinanceProvider")
def test_scan_universe_progress_callback_invoked(mock_provider_cls):
    instance = mock_provider_cls.return_value
    instance.get_ohlcv.return_value = _make_synthetic_ohlcv()

    calls: list[tuple[int, int, str]] = []

    def _cb(done: int, total: int, sym: str) -> None:
        calls.append((done, total, sym))

    scan_universe(
        ["A.NS", "B.NS"],
        lookback_days=300,
        max_workers=1,
        progress_callback=_cb,
    )

    assert len(calls) == 2
    assert calls[-1][0] == 2  # done == total at the end
    assert calls[-1][1] == 2


@patch("stock_platform.analytics.scanner.universe_scanner.YahooFinanceProvider")
def test_scan_universe_handles_provider_exception(mock_provider_cls):
    instance = mock_provider_cls.return_value
    instance.get_ohlcv.side_effect = RuntimeError("yfinance offline")

    results = scan_universe(["A.NS"], lookback_days=300, max_workers=1)
    assert len(results) == 1
    assert results[0].composite_score is None
    assert results[0].error is not None


@patch("stock_platform.analytics.scanner.universe_scanner.YahooFinanceProvider")
def test_scan_universe_stops_on_data_quality_error(mock_provider_cls):
    instance = mock_provider_cls.return_value
    broken = _make_synthetic_ohlcv().drop(columns=["adj_close"])
    instance.get_ohlcv.return_value = broken

    results = scan_universe(["A.NS"], lookback_days=300, max_workers=1)

    assert len(results) == 1
    assert results[0].composite_score is None
    assert "data quality failure" in str(results[0].error)


@patch("stock_platform.analytics.scanner.universe_scanner.YahooFinanceProvider")
def test_scan_universe_clamps_invalid_worker_count(mock_provider_cls):
    instance = mock_provider_cls.return_value
    instance.get_ohlcv.return_value = _make_synthetic_ohlcv()

    results = scan_universe(["A.NS"], lookback_days=300, max_workers=0)

    assert len(results) == 1
    assert results[0].composite_score is not None


def test_scan_universe_empty_input():
    assert scan_universe([], lookback_days=100) == []


@patch("stock_platform.analytics.scanner.universe_scanner.YahooFinanceProvider")
def test_scan_universe_loads_named_universe(mock_provider_cls):
    """When given a string, the scanner should look up the universe in config."""
    instance = mock_provider_cls.return_value
    instance.get_ohlcv.return_value = _make_synthetic_ohlcv()

    # Use a small slice from the real config to avoid scanning all 50 stocks
    with patch("stock_platform.analytics.scanner.universe_scanner.load_universe") as mock_load:
        mock_load.return_value = ["X.NS", "Y.NS"]
        results = scan_universe("nifty_50", lookback_days=300, max_workers=1)

    assert len(results) == 2
    mock_load.assert_called_once_with("nifty_50")
