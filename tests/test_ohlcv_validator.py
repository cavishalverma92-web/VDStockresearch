"""Smoke tests for the OHLCV validator.

Run with:
    pytest -v
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from stock_platform.data.validators import OHLCVValidationError, validate_ohlcv


def _valid_df(n: int = 30) -> pd.DataFrame:
    idx = pd.date_range(end=datetime.utcnow().date() - timedelta(days=1), periods=n, freq="B")
    df = pd.DataFrame(
        {
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "adj_close": 100.5,
            "volume": 10_000,
        },
        index=idx,
    )
    df.index.name = "date"
    return df


def test_valid_frame_passes() -> None:
    report = validate_ohlcv(_valid_df(), "TEST", raise_on_error=False)
    assert report.ok
    assert report.errors == []


def test_empty_frame_fails() -> None:
    empty = pd.DataFrame(columns=["open", "high", "low", "close", "adj_close", "volume"])
    with pytest.raises(OHLCVValidationError):
        validate_ohlcv(empty, "TEST", raise_on_error=True)


def test_negative_close_fails() -> None:
    df = _valid_df()
    df.iloc[5, df.columns.get_loc("close")] = -5.0
    report = validate_ohlcv(df, "TEST", raise_on_error=False)
    assert not report.ok
    assert any("negative_values_in_close" in e for e in report.errors)


def test_duplicate_index_fails() -> None:
    df = _valid_df()
    df = pd.concat([df, df.iloc[[0]]])
    report = validate_ohlcv(df, "TEST", raise_on_error=False)
    assert any("duplicate_index" in e for e in report.errors)


def test_suspicious_jump_warns() -> None:
    df = _valid_df()
    df.iloc[10, df.columns.get_loc("close")] = 50.0  # ~50% drop
    report = validate_ohlcv(df, "TEST", raise_on_error=False)
    # Jump is a warning, not an error
    assert report.ok
    assert any("suspicious_price_moves" in w for w in report.warnings)
