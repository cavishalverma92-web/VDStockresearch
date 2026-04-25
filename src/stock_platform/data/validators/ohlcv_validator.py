"""
OHLCV data quality validator.

Implements the Phase 0 version of §5.1 "Data Quality and Validation Layer":
- Missing values
- Duplicate date indices
- Negative prices
- Zero volume (warning only — may be valid for holidays / suspensions)
- Single-day price moves > threshold (likely unadjusted corporate action)
- Stale data

Rule from the master prompt:
    "If critical data is broken, stop the pipeline. Do not silently continue."

`validate_ohlcv` therefore raises `OHLCVValidationError` on critical failures
and logs warnings for non-critical issues.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pandas as pd

from stock_platform.config import get_thresholds_config
from stock_platform.utils.logging import get_dq_logger

log = get_dq_logger(__name__)


class OHLCVValidationError(Exception):
    """Raised when OHLCV data fails critical validation checks."""


@dataclass
class ValidationReport:
    symbol: str
    rows: int
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def validate_ohlcv(
    df: pd.DataFrame, symbol: str, *, raise_on_error: bool = True
) -> ValidationReport:
    """
    Validate an OHLCV DataFrame.

    Expected schema (columns): open, high, low, close, adj_close, volume
    Expected index: monotonic DatetimeIndex named 'date'.
    """
    report = ValidationReport(symbol=symbol, rows=len(df))
    t = get_thresholds_config().get("data_quality", {})

    # ---- Emptiness ----
    if df.empty:
        report.errors.append("dataframe_empty")
        return _finalize(report, raise_on_error)

    # ---- Schema ----
    required = {"open", "high", "low", "close", "adj_close", "volume"}
    missing = required - set(df.columns)
    if missing:
        report.errors.append(f"missing_columns: {sorted(missing)}")

    # ---- Index ----
    if not isinstance(df.index, pd.DatetimeIndex):
        report.errors.append("index_not_datetime")
    elif not df.index.is_monotonic_increasing:
        report.warnings.append("index_not_sorted")

    if df.index.has_duplicates:
        dups = df.index[df.index.duplicated()].tolist()
        report.errors.append(f"duplicate_index: {len(dups)} duplicates")

    # If schema/index already broken, don't bother with numeric checks.
    if report.errors:
        return _finalize(report, raise_on_error)

    # ---- Missing values ----
    missing_close_pct = df["close"].isna().mean() * 100
    max_missing = float(t.get("max_missing_close_pct", 5.0))
    if missing_close_pct > max_missing:
        report.errors.append(f"too_many_missing_close: {missing_close_pct:.1f}% > {max_missing}%")

    # ---- Negatives ----
    for col in ("open", "high", "low", "close", "adj_close"):
        if (df[col].dropna() < 0).any():
            report.errors.append(f"negative_values_in_{col}")

    # ---- Zero volume (warning only) ----
    if (df["volume"].fillna(0) == 0).any():
        zero_count = int((df["volume"].fillna(0) == 0).sum())
        report.warnings.append(f"zero_volume_rows: {zero_count}")

    # ---- Abnormal single-day moves ----
    max_move = float(t.get("max_single_day_price_move_pct", 40.0))
    pct_change = df["close"].pct_change().abs() * 100
    big_moves = pct_change[pct_change > max_move]
    if not big_moves.empty:
        report.warnings.append(
            f"suspicious_price_moves: {len(big_moves)} day(s) > {max_move}% "
            f"(possible unadjusted corporate action)"
        )

    # ---- Stale data ----
    stale_days = int(t.get("stale_price_days", 5))
    last_date = df.index.max()
    if last_date is not None and isinstance(last_date, pd.Timestamp):
        age = (pd.Timestamp(datetime.now(UTC).date()) - last_date.normalize()).days
        if age > stale_days:
            report.warnings.append(f"stale_data: last bar is {age} day(s) old")

    return _finalize(report, raise_on_error)


def _finalize(report: ValidationReport, raise_on_error: bool) -> ValidationReport:
    for w in report.warnings:
        log.warning("DQ warning [{}]: {}", report.symbol, w)
    for e in report.errors:
        log.error("DQ error   [{}]: {}", report.symbol, e)

    if not report.ok and raise_on_error:
        raise OHLCVValidationError(f"OHLCV validation failed for {report.symbol}: {report.errors}")
    return report


# Convenience: quick CLI sanity check
if __name__ == "__main__":  # pragma: no cover
    idx = pd.date_range(end=datetime.utcnow().date() - timedelta(days=1), periods=10, freq="B")
    sample = pd.DataFrame(
        {
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "adj_close": 100.5,
            "volume": 1_000,
        },
        index=idx,
    )
    sample.index.name = "date"
    r = validate_ohlcv(sample, "SAMPLE", raise_on_error=False)
    print(r)
