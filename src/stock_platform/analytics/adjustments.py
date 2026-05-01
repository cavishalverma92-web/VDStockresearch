"""Corporate-action price adjustment.

Persisted ``price_daily`` rows are always stored **as-traded** so the ledger is
auditable. Indicators (RSI, MACD, EMAs, 52-week high, etc.) need a continuous
price series across split events, so this module produces a split-adjusted view
on demand.

Convention
----------
A 2-for-1 split is reported by yfinance as ``ratio=2.0``. On the ex-date and
after, prices are already post-split (smaller). On dates strictly before the
ex-date, raw prices are pre-split (larger); to make them comparable we divide
by the cumulative ratio of all later splits and multiply volume by the same
factor (so notional traded value stays continuous).

Dividends are intentionally **not** applied to the price series. Dividend
adjustment is only relevant for total-return backtests, not for the
trend-following indicators this platform currently uses.
"""

from __future__ import annotations

import pandas as pd

_PRICE_COLUMNS = ("open", "high", "low", "close")


def compute_split_adjustment_factors(
    dates: pd.DatetimeIndex | pd.Index,
    splits: pd.DataFrame | None,
) -> pd.Series:
    """Return per-date split factor ``f`` such that ``adj = raw / f``.

    The returned Series is indexed by the input ``dates`` and is always 1.0 when
    no splits exist. Splits with non-positive or missing ratios are ignored.
    """
    index = (
        dates if isinstance(dates, pd.DatetimeIndex) else pd.DatetimeIndex(pd.to_datetime(dates))
    )
    factors = pd.Series(1.0, index=index, dtype=float)
    if splits is None or splits.empty:
        return factors

    working = splits.copy()
    if "value" not in working.columns and "ratio" in working.columns:
        working = working.rename(columns={"ratio": "value"})
    if "ex_date" not in working.columns or "value" not in working.columns:
        return factors

    for _, row in working.sort_values("ex_date").iterrows():
        raw_ratio = row.get("value")
        if raw_ratio is None or (isinstance(raw_ratio, float) and pd.isna(raw_ratio)):
            continue
        try:
            ratio = float(raw_ratio)
        except (TypeError, ValueError):
            continue
        if ratio <= 0:
            continue

        ex_date = row.get("ex_date")
        try:
            ex_ts = pd.Timestamp(ex_date).tz_localize(None).normalize()
        except (TypeError, ValueError):
            continue

        mask = factors.index < ex_ts
        if mask.any():
            factors.loc[mask] *= ratio

    return factors


def apply_split_adjustment(
    ohlcv: pd.DataFrame,
    splits: pd.DataFrame | None,
) -> pd.DataFrame:
    """Return a copy of ``ohlcv`` with split-adjusted OHLC and volume.

    The output adds an ``adjustment_factor`` column with the per-row factor so
    callers can audit the transform. ``adj_close`` (when present) is overwritten
    with the split-adjusted close so downstream code does not accidentally mix
    yfinance's split+dividend adjusted column with our split-only one.
    """
    if ohlcv is None or ohlcv.empty:
        return ohlcv.copy() if ohlcv is not None else ohlcv

    factors = compute_split_adjustment_factors(ohlcv.index, splits)
    adjusted = ohlcv.copy()

    for column in _PRICE_COLUMNS:
        if column in adjusted.columns:
            adjusted[column] = adjusted[column] / factors

    if "volume" in adjusted.columns:
        adjusted["volume"] = adjusted["volume"] * factors

    if "adj_close" in adjusted.columns and "close" in adjusted.columns:
        adjusted["adj_close"] = adjusted["close"]

    adjusted["adjustment_factor"] = factors
    return adjusted
