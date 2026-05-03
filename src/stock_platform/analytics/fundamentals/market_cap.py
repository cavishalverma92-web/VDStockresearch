"""Reconstruct point-in-time market cap and enterprise value.

yfinance only exposes the *current* market cap, which we attach to the latest
fiscal year. Historical valuation ratios (P/E, P/B, EV/EBITDA) need MC at the
period end, otherwise backtests can't compute them. This module fills the gap
by multiplying ``shares_outstanding`` for each row by the close price on (or
just before) the row's ``period_end`` date.

The function is pure: it takes an annual fundamentals frame and a price
history frame and returns a copy with ``market_cap`` / ``enterprise_value``
populated where they were missing. Rows with no ``period_end`` or no
``shares_outstanding`` are left as-is.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd


def reconstruct_historical_market_cap(
    annual: pd.DataFrame,
    price_history: pd.DataFrame,
    *,
    overwrite: bool = False,
    max_lookback_days: int = 14,
) -> pd.DataFrame:
    """Fill missing ``market_cap`` and ``enterprise_value`` from price × shares.

    Args:
        annual: fundamentals frame (one row per fiscal year). Must have
            ``period_end`` and ``shares_outstanding``. ``net_debt`` is used
            for EV when present.
        price_history: OHLCV frame indexed by date with at least a ``close``
            column. Typically the output of ``fetch_price_daily``.
        overwrite: when True, replace existing non-null ``market_cap`` /
            ``enterprise_value`` values too. Default is to only fill missing.
        max_lookback_days: when the exact ``period_end`` is not a trading
            day, look up the close from the nearest preceding trading day
            within this window. Beyond it, the row is left as-is.

    Returns:
        A new DataFrame (input is not mutated).
    """
    if annual is None or annual.empty:
        return annual.copy() if annual is not None else pd.DataFrame()

    out = annual.copy()
    if "market_cap" not in out.columns:
        out["market_cap"] = pd.NA
    if "enterprise_value" not in out.columns:
        out["enterprise_value"] = pd.NA

    if (
        price_history is None
        or price_history.empty
        or "close" not in price_history.columns
        or "period_end" not in out.columns
        or "shares_outstanding" not in out.columns
    ):
        return out

    closes_by_date = _close_lookup(price_history)
    if not closes_by_date:
        return out

    sorted_dates = sorted(closes_by_date.keys())

    for idx in out.index:
        period_end = _as_date(out.at[idx, "period_end"])
        shares = _as_float(out.at[idx, "shares_outstanding"])
        if period_end is None or shares is None or shares <= 0:
            continue

        existing_mc = _as_float(out.at[idx, "market_cap"])
        if existing_mc is not None and not overwrite:
            continue

        close = _close_on_or_before(period_end, closes_by_date, sorted_dates, max_lookback_days)
        if close is None:
            continue

        market_cap = shares * close
        out.at[idx, "market_cap"] = market_cap

        net_debt = _as_float(out.at[idx, "net_debt"]) if "net_debt" in out.columns else None
        existing_ev = _as_float(out.at[idx, "enterprise_value"])
        if existing_ev is None or overwrite:
            out.at[idx, "enterprise_value"] = (
                market_cap + net_debt if net_debt is not None else market_cap
            )

    return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _close_lookup(price_history: pd.DataFrame) -> dict[date, float]:
    """Build a {date → close} dict from an OHLCV frame."""
    out: dict[date, float] = {}
    index = price_history.index
    closes = price_history["close"]
    for ts, value in zip(index, closes, strict=False):
        d = _as_date(ts)
        if d is None:
            continue
        try:
            out[d] = float(value)
        except (TypeError, ValueError):
            continue
    return out


def _close_on_or_before(
    target: date,
    closes_by_date: dict[date, float],
    sorted_dates: list[date],
    max_lookback_days: int,
) -> float | None:
    """Find the close on ``target`` or the nearest preceding trading day."""
    if target in closes_by_date:
        return closes_by_date[target]
    earliest = target - timedelta(days=max_lookback_days)
    # Scan backward — sorted_dates is ascending; bisect would be faster but
    # the per-symbol calls here are <50 rows so a simple loop is clearer.
    for d in reversed(sorted_dates):
        if d > target:
            continue
        if d < earliest:
            return None
        return closes_by_date[d]
    return None


def _as_date(value: object) -> date | None:
    if value is None:
        return None
    # pd.Timestamp and datetime.datetime both subclass datetime.date but
    # represent a moment in time; coerce to a pure date.
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        ts = pd.to_datetime(value, errors="coerce")
    except (TypeError, ValueError):
        return None
    if ts is pd.NaT or ts is None or pd.isna(ts):
        return None
    return ts.date()


def _as_float(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if value is pd.NA:
        return None
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return None if pd.isna(result) else result
