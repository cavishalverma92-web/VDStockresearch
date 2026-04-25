"""Corporate actions provider — dividends, splits, and earnings dates via yfinance.

Returns normalised DataFrames with consistent column sets so the UI and
analytics layers never need to touch the raw yfinance API directly.

All methods return an empty DataFrame (or None for the upcoming earnings
helper) when yfinance has no data or raises an exception.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import yfinance as yf

from stock_platform.utils.logging import get_logger

log = get_logger(__name__)

_DIVIDEND_COLUMNS = ["symbol", "ex_date", "amount", "source"]
_SPLIT_COLUMNS = ["symbol", "ex_date", "ratio", "source"]
_EARNINGS_COLUMNS = ["symbol", "earnings_date", "eps_estimate", "source"]


def get_dividends(symbol: str) -> pd.DataFrame:
    """Return dividend history for *symbol*.

    Columns: symbol, ex_date (date), amount (float), source.
    Sorted by ex_date ascending.
    """
    try:
        ticker = yf.Ticker(symbol)
        divs = ticker.dividends
    except Exception as exc:  # noqa: BLE001
        log.warning("yfinance dividends failed for {}: {}", symbol, exc)
        return pd.DataFrame(columns=_DIVIDEND_COLUMNS)

    if divs is None or divs.empty:
        return pd.DataFrame(columns=_DIVIDEND_COLUMNS)

    divs = divs.reset_index()
    divs.columns = [c.lower() for c in divs.columns]
    date_col = next((c for c in divs.columns if "date" in c), None)
    val_col = next((c for c in divs.columns if c not in (date_col, "symbol")), None)

    if date_col is None or val_col is None:
        return pd.DataFrame(columns=_DIVIDEND_COLUMNS)

    frame = pd.DataFrame(
        {
            "symbol": symbol.upper(),
            "ex_date": pd.to_datetime(divs[date_col], errors="coerce").dt.date,
            "amount": pd.to_numeric(divs[val_col], errors="coerce"),
            "source": "yfinance",
        }
    )
    return frame.dropna(subset=["ex_date"]).sort_values("ex_date").reset_index(drop=True)


def get_splits(symbol: str) -> pd.DataFrame:
    """Return stock split history for *symbol*.

    Columns: symbol, ex_date (date), ratio (float), source.
    """
    try:
        ticker = yf.Ticker(symbol)
        splits = ticker.splits
    except Exception as exc:  # noqa: BLE001
        log.warning("yfinance splits failed for {}: {}", symbol, exc)
        return pd.DataFrame(columns=_SPLIT_COLUMNS)

    if splits is None or splits.empty:
        return pd.DataFrame(columns=_SPLIT_COLUMNS)

    splits = splits.reset_index()
    splits.columns = [c.lower() for c in splits.columns]
    date_col = next((c for c in splits.columns if "date" in c), None)
    val_col = next((c for c in splits.columns if c not in (date_col, "symbol")), None)

    if date_col is None or val_col is None:
        return pd.DataFrame(columns=_SPLIT_COLUMNS)

    frame = pd.DataFrame(
        {
            "symbol": symbol.upper(),
            "ex_date": pd.to_datetime(splits[date_col], errors="coerce").dt.date,
            "ratio": pd.to_numeric(splits[val_col], errors="coerce"),
            "source": "yfinance",
        }
    )
    return frame.dropna(subset=["ex_date"]).sort_values("ex_date").reset_index(drop=True)


def get_upcoming_earnings(symbol: str) -> dict | None:
    """Return the next earnings date and EPS estimate if available.

    Returns a dict with keys: earnings_date (date), eps_estimate (float|None),
    or None when yfinance has no calendar data.
    """
    try:
        ticker = yf.Ticker(symbol)
        cal = ticker.calendar
    except Exception as exc:  # noqa: BLE001
        log.warning("yfinance calendar failed for {}: {}", symbol, exc)
        return None

    if cal is None:
        return None

    # yfinance ≥0.2.x returns a dict; older versions returned a DataFrame
    if isinstance(cal, dict):
        raw_date = cal.get("Earnings Date")
        eps = cal.get("EPS Estimate")
        if raw_date is None:
            return None
        if isinstance(raw_date, list):
            raw_date = raw_date[0] if raw_date else None
        if raw_date is None:
            return None
        try:
            earnings_dt = pd.Timestamp(raw_date).date()
        except Exception:  # noqa: BLE001
            return None
        return {
            "earnings_date": earnings_dt,
            "eps_estimate": float(eps) if eps is not None else None,
        }

    # DataFrame form (older yfinance)
    if isinstance(cal, pd.DataFrame) and not cal.empty:
        try:
            earnings_dt = pd.Timestamp(cal.columns[0]).date()
            eps = cal.loc["EPS Estimate", cal.columns[0]] if "EPS Estimate" in cal.index else None
            return {
                "earnings_date": earnings_dt,
                "eps_estimate": float(eps) if eps is not None else None,
            }
        except Exception:  # noqa: BLE001
            return None

    return None


def get_earnings_history(symbol: str) -> pd.DataFrame:
    """Return past earnings dates from yfinance income statement fiscal year ends.

    Columns: symbol, earnings_date (date), eps_estimate (None), source.
    Used to populate result-volatility analysis windows.
    """
    try:
        ticker = yf.Ticker(symbol)
        income = ticker.income_stmt
    except Exception as exc:  # noqa: BLE001
        log.warning("yfinance income_stmt failed for {}: {}", symbol, exc)
        return pd.DataFrame(columns=_EARNINGS_COLUMNS)

    if income is None or income.empty:
        return pd.DataFrame(columns=_EARNINGS_COLUMNS)

    dates = [pd.Timestamp(c).date() for c in income.columns if not pd.isna(pd.Timestamp(c))]
    if not dates:
        return pd.DataFrame(columns=_EARNINGS_COLUMNS)

    return pd.DataFrame(
        {
            "symbol": symbol.upper(),
            "earnings_date": sorted(dates),
            "eps_estimate": None,
            "source": "yfinance_income_stmt",
        }
    )


def dividends_to_cr(amount_inr: float | None, shares: float | None = None) -> float | None:
    """Helper: convert per-share dividend amount to crores given share count."""
    if amount_inr is None or shares is None:
        return None
    return round(amount_inr * shares / 1e7, 2)


def days_to_next_earnings(upcoming: dict | None) -> int | None:
    """Return calendar days until the next earnings event, or None."""
    if upcoming is None:
        return None
    delta = (upcoming["earnings_date"] - date.today()).days
    return delta if delta >= 0 else None
