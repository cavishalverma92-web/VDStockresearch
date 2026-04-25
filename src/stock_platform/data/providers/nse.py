"""NSE data provider — delivery % via bhavcopy archives, bulk/block deals.

Delivery data source:
  NSE publishes daily equity bhavcopy CSV files at:
  https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_DDMMYYYY.csv

  These files are publicly downloadable — no session cookie required.
  Each file is ~370 KB and contains DELIV_QTY + DELIV_PER columns.

  This provider downloads the last N trading days in parallel (5 threads)
  and returns a merged DataFrame for the requested symbol.

Bulk / block deals:
  NSE's deal API (www.nseindia.com) requires browser session cookies that
  httpx cannot obtain because the main site is behind Akamai bot-protection.
  The fetch functions return an empty DataFrame with a warning when blocked,
  so the UI degrades gracefully.

All public methods return an empty DataFrame on failure; they never raise.
Review NSE's Terms of Use before any redistribution.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from io import StringIO

import httpx
import pandas as pd

from stock_platform.utils.logging import get_logger

log = get_logger(__name__)

_BASE = "https://www.nseindia.com"
_ARCHIVE = "https://nsearchives.nseindia.com"

# Archive CDN does not need session cookies — plain Referer + UA is enough.
_ARCHIVE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/csv,*/*",
    "Referer": "https://www.nseindia.com/",
}

# Main-site headers (used for bulk/block deal attempts — may 403).
_MAIN_HEADERS = {**_ARCHIVE_HEADERS, "Accept": "application/json, */*"}

_DELIVERY_COLUMNS = [
    "symbol",
    "trade_date",
    "series",
    "traded_qty",
    "deliverable_qty",
    "delivery_pct",
    "turnover_lacs",
    "source",
]

_DEAL_COLUMNS = [
    "symbol",
    "deal_date",
    "client_name",
    "buy_sell",
    "quantity",
    "price",
    "deal_type",
    "source",
]

_MAX_PARALLEL = 5  # concurrent bhavcopy downloads
_DEFAULT_DAYS = 30  # calendar days to look back for delivery data


def _nse_symbol(yahoo_symbol: str) -> str:
    """Strip exchange suffix: 'RELIANCE.NS' → 'RELIANCE'."""
    return yahoo_symbol.upper().split(".")[0]


def _candidate_trading_dates(n_calendar_days: int) -> list[date]:
    """Return weekday dates from yesterday backwards for n_calendar_days."""
    today = date.today()
    dates: list[date] = []
    for offset in range(1, n_calendar_days + 30):  # buffer for holidays
        d = today - timedelta(days=offset)
        if d.weekday() < 5:  # Mon-Fri
            dates.append(d)
        if len(dates) >= n_calendar_days:
            break
    return dates


def _fetch_one_bhavcopy(trade_date: date) -> tuple[date, pd.DataFrame]:
    """Download one bhavcopy CSV. Returns (date, DataFrame) or (date, empty)."""
    date_str = trade_date.strftime("%d%m%Y")
    url = f"{_ARCHIVE}/products/content/sec_bhavdata_full_{date_str}.csv"
    try:
        with httpx.Client(headers=_ARCHIVE_HEADERS, timeout=15, follow_redirects=True) as client:
            resp = client.get(url)
        if resp.status_code != 200:
            return trade_date, pd.DataFrame()
        df = pd.read_csv(StringIO(resp.text))
        df.columns = [c.strip() for c in df.columns]
        return trade_date, df
    except Exception as exc:  # noqa: BLE001
        log.debug("bhavcopy fetch skipped for {}: {}", trade_date, exc)
        return trade_date, pd.DataFrame()


# ---------------------------------------------------------------------------
# Delivery percentage (NSE bhavcopy archive — works without browser session)
# ---------------------------------------------------------------------------


def fetch_delivery_data(
    symbol: str,
    n_days: int = _DEFAULT_DAYS,
) -> pd.DataFrame:
    """Return delivery % history for *symbol* from NSE bhavcopy archives.

    Downloads the last *n_days* trading days in parallel (up to 5 threads).
    Weekends and market holidays are automatically skipped (the archive returns
    404 for non-trading days).

    Columns: symbol, trade_date, series, traded_qty, deliverable_qty,
             delivery_pct, turnover_lacs, source.

    Returns an empty DataFrame if the archive is unreachable or the symbol
    does not have EQ-series data.
    """
    nse_sym = _nse_symbol(symbol)
    candidates = _candidate_trading_dates(n_days)
    rows: list[dict] = []

    with ThreadPoolExecutor(max_workers=_MAX_PARALLEL) as pool:
        futures = {pool.submit(_fetch_one_bhavcopy, d): d for d in candidates}
        for future in as_completed(futures):
            trade_date, bhav = future.result()
            if bhav.empty:
                continue
            match = bhav[
                (bhav["SYMBOL"].str.strip() == nse_sym)
                & (bhav.get("SERIES", pd.Series(dtype=str)).str.strip() == "EQ")
            ]
            if match.empty:
                continue
            r = match.iloc[0]
            # Use DATE1 from the file so holidays never get mislabelled.
            actual_date = pd.to_datetime(str(r.get("DATE1", "")), dayfirst=True, errors="coerce")
            file_date = actual_date.date() if not pd.isna(actual_date) else trade_date
            rows.append(
                {
                    "symbol": symbol.upper(),
                    "trade_date": file_date,
                    "series": "EQ",
                    "traded_qty": _safe_float(r.get("TTL_TRD_QNTY")),
                    "deliverable_qty": _safe_float(r.get("DELIV_QTY")),
                    "delivery_pct": _safe_float(r.get("DELIV_PER")),
                    "turnover_lacs": _safe_float(r.get("TURNOVER_LACS")),
                    "source": "nse_bhavcopy",
                }
            )

    if not rows:
        log.warning("No bhavcopy delivery data found for {} over {} days", nse_sym, n_days)
        return pd.DataFrame(columns=_DELIVERY_COLUMNS)

    frame = pd.DataFrame(rows)
    frame = frame.drop_duplicates(subset=["trade_date"]).sort_values("trade_date")
    return frame.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Bulk & Block deals
# ---------------------------------------------------------------------------


def fetch_bulk_deals(
    from_date: date | None = None,
    to_date: date | None = None,
) -> pd.DataFrame:
    """Attempt to fetch bulk deals from NSE API.

    Returns empty DataFrame if NSE's main site is unreachable (Akamai blocks
    non-browser clients). The UI degrades gracefully in this case.
    """
    return _fetch_deals("bulk", from_date, to_date)


def fetch_block_deals(
    from_date: date | None = None,
    to_date: date | None = None,
) -> pd.DataFrame:
    """Attempt to fetch block deals from NSE API."""
    return _fetch_deals("block", from_date, to_date)


def fetch_deals_for_symbol(
    symbol: str,
    from_date: date | None = None,
    to_date: date | None = None,
) -> pd.DataFrame:
    """Merge bulk + block deals filtered for *symbol*."""
    nse_sym = _nse_symbol(symbol)
    bulk = fetch_bulk_deals(from_date, to_date)
    block = fetch_block_deals(from_date, to_date)
    all_deals = pd.concat([bulk, block], ignore_index=True)
    if all_deals.empty:
        return pd.DataFrame(columns=_DEAL_COLUMNS)
    mask = all_deals["symbol"].str.upper() == nse_sym
    return all_deals[mask].reset_index(drop=True)


def _fetch_deals(deal_type: str, from_date: date | None, to_date: date | None) -> pd.DataFrame:
    to_dt = to_date or date.today()
    from_dt = from_date or (to_dt - timedelta(days=30))
    from_str = from_dt.strftime("%d-%m-%Y")
    to_str = to_dt.strftime("%d-%m-%Y")

    url = f"{_BASE}/api/historical/bulk-deals?from={from_str}&to={to_str}"
    if deal_type == "block":
        url = url.replace("bulk-deals", "block-deals")

    try:
        with httpx.Client(headers=_MAIN_HEADERS, timeout=20, follow_redirects=True) as client:
            resp = client.get(url)
        if resp.status_code != 200:
            log.warning(
                "NSE {} deals API returned {} — main site may require browser session",
                deal_type,
                resp.status_code,
            )
            return pd.DataFrame(columns=_DEAL_COLUMNS)
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001
        log.warning("NSE {} deals fetch failed: {}", deal_type, exc)
        return pd.DataFrame(columns=_DEAL_COLUMNS)

    records = payload if isinstance(payload, list) else payload.get("data", [])
    if not records:
        return pd.DataFrame(columns=_DEAL_COLUMNS)

    raw = pd.DataFrame(records)
    raw.columns = [c.strip().upper() for c in raw.columns]

    col_map = {
        "SYMBOL": "symbol",
        "DATE": "deal_date",
        "CLIENT NAME": "client_name",
        "BUY / SELL": "buy_sell",
        "QUANTITY TRADED": "quantity",
        "TRADE PRICE / WGHT. AVG. PRICE": "price",
    }
    raw = raw.rename(columns=col_map)
    for col in col_map.values():
        if col not in raw.columns:
            raw[col] = None

    frame = raw[list(col_map.values())].copy()
    frame["deal_type"] = deal_type.upper()
    frame["source"] = "nse"
    frame["deal_date"] = pd.to_datetime(frame["deal_date"], dayfirst=True, errors="coerce").dt.date
    frame["quantity"] = pd.to_numeric(
        frame["quantity"].astype(str).str.replace(",", ""), errors="coerce"
    )
    frame["price"] = pd.to_numeric(frame["price"].astype(str).str.replace(",", ""), errors="coerce")
    return frame.dropna(subset=["deal_date"]).reset_index(drop=True)


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        s = str(value).replace(",", "").strip()
        f = float(s)
        return None if pd.isna(f) else f
    except (ValueError, TypeError):
        return None
