"""NSE FII/DII daily provisional flows.

NSE publishes provisional FII (Foreign Institutional Investors) and DII
(Domestic Institutional Investors) cash-market activity each evening at:
``https://www.nseindia.com/api/fiidiiTradeReact``

The endpoint returns the most recent trading day's buy / sell / net values
for two participants (``FII/FPI`` and ``DII``) in INR crore. Running this
fetch once a night accumulates a long-running history in the
``market_flows_daily`` table.

NSE's main site sits behind Akamai bot-protection, so an unauthenticated
``httpx`` request can return 401 / 403. We try a polite session warmup
(hit the homepage to acquire cookies, then call the API) and fall back to
returning an empty frame on failure — the job log records the reason and
the UI degrades gracefully.
"""

from __future__ import annotations

from datetime import date, datetime

import httpx
import pandas as pd

from stock_platform.utils.logging import get_logger

log = get_logger(__name__)

_BASE = "https://www.nseindia.com"
_FLOW_URL = f"{_BASE}/api/fiidiiTradeReact"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": f"{_BASE}/reports/fii-dii",
}

# NSE returns category strings like "FII/FPI *" — normalize to "FII" / "DII".
_CATEGORY_MAP = {
    "FII": "FII",
    "FII/FPI": "FII",
    "FOREIGN INSTITUTIONAL INVESTOR": "FII",
    "FPI": "FII",
    "DII": "DII",
    "DOMESTIC INSTITUTIONAL INVESTOR": "DII",
}

FLOW_COLUMNS = [
    "trade_date",
    "participant",
    "buy_value_cr",
    "sell_value_cr",
    "net_value_cr",
    "source",
    "source_url",
]


def fetch_fii_dii_latest(
    *,
    timeout: float = 15.0,
) -> pd.DataFrame:
    """Return latest provisional FII/DII flows as a DataFrame.

    Columns: ``trade_date``, ``participant`` (FII/DII), ``buy_value_cr``,
    ``sell_value_cr``, ``net_value_cr``, ``source``, ``source_url``.

    Returns an empty frame on any error (network blocked, schema change,
    parsing failure). Never raises.
    """
    try:
        with httpx.Client(
            headers=_HEADERS,
            timeout=timeout,
            follow_redirects=True,
        ) as client:
            # Warm up cookies — NSE issues bm_sv / akamai cookies on first hit
            try:
                client.get(_BASE, timeout=timeout)
            except Exception as exc:
                log.debug("NSE homepage warmup failed: {}", exc)
            response = client.get(_FLOW_URL, timeout=timeout)
        if response.status_code != 200:
            log.warning(
                "NSE FII/DII API returned {} — main site may require browser session",
                response.status_code,
            )
            return pd.DataFrame(columns=FLOW_COLUMNS)
        payload = response.json()
    except Exception as exc:
        log.warning("NSE FII/DII fetch failed: {}", exc)
        return pd.DataFrame(columns=FLOW_COLUMNS)

    return parse_fii_dii_payload(payload)


def parse_fii_dii_payload(payload: object) -> pd.DataFrame:
    """Parse NSE's FII/DII JSON into the canonical schema.

    Exposed so callers can pass cached / replayed payloads (and for tests).
    Accepts either a list of records or a dict with a ``data`` key.
    """
    if isinstance(payload, dict):
        records = payload.get("data") or payload.get("Data") or []
    elif isinstance(payload, list):
        records = payload
    else:
        return pd.DataFrame(columns=FLOW_COLUMNS)

    rows: list[dict[str, object]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        category_raw = str(record.get("category") or record.get("Category") or "").strip()
        participant = _normalize_category(category_raw)
        if participant is None:
            continue
        trade_date = _parse_date(record.get("date") or record.get("Date"))
        if trade_date is None:
            continue
        rows.append(
            {
                "trade_date": trade_date,
                "participant": participant,
                "buy_value_cr": _safe_float(record.get("buyValue") or record.get("BuyValue")),
                "sell_value_cr": _safe_float(record.get("sellValue") or record.get("SellValue")),
                "net_value_cr": _safe_float(record.get("netValue") or record.get("NetValue")),
                "source": "nse",
                "source_url": _FLOW_URL,
            }
        )

    if not rows:
        return pd.DataFrame(columns=FLOW_COLUMNS)
    frame = pd.DataFrame(rows)
    # NSE sometimes returns one row per participant per category split — keep
    # the most recent fetch per (date, participant).
    return (
        frame.drop_duplicates(subset=["trade_date", "participant"], keep="last")
        .sort_values(["trade_date", "participant"])
        .reset_index(drop=True)
    )


def _normalize_category(raw: str) -> str | None:
    if not raw:
        return None
    cleaned = raw.upper()
    # Strip common decorations: '*', '**', leading/trailing whitespace
    cleaned = cleaned.replace("*", "").strip()
    if cleaned in _CATEGORY_MAP:
        return _CATEGORY_MAP[cleaned]
    # Try a prefix match — e.g. "FII/FPI INVESTMENT"
    for key, value in _CATEGORY_MAP.items():
        if cleaned.startswith(key):
            return value
    return None


def _parse_date(raw: object) -> date | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    # Try multiple formats — NSE uses "DD-MMM-YYYY" most often
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return pd.to_datetime(text, dayfirst=True, errors="coerce").date()
    except (TypeError, ValueError, AttributeError):
        return None


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        text = str(value).replace(",", "").strip()
        if not text:
            return None
        result = float(text)
        return None if pd.isna(result) else result
    except (TypeError, ValueError):
        return None
