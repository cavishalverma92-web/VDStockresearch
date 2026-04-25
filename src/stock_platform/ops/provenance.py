"""Data provenance summaries for UI and audits."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd


def build_provenance_rows(
    *,
    symbol: str,
    price_provider: str,
    fundamentals_provider: str,
    price_frame: pd.DataFrame,
    fundamentals_source: str | None = None,
    delivery_available: bool | None = None,
    deals_available: bool | None = None,
) -> list[dict[str, Any]]:
    """Build human-readable source/provenance rows for the current screen."""
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    normalized_symbol = symbol.upper()

    rows: list[dict[str, Any]] = [
        {
            "area": "Price / OHLCV",
            "source": price_provider,
            "symbol": normalized_symbol,
            "freshness": _price_freshness(price_frame),
            "status": "active",
            "caveat": "Verify adjusted prices and corporate actions before research use.",
            "generated_at_utc": generated_at,
        },
        {
            "area": "Fundamentals",
            "source": fundamentals_source or fundamentals_provider,
            "symbol": normalized_symbol,
            "freshness": "sample/template" if fundamentals_source else "not available",
            "status": "sample" if _is_sample_source(fundamentals_source) else "partial",
            "caveat": "Local CSV rows are placeholders until a verified fundamentals source is connected.",
            "generated_at_utc": generated_at,
        },
        {
            "area": "Delivery %",
            "source": "NSE bhavcopy archive",
            "symbol": normalized_symbol,
            "freshness": "loaded this session"
            if delivery_available
            else "unavailable this session",
            "status": "active" if delivery_available else "degraded",
            "caveat": "NSE access can fail due to network or bot-protection limits.",
            "generated_at_utc": generated_at,
        },
        {
            "area": "Bulk / block deals",
            "source": "NSE public API",
            "symbol": normalized_symbol,
            "freshness": _availability_text(deals_available),
            "status": _availability_status(deals_available),
            "caveat": "NSE deal endpoints may require a browser session; verify on NSE directly.",
            "generated_at_utc": generated_at,
        },
        {
            "area": "Signals / score / backtest",
            "source": "local analytics engine",
            "symbol": normalized_symbol,
            "freshness": "derived from current local data",
            "status": "derived",
            "caveat": "Educational research output only; not investment advice.",
            "generated_at_utc": generated_at,
        },
    ]
    return rows


def provenance_rows_to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert provenance rows to a stable table."""
    columns = ["area", "source", "symbol", "freshness", "status", "caveat", "generated_at_utc"]
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)


def _price_freshness(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "no rows"
    latest = frame.index[-1]
    if hasattr(latest, "date"):
        return str(latest.date())
    return str(latest)


def _availability_text(value: bool | None) -> str:
    if value is True:
        return "loaded this session"
    if value is False:
        return "unavailable this session"
    return "not checked in this panel"


def _availability_status(value: bool | None) -> str:
    if value is True:
        return "active"
    if value is False:
        return "degraded"
    return "manual_check"


def _is_sample_source(source: str | None) -> bool:
    return bool(source and "sample" in source.lower())
