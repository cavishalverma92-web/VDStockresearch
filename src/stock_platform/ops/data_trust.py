"""Selected-stock data trust summaries for the Streamlit UI."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Any

import pandas as pd


def build_data_trust_rows(
    *,
    symbol: str,
    price_frame: pd.DataFrame,
    price_source: str,
    price_warnings: Sequence[str] | None = None,
    price_errors: Sequence[str] | None = None,
    fundamentals_frame: pd.DataFrame | None = None,
    fundamentals_source: str | None = None,
    fundamentals_warnings: Sequence[str] | None = None,
    fundamentals_errors: Sequence[str] | None = None,
    banking_frame: pd.DataFrame | None = None,
    banking_applicable: bool = True,
    banking_warnings: Sequence[str] | None = None,
    banking_errors: Sequence[str] | None = None,
    composite_missing: Sequence[str] | None = None,
    composite_risks: Sequence[str] | None = None,
    active_signal_count: int = 0,
    delivery_available: bool = False,
    result_volatility_available: bool = False,
) -> list[dict[str, Any]]:
    """Return a plain-English trust checklist for the selected stock.

    The rows are intentionally compact for UI use. They do not assert that data is
    correct; they tell the user what is present, missing, stale, or provisional.
    """
    normalized_symbol = symbol.upper()
    price_warning_list = list(price_warnings or [])
    price_error_list = list(price_errors or [])
    fundamentals_warning_list = list(fundamentals_warnings or [])
    fundamentals_error_list = list(fundamentals_errors or [])
    banking_warning_list = list(banking_warnings or [])
    banking_error_list = list(banking_errors or [])
    missing_list = sorted(set(composite_missing or []))
    risk_list = sorted(set(composite_risks or []))

    fundamentals_frame = fundamentals_frame if fundamentals_frame is not None else pd.DataFrame()
    banking_frame = banking_frame if banking_frame is not None else pd.DataFrame()

    rows = [
        {
            "area": "Price data",
            "status": _status(price_error_list, price_warning_list, present=not price_frame.empty),
            "source": price_source,
            "freshness": _price_freshness(price_frame),
            "coverage": f"{len(price_frame):,} OHLCV rows",
            "what_to_check": _issue_text(price_error_list, price_warning_list)
            or "Price data loaded and basic validation ran.",
        },
        {
            "area": "Fundamentals",
            "status": _status(
                fundamentals_error_list,
                fundamentals_warning_list,
                present=not fundamentals_frame.empty,
            ),
            "source": fundamentals_source or "not available",
            "freshness": _fundamentals_freshness(fundamentals_frame),
            "coverage": _fundamentals_coverage(fundamentals_frame),
            "what_to_check": _issue_text(fundamentals_error_list, fundamentals_warning_list)
            or "Annual fundamentals available for this screen.",
        },
        _banking_row(
            banking_frame,
            banking_error_list,
            banking_warning_list,
            applicable=banking_applicable,
        ),
        {
            "area": "Signals",
            "status": "OK" if active_signal_count > 0 else "PARTIAL",
            "source": "local technical engine",
            "freshness": _price_freshness(price_frame),
            "coverage": f"{active_signal_count} active signal(s)",
            "what_to_check": "No active signal fired."
            if active_signal_count == 0
            else "Signal inputs loaded.",
        },
        {
            "area": "Flows / events",
            "status": "OK" if delivery_available and result_volatility_available else "PARTIAL",
            "source": "NSE/yfinance/local analytics",
            "freshness": "loaded this session"
            if delivery_available or result_volatility_available
            else "limited in this session",
            "coverage": _flows_events_coverage(delivery_available, result_volatility_available),
            "what_to_check": "Delivery and result-event data remain limited in the MVP.",
        },
        {
            "area": "Composite score",
            "status": "OK" if not missing_list and not risk_list else "PARTIAL",
            "source": "local scoring config",
            "freshness": "derived from current selected-stock data",
            "coverage": f"{len(missing_list)} missing input(s), {len(risk_list)} risk note(s)",
            "what_to_check": _composite_note(missing_list, risk_list),
        },
    ]
    for row in rows:
        row["symbol"] = normalized_symbol
    return rows


def data_trust_rows_to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert trust rows to stable display columns."""
    columns = [
        "area",
        "status",
        "source",
        "freshness",
        "coverage",
        "what_to_check",
        "symbol",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)


def data_trust_level(rows: list[dict[str, Any]]) -> tuple[str, str]:
    """Return a High/Medium/Low confidence label and beginner-readable reason."""
    if not rows:
        return "Low", "No data trust checks have run yet."
    statuses = [str(row.get("status", "")).upper() for row in rows]
    if any(status == "ACTION" for status in statuses):
        return "Low", "One or more critical inputs are missing or failed validation."
    partial_count = statuses.count("PARTIAL")
    if partial_count >= 3:
        return "Low", "Several inputs are incomplete, so the score is only a rough screen."
    if partial_count > 0:
        return "Medium", "Core data loaded, but some inputs are partial or provisional."
    return "High", "Core data loaded with no major trust warnings from the current checks."


def _status(errors: Sequence[str], warnings: Sequence[str], *, present: bool) -> str:
    if errors or not present:
        return "ACTION"
    if warnings:
        return "PARTIAL"
    return "OK"


def _issue_text(errors: Sequence[str], warnings: Sequence[str]) -> str:
    issues = [*errors, *warnings]
    return "; ".join(str(issue) for issue in issues[:3])


def _price_freshness(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "no rows"
    latest = frame.index[-1]
    latest_date = latest.date() if hasattr(latest, "date") else str(latest)
    return str(latest_date)


def _fundamentals_freshness(frame: pd.DataFrame) -> str:
    if frame.empty or "fiscal_year" not in frame.columns:
        return "not available"
    latest_year = pd.to_numeric(frame["fiscal_year"], errors="coerce").dropna()
    if latest_year.empty:
        return "fiscal year unknown"
    return f"FY{int(latest_year.max())}"


def _fundamentals_coverage(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "0 annual row(s)"
    year_count = frame["fiscal_year"].nunique() if "fiscal_year" in frame.columns else len(frame)
    return f"{year_count} annual row(s)"


def _banking_source(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "manual CSV missing"
    if "source" in frame.columns and not frame["source"].dropna().empty:
        return str(frame["source"].dropna().iloc[-1])
    return "manual CSV"


def _banking_row(
    frame: pd.DataFrame,
    errors: Sequence[str],
    warnings: Sequence[str],
    *,
    applicable: bool,
) -> dict[str, str]:
    if not applicable:
        return {
            "area": "Banking metrics",
            "status": "N/A",
            "source": "not applicable",
            "freshness": "not applicable",
            "coverage": "non-bank / non-financial stock",
            "what_to_check": "Banking metrics are only required for banks and financial services.",
        }
    return {
        "area": "Banking metrics",
        "status": _status(errors, warnings, present=not frame.empty),
        "source": _banking_source(frame),
        "freshness": _banking_freshness(frame),
        "coverage": _banking_coverage(frame),
        "what_to_check": _issue_text(errors, warnings)
        or (
            "Manual banking metrics available."
            if not frame.empty
            else "Add audited NIM, GNPA, NNPA, CASA, growth, and capital rows for banks."
        ),
    }


def _banking_freshness(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "not available"
    if "last_updated" in frame.columns and not frame["last_updated"].dropna().empty:
        updated = str(frame["last_updated"].dropna().iloc[-1])
        return updated
    if "fiscal_year" in frame.columns and not frame["fiscal_year"].dropna().empty:
        return f"FY{int(frame['fiscal_year'].dropna().iloc[-1])}"
    return date.today().isoformat()


def _banking_coverage(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "0 manual banking row(s)"
    return f"{len(frame):,} manual banking row(s)"


def _flows_events_coverage(delivery_available: bool, result_volatility_available: bool) -> str:
    available = []
    missing = []
    if delivery_available:
        available.append("delivery")
    else:
        missing.append("delivery")
    if result_volatility_available:
        available.append("result volatility")
    else:
        missing.append("result volatility")
    return f"available: {', '.join(available) or 'none'}; missing: {', '.join(missing) or 'none'}"


def _composite_note(missing: Sequence[str], risks: Sequence[str]) -> str:
    if not missing and not risks:
        return "No major missing-score notes for the current MVP inputs."
    notes = []
    if missing:
        notes.append(f"Missing: {', '.join(missing[:4])}")
    if risks:
        notes.append(f"Risks: {', '.join(risks[:3])}")
    return " | ".join(notes)
