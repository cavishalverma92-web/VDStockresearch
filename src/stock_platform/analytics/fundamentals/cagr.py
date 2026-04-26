"""Multi-year compound annual growth rate (CAGR) helpers.

Master prompt requires 3Y / 5Y / 10Y CAGR for Revenue, EBITDA, PAT, EPS, OCF,
FCF, Book Value.  This module computes CAGR over an exact horizon when enough
annual snapshots exist; returns None otherwise so the UI can show "N/A" instead
of misleading short-history figures.

CAGR formula:
    CAGR = (end / start) ** (1 / years) - 1

Returns are decimals (e.g. 0.12 = 12%).  Negative or zero start values yield
None — CAGR is undefined for sign changes.
"""

from __future__ import annotations

from stock_platform.analytics.fundamentals.schema import FundamentalSnapshot

_METRICS: dict[str, str] = {
    "revenue": "revenue",
    "ebitda": "ebitda",
    "net_income": "net_income",
    "eps": "eps",
    "operating_cash_flow": "operating_cash_flow",
    "free_cash_flow": "free_cash_flow",
    "book_value": "book_value",
}

_HORIZONS: tuple[int, ...] = (3, 5, 10)


def calculate_cagr(start: float | None, end: float | None, years: int) -> float | None:
    """Return CAGR over *years* years, or None if not computable."""
    if start is None or end is None or years <= 0:
        return None
    if start <= 0 or end <= 0:
        # CAGR is mathematically undefined when sign flips or value is zero
        return None
    return (end / start) ** (1.0 / years) - 1.0


def compute_multi_year_cagr(
    snapshots: list[FundamentalSnapshot],
) -> dict[str, float | None]:
    """Compute 3Y / 5Y / 10Y CAGR for the seven required metrics.

    Snapshots must be sorted ascending by fiscal_year (the providers do this).
    Keys are of the form ``"{metric}_cagr_{n}y"`` (e.g. ``revenue_cagr_3y``).
    Missing horizons return None.
    """
    if not snapshots:
        return {}

    latest = snapshots[-1]
    by_year: dict[int, FundamentalSnapshot] = {s.fiscal_year: s for s in snapshots}

    results: dict[str, float | None] = {}
    for metric, attr in _METRICS.items():
        end_value = getattr(latest, attr, None)
        for n in _HORIZONS:
            key = f"{metric}_cagr_{n}y"
            start_year = latest.fiscal_year - n
            start_snapshot = by_year.get(start_year)
            start_value = (
                getattr(start_snapshot, attr, None) if start_snapshot is not None else None
            )
            results[key] = calculate_cagr(start_value, end_value, n)
    return results


def cagr_summary_for_metric(
    snapshots: list[FundamentalSnapshot], metric: str
) -> dict[str, float | None]:
    """Return only the 3Y / 5Y / 10Y CAGR triple for one metric (for UI cards)."""
    if metric not in _METRICS:
        raise ValueError(f"Unknown CAGR metric: {metric}")
    full = compute_multi_year_cagr(snapshots)
    return {f"{n}y": full.get(f"{metric}_cagr_{n}y") for n in _HORIZONS}
