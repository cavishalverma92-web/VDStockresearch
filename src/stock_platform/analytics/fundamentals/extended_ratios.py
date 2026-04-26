"""Extended balance-sheet health ratios required by the master prompt.

Master prompt §4.1 lists D/E, **interest coverage**, current ratio,
**cash conversion cycle**, **working capital trend**, Net Debt / EBITDA.

Basic ratios are in ``ratios.py``; this module covers the items that need
either extra inputs (interest expense, AR / inventory / AP) or multi-period
history (working-capital trend).

All inputs are annual snapshots; values are in the company's reporting
currency.  Results are decimals (e.g. 0.12 = 12%) unless noted otherwise.
Cash-conversion-cycle outputs are in **days**.
"""

from __future__ import annotations

from stock_platform.analytics.fundamentals.ratios import safe_divide
from stock_platform.analytics.fundamentals.schema import FundamentalSnapshot

# ---------------------------------------------------------------------------
# Interest coverage
# ---------------------------------------------------------------------------


def interest_coverage(snapshot: FundamentalSnapshot) -> float | None:
    """EBIT divided by interest expense.

    Interest coverage > 5 is generally healthy; < 1.5 is a red flag.
    Returns None if either input is missing.  Interest expense from yfinance
    can be reported as a negative number; we use the absolute value.
    """
    if snapshot.ebit is None or snapshot.interest_expense is None:
        return None
    expense = abs(snapshot.interest_expense)
    if expense == 0:
        return None
    return snapshot.ebit / expense


# ---------------------------------------------------------------------------
# Cash conversion cycle (days)
# CCC = DSO + DIO - DPO
#   DSO = (Accounts Receivable / Revenue) * 365
#   DIO = (Inventory / Cost of Revenue) * 365
#   DPO = (Accounts Payable / Cost of Revenue) * 365
# ---------------------------------------------------------------------------


def days_sales_outstanding(snapshot: FundamentalSnapshot) -> float | None:
    if snapshot.accounts_receivable is None or not snapshot.revenue:
        return None
    return (snapshot.accounts_receivable / snapshot.revenue) * 365.0


def days_inventory_outstanding(snapshot: FundamentalSnapshot) -> float | None:
    cogs = snapshot.cost_of_revenue
    if snapshot.inventory is None or not cogs:
        return None
    return (snapshot.inventory / cogs) * 365.0


def days_payables_outstanding(snapshot: FundamentalSnapshot) -> float | None:
    cogs = snapshot.cost_of_revenue
    if snapshot.accounts_payable is None or not cogs:
        return None
    return (snapshot.accounts_payable / cogs) * 365.0


def cash_conversion_cycle(snapshot: FundamentalSnapshot) -> dict[str, float | None]:
    """Return DSO, DIO, DPO, and CCC (days). Any missing input → that field is None."""
    dso = days_sales_outstanding(snapshot)
    dio = days_inventory_outstanding(snapshot)
    dpo = days_payables_outstanding(snapshot)
    ccc = dso + dio - dpo if dso is not None and dio is not None and dpo is not None else None
    return {"dso_days": dso, "dio_days": dio, "dpo_days": dpo, "ccc_days": ccc}


# ---------------------------------------------------------------------------
# Working capital trend (multi-year)
# ---------------------------------------------------------------------------


def working_capital_value(snapshot: FundamentalSnapshot) -> float | None:
    if snapshot.current_assets is None or snapshot.current_liabilities is None:
        return None
    return snapshot.current_assets - snapshot.current_liabilities


def working_capital_trend(
    snapshots: list[FundamentalSnapshot],
) -> dict[str, float | None]:
    """Return latest WC, prior-year WC, YoY change, and 3-year slope.

    ``slope_per_year`` is the simple-average annual change over the last
    available 3-year window (latest minus value 3 years prior, divided by 3).
    Returns None for any field that cannot be computed.
    """
    if not snapshots:
        return {"latest": None, "prior_year": None, "yoy_change": None, "slope_3y": None}

    series: list[tuple[int, float]] = []
    for s in snapshots:
        wc = working_capital_value(s)
        if wc is not None:
            series.append((s.fiscal_year, wc))

    if not series:
        return {"latest": None, "prior_year": None, "yoy_change": None, "slope_3y": None}

    series.sort()
    latest_year, latest_wc = series[-1]
    prior_wc = next((wc for year, wc in series if year == latest_year - 1), None)
    three_y_ago = next((wc for year, wc in series if year == latest_year - 3), None)

    yoy = safe_divide(latest_wc - prior_wc, prior_wc) if prior_wc is not None else None
    slope_3y = (latest_wc - three_y_ago) / 3.0 if three_y_ago is not None else None

    return {
        "latest": latest_wc,
        "prior_year": prior_wc,
        "yoy_change": yoy,
        "slope_3y": slope_3y,
    }


# ---------------------------------------------------------------------------
# Convenience: compose all extended health metrics for the UI
# ---------------------------------------------------------------------------


def compute_extended_health(
    snapshots: list[FundamentalSnapshot],
) -> dict[str, float | None]:
    """Return all extended-health metrics for the latest snapshot."""
    if not snapshots:
        return {}

    latest = snapshots[-1]
    ccc = cash_conversion_cycle(latest)
    wc = working_capital_trend(snapshots)

    return {
        "interest_coverage": interest_coverage(latest),
        "dso_days": ccc["dso_days"],
        "dio_days": ccc["dio_days"],
        "dpo_days": ccc["dpo_days"],
        "ccc_days": ccc["ccc_days"],
        "working_capital_latest": wc["latest"],
        "working_capital_yoy_change": wc["yoy_change"],
        "working_capital_3y_slope": wc["slope_3y"],
    }
