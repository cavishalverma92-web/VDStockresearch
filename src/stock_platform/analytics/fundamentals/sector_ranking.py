"""Sector-relative percentile ranking for Phase 1.

For each key metric, computes a 0-100 percentile rank within three peer groups:
  - sector          (e.g. "Information Technology")
  - industry        (e.g. "IT Services")
  - market_cap_bucket (e.g. "Large Cap")

Rank 100 = best in peer group. Rank 0 = worst. NaN = not enough peer data.
Higher is always better in the returned rank — inverse metrics (debt/equity)
are flipped so a low value still produces a high rank.
"""

from __future__ import annotations

import pandas as pd

# Metrics where a higher raw value is better (higher rank = better).
_HIGH_IS_BETTER: list[str] = [
    "roe_pct",
    "roa_pct",
    "roce_pct",
    "revenue_growth_pct",
    "net_income_growth_pct",
    "eps_growth_pct",
    "free_cash_flow_growth_pct",
    "free_cash_flow_yield_pct",
    "piotroski_f_score",
    "altman_z_score",
]

# Metrics where a lower raw value is better (rank is inverted).
_LOW_IS_BETTER: list[str] = [
    "debt_to_equity",
    "net_debt_to_ebitda",
    "price_to_book",
    "price_to_earnings",
    "ev_to_ebitda",
    "ev_to_sales",
]

_GROUP_COLS: dict[str, str] = {
    "sector": "sector",
    "industry": "industry",
    "market_cap_bucket": "mkt_cap",
}


def compute_sector_percentile_ranks(summary: pd.DataFrame) -> pd.DataFrame:
    """Return `summary` with added percentile rank columns (0–100).

    For each metric + peer group combination, a column is added:
        {metric}_{group_suffix}_rank

    Example: ``roe_pct_sector_rank``, ``debt_to_equity_mkt_cap_rank``.

    Stocks with NaN for a metric receive NaN for that metric's rank.
    A stock that is the sole valid member of its peer group receives rank 100
    (it is, by definition, the best in its group).
    """
    if summary.empty:
        return summary.copy()

    df = summary.copy()

    for group_col, suffix in _GROUP_COLS.items():
        if group_col not in df.columns:
            continue

        for metric in _HIGH_IS_BETTER:
            if metric not in df.columns:
                continue
            col = f"{metric}_{suffix}_rank"
            df[col] = df.groupby(group_col, group_keys=False)[metric].transform(
                lambda s: s.rank(pct=True, na_option="keep") * 100
            )

        for metric in _LOW_IS_BETTER:
            if metric not in df.columns:
                continue
            col = f"{metric}_{suffix}_rank"
            df[col] = df.groupby(group_col, group_keys=False)[metric].transform(
                lambda s: s.rank(pct=True, ascending=False, na_option="keep") * 100
            )

    return df


def sector_rank_summary(ranked: pd.DataFrame, symbol: str) -> dict[str, float | None]:
    """Return a flat dict of rank values for one stock, for UI display."""
    row = ranked[ranked["symbol"].str.upper() == symbol.upper()]
    if row.empty:
        return {}

    result: dict[str, float | None] = {}
    for col in ranked.columns:
        if col.endswith("_rank"):
            value = row.iloc[0][col]
            result[col] = None if pd.isna(value) else float(value)
    return result
