"""Build UI-ready fundamentals summary tables."""

from __future__ import annotations

from typing import Protocol

import pandas as pd

from stock_platform.analytics.fundamentals.quality_scores import (
    calculate_altman_z_score,
    calculate_piotroski_f_score,
)
from stock_platform.analytics.fundamentals.ratios import calculate_basic_ratios, calculate_growth
from stock_platform.analytics.fundamentals.sector_ranking import compute_sector_percentile_ranks
from stock_platform.data.validators import validate_annual_fundamentals


class _FundamentalsProviderLike(Protocol):
    """Structural protocol for any fundamentals provider used by this module."""

    def get_annual_fundamentals(self, symbol: str) -> pd.DataFrame: ...
    def get_snapshots(self, symbol: str) -> list: ...


def build_fundamentals_summary(
    provider: _FundamentalsProviderLike,
    symbols: list[str],
    *,
    include_sector_ranks: bool = True,
) -> pd.DataFrame:
    """Return one latest-year fundamentals summary row per symbol.

    When ``include_sector_ranks`` is True (default), percentile rank columns
    are appended for each metric within sector / industry / market-cap-bucket
    peer groups.
    """
    rows: list[dict[str, object]] = []

    for symbol in symbols:
        frame = provider.get_annual_fundamentals(symbol)
        if frame.empty:
            rows.append(
                {
                    "symbol": symbol,
                    "status": "no_data",
                    "warnings": "No local fundamentals rows",
                }
            )
            continue

        report = validate_annual_fundamentals(frame, symbol, raise_on_error=False)
        snapshots = provider.get_snapshots(symbol)
        if not snapshots:
            rows.append({"symbol": symbol, "status": "no_data", "warnings": "No valid snapshots"})
            continue

        latest = snapshots[-1]
        previous = snapshots[-2] if len(snapshots) > 1 else None
        ratios = calculate_basic_ratios(latest)
        growth = calculate_growth(latest, previous) if previous else {}
        piotroski = calculate_piotroski_f_score(latest, previous) if previous else None
        altman = calculate_altman_z_score(latest)
        source = str(frame.iloc[-1].get("source", "unknown"))

        status = "ok"
        if report.errors:
            status = "error"
        elif "sample_data_source" in report.warnings:
            status = "sample"
        elif any(not warning.startswith("missing_score_inputs") for warning in report.warnings):
            status = "warning"

        rows.append(
            {
                "symbol": symbol,
                "fiscal_year": latest.fiscal_year,
                "revenue": latest.revenue,
                "revenue_growth_pct": _as_percent(growth.get("revenue_growth")),
                "net_income_growth_pct": _as_percent(growth.get("net_income_growth")),
                "eps_growth_pct": _as_percent(growth.get("eps_growth")),
                "free_cash_flow_growth_pct": _as_percent(growth.get("free_cash_flow_growth")),
                "roa_pct": _as_percent(ratios["return_on_assets"]),
                "roe_pct": _as_percent(ratios["return_on_equity"]),
                "roce_pct": _as_percent(ratios["return_on_capital_employed"]),
                "debt_to_equity": ratios["debt_to_equity"],
                "net_debt_to_ebitda": ratios["net_debt_to_ebitda"],
                "ebitda_margin_pct": _as_percent(ratios["ebitda_margin"]),
                "pat_margin_pct": _as_percent(ratios["pat_margin"]),
                "free_cash_flow_yield_pct": _as_percent(ratios["free_cash_flow_yield"]),
                "price_to_book": ratios["price_to_book"],
                "price_to_earnings": ratios["price_to_earnings"],
                "ev_to_ebitda": ratios["ev_to_ebitda"],
                "ev_to_sales": ratios["ev_to_sales"],
                "piotroski_f_score": piotroski.score if piotroski else None,
                "altman_z_score": altman.score,
                "sector": _str_or_none(frame.iloc[-1].get("sector")),
                "industry": _str_or_none(frame.iloc[-1].get("industry")),
                "market_cap_bucket": _str_or_none(frame.iloc[-1].get("market_cap_bucket")),
                "source": source,
                "status": status,
                "warnings": "; ".join(report.warnings),
                "errors": "; ".join(report.errors),
            }
        )

    summary = pd.DataFrame(rows)

    if include_sector_ranks and not summary.empty:
        summary = compute_sector_percentile_ranks(summary)

    return summary


def _as_percent(value: float | None) -> float | None:
    return None if value is None else value * 100


def _str_or_none(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    return s if s else None
