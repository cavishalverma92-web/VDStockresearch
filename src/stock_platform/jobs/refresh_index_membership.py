"""Snapshot current official NSE index membership into history periods.

Example:
    python -m stock_platform.jobs.refresh_index_membership --universe nifty_50
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from stock_platform.data.providers.nse_indices import (
    NSE_INDEX_CSV_SOURCES,
    NseIndexProvider,
    NseIndexProviderError,
)
from stock_platform.data.repositories import sync_index_membership_snapshot
from stock_platform.db import create_all_tables, get_engine, get_session
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


def refresh_index_membership(
    universe_name: str = "nifty_50",
    *,
    effective_date: date | None = None,
) -> object:
    """Fetch current official constituents and sync membership history."""
    source = NSE_INDEX_CSV_SOURCES.get(universe_name)
    if source is None:
        supported = ", ".join(sorted(NSE_INDEX_CSV_SOURCES))
        raise ValueError(f"Unsupported universe '{universe_name}'. Supported: {supported}")

    snapshot_date = effective_date or date.today()
    provider = NseIndexProvider()
    constituents = provider.fetch_constituents(universe_name)

    engine = get_engine()
    create_all_tables(engine)
    with get_session(engine) as session:
        summary = sync_index_membership_snapshot(
            session,
            index_name=source.display_name,
            constituents=constituents,
            effective_date=snapshot_date,
            source="nse_index_csv",
            source_url=source.url,
        )
        session.commit()

    log.info(
        "Index membership snapshot synced: index={}, effective_date={}, current={}, "
        "inserted={}, updated={}, closed={}",
        summary.index_name,
        summary.effective_date,
        summary.current_symbols,
        summary.inserted,
        summary.updated,
        summary.closed,
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Snapshot current official NSE index constituents into history."
    )
    parser.add_argument("--universe", default="nifty_50", help="Supported universe, e.g. nifty_50")
    parser.add_argument(
        "--effective-date",
        default=None,
        help="Membership period start date as YYYY-MM-DD. Defaults to today.",
    )
    args = parser.parse_args()

    effective_date = date.fromisoformat(args.effective_date) if args.effective_date else None
    try:
        summary = refresh_index_membership(args.universe, effective_date=effective_date)
    except (NseIndexProviderError, KeyError, ValueError) as exc:
        print(f"Index membership refresh failed: {exc}", file=sys.stderr)
        print("Nothing was changed. Please retry later or verify the NSE CSV URL.", file=sys.stderr)
        raise SystemExit(1) from exc

    print(f"Index: {summary.index_name}")
    print(f"Effective date: {summary.effective_date}")
    print(f"Current symbols: {summary.current_symbols}")
    print(f"Inserted periods: {summary.inserted}")
    print(f"Updated active periods: {summary.updated}")
    print(f"Closed periods: {summary.closed}")


if __name__ == "__main__":
    main()
