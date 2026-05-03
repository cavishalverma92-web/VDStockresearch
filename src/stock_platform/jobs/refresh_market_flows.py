"""Refresh job for daily FII/DII market flows.

Fetches the latest provisional NSE FII/DII numbers and upserts them into
``market_flows_daily``. Run nightly after market close. Each run only adds
the most recent published day; the long-running history accumulates in the DB.

CLI:
    python -m stock_platform.jobs.refresh_market_flows
    python -m stock_platform.jobs.refresh_market_flows --dry-run
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import Engine

from stock_platform.data.providers.nse_market_flows import fetch_fii_dii_latest
from stock_platform.data.repositories import upsert_market_flows
from stock_platform.db import create_all_tables, get_engine, get_session
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class MarketFlowsRefreshSummary:
    inserted: int
    updated: int
    skipped: int
    rows_fetched: int
    duration_seconds: float
    dry_run: bool
    error: str | None = None


def refresh_market_flows(
    *,
    fetcher: Callable[[], pd.DataFrame] = fetch_fii_dii_latest,
    engine: Engine | None = None,
    dry_run: bool = False,
) -> MarketFlowsRefreshSummary:
    """Fetch latest FII/DII flows and upsert. Never raises on provider failure."""
    started = time.perf_counter()
    active_engine = engine or get_engine()
    if not dry_run:
        create_all_tables(active_engine)

    try:
        frame = fetcher()
    except Exception as exc:
        log.warning("market flows fetcher raised: {}", exc)
        return MarketFlowsRefreshSummary(
            inserted=0,
            updated=0,
            skipped=0,
            rows_fetched=0,
            duration_seconds=round(time.perf_counter() - started, 3),
            dry_run=dry_run,
            error=str(exc),
        )

    rows_fetched = 0 if frame is None else len(frame)
    if frame is None or frame.empty:
        log.info("market flows fetch returned no rows")
        return MarketFlowsRefreshSummary(
            inserted=0,
            updated=0,
            skipped=0,
            rows_fetched=0,
            duration_seconds=round(time.perf_counter() - started, 3),
            dry_run=dry_run,
        )

    if dry_run:
        return MarketFlowsRefreshSummary(
            inserted=rows_fetched,
            updated=0,
            skipped=0,
            rows_fetched=rows_fetched,
            duration_seconds=round(time.perf_counter() - started, 3),
            dry_run=True,
        )

    with get_session(active_engine) as session:
        summary = upsert_market_flows(session, frame, source="nse")
    return MarketFlowsRefreshSummary(
        inserted=summary.inserted,
        updated=summary.updated,
        skipped=summary.skipped,
        rows_fetched=rows_fetched,
        duration_seconds=round(time.perf_counter() - started, 3),
        dry_run=False,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="refresh_market_flows",
        description="Fetch latest NSE FII/DII flows and upsert into market_flows_daily.",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    summary = refresh_market_flows(dry_run=args.dry_run)
    print(f"Rows fetched : {summary.rows_fetched}")
    print(f"Inserted     : {summary.inserted}")
    print(f"Updated      : {summary.updated}")
    print(f"Skipped      : {summary.skipped}")
    if summary.error:
        print(f"Error        : {summary.error}")
    print(f"Duration     : {summary.duration_seconds:.2f} s")
    return 0 if summary.error is None else 2


if __name__ == "__main__":
    raise SystemExit(main())
