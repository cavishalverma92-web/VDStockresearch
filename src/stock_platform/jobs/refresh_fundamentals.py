"""Fundamentals refresh job.

For each symbol in a universe and for each configured source, this job:

1. Fetches annual + quarterly fundamentals from the provider.
2. Validates the frames (best-effort; bad rows logged, never fatal).
3. Upserts ``fundamentals_annual`` / ``fundamentals_quarterly`` keyed on
   (symbol, fiscal_period, source) so multiple sources coexist.

The job never raises on a single bad symbol — failures are isolated, logged,
and rolled up into a summary. Run nightly (or weekly — fundamentals don't
change daily).

CLI:
    python -m stock_platform.jobs.refresh_fundamentals --universe nifty_50
    python -m stock_platform.jobs.refresh_fundamentals --universe nifty_50 --source yfinance
    python -m stock_platform.jobs.refresh_fundamentals --universe nifty_50 --source yfinance --source screener
    python -m stock_platform.jobs.refresh_fundamentals --universe nifty_50 --max-symbols 5 --dry-run
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import Engine

from stock_platform.analytics.scanner.universe_scanner import load_universe
from stock_platform.data.providers.base import FundamentalsDataProvider
from stock_platform.data.providers.yfinance_fundamentals import YFinanceFundamentalsProvider
from stock_platform.data.repositories import (
    upsert_fundamentals_annual,
    upsert_fundamentals_quarterly,
)
from stock_platform.data.validators import (
    validate_annual_fundamentals,
    validate_quarterly_fundamentals,
)
from stock_platform.db import create_all_tables, get_engine, get_session
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SymbolSourceOutcome:
    """One (symbol, source) result."""

    symbol: str
    source: str
    annual_rows_inserted: int
    annual_rows_updated: int
    quarterly_rows_inserted: int
    quarterly_rows_updated: int
    duration_seconds: float
    error: str | None = None
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class FundamentalsRefreshSummary:
    universe_name: str
    sources: tuple[str, ...]
    requested_symbols: int
    successful: int
    failed: int
    annual_rows_upserted: int
    quarterly_rows_upserted: int
    duration_seconds: float
    dry_run: bool
    outcomes: list[SymbolSourceOutcome] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Provider registry
# ---------------------------------------------------------------------------


def _default_provider_factory(source: str) -> FundamentalsDataProvider:
    if source == "yfinance":
        return YFinanceFundamentalsProvider()
    if source == "screener":
        # Imported lazily to keep yfinance-only deployments lean.
        from stock_platform.data.providers.screener_fundamentals import (
            ScreenerFundamentalsProvider,
        )

        return ScreenerFundamentalsProvider()
    raise ValueError(f"Unknown fundamentals source: {source!r}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def refresh_fundamentals(
    universe: str | list[str],
    *,
    sources: list[str] | None = None,
    provider_factory: Callable[[str], FundamentalsDataProvider] | None = None,
    max_symbols: int | None = None,
    engine: Engine | None = None,
    dry_run: bool = False,
    progress_callback: Callable[[int, int, str, SymbolSourceOutcome], None] | None = None,
) -> FundamentalsRefreshSummary:
    """Refresh persisted fundamentals for a universe across one or more sources."""
    started = time.perf_counter()

    universe_name = universe if isinstance(universe, str) else "custom"
    symbols = load_universe(universe) if isinstance(universe, str) else [str(s) for s in universe]
    if max_symbols is not None and max_symbols > 0:
        symbols = symbols[: int(max_symbols)]

    sources = sources or ["yfinance"]
    factory = provider_factory or _default_provider_factory

    active_engine = engine or get_engine()
    if not dry_run:
        create_all_tables(active_engine)

    providers: dict[str, FundamentalsDataProvider] = {src: factory(src) for src in sources}

    log.info(
        "refresh_fundamentals started universe={} sources={} symbols={} dry_run={}",
        universe_name,
        sources,
        len(symbols),
        dry_run,
    )

    outcomes: list[SymbolSourceOutcome] = []
    successful = 0
    failed = 0
    total_annual = 0
    total_quarterly = 0

    total_units = len(symbols) * len(sources)
    done = 0
    for symbol in symbols:
        for source in sources:
            outcome = _refresh_one(
                symbol=symbol,
                source=source,
                provider=providers[source],
                engine=active_engine,
                dry_run=dry_run,
            )
            outcomes.append(outcome)
            if outcome.error is not None:
                failed += 1
            else:
                successful += 1
            total_annual += outcome.annual_rows_inserted + outcome.annual_rows_updated
            total_quarterly += outcome.quarterly_rows_inserted + outcome.quarterly_rows_updated
            done += 1
            if progress_callback:
                progress_callback(done, total_units, symbol, outcome)

    duration = time.perf_counter() - started
    log.info(
        "refresh_fundamentals done universe={} successful={} failed={} "
        "annual_rows={} quarterly_rows={} duration_s={:.2f}",
        universe_name,
        successful,
        failed,
        total_annual,
        total_quarterly,
        duration,
    )

    return FundamentalsRefreshSummary(
        universe_name=universe_name,
        sources=tuple(sources),
        requested_symbols=len(symbols),
        successful=successful,
        failed=failed,
        annual_rows_upserted=total_annual,
        quarterly_rows_upserted=total_quarterly,
        duration_seconds=round(duration, 3),
        dry_run=dry_run,
        outcomes=outcomes,
    )


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _refresh_one(
    *,
    symbol: str,
    source: str,
    provider: FundamentalsDataProvider,
    engine: Engine,
    dry_run: bool,
) -> SymbolSourceOutcome:
    started = time.perf_counter()
    cleaned = symbol.strip().upper()
    warnings: list[str] = []
    try:
        annual_frame: pd.DataFrame = pd.DataFrame()
        quarterly_frame: pd.DataFrame = pd.DataFrame()

        if hasattr(provider, "get_annual_fundamentals"):
            annual_frame = provider.get_annual_fundamentals(cleaned)  # type: ignore[attr-defined]
        if hasattr(provider, "get_quarterly_fundamentals"):
            quarterly_frame = provider.get_quarterly_fundamentals(cleaned)

        if not annual_frame.empty:
            report = validate_annual_fundamentals(annual_frame, cleaned, raise_on_error=False)
            warnings.extend(report.warnings)
        if not quarterly_frame.empty:
            q_report = validate_quarterly_fundamentals(
                quarterly_frame, cleaned, raise_on_error=False
            )
            warnings.extend(q_report.warnings)

        a_inserted = a_updated = q_inserted = q_updated = 0
        if dry_run:
            a_inserted = len(annual_frame)
            q_inserted = len(quarterly_frame)
        else:
            with get_session(engine) as session:
                if not annual_frame.empty:
                    s = upsert_fundamentals_annual(session, cleaned, annual_frame, source=source)
                    a_inserted, a_updated = s.inserted, s.updated
                if not quarterly_frame.empty:
                    s = upsert_fundamentals_quarterly(
                        session, cleaned, quarterly_frame, source=source
                    )
                    q_inserted, q_updated = s.inserted, s.updated

        return SymbolSourceOutcome(
            symbol=cleaned,
            source=source,
            annual_rows_inserted=a_inserted,
            annual_rows_updated=a_updated,
            quarterly_rows_inserted=q_inserted,
            quarterly_rows_updated=q_updated,
            duration_seconds=round(time.perf_counter() - started, 3),
            warnings=tuple(warnings),
        )
    except Exception as exc:
        log.warning("refresh_fundamentals failed symbol={} source={}: {}", cleaned, source, exc)
        return SymbolSourceOutcome(
            symbol=cleaned,
            source=source,
            annual_rows_inserted=0,
            annual_rows_updated=0,
            quarterly_rows_inserted=0,
            quarterly_rows_updated=0,
            duration_seconds=round(time.perf_counter() - started, 3),
            error=str(exc),
            warnings=tuple(warnings),
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_progress(done: int, total: int, symbol: str, outcome: SymbolSourceOutcome) -> None:
    if outcome.error:
        status = f"FAIL {outcome.error}"
    else:
        status = (
            f"OK   src={outcome.source} "
            f"a+{outcome.annual_rows_inserted}/u{outcome.annual_rows_updated} "
            f"q+{outcome.quarterly_rows_inserted}/u{outcome.quarterly_rows_updated}"
        )
    print(f"[{done:>4}/{total}] {symbol:<20} {status}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="refresh_fundamentals",
        description="Refresh persisted annual + quarterly fundamentals.",
    )
    parser.add_argument("--universe", required=True)
    parser.add_argument(
        "--source",
        action="append",
        default=None,
        help="Provider source(s) to fetch from. Repeat for multiple. Default: yfinance.",
    )
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    summary = refresh_fundamentals(
        universe=args.universe,
        sources=args.source,
        max_symbols=args.max_symbols,
        dry_run=args.dry_run,
        progress_callback=_print_progress,
    )

    print()
    print(f"Universe : {summary.universe_name}")
    print(f"Sources  : {', '.join(summary.sources)}")
    print(f"Symbols  : {summary.requested_symbols}")
    print(f"           {summary.successful} ok / {summary.failed} failed")
    print(
        f"Rows     : annual {summary.annual_rows_upserted}, "
        f"quarterly {summary.quarterly_rows_upserted}"
    )
    print(f"Duration : {summary.duration_seconds:.2f} s")

    return 0 if summary.failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
