"""End-of-day OHLCV refresh job.

For each symbol in a universe, this job:

1. Finds the latest persisted ``trade_date`` (per-symbol, source-agnostic).
2. Decides a ``(start, end)`` window — initial 5-year backfill, otherwise
   incremental with a small overlap so recent bars get refreshed in case the
   provider corrects them.
3. Fetches OHLCV through ``MarketDataProvider`` (Kite first, yfinance fallback).
4. Validates the frame.
5. Upserts ``price_daily``.
6. Reads the full persisted history for that symbol, recomputes indicators,
   and upserts new/overlapping rows into ``technical_snapshots``.
7. Records start/finish in ``daily_refresh_runs`` for audit.

The job never raises on a single bad symbol — failures are isolated, logged,
and reported in the summary. Trading and portfolio paths are not touched.

CLI:
    python -m stock_platform.jobs.refresh_eod_candles --universe nifty_50
    python -m stock_platform.jobs.refresh_eod_candles --universe nifty_50 --dry-run
    python -m stock_platform.jobs.refresh_eod_candles --universe nifty_50 --max-symbols 5
"""

from __future__ import annotations

import argparse
import contextlib
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from stock_platform.analytics.adjustments import apply_split_adjustment
from stock_platform.analytics.scanner.universe_scanner import load_universe
from stock_platform.analytics.signals import scan_technical_signals
from stock_platform.analytics.technicals import add_technical_indicators
from stock_platform.data.providers.corporate_actions import get_splits as default_splits_fetcher
from stock_platform.data.providers.market_data_provider import MarketDataProvider
from stock_platform.data.repositories import (
    complete_refresh_run,
    fetch_corporate_actions,
    fetch_price_daily,
    latest_trade_date,
    start_refresh_run,
    upsert_composite_score,
    upsert_corporate_actions,
    upsert_price_daily,
    upsert_technical_snapshots,
)
from stock_platform.data.validators import validate_ohlcv
from stock_platform.db import create_all_tables, get_engine, get_session
from stock_platform.scoring import score_stock
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


_MIN_BARS_FOR_COMPOSITE_SCORE = 30


@dataclass(frozen=True)
class SymbolRefreshOutcome:
    """One symbol's result inside :func:`refresh_eod_candles`."""

    symbol: str
    source: str | None
    start_date: date | None
    end_date: date | None
    fetched_rows: int
    price_rows_inserted: int
    price_rows_updated: int
    technical_rows_inserted: int
    technical_rows_updated: int
    splits_upserted: int
    composite_score_saved: bool
    composite_score: float | None
    duration_seconds: float
    error: str | None = None
    skipped_reason: str | None = None


@dataclass(frozen=True)
class RefreshSummary:
    """Top-level result of :func:`refresh_eod_candles`."""

    run_id: int | None
    universe_name: str
    requested_symbols: int
    successful_symbols: int
    failed_symbols: int
    skipped_symbols: int
    price_rows_upserted: int
    technical_rows_upserted: int
    composite_scores_saved: int
    duration_seconds: float
    dry_run: bool
    outcomes: list[SymbolRefreshOutcome] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


_USE_DEFAULT_SPLITS_FETCHER = object()


def refresh_eod_candles(
    universe: str | list[str],
    *,
    market_data_provider: MarketDataProvider | None = None,
    end_date: date | None = None,
    initial_history_days: int = 365 * 5,
    incremental_overlap_days: int = 5,
    max_symbols: int | None = None,
    engine: Engine | None = None,
    progress_callback: Callable[[int, int, str, SymbolRefreshOutcome], None] | None = None,
    dry_run: bool = False,
    note: str | None = None,
    splits_fetcher: Callable[[str], pd.DataFrame] | None = _USE_DEFAULT_SPLITS_FETCHER,  # type: ignore[assignment]
) -> RefreshSummary:
    """Refresh persisted OHLCV + indicators for a universe.

    Args:
        universe: name from ``config/universes.yaml`` or an explicit symbol list.
        market_data_provider: injected for testing; defaults to a fresh
            ``MarketDataProvider`` configured via env (Kite-first, yfinance fallback).
        end_date: window end; defaults to today.
        initial_history_days: backfill window for symbols with no persisted history.
        incremental_overlap_days: how many trailing days to re-fetch on top of the
            persisted history so corrected bars (e.g. late delivery numbers) flow
            through.
        max_symbols: cap symbols processed per run; useful for smoke tests.
        engine: SQLAlchemy engine override; defaults to settings-derived engine.
        progress_callback: ``fn(done, total, symbol, outcome)`` for UI / CLI bars.
        dry_run: when ``True`` the provider is still called and indicators still
            computed, but no writes hit the database. Useful for "what would this
            run touch?" diagnostics.
        note: free-text annotation persisted in ``daily_refresh_runs.note``.
        splits_fetcher: callable ``symbol -> DataFrame[ex_date, ratio]`` used to
            sync stock splits before computing indicators. Defaults to the
            yfinance corporate-actions provider; pass ``None`` to skip the sync
            entirely (useful for tests and offline runs).

    Returns:
        A ``RefreshSummary`` with per-symbol outcomes; never raises on a single
        bad symbol.
    """
    if splits_fetcher is _USE_DEFAULT_SPLITS_FETCHER:
        splits_fetcher = default_splits_fetcher
    started_perf = time.perf_counter()
    universe_name = universe if isinstance(universe, str) else "custom"
    symbols = load_universe(universe) if isinstance(universe, str) else [str(s) for s in universe]
    if max_symbols is not None and max_symbols > 0:
        symbols = symbols[: int(max_symbols)]

    active_engine = engine or get_engine()
    create_all_tables(active_engine)
    provider = market_data_provider or MarketDataProvider()
    end = end_date or date.today()

    log.info(
        "refresh_eod_candles started universe={} symbols={} dry_run={}",
        universe_name,
        len(symbols),
        dry_run,
    )

    run_id: int | None = None
    if not dry_run:
        with get_session(active_engine) as session:
            run_id = start_refresh_run(
                session,
                universe_name=universe_name,
                requested_symbols=len(symbols),
                source=provider.provider_name,
                note=note,
            )

    outcomes: list[SymbolRefreshOutcome] = []
    successful = 0
    failed = 0
    skipped = 0
    total_price = 0
    total_tech = 0
    total_scores = 0

    for index, symbol in enumerate(symbols, start=1):
        outcome = _refresh_one(
            symbol=symbol,
            provider=provider,
            engine=active_engine,
            end_date=end,
            initial_history_days=initial_history_days,
            incremental_overlap_days=incremental_overlap_days,
            dry_run=dry_run,
            splits_fetcher=splits_fetcher,
        )
        outcomes.append(outcome)
        if outcome.error is not None:
            failed += 1
        elif outcome.skipped_reason is not None:
            skipped += 1
        else:
            successful += 1
        total_price += outcome.price_rows_inserted + outcome.price_rows_updated
        total_tech += outcome.technical_rows_inserted + outcome.technical_rows_updated
        if outcome.composite_score_saved:
            total_scores += 1
        if progress_callback:
            progress_callback(index, len(symbols), symbol, outcome)

    duration = time.perf_counter() - started_perf

    status = "completed"
    if failed and successful == 0:
        status = "failed"
    elif failed:
        status = "completed_with_errors"

    if run_id is not None:
        with get_session(active_engine) as session:
            complete_refresh_run(
                session,
                run_id,
                successful_symbols=successful,
                failed_symbols=failed,
                price_rows_upserted=total_price,
                technical_rows_upserted=total_tech,
                status=status,
                duration_seconds=round(duration, 3),
            )

    log.info(
        "refresh_eod_candles done universe={} successful={} failed={} skipped={} "
        "price_rows={} tech_rows={} composite_scores={} duration_s={:.2f}",
        universe_name,
        successful,
        failed,
        skipped,
        total_price,
        total_tech,
        total_scores,
        duration,
    )

    return RefreshSummary(
        run_id=run_id,
        universe_name=universe_name,
        requested_symbols=len(symbols),
        successful_symbols=successful,
        failed_symbols=failed,
        skipped_symbols=skipped,
        price_rows_upserted=total_price,
        technical_rows_upserted=total_tech,
        composite_scores_saved=total_scores,
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
    provider: MarketDataProvider,
    engine: Engine,
    end_date: date,
    initial_history_days: int,
    incremental_overlap_days: int,
    dry_run: bool,
    splits_fetcher: Callable[[str], pd.DataFrame] | None,
) -> SymbolRefreshOutcome:
    started = time.perf_counter()
    cleaned = symbol.strip().upper()

    try:
        with Session(engine) as session:
            latest = latest_trade_date(session, cleaned)

        if latest is None:
            start = end_date - timedelta(days=max(1, initial_history_days))
        else:
            start = latest - timedelta(days=max(0, incremental_overlap_days))

        if start > end_date:
            return SymbolRefreshOutcome(
                symbol=cleaned,
                source=None,
                start_date=start,
                end_date=end_date,
                fetched_rows=0,
                price_rows_inserted=0,
                price_rows_updated=0,
                technical_rows_inserted=0,
                technical_rows_updated=0,
                splits_upserted=0,
                composite_score_saved=False,
                composite_score=None,
                duration_seconds=round(time.perf_counter() - started, 3),
                skipped_reason="already up to date",
            )

        frame = provider.get_ohlcv(cleaned, start=start, end=end_date)
        source = str(frame.attrs.get("source") or "unknown")

        if frame is None or frame.empty:
            return SymbolRefreshOutcome(
                symbol=cleaned,
                source=source,
                start_date=start,
                end_date=end_date,
                fetched_rows=0,
                price_rows_inserted=0,
                price_rows_updated=0,
                technical_rows_inserted=0,
                technical_rows_updated=0,
                splits_upserted=0,
                composite_score_saved=False,
                composite_score=None,
                duration_seconds=round(time.perf_counter() - started, 3),
                error="provider returned no rows",
            )

        validate_ohlcv(frame, symbol=cleaned, raise_on_error=True)

        if dry_run:
            return SymbolRefreshOutcome(
                symbol=cleaned,
                source=source,
                start_date=start,
                end_date=end_date,
                fetched_rows=len(frame),
                price_rows_inserted=len(frame),
                price_rows_updated=0,
                technical_rows_inserted=len(frame),
                technical_rows_updated=0,
                splits_upserted=0,
                composite_score_saved=False,
                composite_score=None,
                duration_seconds=round(time.perf_counter() - started, 3),
                skipped_reason="dry-run",
            )

        splits_upserted = 0
        composite_score_saved = False
        composite_score_value: float | None = None
        with get_session(engine) as session:
            price_summary = upsert_price_daily(session, cleaned, frame, source=source)
            splits_upserted = _sync_splits(session, cleaned, splits_fetcher)
            full_history = fetch_price_daily(session, cleaned)
            splits_persisted = fetch_corporate_actions(session, cleaned, action_type="split")
            adjusted_history = apply_split_adjustment(full_history, splits_persisted)
            enriched = add_technical_indicators(adjusted_history)
            tech_summary = upsert_technical_snapshots(
                session,
                cleaned,
                enriched,
                source=source,
                only_after=start,
            )
            composite_score_value, composite_score_saved = _persist_latest_composite_score(
                session=session,
                symbol=cleaned,
                enriched=enriched,
                source=source,
            )

        return SymbolRefreshOutcome(
            symbol=cleaned,
            source=source,
            start_date=start,
            end_date=end_date,
            fetched_rows=len(frame),
            price_rows_inserted=price_summary.inserted,
            price_rows_updated=price_summary.updated,
            technical_rows_inserted=tech_summary.inserted,
            technical_rows_updated=tech_summary.updated,
            splits_upserted=splits_upserted,
            composite_score_saved=composite_score_saved,
            composite_score=composite_score_value,
            duration_seconds=round(time.perf_counter() - started, 3),
        )

    except Exception as exc:
        log.warning("refresh_eod_candles failed for {}: {}", cleaned, exc)
        return SymbolRefreshOutcome(
            symbol=cleaned,
            source=None,
            start_date=None,
            end_date=end_date,
            fetched_rows=0,
            price_rows_inserted=0,
            price_rows_updated=0,
            technical_rows_inserted=0,
            technical_rows_updated=0,
            splits_upserted=0,
            composite_score_saved=False,
            composite_score=None,
            duration_seconds=round(time.perf_counter() - started, 3),
            error=str(exc),
        )


def _persist_latest_composite_score(
    *,
    session,
    symbol: str,
    enriched: pd.DataFrame,
    source: str,
) -> tuple[float | None, bool]:
    """Compute and persist the composite score for the latest indicator bar.

    Returns ``(score_value, saved_flag)``. Skips silently if not enough bars
    exist for stable indicators or if scoring raises.
    """
    if enriched is None or enriched.empty or len(enriched) < _MIN_BARS_FOR_COMPOSITE_SCORE:
        return None, False

    try:
        signals = scan_technical_signals(enriched)
        latest_row = enriched.iloc[-1]
        as_of_date = enriched.index[-1]
        with contextlib.suppress(AttributeError):
            as_of_date = as_of_date.date()

        composite = score_stock(
            symbol=symbol,
            fundamentals=None,
            technicals=latest_row,
            signals=signals,
            delivery=None,
            result_volatility=None,
        )
        upsert_composite_score(
            session,
            symbol=symbol,
            as_of_date=as_of_date,
            composite=composite,
            signals=signals,
            source=source,
        )
        return float(composite.score), True
    except Exception as exc:
        log.warning("composite score persist failed for {}: {}", symbol, exc)
        return None, False


def _sync_splits(
    session,
    symbol: str,
    splits_fetcher: Callable[[str], pd.DataFrame] | None,
) -> int:
    """Best-effort sync of yfinance split history into ``corporate_actions``.

    A failure here never breaks the refresh — splits are rare and a stale
    snapshot is better than aborting the whole symbol's price ingest.
    """
    if splits_fetcher is None:
        return 0
    try:
        frame = splits_fetcher(symbol)
    except Exception as exc:
        log.warning("splits fetch failed for {}: {}", symbol, exc)
        return 0
    if frame is None or frame.empty:
        return 0
    if "ex_date" not in frame.columns:
        return 0
    if "value" in frame.columns:
        prepared = frame[["ex_date", "value"]].copy()
    elif "ratio" in frame.columns:
        prepared = frame[["ex_date", "ratio"]].rename(columns={"ratio": "value"})
    else:
        return 0
    summary = upsert_corporate_actions(
        session,
        symbol,
        prepared,
        action_type="split",
        source="yfinance",
    )
    return summary.inserted + summary.updated


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_progress(done: int, total: int, symbol: str, outcome: SymbolRefreshOutcome) -> None:
    if outcome.error:
        status = f"FAIL {outcome.error}"
    elif outcome.skipped_reason:
        status = f"SKIP ({outcome.skipped_reason})"
    else:
        score_part = (
            f" score={outcome.composite_score:.1f}" if outcome.composite_score is not None else ""
        )
        status = (
            f"OK   src={outcome.source} "
            f"+{outcome.price_rows_inserted}p/{outcome.price_rows_updated}u "
            f"+{outcome.technical_rows_inserted}t/{outcome.technical_rows_updated}u"
            f"{score_part}"
        )
    print(f"[{done:>4}/{total}] {symbol:<20} {status}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="refresh_eod_candles",
        description="Refresh persisted OHLCV and indicators for a configured universe.",
    )
    parser.add_argument(
        "--universe",
        required=True,
        help="Universe name from config/universes.yaml (e.g. nifty_50).",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Cap how many symbols to process this run.",
    )
    parser.add_argument(
        "--initial-history-days",
        type=int,
        default=365 * 5,
        help="Backfill window (days) for symbols with no persisted history.",
    )
    parser.add_argument(
        "--incremental-overlap-days",
        type=int,
        default=5,
        help="Trailing days to re-fetch on top of persisted history.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and validate but do not write to the database.",
    )
    parser.add_argument(
        "--note",
        type=str,
        default=None,
        help="Free-text note saved to daily_refresh_runs.note.",
    )
    args = parser.parse_args(argv)

    summary = refresh_eod_candles(
        universe=args.universe,
        initial_history_days=args.initial_history_days,
        incremental_overlap_days=args.incremental_overlap_days,
        max_symbols=args.max_symbols,
        dry_run=args.dry_run,
        note=args.note,
        progress_callback=_print_progress,
    )

    print()
    print(f"Universe: {summary.universe_name}")
    print(f"Run id  : {summary.run_id if summary.run_id is not None else '(dry-run)'}")
    print(f"Symbols : {summary.requested_symbols} requested")
    print(f"          {summary.successful_symbols} ok")
    print(f"          {summary.skipped_symbols} skipped")
    print(f"          {summary.failed_symbols} failed")
    print(
        f"Rows    : price {summary.price_rows_upserted}, "
        f"technicals {summary.technical_rows_upserted}, "
        f"scores {summary.composite_scores_saved}"
    )
    print(f"Duration: {summary.duration_seconds:.2f} s")

    return 0 if summary.failed_symbols == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
