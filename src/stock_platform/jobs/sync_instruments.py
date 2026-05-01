"""Persist the Kite instrument master locally.

The platform never trades; this job exists so the rest of the app can resolve
``RELIANCE.NS`` → Kite instrument token without re-downloading the full
instrument list (~1MB, tens of thousands of rows) on every process start.

Writes:
- ``instrument_master`` rows (idempotent on the existing unique constraint).
- A timestamped CSV under ``data/raw/kite/`` (audit copy, never overwritten).
- A stable ``data/processed/kite/<exchange>_instruments.csv`` (latest snapshot).
- A ``data/cache/kite/<exchange>_instruments_latest.csv`` for offline browsing.

CLI:
    python -m stock_platform.jobs.sync_instruments
    python -m stock_platform.jobs.sync_instruments --exchange BSE
    python -m stock_platform.jobs.sync_instruments --no-csv
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine

from stock_platform.auth import load_kite_access_token
from stock_platform.config import DATA_DIR, get_settings
from stock_platform.data.providers.kite_provider import KiteProvider
from stock_platform.data.repositories import upsert_instruments
from stock_platform.db import create_all_tables, get_engine, get_session
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class InstrumentSyncSummary:
    """Top-level result of :func:`sync_instruments`."""

    exchange: str
    fetched_rows: int
    inserted: int
    updated: int
    skipped: int
    duration_seconds: float
    raw_csv_path: Path | None
    processed_csv_path: Path | None
    cache_csv_path: Path | None
    error: str | None = None


def sync_instruments(
    *,
    exchange: str = "NSE",
    kite_provider: KiteProvider | None = None,
    engine: Engine | None = None,
    write_csv_snapshot: bool = True,
    csv_root: Path | None = None,
) -> InstrumentSyncSummary:
    """Fetch the Kite instrument master and persist it locally.

    Args:
        exchange: Kite exchange code (``NSE`` / ``BSE`` / ``NFO`` / etc.).
        kite_provider: injected for testing; default reads creds from ``.env``.
        engine: SQLAlchemy engine override; defaults to settings-derived engine.
        write_csv_snapshot: when ``True`` (default), also writes the audit /
            processed / cache CSV files alongside the database rows.
        csv_root: override the base directory for CSV files; defaults to the
            project ``data/`` folder. Useful for tests.

    Returns:
        :class:`InstrumentSyncSummary` — never raises on a single failure;
        problems are recorded in ``error``.
    """
    started = time.perf_counter()
    normalized_exchange = exchange.upper()

    provider = kite_provider or _build_kite_provider()
    if not provider.is_configured():
        return _failed(
            normalized_exchange,
            started,
            "KITE_API_KEY or KITE_API_SECRET is missing.",
        )
    if not provider.has_access_token():
        return _failed(
            normalized_exchange,
            started,
            "KITE_ACCESS_TOKEN is missing or expired. "
            "Generate a fresh token from Zerodha API Setup.",
        )

    try:
        frame = provider.get_instruments(normalized_exchange)
    except Exception as exc:
        log.warning(
            "Kite instrument fetch failed: exchange={}, error={}",
            normalized_exchange,
            exc,
        )
        return _failed(normalized_exchange, started, str(exc))

    raw_path = processed_path = cache_path = None
    if write_csv_snapshot and frame is not None and not frame.empty:
        raw_path, processed_path, cache_path = _write_csv_snapshot(
            frame, normalized_exchange, csv_root
        )

    active_engine = engine or get_engine()
    create_all_tables(active_engine)
    with get_session(active_engine) as session:
        upsert_summary = upsert_instruments(session, frame, source="kite")

    duration = time.perf_counter() - started
    log.info(
        "sync_instruments exchange={} fetched={} inserted={} updated={} "
        "skipped={} duration_s={:.2f}",
        normalized_exchange,
        len(frame),
        upsert_summary.inserted,
        upsert_summary.updated,
        upsert_summary.skipped,
        duration,
    )
    return InstrumentSyncSummary(
        exchange=normalized_exchange,
        fetched_rows=len(frame),
        inserted=upsert_summary.inserted,
        updated=upsert_summary.updated,
        skipped=upsert_summary.skipped,
        duration_seconds=round(duration, 3),
        raw_csv_path=raw_path,
        processed_csv_path=processed_path,
        cache_csv_path=cache_path,
        error=None,
    )


def _build_kite_provider() -> KiteProvider:
    settings = get_settings()
    return KiteProvider(
        api_key=settings.kite_api_key,
        api_secret=settings.kite_api_secret,
        access_token=load_kite_access_token() or "",
    )


def _failed(exchange: str, started: float, error: str) -> InstrumentSyncSummary:
    return InstrumentSyncSummary(
        exchange=exchange,
        fetched_rows=0,
        inserted=0,
        updated=0,
        skipped=0,
        duration_seconds=round(time.perf_counter() - started, 3),
        raw_csv_path=None,
        processed_csv_path=None,
        cache_csv_path=None,
        error=error,
    )


def _write_csv_snapshot(
    frame: pd.DataFrame,
    exchange: str,
    csv_root: Path | None,
) -> tuple[Path, Path, Path]:
    base = csv_root or DATA_DIR
    raw_dir = base / "raw" / "kite"
    processed_dir = base / "processed" / "kite"
    cache_dir = base / "cache" / "kite"
    for folder in (raw_dir, processed_dir, cache_dir):
        folder.mkdir(parents=True, exist_ok=True)

    timestamp = pd.Timestamp.utcnow().strftime("%Y%m%d-%H%M%S")
    exchange_lower = exchange.lower()
    raw_path = raw_dir / f"{exchange_lower}_instruments_{timestamp}.csv"
    processed_path = processed_dir / f"{exchange_lower}_instruments.csv"
    cache_path = cache_dir / f"{exchange_lower}_instruments_latest.csv"

    frame.to_csv(raw_path, index=False)
    frame.to_csv(processed_path, index=False)
    frame.to_csv(cache_path, index=False)

    log.info(
        "instrument CSV snapshot written: raw={}, processed={}, cache={}",
        raw_path,
        processed_path,
        cache_path,
    )
    return raw_path, processed_path, cache_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sync_instruments",
        description="Sync the Kite instrument master to the local database and CSV files.",
    )
    parser.add_argument(
        "--exchange",
        default="NSE",
        help="Kite exchange code to sync (default: NSE).",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Skip writing the audit / processed / cache CSV files.",
    )
    args = parser.parse_args(argv)

    summary = sync_instruments(
        exchange=args.exchange,
        write_csv_snapshot=not args.no_csv,
    )

    if summary.error:
        print(f"FAIL: {summary.error}")
        return 1

    print(f"Exchange : {summary.exchange}")
    print(f"Fetched  : {summary.fetched_rows:,} rows")
    print(f"Inserted : {summary.inserted:,}")
    print(f"Updated  : {summary.updated:,}")
    print(f"Skipped  : {summary.skipped:,}")
    if summary.processed_csv_path:
        print(f"CSV      : {summary.processed_csv_path}")
    print(f"Duration : {summary.duration_seconds:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
