"""Audit rows for EOD refresh job runs."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import update
from sqlalchemy.orm import Session

from stock_platform.db.models import DailyRefreshRun, utc_now


def start_refresh_run(
    session: Session,
    *,
    universe_name: str,
    requested_symbols: int,
    source: str,
    note: str | None = None,
) -> int:
    """Insert a started ``DailyRefreshRun`` row and return its id."""
    run = DailyRefreshRun(
        universe_name=universe_name,
        requested_symbols=int(requested_symbols),
        source=source,
        status="started",
        note=note,
    )
    session.add(run)
    session.flush()
    return int(run.id)


def complete_refresh_run(
    session: Session,
    run_id: int,
    *,
    successful_symbols: int,
    failed_symbols: int,
    price_rows_upserted: int,
    technical_rows_upserted: int,
    instrument_rows_upserted: int = 0,
    signal_rows_saved: int = 0,
    status: str = "completed",
    error: str | None = None,
    finished_at: datetime | None = None,
    duration_seconds: float | None = None,
    scan_run_id: int | None = None,
    note: str | None = None,
) -> None:
    """Mark a refresh run as completed/failed with the recorded counts."""
    values: dict[str, object] = {
        "successful_symbols": int(successful_symbols),
        "failed_symbols": int(failed_symbols),
        "price_rows_upserted": int(price_rows_upserted),
        "technical_rows_upserted": int(technical_rows_upserted),
        "instrument_rows_upserted": int(instrument_rows_upserted),
        "signal_rows_saved": int(signal_rows_saved),
        "status": status,
        "error": error,
        "finished_at": finished_at or utc_now(),
        "duration_seconds": duration_seconds,
        "scan_run_id": scan_run_id,
    }
    if note is not None:
        values["note"] = note

    session.execute(
        update(DailyRefreshRun).where(DailyRefreshRun.id == int(run_id)).values(**values)
    )
