"""SQLite persistence for Phase 8 universe scan results."""

from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import Engine, desc, select
from sqlalchemy.orm import selectinload

from stock_platform.analytics.scanner.universe_scanner import ScanResult
from stock_platform.db import create_all_tables, get_engine, get_session
from stock_platform.db.models import UniverseScanResult, UniverseScanRun


def save_universe_scan(
    *,
    universe_name: str,
    results: list[ScanResult],
    lookback_days: int,
    min_score_filter: float | None = None,
    min_signals_filter: int | None = None,
    source: str = "yfinance",
    note: str | None = None,
    engine: Engine | None = None,
) -> int:
    """Persist one universe scan run and all symbol-level results.

    Returns the saved ``universe_scan_runs.id`` value. The scan is append-only:
    each button click creates a new auditable run instead of overwriting the
    previous one.
    """
    active_engine = engine or get_engine()
    create_all_tables(active_engine)

    successful = sum(1 for row in results if row.error is None)
    failed = len(results) - successful

    with get_session(active_engine) as session:
        run = UniverseScanRun(
            universe_name=universe_name,
            requested_symbols=len(results),
            successful_symbols=successful,
            failed_symbols=failed,
            lookback_days=lookback_days,
            min_score_filter=min_score_filter,
            min_signals_filter=min_signals_filter,
            source=source,
            note=note,
        )
        session.add(run)
        session.flush()

        for row in results:
            session.add(_result_to_model(run.id, row))

        return int(run.id)


def fetch_latest_universe_scan(
    universe_name: str | None = None,
    *,
    engine: Engine | None = None,
) -> UniverseScanRun | None:
    """Return the latest saved universe scan, optionally filtered by universe."""
    active_engine = engine or get_engine()
    create_all_tables(active_engine)

    statement = (
        select(UniverseScanRun)
        .options(selectinload(UniverseScanRun.results))
        .order_by(desc(UniverseScanRun.created_at), desc(UniverseScanRun.id))
        .limit(1)
    )
    if universe_name:
        statement = statement.where(UniverseScanRun.universe_name == universe_name)

    with get_session(active_engine) as session:
        run = session.scalar(statement)
        if run is None:
            return None
        _ = list(run.results)
        session.expunge_all()
        return run


def fetch_recent_universe_scans(
    universe_name: str,
    *,
    limit: int = 2,
    engine: Engine | None = None,
) -> list[UniverseScanRun]:
    """Return recent saved scans for one universe, newest first."""
    active_engine = engine or get_engine()
    create_all_tables(active_engine)

    statement = (
        select(UniverseScanRun)
        .where(UniverseScanRun.universe_name == universe_name)
        .options(selectinload(UniverseScanRun.results))
        .order_by(desc(UniverseScanRun.created_at), desc(UniverseScanRun.id))
        .limit(max(1, int(limit)))
    )

    with get_session(active_engine) as session:
        runs = list(session.scalars(statement).all())
        for run in runs:
            _ = list(run.results)
        session.expunge_all()
        return runs


def compare_latest_universe_scans(
    universe_name: str,
    *,
    engine: Engine | None = None,
) -> tuple[UniverseScanRun | None, UniverseScanRun | None, pd.DataFrame]:
    """Compare the latest saved scan with the previous scan for a universe."""
    recent = fetch_recent_universe_scans(universe_name, limit=2, engine=engine)
    latest = recent[0] if recent else None
    previous = recent[1] if len(recent) > 1 else None
    return latest, previous, compare_universe_scan_runs(latest, previous)


def compare_universe_scan_runs(
    latest: UniverseScanRun | None,
    previous: UniverseScanRun | None,
) -> pd.DataFrame:
    """Return latest saved scan rows enriched with previous-score changes."""
    latest_frame = scan_storage_to_frame(latest)
    if latest_frame.empty:
        return latest_frame.assign(
            previous_score=pd.NA,
            score_change=pd.NA,
            signal_count_change=pd.NA,
            new_active_signals="",
            dropped_active_signals="",
            comparison_status="no latest scan",
        )

    previous_frame = scan_storage_to_frame(previous)
    if previous_frame.empty:
        comparison = latest_frame.copy()
        comparison["previous_score"] = pd.NA
        comparison["score_change"] = pd.NA
        comparison["signal_count_change"] = pd.NA
        comparison["new_active_signals"] = comparison["active_signals"]
        comparison["dropped_active_signals"] = ""
        comparison["comparison_status"] = "new scan row"
        return comparison

    previous_lookup = previous_frame.set_index("symbol")
    rows: list[dict[str, object]] = []
    for row in latest_frame.to_dict(orient="records"):
        symbol = str(row["symbol"])
        previous_row = (
            previous_lookup.loc[symbol].to_dict() if symbol in previous_lookup.index else None
        )
        current_signals = _split_signals(row.get("active_signals"))
        previous_signals = (
            _split_signals(previous_row.get("active_signals")) if previous_row else set()
        )
        previous_score = previous_row.get("composite_score") if previous_row else pd.NA
        score_change = _score_change(row.get("composite_score"), previous_score)
        signal_count_change = _score_change(
            row.get("active_signal_count"),
            previous_row.get("active_signal_count") if previous_row else pd.NA,
        )
        enriched = dict(row)
        enriched["previous_score"] = previous_score
        enriched["score_change"] = score_change
        enriched["signal_count_change"] = signal_count_change
        enriched["new_active_signals"] = ", ".join(sorted(current_signals - previous_signals))
        enriched["dropped_active_signals"] = ", ".join(sorted(previous_signals - current_signals))
        enriched["comparison_status"] = _comparison_status(previous_row, score_change)
        rows.append(enriched)

    comparison = pd.DataFrame(rows)
    return comparison.sort_values(
        by=["score_change", "composite_score", "symbol"],
        ascending=[False, False, True],
        na_position="last",
    ).reset_index(drop=True)


def scan_storage_to_frame(run: UniverseScanRun | None) -> pd.DataFrame:
    """Convert a saved scan run into a UI-ready DataFrame."""
    columns = [
        "symbol",
        "composite_score",
        "band",
        "fundamentals",
        "technicals",
        "flows",
        "events_quality",
        "macro_sector",
        "active_signal_count",
        "active_signals",
        "last_close",
        "rsi_14",
        "ma_stack",
        "data_quality_warnings",
        "error",
    ]
    if run is None or not run.results:
        return pd.DataFrame(columns=columns)

    rows = [_model_to_row(row) for row in run.results]
    frame = pd.DataFrame(rows, columns=columns)
    return frame.sort_values(
        by=["composite_score", "symbol"],
        ascending=[False, True],
        na_position="last",
    ).reset_index(drop=True)


def _result_to_model(run_id: int, row: ScanResult) -> UniverseScanResult:
    return UniverseScanResult(
        run_id=run_id,
        symbol=row.symbol.upper(),
        composite_score=row.composite_score,
        band=row.band,
        fundamentals_score=row.sub_scores.get("fundamentals"),
        technicals_score=row.sub_scores.get("technicals"),
        flows_score=row.sub_scores.get("flows"),
        events_quality_score=row.sub_scores.get("events_quality"),
        macro_sector_score=row.sub_scores.get("macro_sector"),
        active_signal_count=row.active_signal_count,
        active_signals_json=json.dumps(row.active_signals),
        last_close=row.last_close,
        rsi_14=row.rsi_14,
        ma_stack=row.ma_stack,
        data_quality_warnings_json=json.dumps(row.data_quality_warnings),
        error=row.error,
    )


def _model_to_row(row: UniverseScanResult) -> dict[str, object]:
    return {
        "symbol": row.symbol,
        "composite_score": row.composite_score,
        "band": row.band,
        "fundamentals": row.fundamentals_score,
        "technicals": row.technicals_score,
        "flows": row.flows_score,
        "events_quality": row.events_quality_score,
        "macro_sector": row.macro_sector_score,
        "active_signal_count": row.active_signal_count,
        "active_signals": ", ".join(_json_list(row.active_signals_json)),
        "last_close": row.last_close,
        "rsi_14": row.rsi_14,
        "ma_stack": row.ma_stack,
        "data_quality_warnings": "; ".join(_json_list(row.data_quality_warnings_json)),
        "error": row.error,
    }


def _json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]


def _split_signals(value: object) -> set[str]:
    if value is None or pd.isna(value):
        return set()
    return {part.strip() for part in str(value).split(",") if part.strip()}


def _score_change(current: object, previous: object) -> float | None:
    if current is None or previous is None or pd.isna(current) or pd.isna(previous):
        return None
    return round(float(current) - float(previous), 2)


def _comparison_status(previous_row: dict[str, object] | None, score_change: float | None) -> str:
    if previous_row is None:
        return "new symbol"
    if score_change is None:
        return "not comparable"
    if score_change >= 5:
        return "improved"
    if score_change <= -5:
        return "weakened"
    return "stable"
