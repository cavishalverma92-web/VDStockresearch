"""Strategy scanner runner built on persisted EOD data."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import Engine

from stock_platform.analytics.scanner.result_schema import StrategyScanResult
from stock_platform.analytics.scanner.universe_scanner import load_universe
from stock_platform.analytics.strategies.base import (
    build_strategy_context,
    evaluate_default_strategies,
)
from stock_platform.data.repositories import fetch_price_daily
from stock_platform.data.validators import validate_ohlcv
from stock_platform.db import create_all_tables, get_engine, get_session
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)

SOURCE_PRIORITY = {
    "yfinance": 1,
    "kite": 2,
}


@dataclass(frozen=True)
class StrategyScanSummary:
    """Top-level result for one read-only strategy scan."""

    requested_symbols: int
    scanned_symbols: int
    failed_symbols: int
    results: list[StrategyScanResult]
    errors: dict[str, str]


def scan_persisted_strategy_universe(
    universe: str | list[str],
    *,
    max_symbols: int | None = None,
    min_history_rows: int = 220,
    engine: Engine | None = None,
) -> StrategyScanSummary:
    """Scan persisted local OHLCV for the first strategy scanner MVP.

    This runner intentionally avoids live provider calls. The daily refresh job
    owns Kite/yfinance fetching; this function consumes validated local history.
    """
    symbols = load_universe(universe) if isinstance(universe, str) else list(universe)
    if max_symbols is not None and max_symbols > 0:
        symbols = symbols[: int(max_symbols)]

    active_engine = engine or get_engine()
    create_all_tables(active_engine)
    results: list[StrategyScanResult] = []
    errors: dict[str, str] = {}
    scanned = 0

    with get_session(active_engine) as session:
        for raw_symbol in symbols:
            symbol = str(raw_symbol).strip().upper()
            if not symbol:
                continue
            try:
                raw_frame = fetch_price_daily(session, symbol)
                frame, source_label, source_warning = prepare_persisted_price_frame(raw_frame)
                if frame.empty or len(frame) < min_history_rows:
                    errors[symbol] = f"insufficient persisted price history ({len(frame)} rows)"
                    continue
                report = validate_ohlcv(frame, symbol=symbol, raise_on_error=False)
                if not report.ok:
                    errors[symbol] = f"data quality failure: {'; '.join(report.errors)}"
                    continue
                warnings = [*report.warnings]
                if source_warning:
                    warnings.append(source_warning)
                context = build_strategy_context(
                    symbol=symbol,
                    frame=frame,
                    data_source=source_label,
                    warnings=warnings,
                )
                results.extend(evaluate_default_strategies(context))
                scanned += 1
            except Exception as exc:  # noqa: BLE001
                log.warning("Strategy scan failed for {}: {}", symbol, exc)
                errors[symbol] = str(exc)

    return StrategyScanSummary(
        requested_symbols=len(symbols),
        scanned_symbols=scanned,
        failed_symbols=len(errors),
        results=sorted(
            results,
            key=lambda row: (
                -row.confidence_score,
                -(row.risk_reward or 0),
                row.symbol,
                row.strategy,
            ),
        ),
        errors=errors,
    )


def prepare_persisted_price_frame(frame: pd.DataFrame) -> tuple[pd.DataFrame, str, str]:
    """Deduplicate mixed-source persisted price rows into one OHLCV series."""
    if frame is None or frame.empty:
        return pd.DataFrame(), "none", ""

    working = frame.copy()
    if not isinstance(working.index, pd.DatetimeIndex):
        working.index = pd.to_datetime(working.index, errors="coerce")
    working = working[~working.index.isna()].copy()
    working.index = pd.DatetimeIndex(working.index).tz_localize(None).normalize()
    if "source" not in working.columns:
        working["source"] = "unknown"
    working["_source_priority"] = (
        working["source"].astype(str).str.lower().map(lambda value: SOURCE_PRIORITY.get(value, 0))
    )
    source_values = sorted(set(working["source"].dropna().astype(str)))
    warning = ""
    if len(source_values) > 1:
        warning = "Mixed persisted price sources were deduplicated by date, preferring Kite."
    working = working.reset_index().sort_values(["date", "_source_priority"]).set_index("date")
    working = working[~working.index.duplicated(keep="last")].drop(columns=["_source_priority"])
    working.index.name = "date"
    if "adj_close" not in working.columns and "close" in working.columns:
        working["adj_close"] = working["close"]
    source_label = source_values[-1] if len(source_values) == 1 else "mixed"
    return working, source_label, warning


_prepare_persisted_price_frame = prepare_persisted_price_frame
