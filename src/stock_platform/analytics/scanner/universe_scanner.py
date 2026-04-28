"""Universe scanner: rank an entire index by composite score in one pass.

For each ticker in a chosen universe, the scanner:
1. Fetches OHLCV (last `lookback_days` calendar days).
2. Computes technical indicators.
3. Runs the educational signal scanner.
4. Computes the composite 0–100 research score.

Results are returned as ranked ``ScanResult`` rows. The default scan is
sequential because yfinance can behave unpredictably under concurrent access.
Callers may raise ``max_workers`` for experiments, but local daily use should
prefer reliability over speed.

This is **research support, not investment advice** — the same caveat that
applies to every other module of the platform.
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from stock_platform.analytics.signals import SignalResult, scan_technical_signals
from stock_platform.analytics.technicals import add_technical_indicators
from stock_platform.config import ROOT_DIR, get_universes_config
from stock_platform.data.providers import YahooFinanceProvider
from stock_platform.data.validators import validate_ohlcv
from stock_platform.scoring import CompositeScore, score_stock
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)

_CONFIG_METADATA_KEYS = {"version", "csv_universes"}


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScanResult:
    """One row in a universe scan."""

    symbol: str
    composite_score: float | None
    band: str | None
    sub_scores: dict[str, float]
    active_signal_count: int
    active_signals: list[str]
    last_close: float | None
    rsi_14: float | None
    ma_stack: str | None
    data_quality_warnings: list[str]
    error: str | None = None


# ---------------------------------------------------------------------------
# Universe loading
# ---------------------------------------------------------------------------


def list_available_universes() -> list[str]:
    """Return the names of all universes defined in ``config/universes.yaml``."""
    config = get_universes_config()
    inline = [
        k for k, v in config.items() if k not in _CONFIG_METADATA_KEYS and isinstance(v, list)
    ]
    csv_universes = [k for k, v in config.get("csv_universes", {}).items() if isinstance(v, dict)]
    return [*inline, *csv_universes]


def load_universe(name: str) -> list[str]:
    """Return ticker symbols for the named universe (e.g. ``nifty_50``)."""
    config = get_universes_config()
    if name in config and isinstance(config[name], list):
        return [_normalize_symbol(s) for s in config[name] if str(s).strip()]

    csv_config = config.get("csv_universes", {}).get(name)
    if isinstance(csv_config, dict):
        return _load_csv_universe(name, csv_config)

    if name not in config or not isinstance(config.get(name), list):
        raise KeyError(f"Unknown universe '{name}'. Available: {list_available_universes()}")
    return []


def universe_size(name: str) -> int:
    """Return the number of symbols in a universe."""
    return len(load_universe(name))


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------


def scan_universe(
    universe: str | list[str],
    *,
    lookback_days: int = 365,
    max_workers: int = 1,
    end_date: date | None = None,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> list[ScanResult]:
    """Scan an entire universe and return ranked ``ScanResult`` rows.

    Args:
        universe: name from ``universes.yaml`` (e.g. ``"nifty_50"``) or an
            explicit list of ticker symbols.
        lookback_days: how many calendar days of price history to load.  At
            least ~250 trading days is recommended so 200 EMA and 52W metrics
            populate.
        max_workers: parallel yfinance threads. Defaults to 1 because yfinance
            can be unreliable under concurrent access. Keep <= 8 for experiments.
        end_date: defaults to today; useful for deterministic tests.
        progress_callback: optional ``fn(done, total, current_symbol)`` for
            UI progress bars.

    Returns:
        Results sorted by composite score descending; rows that errored
        appear at the bottom with ``composite_score=None``.
    """
    symbols = load_universe(universe) if isinstance(universe, str) else list(universe)
    if not symbols:
        return []

    end = end_date or date.today()
    start = end - timedelta(days=lookback_days)
    max_workers = max(1, min(int(max_workers), 8))

    results: list[ScanResult] = []
    total = len(symbols)
    done = 0

    log.info("Universe scan: {} symbols, {}-day lookback", total, lookback_days)

    def _scan_one(sym: str) -> ScanResult:
        try:
            provider = YahooFinanceProvider()
            return _scan_single_symbol(sym, provider, start, end)
        except Exception as exc:
            log.warning("Universe scan failed for {}: {}", sym, exc)
            return _empty_result(sym, error=str(exc))

    if max_workers == 1:
        for sym in symbols:
            result = _scan_one(sym)
            results.append(result)
            done += 1
            if progress_callback:
                progress_callback(done, total, sym)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_scan_one, sym): sym for sym in symbols}
            for fut in as_completed(futures):
                sym = futures[fut]
                result = fut.result()
                results.append(result)
                done += 1
                if progress_callback:
                    progress_callback(done, total, sym)

    # Sort: highest composite first; errors / None scores last
    def _sort_key(r: ScanResult) -> tuple[int, float]:
        if r.composite_score is None:
            return (1, 0.0)
        return (0, -r.composite_score)

    results.sort(key=_sort_key)
    return results


def scan_results_to_frame(results: list[ScanResult]) -> pd.DataFrame:
    """Convert scan results to a UI-ready DataFrame."""
    if not results:
        return pd.DataFrame(
            columns=[
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
        )

    rows = []
    for r in results:
        rows.append(
            {
                "symbol": r.symbol,
                "composite_score": r.composite_score,
                "band": r.band,
                "fundamentals": r.sub_scores.get("fundamentals"),
                "technicals": r.sub_scores.get("technicals"),
                "flows": r.sub_scores.get("flows"),
                "events_quality": r.sub_scores.get("events_quality"),
                "macro_sector": r.sub_scores.get("macro_sector"),
                "active_signal_count": r.active_signal_count,
                "active_signals": ", ".join(r.active_signals),
                "last_close": r.last_close,
                "rsi_14": r.rsi_14,
                "ma_stack": r.ma_stack,
                "data_quality_warnings": "; ".join(r.data_quality_warnings),
                "error": r.error,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _scan_single_symbol(
    symbol: str, provider: YahooFinanceProvider, start: date, end: date
) -> ScanResult:
    df = provider.get_ohlcv(symbol, start=start, end=end)
    if df is None or df.empty or len(df) < 30:
        return _empty_result(symbol, error="insufficient price history")

    report = validate_ohlcv(df, symbol=symbol, raise_on_error=False)
    if not report.ok:
        return _empty_result(symbol, error=f"data quality failure: {'; '.join(report.errors)}")

    enriched = add_technical_indicators(df)
    latest = enriched.iloc[-1]

    signals: list[SignalResult] = scan_technical_signals(enriched)
    active = [s for s in signals if s.active]
    active_names = [s.name for s in active]

    score: CompositeScore = score_stock(
        symbol=symbol,
        fundamentals=None,
        technicals=latest,
        signals=signals,
        delivery=None,
        result_volatility=None,
    )

    return ScanResult(
        symbol=symbol.upper(),
        composite_score=score.score,
        band=score.band,
        sub_scores=dict(score.sub_scores),
        active_signal_count=len(active),
        active_signals=active_names,
        last_close=float(latest.get("close", 0.0)) or None,
        rsi_14=_safe_float(latest.get("rsi_14")),
        ma_stack=str(latest.get("ma_stack_status", "mixed")),
        data_quality_warnings=report.warnings,
        error=None,
    )


def _empty_result(symbol: str, *, error: str | None = None) -> ScanResult:
    return ScanResult(
        symbol=symbol.upper(),
        composite_score=None,
        band=None,
        sub_scores={},
        active_signal_count=0,
        active_signals=[],
        last_close=None,
        rsi_14=None,
        ma_stack=None,
        data_quality_warnings=[],
        error=error,
    )


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return None


def _load_csv_universe(name: str, csv_config: dict[str, object]) -> list[str]:
    path_value = str(csv_config.get("path", "")).strip()
    if not path_value:
        raise FileNotFoundError(f"Universe '{name}' has no CSV path configured.")

    path = _resolve_project_path(path_value)
    if not path.exists():
        raise FileNotFoundError(
            f"Universe '{name}' expects a CSV at {path}. "
            "Run scripts\\update_nse_universe.ps1 or place the official NSE equity list there."
        )

    frame = pd.read_csv(path)
    frame.columns = [str(column).strip().lstrip("\ufeff") for column in frame.columns]
    symbol_column = str(csv_config.get("symbol_column", "SYMBOL"))
    if symbol_column not in frame.columns:
        raise KeyError(
            f"Universe '{name}' CSV missing symbol column '{symbol_column}'. "
            f"Available columns: {list(frame.columns)}"
        )

    series_column = str(csv_config.get("series_column", "")).strip()
    series_value = str(csv_config.get("series_value", "")).strip()
    if series_column and series_value and series_column in frame.columns:
        expected_series = series_value.strip().upper()
        frame = frame[frame[series_column].astype(str).str.strip().str.upper() == expected_series]

    symbols = [_normalize_symbol(value) for value in frame[symbol_column] if str(value).strip()]
    return sorted(set(symbols))


def _normalize_symbol(value: object) -> str:
    symbol = str(value).strip().upper()
    if not symbol:
        return ""
    return symbol if "." in symbol else f"{symbol}.NS"


def _resolve_project_path(path_value: str) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else ROOT_DIR / path
