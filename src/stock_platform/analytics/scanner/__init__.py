"""Universe scanner — Phase 8.

Runs the platform's per-stock pipeline (price load → indicators → signals →
composite score) across an entire index list and ranks the results.
"""

from stock_platform.analytics.scanner.daily_brief import (
    DailyResearchBrief,
    build_daily_research_brief,
    daily_brief_freshness,
    daily_brief_headline,
    daily_brief_table,
)
from stock_platform.analytics.scanner.persistence import (
    compare_latest_universe_scans,
    compare_universe_scan_runs,
    fetch_latest_universe_scan,
    fetch_recent_universe_scans,
    save_universe_scan,
    scan_storage_to_frame,
)
from stock_platform.analytics.scanner.result_schema import (
    ADVANCED_STRATEGY_SCAN_COLUMNS,
    DEFAULT_STRATEGY_SCAN_COLUMNS,
    StrategyScanFrameSummary,
    StrategyScanResult,
    strategy_results_to_frame,
    summarize_strategy_scan_frame,
)
from stock_platform.analytics.scanner.strategy_persistence import (
    fetch_latest_strategy_scan,
    save_strategy_scan,
    strategy_scan_errors,
    strategy_scan_storage_to_frame,
)
from stock_platform.analytics.scanner.strategy_scanner import (
    StrategyScanSummary,
    prepare_persisted_price_frame,
    scan_persisted_strategy_universe,
)
from stock_platform.analytics.scanner.universe_scanner import (
    ScanResult,
    list_available_universes,
    load_universe,
    scan_results_to_frame,
    scan_universe,
    universe_size,
)
from stock_platform.analytics.scanner.watchlist import (
    add_symbols_to_watchlist,
    enrich_watchlist_with_latest_scores,
    fetch_watchlist_items,
    update_watchlist_reviews,
    watchlist_to_frame,
)

__all__ = [
    "DailyResearchBrief",
    "ADVANCED_STRATEGY_SCAN_COLUMNS",
    "DEFAULT_STRATEGY_SCAN_COLUMNS",
    "ScanResult",
    "StrategyScanResult",
    "StrategyScanFrameSummary",
    "StrategyScanSummary",
    "add_symbols_to_watchlist",
    "build_daily_research_brief",
    "compare_latest_universe_scans",
    "compare_universe_scan_runs",
    "daily_brief_freshness",
    "daily_brief_headline",
    "daily_brief_table",
    "enrich_watchlist_with_latest_scores",
    "fetch_latest_universe_scan",
    "fetch_latest_strategy_scan",
    "fetch_recent_universe_scans",
    "fetch_watchlist_items",
    "list_available_universes",
    "load_universe",
    "prepare_persisted_price_frame",
    "save_universe_scan",
    "save_strategy_scan",
    "scan_persisted_strategy_universe",
    "scan_results_to_frame",
    "scan_storage_to_frame",
    "scan_universe",
    "strategy_results_to_frame",
    "summarize_strategy_scan_frame",
    "strategy_scan_errors",
    "strategy_scan_storage_to_frame",
    "universe_size",
    "update_watchlist_reviews",
    "watchlist_to_frame",
]
