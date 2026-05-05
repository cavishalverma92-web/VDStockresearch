"""Strategy Scanner page."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from stock_platform.analytics.scanner import (
    DEFAULT_STRATEGY_SCAN_COLUMNS,
    fetch_latest_strategy_scan,
    list_available_universes,
    load_universe,
    save_strategy_scan,
    scan_persisted_strategy_universe,
    strategy_scan_errors,
    strategy_scan_storage_to_frame,
)
from stock_platform.ui.components.common import research_pick_button, universe_label
from stock_platform.ui.components.layout import render_page_shell

render_page_shell(
    "Strategy Scanner",
    "Read-only swing-trading research setups from saved EOD data. Research aid only.",
)

st.caption(
    "This page scans locally persisted daily OHLCV. Run the EOD refresh from Data Health "
    "when data is stale. No trading, portfolio, holdings, funds, or order APIs are used."
)

universes = list_available_universes()
if not universes:
    st.warning("No universes configured. Add lists to config/universes.yaml.")
    st.stop()

top = st.columns([2, 1, 1, 1])
with top[0]:
    universe = st.selectbox("Universe", universes, format_func=universe_label)

try:
    symbols = load_universe(universe)
    universe_error = None
except (FileNotFoundError, KeyError) as exc:
    symbols = []
    universe_error = str(exc)

with top[1]:
    max_symbols = st.number_input(
        "Max symbols",
        min_value=1,
        max_value=max(1, len(symbols)),
        value=min(50, max(1, len(symbols))),
        step=5,
        disabled=not symbols,
    )
with top[2]:
    min_confidence = st.slider("Min confidence", 0, 90, 60, 5)
with top[3]:
    min_rr = st.slider("Min R/R", 0.0, 5.0, 1.5, 0.5)

if universe_error:
    st.warning(universe_error)
    st.stop()

st.caption(f"{universe_label(universe)} contains {len(symbols):,} symbol(s).")

run_scan = st.button("Scan saved EOD data", type="primary", disabled=not symbols)
if run_scan:
    with st.spinner("Scanning saved OHLCV strategy setups..."):
        summary = scan_persisted_strategy_universe(
            universe,
            max_symbols=int(max_symbols),
        )
    run_id = save_strategy_scan(
        universe_name=universe,
        summary=summary,
        min_confidence_filter=float(min_confidence),
        min_rr_filter=float(min_rr),
        note=f"UI strategy scan capped at {int(max_symbols)} symbol(s).",
    )
    st.success(
        f"Saved strategy scan #{run_id}: {len(summary.results)} setup(s), "
        f"{summary.failed_symbols} data action(s)."
    )

latest_run = fetch_latest_strategy_scan(universe)

if latest_run is None:
    st.info(
        "Run a scan to see EMA stack, RSI momentum, and EMA pullback setups from "
        "your saved EOD database."
    )
    st.stop()

metrics = st.columns(4)
metrics[0].metric("Latest run", f"#{latest_run.id}")
metrics[1].metric("Scanned", latest_run.scanned_symbols)
metrics[2].metric("Setups found", latest_run.result_count)
metrics[3].metric("Data actions", latest_run.failed_symbols)

st.caption(
    f"Latest saved scan: {latest_run.created_at} | "
    f"source: {latest_run.source} | requested: {latest_run.requested_symbols}"
)

frame = strategy_scan_storage_to_frame(latest_run)
if frame.empty:
    st.info("No strategy setups matched the first MVP rules in this saved data.")
else:
    filters = st.columns([2, 1, 1, 1])
    with filters[0]:
        selected_strategies = st.multiselect(
            "Strategy",
            sorted(frame["strategy"].dropna().unique()),
            default=sorted(frame["strategy"].dropna().unique()),
        )
    with filters[1]:
        selected_liquidity = st.multiselect(
            "Liquidity",
            sorted(frame["liquidity_status"].dropna().unique()),
            default=sorted(frame["liquidity_status"].dropna().unique()),
        )
    with filters[2]:
        min_rsi, max_rsi = st.slider("RSI range", 0, 100, (35, 80), 5)
    with filters[3]:
        selected_sources = st.multiselect(
            "Data source",
            sorted(frame["data_source"].dropna().unique()),
            default=sorted(frame["data_source"].dropna().unique()),
        )

    filtered = frame.copy()
    if selected_strategies:
        filtered = filtered[filtered["strategy"].isin(selected_strategies)]
    if selected_liquidity:
        filtered = filtered[filtered["liquidity_status"].isin(selected_liquidity)]
    if selected_sources:
        filtered = filtered[filtered["data_source"].isin(selected_sources)]
    filtered = filtered[
        (filtered["confidence_score"].fillna(0) >= float(min_confidence))
        & (filtered["risk_reward"].fillna(0) >= float(min_rr))
    ]
    if "rsi" in filtered.columns:
        rsi_values = pd.to_numeric(filtered["rsi"], errors="coerce")
        filtered = filtered[(rsi_values.isna()) | (rsi_values.between(min_rsi, max_rsi))]

    default_cols = [column for column in DEFAULT_STRATEGY_SCAN_COLUMNS if column in filtered]
    st.subheader("Strategy Results")
    research_pick_button(filtered[default_cols], key="strategy_scanner_results")

    with st.expander("Advanced scanner columns"):
        advanced_cols = [
            column
            for column in filtered.columns
            if column not in default_cols and column != "company_name"
        ]
        st.dataframe(
            filtered[[*default_cols[:1], *advanced_cols]], width="stretch", hide_index=True
        )

    st.download_button(
        "Download strategy scan CSV",
        data=filtered.to_csv(index=False),
        file_name=f"{universe}_strategy_scan.csv",
        mime="text/csv",
    )

errors = strategy_scan_errors(latest_run)
if errors:
    with st.expander("Data actions and skipped symbols", expanded=False):
        error_frame = pd.DataFrame(
            [{"symbol": symbol, "note": note} for symbol, note in errors.items()]
        )
        st.dataframe(error_frame, width="stretch", hide_index=True)
