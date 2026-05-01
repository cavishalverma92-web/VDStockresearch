"""Top Opportunities scanner page."""

from __future__ import annotations

import streamlit as st

from stock_platform.analytics.scanner import (
    compare_latest_universe_scans,
    list_available_universes,
    load_universe,
    save_universe_scan,
    scan_results_to_frame,
    scan_universe,
)
from stock_platform.ui.components.common import research_pick_button, universe_label
from stock_platform.ui.components.layout import render_page_shell

render_page_shell(
    "Top Opportunities",
    "Run the score and signal pipeline across a chosen universe. Research aid only.",
)

universes = list_available_universes()
if not universes:
    st.warning("No universes configured. Add lists to config/universes.yaml.")
    st.stop()

col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
with col1:
    universe = st.selectbox("Universe", universes, format_func=universe_label)
with col2:
    min_score = st.slider("Min score", 0, 100, 60, 5)
with col3:
    min_signals = st.slider("Min active signals", 0, 7, 1)

try:
    tickers = load_universe(universe)
    universe_error = None
except (FileNotFoundError, KeyError) as exc:
    tickers = []
    universe_error = str(exc)

with col4:
    max_symbols = st.number_input(
        "Max symbols",
        min_value=1,
        max_value=max(1, len(tickers)),
        value=min(25, max(1, len(tickers))),
        step=5,
        disabled=not tickers,
    )

if universe_error:
    st.warning(universe_error)
    st.stop()

st.caption(f"{universe_label(universe)} contains {len(tickers):,} symbol(s).")
lookback_days = 400

if st.button("Run universe scan", type="primary", disabled=not tickers):
    scan_tickers = tickers[: int(max_symbols)]
    progress = st.progress(0.0, text=f"Scanning 0/{len(scan_tickers)} symbols...")

    def _on_progress(done: int, total: int, sym: str) -> None:
        progress.progress(done / total, text=f"Scanned {done}/{total}: {sym}")

    results = scan_universe(
        scan_tickers,
        lookback_days=lookback_days,
        max_workers=1,
        progress_callback=_on_progress,
    )
    progress.empty()
    run_id = save_universe_scan(
        universe_name=universe,
        results=results,
        lookback_days=lookback_days,
        min_score_filter=float(min_score),
        min_signals_filter=int(min_signals),
        note=f"UI scan capped at {len(scan_tickers)} symbol(s).",
    )
    frame = scan_results_to_frame(results)
    success = frame[frame["error"].isna()]
    filtered = success[
        (success["composite_score"].fillna(0) >= min_score)
        & (success["active_signal_count"].fillna(0) >= min_signals)
    ]
    st.success(
        f"Scan #{run_id} saved. {len(success)} successful, {len(frame) - len(success)} failed."
    )
    research_pick_button(filtered, key="top_opportunities_live")
    st.download_button(
        "Download scan CSV",
        data=frame.to_csv(index=False),
        file_name=f"{universe}_scan.csv",
        mime="text/csv",
    )

latest_run, previous_run, comparison = compare_latest_universe_scans(universe)
st.subheader("Latest Saved Scan")
if latest_run is None:
    st.info("No saved scan yet for this universe.")
else:
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Run", f"#{latest_run.id}")
    m2.metric("Saved rows", latest_run.requested_symbols)
    m3.metric("Successful", latest_run.successful_symbols)
    m4.metric("Failed", latest_run.failed_symbols)
    success = comparison[comparison["error"].isna()].head(50)
    research_pick_button(success, key="top_opportunities_saved")
    st.download_button(
        "Download latest saved scan CSV",
        data=comparison.to_csv(index=False),
        file_name=f"{universe}_latest_saved_scan.csv",
        mime="text/csv",
    )
