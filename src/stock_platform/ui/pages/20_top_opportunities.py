"""Top Opportunities scanner page."""

from __future__ import annotations

import time

import streamlit as st

from stock_platform.analytics.scanner import (
    build_daily_research_brief,
    compare_latest_universe_scans,
    daily_brief_freshness,
    daily_brief_headline,
    daily_brief_table,
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

last_scan = st.session_state.get("last_scan_summary")
if last_scan and last_scan.get("universe") == universe:
    st.success(
        f"Scan #{last_scan['run_id']} · {last_scan['successful']} ok · "
        f"{last_scan['failed']} failed · {last_scan['duration_s']:.1f}s · "
        f"{last_scan['matched']} matched filters"
    )

if st.button("Run universe scan", type="primary", disabled=not tickers):
    scan_tickers = tickers[: int(max_symbols)]
    progress = st.progress(0.0, text=f"Scanning 0/{len(scan_tickers)} symbols...")

    def _on_progress(done: int, total: int, sym: str) -> None:
        progress.progress(done / total, text=f"Scanned {done}/{total}: {sym}")

    started = time.monotonic()
    results = scan_universe(
        scan_tickers,
        lookback_days=lookback_days,
        max_workers=1,
        progress_callback=_on_progress,
    )
    duration = time.monotonic() - started
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
    st.session_state["last_scan_summary"] = {
        "universe": universe,
        "run_id": run_id,
        "successful": len(success),
        "failed": len(frame) - len(success),
        "matched": len(filtered),
        "duration_s": duration,
    }
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

st.divider()

# --- Daily Research Brief (moved from Market Today) ---------------------------
st.subheader("Daily Research Brief")
st.caption("Universe-level diff against the previous saved scan.")

brief_min_score = st.slider("Brief opportunity score", 0, 100, 60, 5, key="brief_min_score")
brief = build_daily_research_brief(universe, min_opportunity_score=float(brief_min_score))

if not brief.has_latest_scan:
    st.info("No saved scan yet. Run a scan above to populate the brief.")
else:
    status, age = daily_brief_freshness(brief.latest_run_at)
    st.markdown(f"**{daily_brief_headline(brief)}**")
    if status == "fresh":
        st.success(f"Latest scan is fresh: {age}.")
    elif status == "aging":
        st.warning(f"Latest scan is {age}. Consider refreshing.")
    else:
        st.warning(f"Latest scan is {age}. Run a fresh scan before relying on new decisions.")

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Run", f"#{brief.latest_run_id}")
    m2.metric("Successful", brief.successful_symbols)
    m3.metric("Failed", brief.failed_symbols)
    m4.metric(
        "Avg score", "N/A" if brief.average_score is None else f"{brief.average_score:.1f}"
    )
    m5.metric("Top score", "N/A" if brief.top_score is None else f"{brief.top_score:.1f}")

    tabs = st.tabs(["New opportunities", "Score movers", "New signals", "Action queue"])
    with tabs[0]:
        research_pick_button(daily_brief_table(brief.new_opportunities), key="brief_new_opps")
    with tabs[1]:
        improved = daily_brief_table(brief.improved)
        weakened = daily_brief_table(brief.weakened)
        left, right = st.columns(2)
        with left:
            st.markdown("##### Improved")
            research_pick_button(improved, key="brief_improved")
        with right:
            st.markdown("##### Weakened")
            research_pick_button(weakened, key="brief_weakened")
    with tabs[2]:
        research_pick_button(daily_brief_table(brief.new_signals), key="brief_new_signals")
    with tabs[3]:
        action_frame = daily_brief_table(brief.data_quality_actions, limit=15)
        if action_frame.empty:
            st.success("No saved-scan data-quality action rows.")
        else:
            st.dataframe(action_frame, width="stretch", hide_index=True)
