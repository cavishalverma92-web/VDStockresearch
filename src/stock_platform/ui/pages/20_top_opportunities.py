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


def _batch_defaults(universe_name: str, total: int, fallback: int) -> int:
    if universe_name == "all_nse_listed" or total > 500:
        return min(100, max(1, total))
    return min(fallback, max(1, total))


def _batch_range(symbols: list[str], start_at: int, batch_size: int) -> tuple[list[str], int, int]:
    start_idx = max(0, int(start_at) - 1)
    end_idx = min(len(symbols), start_idx + int(batch_size))
    return symbols[start_idx:end_idx], start_idx + 1, end_idx


render_page_shell(
    "Top Opportunities",
    "Run the score and signal pipeline across a chosen universe. Research aid only.",
)

universes = list_available_universes()
if not universes:
    st.warning("No universes configured. Add lists to config/universes.yaml.")
    st.stop()

loadable_universes: dict[str, list[str]] = {}
unavailable_universes: dict[str, str] = {}
for candidate in universes:
    try:
        loaded = load_universe(candidate)
    except (FileNotFoundError, KeyError) as exc:
        unavailable_universes[candidate] = str(exc)
        continue
    if loaded:
        loadable_universes[candidate] = loaded
    else:
        unavailable_universes[candidate] = "Universe has no symbols."

if not loadable_universes:
    st.warning("No loadable universes are available. Check config/universes.yaml.")
    with st.expander("Unavailable universes"):
        st.dataframe(
            [
                {"universe": universe_label(name), "reason": reason}
                for name, reason in unavailable_universes.items()
            ],
            width="stretch",
            hide_index=True,
        )
    st.stop()

if unavailable_universes:
    with st.expander("Unavailable universes", expanded=False):
        st.caption(
            "CSV-backed universes such as All NSE Listed require local files that are not "
            "included in the hosted Render demo. Use bundled watchlists such as Nifty 50 first."
        )
        st.dataframe(
            [
                {"universe": universe_label(name), "reason": reason}
                for name, reason in unavailable_universes.items()
            ],
            width="stretch",
            hide_index=True,
        )

default_universe = (
    "nifty_50" if "nifty_50" in loadable_universes else next(iter(loadable_universes))
)
loadable_names = list(loadable_universes)

col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
with col1:
    universe = st.selectbox(
        "Universe",
        loadable_names,
        index=loadable_names.index(default_universe),
        format_func=universe_label,
        key="top_opportunities_universe",
    )
with col2:
    min_score = st.slider("Min score", 0, 100, 60, 5)
with col3:
    min_signals = st.slider("Min active signals", 0, 7, 1)

tickers = loadable_universes[universe]
large_universe = universe == "all_nse_listed" or len(tickers) > 500

with col4:
    batch_size = st.number_input(
        "Batch size",
        min_value=1,
        max_value=max(1, len(tickers)),
        value=_batch_defaults(universe, len(tickers), 25),
        step=25 if large_universe else 5,
        disabled=not tickers,
    )

batch_start = 1
if large_universe:
    start_col, hint_col = st.columns([1, 3])
    with start_col:
        batch_start = st.number_input(
            "Start at symbol #",
            min_value=1,
            max_value=max(1, len(tickers)),
            value=1,
            step=int(batch_size),
            disabled=not tickers,
            key="top_opportunities_batch_start",
        )
    with hint_col:
        st.warning(
            "Large universe mode is batch-safe. Start with 100 symbols, review failures, "
            "then continue with the next start number. This avoids rate-limit and timeout noise."
        )

scan_tickers, batch_from, batch_to = _batch_range(tickers, int(batch_start), int(batch_size))
st.caption(
    f"{universe_label(universe)} contains {len(tickers):,} symbol(s). "
    f"Current batch: {batch_from:,}-{batch_to:,} ({len(scan_tickers):,} symbol(s))."
)
lookback_days = 400

last_scan = st.session_state.get("last_scan_summary")
if last_scan and last_scan.get("universe") == universe:
    st.success(
        f"Scan #{last_scan['run_id']} · {last_scan['successful']} ok · "
        f"{last_scan['failed']} failed · {last_scan['duration_s']:.1f}s · "
        f"{last_scan['matched']} matched filters"
    )

if st.button("Run universe scan", type="primary", disabled=not scan_tickers):
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
        note=(
            f"UI batch scan for {universe}: rows {batch_from}-{batch_to}; "
            f"{len(scan_tickers)} symbol(s)."
        ),
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
        "batch": f"{batch_from}-{batch_to}",
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
    m4.metric("Avg score", "N/A" if brief.average_score is None else f"{brief.average_score:.1f}")
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
