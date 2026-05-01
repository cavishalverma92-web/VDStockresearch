"""Market Today dashboard."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from stock_platform.analytics.scanner import (
    build_daily_research_brief,
    daily_brief_freshness,
    daily_brief_headline,
    daily_brief_table,
    list_available_universes,
)
from stock_platform.data.providers import MarketDataProvider
from stock_platform.ops import build_market_today_summary
from stock_platform.ui.components.common import research_pick_button, universe_label
from stock_platform.ui.components.layout import render_page_shell

render_page_shell(
    "Market Today",
    "A focused morning dashboard: data freshness, provider health, score movers, and attention queue.",
)

summary = build_market_today_summary()
health = summary.health
latest_run = health.recent_refresh_runs[0] if health.recent_refresh_runs else None
breadth = summary.breadth
token = summary.kite_token

st.markdown("#### Morning Control Panel")
top_cols = st.columns(5)
top_cols[0].metric(
    "Provider health",
    summary.provider_health.label,
    help=summary.provider_health.detail,
)
top_cols[1].metric(
    "Breadth",
    f"{breadth.advances} / {breadth.declines}",
    help="Advances / declines from the latest two persisted daily closes per symbol.",
)
top_cols[2].metric(
    "A/D ratio",
    "N/A" if breadth.advance_decline_ratio is None else f"{breadth.advance_decline_ratio:.2f}",
    help="Advance-decline ratio from persisted prices.",
)
top_cols[3].metric(
    "Score rows",
    f"{health.composite_score_coverage.total_rows:,}" if health.composite_score_coverage else "0",
    help="Persisted Research Conviction snapshots available locally.",
)
top_cols[4].metric(
    "Kite token",
    token.status.title(),
    help=token.message,
)

if summary.provider_health.color == "green":
    st.success(summary.provider_health.detail)
elif summary.provider_health.color == "amber":
    st.warning(summary.provider_health.detail)
else:
    st.error(summary.provider_health.detail)

if token.status in {"missing", "expired", "warning"}:
    st.warning(token.message)
else:
    st.info(token.message)

if breadth.latest_trade_date:
    st.caption(
        f"Breadth uses {breadth.compared_symbols} symbol(s), latest saved bar "
        f"{breadth.latest_trade_date}."
    )
else:
    st.caption("Breadth will appear after the first EOD refresh stores local price rows.")

st.divider()

snapshot_col, stale_col = st.columns([2, 1])
with snapshot_col:
    st.subheader("Top Attention List")
    st.caption("Highest persisted Research Conviction rows from the latest saved score date.")
    if summary.top_attention.empty:
        st.info("No persisted score rows yet. Run the EOD refresh job to populate this.")
    else:
        research_pick_button(summary.top_attention, key="market_attention")

with stale_col:
    st.subheader("Data Freshness")
    if summary.stale_symbols.empty:
        st.success("No stale persisted symbols at the current threshold.")
    else:
        st.warning(f"{len(summary.stale_symbols)} stale symbol(s) need refresh.")
        st.dataframe(summary.stale_symbols, width="stretch", hide_index=True)

st.subheader("Score Movers")
st.caption("Largest positive or negative movement between each symbol's latest two saved scores.")
if summary.score_movers.empty:
    st.info("Score movers need at least two persisted score dates per symbol.")
else:
    positive = summary.score_movers[summary.score_movers["score_change"] > 0]
    negative = summary.score_movers[summary.score_movers["score_change"] < 0]
    tab_up, tab_down, tab_all = st.tabs(["Improving", "Weakening", "All movers"])
    with tab_up:
        if positive.empty:
            st.info("No improving score movers in the saved data yet.")
        else:
            research_pick_button(positive, key="market_score_up")
    with tab_down:
        if negative.empty:
            st.info("No weakening score movers in the saved data yet.")
        else:
            research_pick_button(negative, key="market_score_down")
    with tab_all:
        research_pick_button(summary.score_movers, key="market_score_all")

event_col, live_col = st.columns([2, 1])
with event_col:
    st.subheader("Upcoming Event Risk")
    st.caption("Corporate actions or result-style events saved locally in the next 5 trading days.")
    if summary.upcoming_events.empty:
        st.info("No saved upcoming events in the next 5 trading days.")
    else:
        research_pick_button(summary.upcoming_events, key="market_events")

with live_col:
    st.subheader("Live Index Check")
    st.caption("Optional. Uses provider router; saved dashboard data above does not need this.")
    if st.button("Refresh live index snapshot"):
        provider = MarketDataProvider()
        try:
            snapshot = provider.get_ltp(["NIFTY 50", "NIFTY BANK", "NIFTY MIDCAP 100"])
        except Exception as exc:  # noqa: BLE001
            snapshot = pd.DataFrame()
            st.warning(f"Live index snapshot unavailable: {type(exc).__name__}")
        if snapshot.empty:
            st.info("No live index rows returned. Kite index symbol names may differ.")
        else:
            safe_cols = [col for col in ["symbol", "exchange", "ltp", "source"] if col in snapshot]
            st.dataframe(snapshot[safe_cols], width="stretch", hide_index=True)

st.divider()

st.subheader("Daily Research Brief")
universes = list_available_universes()
if not universes:
    st.info("No universes configured yet.")
else:
    col1, col2 = st.columns([2, 1])
    with col1:
        universe = st.selectbox("Brief universe", universes, format_func=universe_label)
    with col2:
        min_score = st.slider("Opportunity score", 0, 100, 60, 5)

    brief = build_daily_research_brief(universe, min_opportunity_score=float(min_score))
    if not brief.has_latest_scan:
        st.info("No saved scan exists yet. Open Top Opportunities and run a small scan.")
    else:
        status, age = daily_brief_freshness(brief.latest_run_at)
        st.markdown(f"**{daily_brief_headline(brief)}**")
        if status == "fresh":
            st.success(f"Latest scan is fresh: {age}.")
        elif status == "aging":
            st.warning(f"Latest scan is {age}. Consider refreshing before decisions.")
        else:
            st.warning(f"Latest scan is {age}. Run a fresh scan before relying on new decisions.")

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Run", f"#{brief.latest_run_id}")
        m2.metric("Successful", brief.successful_symbols)
        m3.metric("Failed", brief.failed_symbols)
        m4.metric(
            "Average score", "N/A" if brief.average_score is None else f"{brief.average_score:.1f}"
        )
        m5.metric("Top score", "N/A" if brief.top_score is None else f"{brief.top_score:.1f}")

        tabs = st.tabs(["New opportunities", "Score movers", "New signals", "Action queue"])
        with tabs[0]:
            research_pick_button(daily_brief_table(brief.new_opportunities), key="market_new_opps")
        with tabs[1]:
            improved = daily_brief_table(brief.improved)
            weakened = daily_brief_table(brief.weakened)
            left, right = st.columns(2)
            with left:
                st.markdown("##### Improved")
                research_pick_button(improved, key="market_improved")
            with right:
                st.markdown("##### Weakened")
                research_pick_button(weakened, key="market_weakened")
        with tabs[2]:
            research_pick_button(daily_brief_table(brief.new_signals), key="market_new_signals")
        with tabs[3]:
            action_frame = daily_brief_table(brief.data_quality_actions, limit=15)
            if action_frame.empty:
                st.success("No saved-scan data-quality action rows.")
            else:
                st.dataframe(action_frame, width="stretch", hide_index=True)

if latest_run:
    st.caption(
        f"Last refresh #{latest_run.run_id}: {latest_run.status}, "
        f"{latest_run.successful_symbols} successful, {latest_run.failed_symbols} failed."
    )
