"""Market Today dashboard."""

from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from stock_platform.analytics.scanner import (
    build_daily_research_brief,
    daily_brief_freshness,
    daily_brief_headline,
    daily_brief_table,
    list_available_universes,
)
from stock_platform.data.providers import MarketDataProvider
from stock_platform.db import create_all_tables, get_engine
from stock_platform.db.models import CompositeScoreSnapshot
from stock_platform.ops import build_data_health_report
from stock_platform.ui.components.common import research_pick_button, universe_label
from stock_platform.ui.components.layout import render_page_shell

render_page_shell(
    "Market Today",
    "A focused morning dashboard: data freshness, provider health, score movers, and attention queue.",
)

health = build_data_health_report()
latest_run = health.recent_refresh_runs[0] if health.recent_refresh_runs else None

top_cols = st.columns(4)
top_cols[0].metric("Provider health", latest_run.status.title() if latest_run else "No refresh")
top_cols[1].metric(
    "Latest refresh",
    f"#{latest_run.run_id}" if latest_run else "None",
    help="Green/amber/red logic will become stricter once daily refreshes are scheduled.",
)
top_cols[2].metric(
    "Persisted price rows",
    f"{health.price_coverage.total_rows:,}" if health.price_coverage else "0",
)
top_cols[3].metric(
    "Kite token",
    "Configured" if health.kite_token.configured else "Missing",
    help="Kite token usually needs a fresh login each trading day.",
)

if health.stale_symbols:
    st.warning(
        f"{len(health.stale_symbols)} persisted symbol(s) are stale at the "
        f"{health.stale_threshold_days}-day threshold. Open Data Health for details."
    )
else:
    st.success("No stale persisted symbols at the current threshold.")

st.subheader("Market Snapshot")
if st.button("Refresh live index snapshot"):
    provider = MarketDataProvider()
    try:
        snapshot = provider.get_ltp(["NIFTY 50", "NIFTY BANK", "NIFTY MIDCAP 100"])
    except Exception as exc:  # noqa: BLE001
        snapshot = pd.DataFrame()
        st.warning(f"Live index snapshot unavailable: {type(exc).__name__}")
    if snapshot.empty:
        st.info("No live index rows returned. This can happen if Kite index symbols differ.")
    else:
        safe_cols = [col for col in ["symbol", "exchange", "ltp", "source"] if col in snapshot]
        st.dataframe(snapshot[safe_cols], width="stretch", hide_index=True)
else:
    st.caption(
        "Click once when you want a fresh Kite LTP check. The rest of this page uses saved data."
    )

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

st.subheader("Top Persisted Research Conviction")
engine = get_engine()
create_all_tables(engine)
with Session(engine) as session:
    rows = session.scalars(
        select(CompositeScoreSnapshot)
        .order_by(desc(CompositeScoreSnapshot.as_of_date), desc(CompositeScoreSnapshot.score))
        .limit(10)
    ).all()

if not rows:
    st.info("No persisted composite scores yet. Run the EOD refresh job to populate this.")
else:
    top_frame = pd.DataFrame(
        [
            {
                "symbol": row.symbol,
                "as_of_date": row.as_of_date,
                "score": row.score,
                "band": row.band,
                "signals": row.active_signal_count,
                "source": row.source,
            }
            for row in rows
        ]
    )
    research_pick_button(top_frame, key="market_top_scores")
