"""Research watchlist page."""

from __future__ import annotations

import streamlit as st

from stock_platform.analytics.scanner import (
    enrich_watchlist_with_latest_scores,
    fetch_watchlist_items,
    update_watchlist_reviews,
    watchlist_to_frame,
)
from stock_platform.ui.components.common import research_pick_button
from stock_platform.ui.components.layout import render_page_shell

render_page_shell("Watchlist", "Local research shortlist and review notes.")

show_inactive = st.checkbox("Show inactive rows", value=False)
frame = watchlist_to_frame(fetch_watchlist_items(active_only=not show_inactive))
if frame.empty:
    st.info("No shortlisted stocks yet. Add symbols from Top Opportunities.")
    st.stop()

frame = enrich_watchlist_with_latest_scores(frame)
display = frame[
    [
        "symbol",
        "review_status",
        "tags",
        "notes",
        "active",
        "latest_score",
        "latest_band",
        "latest_active_signals",
        "latest_close",
        "latest_run_id",
        "source_universe",
        "reason",
        "updated_at",
    ]
].copy()

edited = st.data_editor(
    display,
    width="stretch",
    hide_index=True,
    column_config={
        "review_status": st.column_config.SelectboxColumn(
            "Review status", options=["watch", "deep_dive", "avoid", "done"]
        ),
        "active": st.column_config.CheckboxColumn("Active"),
        "latest_score": st.column_config.NumberColumn("Latest score", format="%.1f"),
        "latest_close": st.column_config.NumberColumn("Last close", format="%.2f"),
    },
    disabled=[
        "symbol",
        "latest_score",
        "latest_active_signals",
        "latest_close",
        "latest_run_id",
        "latest_band",
        "source_universe",
        "reason",
        "updated_at",
    ],
)

col1, col2 = st.columns([1, 2])
with col1:
    if st.button("Save review edits"):
        count = update_watchlist_reviews(edited.to_dict(orient="records"))
        st.success(f"Saved review edits for {count} row(s).")
        st.rerun()
with col2:
    st.download_button(
        "Download watchlist CSV",
        data=edited.to_csv(index=False),
        file_name="research_watchlist.csv",
        mime="text/csv",
    )

st.subheader("Open a stock")
research_pick_button(
    edited[["symbol", "latest_score", "latest_band", "review_status"]], key="watchlist_open"
)
