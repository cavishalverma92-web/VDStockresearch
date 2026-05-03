"""Backtests page."""

from __future__ import annotations

import streamlit as st

from stock_platform.analytics.backtest import (
    compute_portfolio_metrics,
    filter_events_by_index_membership,
    portfolio_metrics_to_frame,
    run_signal_backtest,
    run_walk_forward_validation,
    summaries_to_frame,
    trades_to_frame,
)
from stock_platform.analytics.signals.audit import fetch_signal_event_export
from stock_platform.data.providers import YahooFinanceProvider
from stock_platform.db import get_engine, get_session
from stock_platform.ops import build_data_health_report
from stock_platform.ui.components.layout import render_page_shell

render_page_shell(
    "Backtests",
    "Directional signal backtests from saved signal observations. Not investment advice.",
)

symbol = st.text_input("Optional symbol filter", value="")
holding_days = st.select_slider("Holding period", options=[5, 10, 20, 60], value=20)
membership_filter = st.checkbox(
    "Filter by Nifty 50 membership on signal date",
    value=False,
    help=(
        "Use the index_membership_history table so signals are tested only when the stock "
        "belonged to Nifty 50 on that signal date."
    ),
)
scope_symbol = symbol.strip().upper() or None
health_report = build_data_health_report()
membership = health_report.index_membership_coverage

if membership_filter:
    if membership is None or membership.active_members == 0:
        st.error(
            "No active Nifty 50 membership snapshot is available yet. Run "
            "`scripts\\refresh_index_membership.ps1 -Universe nifty_50` first."
        )
    elif membership.warning:
        st.warning(membership.warning)
    st.caption(
        "Membership-aware backtests reduce survivorship bias. Full historical accuracy still "
        "depends on importing archived index constituent files."
    )

if st.button("Run backtest", type="primary"):
    events = fetch_signal_event_export(symbol=scope_symbol, active_only=True)
    if events.empty:
        st.info("No active signal events saved yet. Open Stock Research for a few symbols first.")
        st.stop()
    original_event_count = len(events)
    excluded_event_count = 0
    if membership_filter:
        if membership is None or membership.active_members == 0:
            st.stop()
        engine = get_engine()
        with get_session(engine) as session:
            events = filter_events_by_index_membership(events, session, "Nifty 50")
        excluded_event_count = original_event_count - len(events)
        st.info(
            f"Nifty 50 membership filter excluded {excluded_event_count} of "
            f"{original_event_count} active signal event(s)."
        )
        if events.empty:
            st.warning("No signal events remain after the Nifty 50 membership filter.")
            st.stop()
    provider = YahooFinanceProvider()
    trades, summaries = run_signal_backtest(
        events, price_provider=provider, holding_days=holding_days
    )
    completed = [trade for trade in trades if trade.return_pct is not None]
    pending = [trade for trade in trades if trade.return_pct is None]
    m1, m2, m3 = st.columns(3)
    m1.metric("Completed trades", len(completed))
    m2.metric("Too recent", len(pending), help="Signal fired but holding period not yet reached.")
    m3.metric("Holding period", f"{holding_days} days")
    if summaries:
        st.subheader("Per-signal summary")
        st.dataframe(summaries_to_frame(summaries), width="stretch", hide_index=True)
    if completed:
        st.subheader("Portfolio diagnostics")
        st.dataframe(
            portfolio_metrics_to_frame(compute_portfolio_metrics(trades)),
            width="stretch",
            hide_index=True,
        )
        walk_forward = run_walk_forward_validation(completed, train_years=3, validate_years=1)
        if walk_forward.empty:
            st.info("Walk-forward validation needs signals spread across more than three years.")
        else:
            st.dataframe(walk_forward, width="stretch", hide_index=True)
        trades_frame = trades_to_frame(completed)
        st.subheader("Trades")
        st.dataframe(trades_frame, width="stretch", hide_index=True)
        st.download_button(
            "Download backtest CSV",
            data=trades_frame.to_csv(index=False),
            file_name=f"signal_backtest_{holding_days}d.csv",
            mime="text/csv",
        )
