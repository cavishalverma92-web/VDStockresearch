"""Backtests page."""

from __future__ import annotations

import streamlit as st

from stock_platform.analytics.backtest import (
    compute_portfolio_metrics,
    portfolio_metrics_to_frame,
    run_signal_backtest,
    run_walk_forward_validation,
    summaries_to_frame,
    trades_to_frame,
)
from stock_platform.analytics.signals.audit import fetch_signal_event_export
from stock_platform.data.providers import YahooFinanceProvider
from stock_platform.ui.components.layout import render_page_shell

render_page_shell(
    "Backtests",
    "Directional signal backtests from saved signal observations. Not investment advice.",
)

symbol = st.text_input("Optional symbol filter", value="")
holding_days = st.select_slider("Holding period", options=[5, 10, 20, 60], value=20)
scope_symbol = symbol.strip().upper() or None

if st.button("Run backtest", type="primary"):
    events = fetch_signal_event_export(symbol=scope_symbol, active_only=True)
    if events.empty:
        st.info("No active signal events saved yet. Open Stock Research for a few symbols first.")
        st.stop()
    provider = YahooFinanceProvider()
    trades, summaries = run_signal_backtest(
        events, price_provider=provider, holding_days=holding_days
    )
    completed = [trade for trade in trades if trade.return_pct is not None]
    pending = [trade for trade in trades if trade.return_pct is None]
    st.markdown(
        f"**{len(completed)}** completed trade(s) | **{len(pending)}** too recent | "
        f"**{holding_days}-day** holding period"
    )
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
