"""Data Health page."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from stock_platform.ops import build_data_health_report
from stock_platform.ui.components.layout import render_page_shell

render_page_shell(
    "Data Health",
    "Freshness, coverage, provider status, and refresh-run audit trail.",
)

report = build_data_health_report()
latest_run = report.recent_refresh_runs[0] if report.recent_refresh_runs else None

cols = st.columns(4)
cols[0].metric("Last refresh", f"#{latest_run.run_id}" if latest_run else "None")
cols[1].metric("Refresh status", latest_run.status.title() if latest_run else "No refresh")
cols[2].metric("Kite token", "Configured" if report.kite_token.configured else "Missing")
cols[3].metric("Generated", report.generated_at.strftime("%H:%M:%S"))

coverage_cols = st.columns(3)
price = report.price_coverage
score = report.composite_score_coverage
inst = report.instrument_coverage
coverage_cols[0].metric("Price rows", f"{price.total_rows:,}" if price else "0")
coverage_cols[1].metric("Composite rows", f"{score.total_rows:,}" if score else "0")
coverage_cols[2].metric("Instrument rows", f"{inst.total:,}" if inst else "0")

st.subheader("Recent Refresh Runs")
if not report.recent_refresh_runs:
    st.info("No refresh runs recorded yet.")
else:
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "run_id": run.run_id,
                    "universe": run.universe,
                    "status": run.status,
                    "successful": run.successful_symbols,
                    "failed": run.failed_symbols,
                    "price_rows": run.price_rows_upserted,
                    "indicator_rows": run.technical_rows_upserted,
                    "finished_at": run.finished_at,
                }
                for run in report.recent_refresh_runs
            ]
        ),
        width="stretch",
        hide_index=True,
    )

st.subheader("Stale Symbols")
if not report.stale_symbols:
    st.success("No stale persisted symbols at the current threshold.")
else:
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "symbol": row.symbol,
                    "latest_trade_date": row.latest_trade_date,
                    "days_stale": row.days_stale,
                }
                for row in report.stale_symbols
            ]
        ),
        width="stretch",
        hide_index=True,
    )

st.subheader("Source Mix")
if price and price.by_source:
    st.dataframe(
        pd.DataFrame(
            [{"source": source, "rows": rows} for source, rows in price.by_source.items()]
        ),
        width="stretch",
        hide_index=True,
    )
else:
    st.info("No persisted price rows yet.")
