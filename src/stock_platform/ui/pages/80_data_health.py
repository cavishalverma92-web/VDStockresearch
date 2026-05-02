"""Data Health page."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from stock_platform.analytics.scanner import list_available_universes
from stock_platform.jobs.refresh_eod_candles import RefreshSummary, refresh_eod_candles
from stock_platform.ops import build_data_health_report
from stock_platform.ui.components.layout import render_page_shell

render_page_shell(
    "Data Health",
    "Freshness, coverage, provider status, and refresh-run audit trail.",
)

report = build_data_health_report()
latest_run = report.recent_refresh_runs[0] if report.recent_refresh_runs else None


def _summary_frame(summary: RefreshSummary) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": outcome.symbol,
                "status": (
                    "failed"
                    if outcome.error
                    else ("skipped" if outcome.skipped_reason else "completed")
                ),
                "source": outcome.source,
                "fetched_rows": outcome.fetched_rows,
                "price_rows": outcome.price_rows_inserted + outcome.price_rows_updated,
                "indicator_rows": outcome.technical_rows_inserted + outcome.technical_rows_updated,
                "score": outcome.composite_score,
                "note": outcome.error or outcome.skipped_reason or "",
            }
            for outcome in summary.outcomes
        ]
    )


st.subheader("Run Daily EOD Refresh")
st.caption(
    "Manual local refresh for prices, indicators, signal audit, and persisted Research "
    "Conviction scores. This does not touch portfolio, holdings, funds, orders, or trading APIs."
)

universes = list_available_universes()
if not universes:
    st.info("No configured universe list found yet.")
else:
    with st.form("manual_eod_refresh_form"):
        left, middle, right = st.columns(3)
        with left:
            refresh_universe = st.selectbox("Universe", universes, index=0)
            dry_run = st.checkbox(
                "Dry run only",
                value=True,
                help="Fetch and validate data without writing rows to the database.",
            )
        with middle:
            max_symbols = st.number_input(
                "Max symbols",
                min_value=1,
                max_value=500,
                value=5,
                step=1,
                help="Use a small number first. Increase after a successful test run.",
            )
            initial_history_days = st.number_input(
                "Initial backfill days",
                min_value=30,
                max_value=3650,
                value=365 * 5,
                step=30,
            )
        with right:
            overlap_days = st.number_input(
                "Overlap days",
                min_value=0,
                max_value=30,
                value=5,
                step=1,
                help="Recent days to re-fetch so corrected bars can update.",
            )
            note = st.text_input("Run note", value="manual Streamlit refresh")

        run_refresh = st.form_submit_button(
            "Run EOD Refresh",
            type="primary",
            help="Start with Dry run only. Uncheck dry run when the result looks sensible.",
        )

    if run_refresh:
        with st.spinner("Refreshing saved market data. This can take a few minutes..."):
            summary = refresh_eod_candles(
                universe=refresh_universe,
                max_symbols=int(max_symbols),
                initial_history_days=int(initial_history_days),
                incremental_overlap_days=int(overlap_days),
                dry_run=bool(dry_run),
                note=note,
            )
        st.success(
            "Dry run completed." if summary.dry_run else f"Refresh run #{summary.run_id} completed."
        )
        run_cols = st.columns(5)
        run_cols[0].metric("Requested", summary.requested_symbols)
        run_cols[1].metric("Successful", summary.successful_symbols)
        run_cols[2].metric("Failed", summary.failed_symbols)
        run_cols[3].metric("Price rows", summary.price_rows_upserted)
        run_cols[4].metric("Scores", summary.composite_scores_saved)
        st.dataframe(_summary_frame(summary), width="stretch", hide_index=True)

st.divider()

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

st.subheader("Index Membership History")
membership = report.index_membership_coverage
if membership is None or membership.active_members == 0:
    st.warning(
        "No active Nifty 50 membership snapshot is recorded yet. Run "
        "`scripts\\refresh_index_membership.ps1 -Universe nifty_50`."
    )
else:
    member_cols = st.columns(4)
    member_cols[0].metric("Index", membership.index_name)
    member_cols[1].metric("Active members", membership.active_members)
    member_cols[2].metric("Total periods", membership.total_periods)
    member_cols[3].metric(
        "Historical backfill",
        "Ready" if membership.historical_backfill_ready else "Pending",
    )
    st.caption(
        "This guards backtests against using today's index list for old dates. "
        "Current snapshots are useful, but archived historical constituent files are still needed "
        "for fully survivorship-safe long-range backtests."
    )
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "index": membership.index_name,
                    "active_members": membership.active_members,
                    "total_periods": membership.total_periods,
                    "earliest_from_date": membership.earliest_from_date,
                    "latest_from_date": membership.latest_from_date,
                    "latest_observed_at": membership.latest_observed_at,
                    "source_url": membership.source_url,
                }
            ]
        ),
        width="stretch",
        hide_index=True,
    )
    if membership.warning:
        st.warning(membership.warning)

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
