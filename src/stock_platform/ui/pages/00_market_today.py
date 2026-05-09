"""Market Today dashboard."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from stock_platform.analytics.flows import compute_institutional_flow_snapshots
from stock_platform.analytics.scanner import fetch_latest_strategy_scan, top_clean_strategy_hits
from stock_platform.data.providers import MarketDataProvider
from stock_platform.data.repositories import fetch_market_flows
from stock_platform.db import get_session
from stock_platform.ops import build_market_today_summary
from stock_platform.ui.components.common import render_hosted_demo_empty_state, research_pick_button
from stock_platform.ui.components.layout import render_page_shell


def _brief_card(label: str, value: str, note: str) -> None:
    st.markdown(
        f"""
        <div class="brief-card">
          <div class="brief-label">{label}</div>
          <div class="brief-value">{value}</div>
          <div class="brief-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _source_mix_note(price_coverage) -> str:
    if not price_coverage or not price_coverage.total_rows:
        return "Source mix pending until the first EOD refresh."
    total = max(1, int(price_coverage.total_rows))
    rows = sorted(price_coverage.by_source.items(), key=lambda item: item[1], reverse=True)
    if not rows:
        return "Source mix not labelled yet."
    source, count = rows[0]
    pct = (int(count) / total) * 100
    return f"Main source: {str(source).title()} ({pct:.0f}% of saved rows)."


def _latest_bar_label(price_coverage) -> tuple[str, str]:
    if not price_coverage or price_coverage.newest_trade_date is None:
        return "No saved bars", "Run an EOD refresh before using the daily brief."
    return (
        price_coverage.newest_trade_date.isoformat(),
        f"{price_coverage.distinct_symbols:,} symbol(s), {price_coverage.total_rows:,} rows saved.",
    )


def _brief_action_notes(summary, latest_run, top_strategy_hits: pd.DataFrame) -> list[str]:
    notes: list[str] = []
    if latest_run is None:
        notes.append("Run Data Health refresh first; the homepage has no completed EOD run yet.")
    elif latest_run.failed_symbols:
        notes.append(
            f"Review Data Health: last refresh had {latest_run.failed_symbols} failed symbol(s)."
        )
    if top_strategy_hits.empty:
        notes.append("Run Strategy Scanner after refresh to populate clean setup candidates.")
    else:
        notes.append("Review the top clean strategy setups before looking at lower-trust rows.")
    if not summary.stale_symbols.empty:
        notes.append(f"Refresh or exclude {len(summary.stale_symbols)} stale symbol(s).")
    if not summary.upcoming_events.empty:
        notes.append("Check event risk before relying on any fresh signal.")
    return notes[:4]


render_page_shell(
    "Market Today",
    "Morning snapshot: data freshness, breadth, and what changed in the saved scores.",
)

summary = build_market_today_summary()
health = summary.health
latest_run = health.recent_refresh_runs[0] if health.recent_refresh_runs else None
breadth = summary.breadth
token = summary.kite_token
price_coverage = health.price_coverage
latest_strategy_run = fetch_latest_strategy_scan()
top_strategy_hits = top_clean_strategy_hits(latest_strategy_run, limit=5)

# --- KPI strip (4 tiles, no scroll-wrap on a 13" screen) ----------------------
k1, k2, k3, k4 = st.columns(4)
k1.metric(
    "Provider",
    summary.provider_health.label,
    help=summary.provider_health.detail,
)
k2.metric(
    "Breadth (A/D)",
    f"{breadth.advances} / {breadth.declines}",
    delta=None
    if breadth.advance_decline_ratio is None
    else f"ratio {breadth.advance_decline_ratio:.2f}",
    help="Advances / declines from latest two persisted daily closes.",
)
k3.metric(
    "Score coverage",
    f"{health.composite_score_coverage.total_rows:,}" if health.composite_score_coverage else "0",
    help="Persisted Research Conviction snapshots available locally.",
)
k4.metric(
    "Kite token",
    token.status.title(),
    help=token.message,
)

# Inline status banner (single, not stacked).
banner = []
if summary.provider_health.color != "green":
    banner.append((summary.provider_health.color, summary.provider_health.detail))
if token.status in {"missing", "expired", "warning"}:
    banner.append(("amber", token.message))
if breadth.latest_trade_date is None:
    banner.append(("amber", "Breadth will appear after the first EOD refresh."))

if banner:
    color = "red" if any(c == "red" for c, _ in banner) else "amber"
    msg = " · ".join(m for _, m in banner)
    (st.error if color == "red" else st.warning)(msg)

if (
    latest_run is None
    and health.composite_score_coverage is not None
    and health.composite_score_coverage.total_rows == 0
):
    render_hosted_demo_empty_state(page="Market Today")

st.subheader("Daily Brief")
st.caption("A compact queue for what deserves attention today. Research aid only.")
latest_bar_value, latest_bar_note = _latest_bar_label(price_coverage)
brief_cols = st.columns(4)
with brief_cols[0]:
    _brief_card("Data status", summary.provider_health.label, summary.provider_health.detail)
with brief_cols[1]:
    _brief_card("Latest saved bar", latest_bar_value, latest_bar_note)
with brief_cols[2]:
    strategy_value = "No scan" if latest_strategy_run is None else str(len(top_strategy_hits))
    strategy_note = (
        "Run Strategy Scanner after EOD refresh."
        if latest_strategy_run is None
        else f"Clean rows from scan #{latest_strategy_run.id}."
    )
    _brief_card("Clean setups", strategy_value, strategy_note)
with brief_cols[3]:
    risk_count = len(summary.stale_symbols) + len(summary.upcoming_events)
    _brief_card(
        "Risk queue",
        str(risk_count),
        f"{len(summary.stale_symbols)} stale, {len(summary.upcoming_events)} event item(s).",
    )

notes = _brief_action_notes(summary, latest_run, top_strategy_hits)
if notes:
    st.info("Daily focus: " + " ".join(notes))
else:
    st.success(
        "Daily focus: saved data is current, clean setups are available, and no risk queue is flagged."
    )

if price_coverage and price_coverage.total_rows:
    st.caption("Persisted market-data source mix. " + _source_mix_note(price_coverage))
    source_total = max(1, int(price_coverage.total_rows))
    mix_cols = st.columns(4)
    source_rows = sorted(
        price_coverage.by_source.items(),
        key=lambda item: item[1],
        reverse=True,
    )
    for idx, (source, rows) in enumerate(source_rows[:4]):
        pct = (int(rows) / source_total) * 100
        mix_cols[idx].metric(str(source).title(), f"{int(rows):,}", delta=f"{pct:.1f}%")
    if len(source_rows) > 4:
        st.caption(
            "Other sources: " + ", ".join(f"{source} {rows:,}" for source, rows in source_rows[4:])
        )
else:
    st.caption("Persisted market-data source mix will appear after the first refresh.")

st.divider()

# --- What changed today (single tabbed panel) ---------------------------------
st.subheader("What changed today")
st.caption(
    f"From the latest saved scores. Breadth basis: "
    f"{breadth.compared_symbols} symbol(s), bar {breadth.latest_trade_date or 'N/A'}."
)

if summary.score_movers.empty:
    improving = weakening = pd.DataFrame()
else:
    improving = summary.score_movers[summary.score_movers["score_change"] > 0]
    weakening = summary.score_movers[summary.score_movers["score_change"] < 0]

tab_top, tab_up, tab_down, tab_events = st.tabs(
    ["Top attention", "Improving", "Weakening", "Events"]
)
with tab_top:
    if summary.top_attention.empty:
        st.info("No persisted score rows yet. Run the EOD refresh job to populate this.")
    else:
        research_pick_button(summary.top_attention, key="market_attention")
with tab_up:
    if improving.empty:
        st.info("No improving score movers in saved data yet.")
    else:
        research_pick_button(improving, key="market_score_up")
with tab_down:
    if weakening.empty:
        st.info("No weakening score movers in saved data yet.")
    else:
        research_pick_button(weakening, key="market_score_down")
with tab_events:
    if summary.upcoming_events.empty:
        st.info("No saved upcoming events in the next 5 trading days.")
    else:
        research_pick_button(summary.upcoming_events, key="market_events")

st.divider()

# --- Top strategy setups ------------------------------------------------------
st.subheader("Top clean strategy setups")
if latest_strategy_run is None:
    st.info("No saved strategy scan yet. Run Strategy Scanner after EOD refresh.")
elif top_strategy_hits.empty:
    st.warning(
        "Latest strategy scan has no clean, liquid setups after default trust filters. "
        "Open Strategy Scanner to review warnings manually."
    )
else:
    st.caption(
        f"From strategy scan #{latest_strategy_run.id} on {latest_strategy_run.created_at}. "
        "Only Good data + Pass liquidity rows are shown here."
    )
    research_pick_button(top_strategy_hits, key="market_strategy_setups")

st.divider()

# --- FII / DII institutional flows -------------------------------------------
st.subheader("Institutional flows")
st.caption("FII/DII cash market activity from NSE provisional figures.")

try:
    with get_session() as _flow_session:
        flows_frame = fetch_market_flows(_flow_session, source="nse")
except Exception as _exc:  # noqa: BLE001
    flows_frame = pd.DataFrame()
    st.warning(f"Could not load institutional flows: {type(_exc).__name__}")

if flows_frame.empty:
    st.info(
        "No FII/DII rows persisted yet. Run "
        "`python -m stock_platform.jobs.refresh_market_flows` to fetch the latest."
    )
else:
    snapshots = compute_institutional_flow_snapshots(flows_frame)
    fii = snapshots.get("FII")
    dii = snapshots.get("DII")
    f1, f2, f3, f4 = st.columns(4)
    f1.metric(
        "FII latest net (Cr)",
        "N/A" if fii is None or fii.latest_net_cr is None else f"{fii.latest_net_cr:+,.0f}",
        help=f"As of {fii.latest_date.date() if fii and fii.latest_date is not None else 'N/A'}",
    )
    f2.metric(
        "FII 5d net (Cr)",
        "N/A" if fii is None or fii.rolling_5d_net_cr is None else f"{fii.rolling_5d_net_cr:+,.0f}",
        delta=None if fii is None else fii.trend.title(),
    )
    f3.metric(
        "DII latest net (Cr)",
        "N/A" if dii is None or dii.latest_net_cr is None else f"{dii.latest_net_cr:+,.0f}",
        help=f"As of {dii.latest_date.date() if dii and dii.latest_date is not None else 'N/A'}",
    )
    f4.metric(
        "DII 5d net (Cr)",
        "N/A" if dii is None or dii.rolling_5d_net_cr is None else f"{dii.rolling_5d_net_cr:+,.0f}",
        delta=None if dii is None else dii.trend.title(),
    )

    with st.expander("Recent FII/DII rows"):
        st.dataframe(
            flows_frame.tail(20).sort_values(
                ["trade_date", "participant"], ascending=[False, True]
            ),
            width="stretch",
            hide_index=True,
        )

st.divider()

# --- Action queue (data hygiene + live snapshot in one row) -------------------
st.subheader("Action queue")
left, right = st.columns([2, 1])

with left:
    if summary.stale_symbols.empty:
        st.success("No stale persisted symbols at the current threshold.")
    else:
        st.warning(f"{len(summary.stale_symbols)} stale symbol(s) need refresh.")
        st.dataframe(summary.stale_symbols, width="stretch", hide_index=True)

with right:
    st.caption("Optional: live index check via provider router.")
    if st.button("Refresh live index snapshot"):
        provider = MarketDataProvider()
        try:
            snapshot = provider.get_ltp(["NIFTY 50", "NIFTY BANK", "NIFTY MIDCAP 100"])
        except Exception as exc:  # noqa: BLE001
            snapshot = pd.DataFrame()
            st.warning(f"Live index snapshot unavailable: {type(exc).__name__}")
        if snapshot.empty:
            st.info("No live index rows returned.")
        else:
            safe_cols = [c for c in ["symbol", "exchange", "ltp", "source"] if c in snapshot]
            st.dataframe(snapshot[safe_cols], width="stretch", hide_index=True)

if latest_run:
    st.caption(
        f"Last refresh #{latest_run.run_id}: {latest_run.status} · "
        f"{latest_run.successful_symbols} ok · {latest_run.failed_symbols} failed. "
        f"For the universe-level Daily Brief, see Top Opportunities."
    )
