"""Strategy Scanner page."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from stock_platform.analytics.scanner.result_schema import (
    DEFAULT_STRATEGY_SCAN_COLUMNS,
    summarize_strategy_scan_frame,
)
from stock_platform.analytics.scanner.strategy_persistence import (
    fetch_latest_strategy_scan,
    save_strategy_scan,
    strategy_scan_errors,
    strategy_scan_storage_to_frame,
)
from stock_platform.analytics.scanner.strategy_scanner import (
    prepare_persisted_price_frame,
    scan_persisted_strategy_universe,
)
from stock_platform.analytics.scanner.universe_scanner import (
    list_available_universes,
    load_universe,
)
from stock_platform.analytics.technicals import add_technical_indicators
from stock_platform.data.repositories import fetch_price_daily
from stock_platform.db import get_session
from stock_platform.ui.components.common import (
    render_hosted_demo_empty_state,
    research_pick_button,
    universe_label,
)
from stock_platform.ui.components.layout import render_page_shell
from stock_platform.ui.components.price_chart import build_price_chart


def _result_key(symbol: object, strategy: object, signal_date: object) -> str:
    parsed_date = pd.to_datetime(signal_date, errors="coerce")
    date_label = str(signal_date) if pd.isna(parsed_date) else parsed_date.date().isoformat()
    return f"{str(symbol).upper()}|{strategy}|{date_label}"


def _result_option_label(row: pd.Series) -> str:
    confidence = pd.to_numeric(row.get("confidence_score"), errors="coerce")
    confidence_label = "N/A" if pd.isna(confidence) else f"{confidence:.0f}"
    return (
        f"{row.get('symbol')} | {row.get('strategy')} | {row.get('signal_date')} | "
        f"confidence {confidence_label}"
    )


@st.cache_data(ttl=300, show_spinner=False)
def _load_strategy_chart_data(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame, str, str]:
    with get_session() as session:
        raw_frame = fetch_price_daily(session, symbol)
    price_frame, source_label, source_warning = prepare_persisted_price_frame(raw_frame)
    if price_frame.empty:
        return price_frame, pd.DataFrame(), source_label, source_warning
    technical_frame = add_technical_indicators(price_frame)
    return price_frame.tail(360), technical_frame.tail(360), source_label, source_warning


render_page_shell(
    "Strategy Scanner",
    "Read-only swing-trading research setups from saved EOD data. Research aid only.",
)

st.caption(
    "This page scans locally persisted daily OHLCV. Run the EOD refresh from Data Health "
    "when data is stale. No trading, portfolio, holdings, funds, or order APIs are used."
)

universes = list_available_universes()
if not universes:
    st.warning("No universes configured. Add lists to config/universes.yaml.")
    st.stop()

top = st.columns([2, 1, 1, 1])
with top[0]:
    universe = st.selectbox("Universe", universes, format_func=universe_label)

try:
    symbols = load_universe(universe)
    universe_error = None
except (FileNotFoundError, KeyError) as exc:
    symbols = []
    universe_error = str(exc)

with top[1]:
    max_symbols = st.number_input(
        "Max symbols",
        min_value=1,
        max_value=max(1, len(symbols)),
        value=min(50, max(1, len(symbols))),
        step=5,
        disabled=not symbols,
    )
with top[2]:
    min_confidence = st.slider("Min confidence", 0, 90, 60, 5)
with top[3]:
    min_rr = st.slider("Min R/R", 0.0, 5.0, 1.5, 0.5)

if universe_error:
    st.warning(universe_error)
    st.stop()

st.caption(f"{universe_label(universe)} contains {len(symbols):,} symbol(s).")

run_scan = st.button("Scan saved EOD data", type="primary", disabled=not symbols)
if run_scan:
    with st.spinner("Scanning saved OHLCV strategy setups..."):
        summary = scan_persisted_strategy_universe(
            universe,
            max_symbols=int(max_symbols),
        )
    run_id = save_strategy_scan(
        universe_name=universe,
        summary=summary,
        min_confidence_filter=float(min_confidence),
        min_rr_filter=float(min_rr),
        note=f"UI strategy scan capped at {int(max_symbols)} symbol(s).",
    )
    st.success(
        f"Saved strategy scan #{run_id}: {len(summary.results)} setup(s), "
        f"{summary.failed_symbols} data action(s)."
    )

latest_run = fetch_latest_strategy_scan(universe)

if latest_run is None:
    render_hosted_demo_empty_state(page="Strategy Scanner")
    st.info(
        "Run a scan to see EMA stack, RSI momentum, and EMA pullback setups from "
        "your saved EOD database."
    )
    st.stop()

metrics = st.columns(4)
metrics[0].metric("Latest run", f"#{latest_run.id}")
metrics[1].metric("Scanned", latest_run.scanned_symbols)
metrics[2].metric("Setups found", latest_run.result_count)
metrics[3].metric("Data actions", latest_run.failed_symbols)

st.caption(
    f"Latest saved scan: {latest_run.created_at} | "
    f"source: {latest_run.source} | requested: {latest_run.requested_symbols}"
)

frame = strategy_scan_storage_to_frame(latest_run)
if frame.empty:
    st.info("No strategy setups matched the first MVP rules in this saved data.")
else:
    summary_cards = summarize_strategy_scan_frame(frame)
    st.subheader("Scan Summary")
    card_row_1 = st.columns(4)
    card_row_1[0].metric("Total setups", summary_cards.total_setups)
    card_row_1[1].metric("Unique symbols", summary_cards.unique_symbols)
    card_row_1[2].metric("Clean setups", summary_cards.clean_setups)
    card_row_1[3].metric("Needs review", summary_cards.warning_setups)

    card_row_2 = st.columns(4)
    card_row_2[0].metric("Do not trust", summary_cards.untrusted_setups)
    card_row_2[1].metric("Breakouts", summary_cards.breakout_setups)
    card_row_2[2].metric("Top strategy", summary_cards.top_strategy)
    card_row_2[3].metric("Top strategy rows", summary_cards.top_strategy_count)

    filters = st.columns([2, 1, 1, 1])
    with filters[0]:
        selected_strategies = st.multiselect(
            "Strategy",
            sorted(frame["strategy"].dropna().unique()),
            default=sorted(frame["strategy"].dropna().unique()),
        )
    with filters[1]:
        liquidity_options = sorted(frame["liquidity_status"].dropna().unique())
        default_liquidity = [
            value for value in liquidity_options if value in {"Pass", "Watch", "Unknown"}
        ]
        selected_liquidity = st.multiselect(
            "Liquidity",
            liquidity_options,
            default=default_liquidity or liquidity_options,
        )
    with filters[2]:
        min_rsi, max_rsi = st.slider("RSI range", 0, 100, (35, 80), 5)
    with filters[3]:
        selected_sources = st.multiselect(
            "Data source",
            sorted(frame["data_source"].dropna().unique()),
            default=sorted(frame["data_source"].dropna().unique()),
        )

    risk_filters = st.columns([1, 1, 1])
    with risk_filters[0]:
        trust_options = sorted(frame["data_trust"].dropna().unique())
        default_trust = [value for value in trust_options if value != "Do not trust signal"]
        selected_trust = st.multiselect(
            "Data trust",
            trust_options,
            default=default_trust or trust_options,
        )
    with risk_filters[1]:
        min_traded_value = st.number_input(
            "Min traded value (INR cr)",
            min_value=0.0,
            max_value=500.0,
            value=5.0,
            step=1.0,
        )
    with risk_filters[2]:
        max_atr_pct = st.number_input(
            "Max ATR %",
            min_value=0.0,
            max_value=50.0,
            value=12.0,
            step=1.0,
        )

    filtered = frame.copy()
    if selected_strategies:
        filtered = filtered[filtered["strategy"].isin(selected_strategies)]
    if selected_liquidity:
        filtered = filtered[filtered["liquidity_status"].isin(selected_liquidity)]
    if selected_sources:
        filtered = filtered[filtered["data_source"].isin(selected_sources)]
    if selected_trust:
        filtered = filtered[filtered["data_trust"].isin(selected_trust)]
    filtered = filtered[
        (filtered["confidence_score"].fillna(0) >= float(min_confidence))
        & (filtered["risk_reward"].fillna(0) >= float(min_rr))
    ]
    if "avg_traded_value_cr" in filtered.columns:
        traded_values = pd.to_numeric(filtered["avg_traded_value_cr"], errors="coerce")
        filtered = filtered[(traded_values.isna()) | (traded_values >= float(min_traded_value))]
    if "atr_pct" in filtered.columns:
        atr_values = pd.to_numeric(filtered["atr_pct"], errors="coerce")
        filtered = filtered[(atr_values.isna()) | (atr_values <= float(max_atr_pct))]
    if "rsi" in filtered.columns:
        rsi_values = pd.to_numeric(filtered["rsi"], errors="coerce")
        filtered = filtered[(rsi_values.isna()) | (rsi_values.between(min_rsi, max_rsi))]
    filtered = filtered.copy()
    filtered["_result_key"] = filtered.apply(
        lambda row: _result_key(row["symbol"], row["strategy"], row["signal_date"]),
        axis=1,
    )

    default_cols = [column for column in DEFAULT_STRATEGY_SCAN_COLUMNS if column in filtered]
    st.subheader("Strategy Results")
    st.caption(
        "Default filters hide low-liquidity and do-not-trust rows. Expand filters to review "
        "them manually; this is still a research aid, not investment advice."
    )
    research_pick_button(filtered[default_cols], key="strategy_scanner_results")

    result_lookup = {
        _result_key(result.symbol, result.strategy, result.signal_date): result
        for result in latest_run.results
    }
    chart_options = [
        key for key in filtered["_result_key"].dropna().astype(str).tolist() if key in result_lookup
    ]
    if chart_options:
        st.subheader("Strategy Chart")
        option_labels = {
            str(row["_result_key"]): _result_option_label(row)
            for _, row in filtered.iterrows()
            if str(row["_result_key"]) in result_lookup
        }
        selected_key = st.selectbox(
            "Chart setup",
            chart_options,
            format_func=lambda key: option_labels.get(key, key),
        )
        selected_result = result_lookup[selected_key]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Symbol", selected_result.symbol)
        c2.metric("Strategy", selected_result.strategy)
        c3.metric("Signal date", selected_result.signal_date.isoformat())
        c4.metric("Data trust", selected_result.data_trust)

        st.caption(selected_result.why_this_appeared)
        if selected_result.key_risk:
            st.warning(selected_result.key_risk)

        chart_controls = st.columns(4)
        show_volume = chart_controls[0].checkbox("Volume", value=True, key="strategy_chart_volume")
        show_20 = chart_controls[1].checkbox("20 EMA", value=True, key="strategy_chart_20")
        show_200 = chart_controls[2].checkbox("200 EMA", value=True, key="strategy_chart_200")
        show_52w = chart_controls[3].checkbox("52W levels", value=False, key="strategy_chart_52w")

        price_frame, technical_frame, chart_source, source_warning = _load_strategy_chart_data(
            selected_result.symbol
        )
        if price_frame.empty:
            st.info("No persisted OHLCV is available for this setup yet. Run EOD refresh first.")
        else:
            if source_warning:
                st.warning(source_warning)
            chart_signal = {
                "name": selected_result.strategy,
                "entry_zone_low": selected_result.entry_zone_low,
                "entry_zone_high": selected_result.entry_zone_high,
                "stop_loss": selected_result.stop_loss,
                "target_price": selected_result.target_price,
                "breakout_level": selected_result.breakout_level,
            }
            fig = build_price_chart(
                price_frame,
                technical_frame,
                symbol=selected_result.symbol,
                source_label=chart_source,
                show_20_ema=show_20,
                show_200_ema=show_200,
                show_52w=show_52w,
                show_volume=show_volume,
                active_signals=[chart_signal],
                freshness_note=selected_result.data_freshness,
            )
            st.plotly_chart(fig, width="stretch")

    with st.expander("Advanced scanner columns"):
        advanced_cols = [
            column
            for column in filtered.columns
            if column not in default_cols and column not in {"company_name", "_result_key"}
        ]
        st.dataframe(
            filtered[[*default_cols[:1], *advanced_cols]], width="stretch", hide_index=True
        )

    st.download_button(
        "Download strategy scan CSV",
        data=filtered.to_csv(index=False),
        file_name=f"{universe}_strategy_scan.csv",
        mime="text/csv",
    )

errors = strategy_scan_errors(latest_run)
if errors:
    with st.expander("Data actions and skipped symbols", expanded=False):
        error_frame = pd.DataFrame(
            [{"symbol": symbol, "note": note} for symbol, note in errors.items()]
        )
        st.dataframe(error_frame, width="stretch", hide_index=True)
