"""Stock Research page."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from stock_platform.analytics.flows import (
    compute_delivery_analytics,
    compute_result_volatility,
    delivery_stats,
)
from stock_platform.analytics.fundamentals import (
    calculate_altman_z_score,
    calculate_basic_ratios,
    calculate_growth,
    calculate_piotroski_f_score,
    compute_multi_year_cagr,
    is_financial_sector,
)
from stock_platform.analytics.fundamentals.summary import build_fundamentals_summary
from stock_platform.config import get_settings, get_universe_config
from stock_platform.data.providers import CsvFundamentalsProvider, YFinanceFundamentalsProvider
from stock_platform.data.providers.corporate_actions import (
    days_to_next_earnings,
    get_earnings_history,
    get_upcoming_earnings,
)
from stock_platform.data.providers.nse import fetch_delivery_data
from stock_platform.ops import build_data_trust_rows, data_trust_level, data_trust_rows_to_frame
from stock_platform.scoring import score_stock
from stock_platform.ui.components.common import (
    active_signal_names,
    format_currency,
    format_number,
    format_pct,
    format_score,
    help_text,
    position_size,
    pros_cons,
    render_verdict_card,
    research_stance,
    resolve_project_path,
    risk_per_share,
    unique_symbols,
)
from stock_platform.ui.components.layout import render_page_shell
from stock_platform.ui.components.price_chart import build_price_chart
from stock_platform.ui.components.stock_context import load_stock_context, render_stock_sidebar

render_page_shell(
    "Stock Research", "Drill into one stock without loading the whole platform scroll."
)
inputs = render_stock_sidebar()
ctx = load_stock_context(inputs)
symbol = inputs.symbol
df = ctx.df
latest = df.iloc[-1]
prev = df.iloc[-2] if len(df) > 1 else latest
pct = ((latest["close"] - prev["close"]) / prev["close"]) * 100 if prev["close"] else 0.0

c1, c2, c3 = st.columns(3)
c1.metric(symbol, f"INR {latest['close']:.2f}", f"{pct:+.2f}% d/d")
c2.metric("Last bar", df.index[-1].strftime("%Y-%m-%d"), help=f"{len(df):,} rows loaded")
c3.metric("Data source", ctx.price_provider_label)

if ctx.fallback_reason:
    st.warning(ctx.fallback_reason)

warning_count = len(ctx.report.warnings)
error_count = len(ctx.report.errors)
if error_count:
    dq_label = f"Data quality: {error_count} error(s), {warning_count} warning(s)"
elif warning_count:
    dq_label = f"Data quality: {warning_count} warning(s)"
else:
    dq_label = "Data quality: all checks passed"

with st.expander(dq_label, expanded=bool(error_count)):
    if ctx.report.ok and not ctx.report.warnings:
        st.success("All checks passed.")
    for warning in ctx.report.warnings:
        st.warning(warning)
    for error in ctx.report.errors:
        st.error(error)

settings = get_settings()
starter = get_universe_config().get("starter_watchlist", ["RELIANCE.NS"])
yf_provider = YFinanceFundamentalsProvider()
csv_provider = CsvFundamentalsProvider(
    annual_path=resolve_project_path(settings.fundamentals_csv_path)
)


@st.cache_data(ttl=3600, show_spinner=False)
def _yf_has_fundamentals(sym: str) -> bool:
    return not yf_provider.get_annual_fundamentals(sym).empty


fundamentals_provider = yf_provider if _yf_has_fundamentals(symbol) else csv_provider
fund_source = "yfinance (live)" if fundamentals_provider is yf_provider else "local CSV (sample)"
summary_symbols = unique_symbols([*starter, symbol])
summary_frame = build_fundamentals_summary(fundamentals_provider, summary_symbols)
selected_summary = None
if not summary_frame.empty and "symbol" in summary_frame.columns:
    matches = summary_frame[summary_frame["symbol"].str.upper() == symbol.upper()]
    if not matches.empty:
        selected_summary = matches.iloc[0].to_dict()

score_delivery = fetch_delivery_data(symbol)
score_delivery_stats = delivery_stats(score_delivery) if not score_delivery.empty else None
earnings_hist = get_earnings_history(symbol)
earnings_dates = list(earnings_hist["earnings_date"].dropna()) if not earnings_hist.empty else []
result_vol = compute_result_volatility(df, earnings_dates) if earnings_dates else None
composite = score_stock(
    symbol=symbol,
    fundamentals=selected_summary,
    technicals=ctx.latest_technical,
    signals=ctx.signals,
    delivery=score_delivery_stats,
    result_volatility=result_vol,
)

tab_overview, tab_chart, tab_fund, tab_tech, tab_flows = st.tabs(
    ["Overview", "Chart", "Fundamentals", "Technicals & signals", "Flows & events"]
)

with tab_overview:
    trust_rows = build_data_trust_rows(
        symbol=symbol,
        price_frame=df,
        price_source=ctx.price_provider_label,
        price_warnings=ctx.report.warnings,
        price_errors=ctx.report.errors,
        fundamentals_frame=fundamentals_provider.get_annual_fundamentals(symbol),
        fundamentals_source=fund_source,
        fundamentals_warnings=[],
        fundamentals_errors=[],
        banking_frame=pd.DataFrame(),
        banking_applicable=False,
        banking_warnings=[],
        banking_errors=[],
        composite_missing=composite.missing_data,
        composite_risks=composite.risks,
        active_signal_count=sum(1 for signal in ctx.signals if signal.active),
        delivery_available=score_delivery_stats is not None,
        result_volatility_available=result_vol is not None,
    )
    trust_level, trust_reason = data_trust_level(trust_rows)
    active = active_signal_names(ctx.signals)
    stance, detail = research_stance(composite, trust_level, active)
    pros, cons = pros_cons(composite, trust_rows, active)

    render_verdict_card(
        stance=stance,
        detail=detail,
        score=float(composite.score),
        band=composite.band,
        trust_level=trust_level,
        active_signal_count=len(active),
    )

    left, right = st.columns(2)
    with left:
        st.markdown("##### Why it's interesting")
        for item in pros:
            st.markdown(f"- {item}")
    with right:
        st.markdown("##### What to verify")
        for item in cons:
            st.markdown(f"- {item}")

    with st.expander("Inside the score"):
        sub_cols = st.columns(4)
        sub_cols[0].metric("Fundamentals", f"{composite.sub_scores['fundamentals']:.1f}")
        sub_cols[1].metric("Technicals", f"{composite.sub_scores['technicals']:.1f}")
        sub_cols[2].metric("Flows", f"{composite.sub_scores['flows']:.1f}")
        sub_cols[3].metric("Events", f"{composite.sub_scores['events_quality']:.1f}")
        st.caption(trust_reason)

    with st.expander("Data trust details"):
        st.dataframe(data_trust_rows_to_frame(trust_rows), width="stretch", hide_index=True)

with tab_chart:
    st.subheader("Interactive Chart")
    st.caption(
        "Default view is candlesticks, volume, and 50 EMA. Add overlays only when you need them."
    )
    o1, o2, o3, o4, o5 = st.columns(5)
    show_volume = o1.checkbox("Volume", value=True)
    show_20 = o2.checkbox("20 EMA", value=False)
    show_200 = o3.checkbox("200 EMA", value=False)
    show_bb = o4.checkbox("Bollinger Bands", value=False)
    show_52w = o5.checkbox("52W levels", value=False)
    active_signals = [signal for signal in ctx.signals if signal.active]
    event_markers = (
        [
            {"date": date, "label": "Result"}
            for date in earnings_hist["earnings_date"].dropna().tail(4)
        ]
        if not earnings_hist.empty and "earnings_date" in earnings_hist.columns
        else []
    )
    upcoming = get_upcoming_earnings(symbol)
    if upcoming and upcoming.get("earnings_date"):
        event_markers.append({"date": upcoming["earnings_date"], "label": "Upcoming result"})
    fig = build_price_chart(
        df,
        ctx.technical_df,
        symbol=symbol,
        source_label=ctx.price_provider_label,
        show_20_ema=show_20,
        show_200_ema=show_200,
        show_bollinger=show_bb,
        show_52w=show_52w,
        show_volume=show_volume,
        active_signals=active_signals,
        event_markers=event_markers,
        freshness_note="fallback used" if ctx.fallback_reason else None,
    )
    st.plotly_chart(fig, width="stretch")

with tab_fund:
    st.subheader("Fundamentals")
    fundamentals_frame = fundamentals_provider.get_annual_fundamentals(symbol)
    if fundamentals_frame.empty:
        st.info("No fundamentals rows found for this symbol.")
    else:
        snapshots = fundamentals_provider.get_snapshots(symbol)
        latest_f = snapshots[-1]
        previous_f = snapshots[-2] if len(snapshots) > 1 else None
        ratios = calculate_basic_ratios(latest_f)
        financial = is_financial_sector(row=selected_summary)
        piotroski = calculate_piotroski_f_score(latest_f, previous_f) if previous_f else None
        altman = None if financial else calculate_altman_z_score(latest_f)
        growth = calculate_growth(latest_f, previous_f) if previous_f else {}
        f1, f2, f3, f4 = st.columns(4)
        f1.metric("Fiscal year", str(latest_f.fiscal_year))
        f2.metric("Revenue growth", format_pct(growth.get("revenue_growth")))
        f3.metric("Piotroski", format_score(piotroski.score if piotroski else None, 9))
        f4.metric(
            "Altman Z",
            "N/A for financials" if financial else format_number(altman.score if altman else None),
        )
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("ROA", format_pct(ratios["return_on_assets"]))
        r2.metric("ROE", format_pct(ratios["return_on_equity"]))
        r3.metric(
            "Debt / Equity",
            "N/A for financials" if financial else format_number(ratios["debt_to_equity"]),
        )
        r4.metric("Source", fund_source)
        cagr = compute_multi_year_cagr(snapshots)
        st.dataframe(
            [
                {
                    "Metric": "Revenue",
                    "3Y CAGR": format_pct(cagr.get("revenue_cagr_3y")),
                    "5Y CAGR": format_pct(cagr.get("revenue_cagr_5y")),
                },
                {
                    "Metric": "PAT",
                    "3Y CAGR": format_pct(cagr.get("net_income_cagr_3y")),
                    "5Y CAGR": format_pct(cagr.get("net_income_cagr_5y")),
                },
                {
                    "Metric": "EPS",
                    "3Y CAGR": format_pct(cagr.get("eps_cagr_3y")),
                    "5Y CAGR": format_pct(cagr.get("eps_cagr_5y")),
                },
            ],
            width="stretch",
            hide_index=True,
        )
        with st.expander("Annual fundamentals rows"):
            st.dataframe(fundamentals_frame, width="stretch")

with tab_tech:
    st.subheader("Technicals & Signals")
    latest_t = ctx.latest_technical
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("RSI 14", format_number(latest_t.get("rsi_14")), help=help_text("RSI 14"))
    m2.metric("MACD", format_number(latest_t.get("macd")), help=help_text("MACD"))
    m3.metric("ATR 14", format_currency(latest_t.get("atr_14")), help=help_text("ATR 14"))
    m4.metric(
        "Relative volume",
        format_number(latest_t.get("relative_volume")),
        help=help_text("Relative volume"),
    )
    signal_rows = [
        {
            "Signal": signal.name,
            "Status": "Active" if signal.active else "Inactive",
            "Trigger": signal.trigger_price,
            "Entry low": signal.entry_zone_low,
            "Entry high": signal.entry_zone_high,
            "Stop-loss": signal.stop_loss,
            "Target": signal.target_price,
            "R/R": signal.risk_reward,
            "Risk / share": risk_per_share(signal),
            "Position size": position_size(signal, inputs.portfolio_value),
            "Detail": signal.detail,
        }
        for signal in ctx.signals
    ]
    st.dataframe(signal_rows, width="stretch", hide_index=True)
    st.caption(f"Saved {ctx.saved_signal_count} signal observations from this run.")

with tab_flows:
    st.subheader("Flows & Events")
    if score_delivery.empty:
        st.info("Delivery data unavailable for this symbol.")
    else:
        enriched = compute_delivery_analytics(score_delivery)
        stats = delivery_stats(score_delivery)
        d1, d2, d3 = st.columns(3)
        d1.metric(
            "Latest delivery %",
            f"{stats['latest_pct']}%" if stats["latest_pct"] is not None else "N/A",
        )
        d2.metric(
            "20D avg delivery %",
            f"{stats['ma20_pct']}%" if stats["ma20_pct"] is not None else "N/A",
        )
        d3.metric("Trend", (stats["trend"] or "N/A").title())
        fig = go.Figure(
            go.Bar(x=enriched["trade_date"], y=enriched["delivery_pct"], name="Delivery %")
        )
        fig.update_layout(height=320, margin=dict(l=10, r=10, t=30, b=20))
        st.plotly_chart(fig, width="stretch")
    days_left = days_to_next_earnings(upcoming)
    if upcoming:
        st.warning(f"Next earnings: {upcoming['earnings_date']} ({days_left} days away)")
