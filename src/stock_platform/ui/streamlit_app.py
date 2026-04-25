"""
Phase 0 Streamlit app — the first milestone from the master prompt:

    "Run one command and see a local Streamlit page showing RELIANCE.NS
     historical price data with a disclaimer, basic logging, and basic
     data quality validation."

Run with:
    streamlit run src/stock_platform/ui/streamlit_app.py

Nothing here constitutes investment advice. See DISCLAIMER.md.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Path bootstrap so `streamlit run <file>` works without PYTHONPATH tweaks.
# Keep this block at the very top.
# ---------------------------------------------------------------------------
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2]
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# ---------------------------------------------------------------------------

from datetime import date, timedelta  # noqa: E402

import pandas as pd  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402

from stock_platform.alerts import (  # noqa: E402
    alert_candidates_to_frame,
    build_alert_candidates,
)
from stock_platform.analytics.backtest import (  # noqa: E402
    compute_portfolio_metrics,
    portfolio_metrics_to_frame,
    run_signal_backtest,
    run_walk_forward_validation,
    summaries_to_frame,
    trades_to_frame,
)
from stock_platform.analytics.flows import (  # noqa: E402
    compute_delivery_analytics,
    compute_result_volatility,
    delivery_stats,
)
from stock_platform.analytics.fundamentals import (  # noqa: E402
    calculate_altman_z_score,
    calculate_basic_ratios,
    calculate_growth,
    calculate_piotroski_f_score,
)
from stock_platform.analytics.fundamentals.sector_ranking import sector_rank_summary  # noqa: E402
from stock_platform.analytics.fundamentals.summary import build_fundamentals_summary  # noqa: E402
from stock_platform.analytics.signals import scan_technical_signals  # noqa: E402
from stock_platform.analytics.signals.audit import (  # noqa: E402
    audits_to_frame,
    fetch_recent_signal_audits,
    fetch_signal_event_export,
    save_signal_audit,
)
from stock_platform.analytics.technicals import add_technical_indicators  # noqa: E402
from stock_platform.config import (  # noqa: E402
    ROOT_DIR,
    get_settings,
    get_thresholds_config,
    get_universe_config,
)
from stock_platform.data.providers import (  # noqa: E402
    CsvFundamentalsProvider,
    YahooFinanceProvider,
    YFinanceFundamentalsProvider,
)
from stock_platform.data.providers.corporate_actions import (  # noqa: E402
    days_to_next_earnings,
    get_dividends,
    get_earnings_history,
    get_splits,
    get_upcoming_earnings,
)
from stock_platform.data.providers.institutional_holdings import (  # noqa: E402
    get_institutional_holders,
    get_major_holders,
    get_mutualfund_holders,
    holdings_summary,
)
from stock_platform.data.providers.nse import (  # noqa: E402
    fetch_deals_for_symbol,
    fetch_delivery_data,
)
from stock_platform.data.validators import (  # noqa: E402
    OHLCVValidationError,
    validate_annual_fundamentals,
    validate_ohlcv,
)
from stock_platform.ops import (  # noqa: E402
    build_provenance_rows,
    provenance_rows_to_frame,
    run_health_checks,
)
from stock_platform.scoring import score_stock  # noqa: E402
from stock_platform.utils.logging import get_logger  # noqa: E402

log = get_logger(__name__)


@st.cache_data(ttl=3600, show_spinner=False)
def _load_delivery(sym: str) -> pd.DataFrame:
    return fetch_delivery_data(sym)


@st.cache_data(ttl=86400, show_spinner=False)
def _load_dividends(sym: str) -> pd.DataFrame:
    return get_dividends(sym)


@st.cache_data(ttl=86400, show_spinner=False)
def _load_splits(sym: str) -> pd.DataFrame:
    return get_splits(sym)


@st.cache_data(ttl=3600, show_spinner=False)
def _load_upcoming(sym: str) -> dict | None:
    return get_upcoming_earnings(sym)


@st.cache_data(ttl=86400, show_spinner=False)
def _load_earnings_hist(sym: str) -> pd.DataFrame:
    return get_earnings_history(sym)


@st.cache_data(ttl=3600, show_spinner=False)
def _load_deals(sym: str) -> pd.DataFrame:
    return fetch_deals_for_symbol(sym)


def _resolve_project_path(path_value: str) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else ROOT_DIR / path


def _format_pct(value: float | None) -> str:
    return "N/A" if value is None or pd.isna(value) else f"{value * 100:.1f}%"


def _format_number(value: float | None) -> str:
    return "N/A" if value is None or pd.isna(value) else f"{value:.2f}"


def _format_score(value: float | None, max_score: int) -> str:
    return "N/A" if value is None else f"{value:.0f}/{max_score}"


def _format_currency(value: float | None) -> str:
    return "N/A" if value is None or pd.isna(value) else f"₹{value:.2f}"


def _format_rank(value: float | None) -> str:
    """Format a 0-100 percentile rank for display."""
    if value is None:
        return "N/A"
    return f"{value:.0f}th %ile"


def _unique_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for raw_symbol in symbols:
        normalized = raw_symbol.strip().upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique.append(normalized)
    return unique


def _risk_per_share(signal) -> float | None:
    if signal.trigger_price is None or signal.stop_loss is None:
        return None
    risk = signal.trigger_price - signal.stop_loss
    return round(risk, 2) if risk > 0 else None


def _position_size(signal, portfolio_value: float) -> int | None:
    risk_per_share = _risk_per_share(signal)
    if risk_per_share is None or portfolio_value <= 0:
        return None
    max_risk_pct = float(
        get_thresholds_config().get("risk", {}).get("max_portfolio_risk_per_trade_pct", 1.0)
    )
    max_rupee_risk = portfolio_value * (max_risk_pct / 100)
    return int(max_rupee_risk // risk_per_share)


# --------------------------------------------------------------------------- #
# Page setup
# --------------------------------------------------------------------------- #

st.set_page_config(
    page_title="Indian Stock Research Platform",
    page_icon="📈",
    layout="wide",
)

# --------------------------------------------------------------------------- #
# Disclaimer banner (always visible)
# --------------------------------------------------------------------------- #

st.warning(
    "**Disclaimer** — This platform is a personal research aid. It is **not** "
    "investment advice and **not** a SEBI-registered RA/RIA service. "
    "Data may contain errors. Past performance is not indicative of future results. "
    "See `DISCLAIMER.md` in the repository for full terms."
)

st.title("📈 Indian Stock Research Platform")
st.caption(
    "Phase 6 - Local research platform with fundamentals, technicals, flows, "
    "composite scoring, backtesting, health checks, and alert previews"
)

# --------------------------------------------------------------------------- #
# Sidebar: inputs
# --------------------------------------------------------------------------- #

settings = get_settings()
universe = get_universe_config()
starter = universe.get("starter_watchlist", ["RELIANCE.NS"])

with st.sidebar:
    st.header("Inputs")
    symbol = st.selectbox(
        "Ticker",
        options=starter,
        index=0,
        help="Yahoo Finance symbol. `.NS` for NSE, `.BO` for BSE.",
    )
    custom = st.text_input("Or enter a custom ticker", value="", placeholder="e.g. TATAMOTORS.NS")
    if custom.strip():
        symbol = custom.strip().upper()

    today = date.today()
    default_start = today - timedelta(days=5 * 365)
    start = st.date_input("Start date", value=default_start, max_value=today - timedelta(days=1))
    end = st.date_input(
        "End date", value=today, min_value=start + timedelta(days=1), max_value=today
    )

    st.markdown("---")
    st.caption(f"Environment: `{settings.app_env}`")
    st.caption(f"Log level: `{settings.app_log_level}`")
    st.caption(f"Price provider: `{settings.provider_price}`")
    st.caption(f"Fundamentals provider: `{settings.provider_fundamentals}`")
    portfolio_value = st.number_input(
        "Portfolio value for position sizing",
        min_value=0.0,
        value=1_000_000.0,
        step=50_000.0,
        help="Educational risk sizing only. This does not make the app investment advice.",
    )

# --------------------------------------------------------------------------- #
# Fetch + validate
# --------------------------------------------------------------------------- #

provider = YahooFinanceProvider()

with st.spinner(f"Downloading {symbol}…"):
    try:
        df = provider.get_ohlcv(symbol=symbol, start=start, end=end)
    except Exception as exc:  # noqa: BLE001
        log.exception("Download failed for {}: {}", symbol, exc)
        st.error(f"Could not fetch data for **{symbol}**: {exc}")
        st.stop()

if df.empty:
    st.error(
        f"No data returned for **{symbol}**. "
        "Check that the ticker is valid and that the date range contains trading days."
    )
    st.stop()

try:
    report = validate_ohlcv(df, symbol=symbol, raise_on_error=False)
except OHLCVValidationError as exc:
    st.error(f"Data quality failure: {exc}")
    st.stop()

technical_df = add_technical_indicators(df)
latest_technical = technical_df.iloc[-1]
technical_signals = scan_technical_signals(df)
try:
    saved_signal_count = save_signal_audit(
        symbol,
        df,
        technical_signals,
        source=settings.provider_price,
    )
except Exception as exc:  # noqa: BLE001
    saved_signal_count = 0
    log.exception("Could not save signal audit for {}: {}", symbol, exc)

# --------------------------------------------------------------------------- #
# Top-line summary
# --------------------------------------------------------------------------- #

latest = df.iloc[-1]
prev = df.iloc[-2] if len(df) > 1 else latest
pct = ((latest["close"] - prev["close"]) / prev["close"]) * 100 if prev["close"] else 0.0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Symbol", symbol)
c2.metric("Last close", f"₹{latest['close']:.2f}", f"{pct:+.2f}% d/d")
c3.metric("Rows", f"{len(df):,}")
c4.metric("Last date", df.index[-1].strftime("%Y-%m-%d"))

# --------------------------------------------------------------------------- #
# Data quality panel
# --------------------------------------------------------------------------- #

with st.expander("🔍 Data quality report", expanded=not report.ok):
    if report.ok and not report.warnings:
        st.success("All checks passed.")
    if report.warnings:
        st.warning("Warnings:")
        for w in report.warnings:
            st.markdown(f"- {w}")
    if report.errors:
        st.error("Errors:")
        for e in report.errors:
            st.markdown(f"- {e}")

# --------------------------------------------------------------------------- #
# Fundamentals panel
# --------------------------------------------------------------------------- #

st.subheader("Fundamentals")

# Try yfinance for real data; fall back to local CSV for the overview table
_yf_provider = YFinanceFundamentalsProvider()
_csv_provider = CsvFundamentalsProvider(
    annual_path=_resolve_project_path(settings.fundamentals_csv_path)
)


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_yf_fundamentals(sym: str) -> bool:
    """Return True if yfinance has ≥1 annual row for sym."""
    return not _yf_provider.get_annual_fundamentals(sym).empty


_yf_available = _fetch_yf_fundamentals(symbol)
fundamentals_provider = _yf_provider if _yf_available else _csv_provider
_fundamentals_source_label = "yfinance (live)" if _yf_available else "local CSV (sample)"

summary_symbols = _unique_symbols([*starter, symbol])
summary_frame = build_fundamentals_summary(fundamentals_provider, summary_symbols)

overview_tab, selected_tab, sector_ranks_tab = st.tabs(
    ["Overview", "Selected stock", "Sector rankings"]
)

with overview_tab:
    if summary_frame.empty:
        st.info("No local fundamentals summary available.")
    else:
        display_cols = [
            "symbol",
            "sector",
            "market_cap_bucket",
            "status",
            "fiscal_year",
            "revenue_growth_pct",
            "roe_pct",
            "roce_pct",
            "free_cash_flow_yield_pct",
            "price_to_earnings",
            "debt_to_equity",
            "piotroski_f_score",
            "altman_z_score",
            "source",
            "warnings",
        ]
        st.dataframe(
            summary_frame[[c for c in display_cols if c in summary_frame.columns]],
            width="stretch",
            hide_index=True,
            column_config={
                "revenue_growth_pct": st.column_config.NumberColumn(
                    "Revenue growth %", format="%.1f"
                ),
                "roe_pct": st.column_config.NumberColumn("ROE %", format="%.1f"),
                "roce_pct": st.column_config.NumberColumn("ROCE %", format="%.1f"),
                "free_cash_flow_yield_pct": st.column_config.NumberColumn(
                    "FCF yield %", format="%.1f"
                ),
                "price_to_earnings": st.column_config.NumberColumn("P/E", format="%.1f"),
                "debt_to_equity": st.column_config.NumberColumn("Debt / Equity", format="%.2f"),
                "piotroski_f_score": st.column_config.NumberColumn("Piotroski", format="%.0f"),
                "altman_z_score": st.column_config.NumberColumn("Altman Z", format="%.2f"),
            },
        )
        if _yf_available:
            st.success(
                f"Fundamentals source: **{_fundamentals_source_label}** — real annual data from yfinance."
            )
        elif (summary_frame["status"] == "sample").any():
            st.warning("Some fundamentals rows are sample placeholders, not verified source data.")

with selected_tab:
    fundamentals_frame = fundamentals_provider.get_annual_fundamentals(symbol)

    if fundamentals_frame.empty:
        st.info(f"No fundamentals rows found for **{symbol}**.")
    else:
        fundamentals_report = validate_annual_fundamentals(
            fundamentals_frame,
            symbol=symbol,
            raise_on_error=False,
        )
        if fundamentals_report.errors:
            st.error("Fundamentals data quality errors:")
            for error in fundamentals_report.errors:
                st.markdown(f"- {error}")
        if fundamentals_report.warnings:
            st.warning("Fundamentals data quality warnings:")
            for warning in fundamentals_report.warnings:
                st.markdown(f"- {warning}")

        snapshots = fundamentals_provider.get_snapshots(symbol)
        latest_fundamentals = snapshots[-1]
        previous_fundamentals = snapshots[-2] if len(snapshots) > 1 else None
        ratios = calculate_basic_ratios(latest_fundamentals)
        altman = calculate_altman_z_score(latest_fundamentals)
        piotroski = (
            calculate_piotroski_f_score(latest_fundamentals, previous_fundamentals)
            if previous_fundamentals
            else None
        )
        growth = (
            calculate_growth(latest_fundamentals, previous_fundamentals)
            if previous_fundamentals
            else {}
        )

        source = _fundamentals_source_label
        if not _yf_available:
            st.warning(
                "Displayed fundamentals are sample placeholder rows. "
                "yfinance returned no data for this symbol."
            )

        f1, f2, f3, f4 = st.columns(4)
        f1.metric("Fiscal year", str(latest_fundamentals.fiscal_year))
        f2.metric("Revenue growth", _format_pct(growth.get("revenue_growth")))
        f3.metric("Piotroski F-Score", _format_score(piotroski.score if piotroski else None, 9))
        f4.metric("Altman Z-Score", _format_number(altman.score))

        r1, r2, r3, r4 = st.columns(4)
        r1.metric("ROA", _format_pct(ratios["return_on_assets"]))
        r2.metric("ROE", _format_pct(ratios["return_on_equity"]))
        r3.metric("Debt / Equity", _format_number(ratios["debt_to_equity"]))
        r4.metric("Source", source)

        trend_fig = go.Figure()
        trend_fig.add_trace(
            go.Bar(
                x=fundamentals_frame["fiscal_year"],
                y=fundamentals_frame["revenue"],
                name="Revenue",
            )
        )
        trend_fig.add_trace(
            go.Scatter(
                x=fundamentals_frame["fiscal_year"],
                y=fundamentals_frame["net_income"],
                mode="lines+markers",
                name="Net income",
                yaxis="y2",
            )
        )
        trend_fig.update_layout(
            title=f"{symbol} - Annual fundamentals trend",
            xaxis_title="Fiscal year",
            yaxis_title="Revenue",
            yaxis2=dict(title="Net income", overlaying="y", side="right"),
            height=360,
            margin=dict(l=20, r=20, t=50, b=20),
        )
        st.plotly_chart(trend_fig, width="stretch")

        with st.expander("Show annual fundamentals rows"):
            st.dataframe(fundamentals_frame, width="stretch")

with sector_ranks_tab:
    st.caption(
        "Percentile rank (0–100) within peer group. "
        "100 = best in peer group. "
        "Based on sample placeholder data until a real fundamentals source is connected."
    )

    if summary_frame.empty:
        st.info("No fundamentals data available for sector ranking.")
    else:
        ranks = sector_rank_summary(summary_frame, symbol)

        if not ranks:
            st.info(f"No rank data available for **{symbol}**.")
        else:
            # Show ranks in a 3-column layout: sector / industry / market cap bucket
            rank_metrics = [
                ("roe_pct", "ROE"),
                ("roa_pct", "ROA"),
                ("roce_pct", "ROCE"),
                ("revenue_growth_pct", "Revenue growth"),
                ("net_income_growth_pct", "Net income growth"),
                ("free_cash_flow_yield_pct", "FCF yield"),
                ("piotroski_f_score", "Piotroski score"),
                ("altman_z_score", "Altman Z-score"),
                ("debt_to_equity", "Debt / Equity"),
                ("price_to_earnings", "P/E"),
            ]

            headers = ["Metric", "vs Sector", "vs Industry", "vs Mkt Cap bucket"]
            rank_rows = []
            for metric_key, metric_label in rank_metrics:
                sector_rank = ranks.get(f"{metric_key}_sector_rank")
                industry_rank = ranks.get(f"{metric_key}_industry_rank")
                mkt_rank = ranks.get(f"{metric_key}_mkt_cap_rank")
                rank_rows.append(
                    {
                        "Metric": metric_label,
                        "vs Sector": _format_rank(sector_rank),
                        "vs Industry": _format_rank(industry_rank),
                        "vs Mkt Cap bucket": _format_rank(mkt_rank),
                    }
                )

            st.dataframe(rank_rows, width="stretch", hide_index=True)

            # Show peer context: which stocks are in the same sector/industry
            symbol_row = summary_frame[summary_frame["symbol"].str.upper() == symbol.upper()]
            if not symbol_row.empty:
                s_sector = symbol_row.iloc[0].get("sector")
                s_industry = symbol_row.iloc[0].get("industry")
                s_mkt = symbol_row.iloc[0].get("market_cap_bucket")
                st.caption(
                    f"**{symbol}** — Sector: {s_sector or 'N/A'}  |  "
                    f"Industry: {s_industry or 'N/A'}  |  "
                    f"Market cap: {s_mkt or 'N/A'}"
                )

                if s_sector:
                    peers = summary_frame[
                        summary_frame["sector"].str.upper() == str(s_sector).upper()
                    ]["symbol"].tolist()
                    st.caption(f"Sector peers in this dataset: {', '.join(peers)}")

# --------------------------------------------------------------------------- #
# Composite score panel
# --------------------------------------------------------------------------- #

st.subheader("Composite Score")
st.caption(
    "Config-driven research score from available fundamentals, technicals, flows, "
    "and event-risk inputs. Educational only; not investment advice."
)

selected_summary = None
if not summary_frame.empty and "symbol" in summary_frame.columns:
    symbol_matches = summary_frame[summary_frame["symbol"].str.upper() == symbol.upper()]
    if not symbol_matches.empty:
        selected_summary = symbol_matches.iloc[0].to_dict()

with st.spinner("Building composite score..."):
    score_delivery = _load_delivery(symbol)
    score_delivery_stats = delivery_stats(score_delivery) if not score_delivery.empty else None
    score_earnings = _load_earnings_hist(symbol)
    earnings_dates = (
        list(score_earnings["earnings_date"].dropna()) if not score_earnings.empty else []
    )
    score_result_volatility = (
        compute_result_volatility(df, earnings_dates) if earnings_dates else None
    )
    composite = score_stock(
        symbol=symbol,
        fundamentals=selected_summary,
        technicals=latest_technical,
        signals=technical_signals,
        delivery=score_delivery_stats,
        result_volatility=score_result_volatility,
    )

score_cols = st.columns(6)
score_cols[0].metric("Composite", f"{composite.score:.1f}/100")
score_cols[1].metric("Band", composite.band)
score_cols[2].metric("Fundamentals", f"{composite.sub_scores['fundamentals']:.1f}")
score_cols[3].metric("Technicals", f"{composite.sub_scores['technicals']:.1f}")
score_cols[4].metric("Flows", f"{composite.sub_scores['flows']:.1f}")
score_cols[5].metric("Events", f"{composite.sub_scores['events_quality']:.1f}")

score_detail_tab, score_risk_tab, score_config_tab = st.tabs(
    ["Why this score", "Risks / missing data", "Score inputs"]
)

with score_detail_tab:
    if composite.reasons:
        for reason in composite.reasons:
            st.markdown(f"- {reason}")
    else:
        st.info("No strong positive drivers yet from the available MVP data.")

with score_risk_tab:
    if composite.risks:
        st.markdown("Risk notes:")
        for risk in composite.risks:
            st.markdown(f"- {risk}")
    if composite.missing_data:
        st.markdown("Missing or provisional inputs:")
        for item in composite.missing_data:
            st.markdown(f"- {item}")
    if not composite.risks and not composite.missing_data:
        st.success("No major missing-data notes from the MVP score inputs.")

with score_config_tab:
    st.dataframe(
        [{"bucket": bucket, "score": score} for bucket, score in composite.sub_scores.items()],
        width="stretch",
        hide_index=True,
    )

# --------------------------------------------------------------------------- #
# Operations and alert readiness panel
# --------------------------------------------------------------------------- #

st.subheader("Operations & Alerts")
st.caption(
    "Phase 6 local readiness. Alert rows are previews only; no Telegram or email is sent yet."
)

alert_tab, provenance_tab, health_tab, backup_tab = st.tabs(
    ["Alert preview", "Data provenance", "Health checks", "Backup / GitHub"]
)

with alert_tab:
    alert_candidates = build_alert_candidates(
        symbol=symbol,
        composite=composite,
        signals=technical_signals,
        data_warnings=[*report.warnings, *report.errors],
    )
    alert_frame = alert_candidates_to_frame(alert_candidates)
    if alert_frame.empty:
        st.success("No alert candidates from the current scan.")
    else:
        st.dataframe(alert_frame, width="stretch", hide_index=True)
        st.caption(
            "These rows are deliberately worded as research-aid notifications, "
            "not buy/sell instructions."
        )

with provenance_tab:
    provenance_rows = build_provenance_rows(
        symbol=symbol,
        price_provider=settings.provider_price,
        fundamentals_provider=_fundamentals_source_label,
        price_frame=df,
        fundamentals_source=_fundamentals_source_label,
        delivery_available=score_delivery_stats is not None,
        deals_available=None,
    )
    st.dataframe(provenance_rows_to_frame(provenance_rows), width="stretch", hide_index=True)
    st.caption(
        "This table is the plain-English audit trail for the current screen. "
        "It separates source data from locally derived analytics."
    )

with health_tab:
    health_checks = run_health_checks()
    health_frame = pd.DataFrame(
        [
            {
                "check": check.name,
                "status": "PASS" if check.ok else "ACTION",
                "detail": check.detail,
                "next_action": check.action,
            }
            for check in health_checks
        ]
    )
    st.dataframe(health_frame, width="stretch", hide_index=True)
    failures = [check for check in health_checks if not check.ok]
    if failures:
        st.warning(f"{len(failures)} setup item(s) need attention before serious daily use.")
    else:
        st.success("Local setup checks passed.")

with backup_tab:
    st.markdown("##### Local backup command")
    st.code(r".\scripts\backup_local.ps1", language="powershell")
    st.markdown("##### Health-check command")
    st.code(r".\scripts\health_check.ps1", language="powershell")
    st.markdown("##### Remaining GitHub setup")
    st.info(
        "The local repository exists. The remaining manual steps are to configure Git name/email, "
        "connect a private GitHub remote, commit, and push."
    )

# --------------------------------------------------------------------------- #
# Chart
# --------------------------------------------------------------------------- #

fig = go.Figure(
    data=[
        go.Candlestick(
            x=df.index,
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name=symbol,
        )
    ]
)
fig.update_layout(
    title=f"{symbol} — Daily candlestick",
    xaxis_title="Date",
    yaxis_title="Price (₹)",
    xaxis_rangeslider_visible=False,
    height=600,
    margin=dict(l=20, r=20, t=50, b=20),
)
fig.add_trace(
    go.Scatter(x=technical_df.index, y=technical_df["ema_20"], mode="lines", name="20 EMA")
)
fig.add_trace(
    go.Scatter(x=technical_df.index, y=technical_df["ema_50"], mode="lines", name="50 EMA")
)
fig.add_trace(
    go.Scatter(x=technical_df.index, y=technical_df["ema_200"], mode="lines", name="200 EMA")
)
st.plotly_chart(fig, width="stretch")

# --------------------------------------------------------------------------- #
# Technicals panel
# --------------------------------------------------------------------------- #

st.subheader("Technicals")
st.caption("Educational pattern observations only; not investment advice.")

tech_tab, signal_tab, backtest_tab = st.tabs(["Indicators", "Signals", "Signal backtest"])

with tech_tab:
    t1, t2, t3, t4 = st.columns(4)
    t1.metric("RSI 14", _format_number(latest_technical.get("rsi_14")))
    t2.metric("MACD", _format_number(latest_technical.get("macd")))
    t3.metric("ATR 14", _format_currency(latest_technical.get("atr_14")))
    t4.metric("Relative volume", _format_number(latest_technical.get("relative_volume")))

    t5, t6, t7, t8 = st.columns(4)
    t5.metric("20 EMA", _format_currency(latest_technical.get("ema_20")))
    t6.metric("50 EMA", _format_currency(latest_technical.get("ema_50")))
    t7.metric("100 EMA", _format_currency(latest_technical.get("ema_100")))
    t8.metric("200 EMA", _format_currency(latest_technical.get("ema_200")))

    t9, t10, t11, t12 = st.columns(4)
    t9.metric("ATR %", _format_pct(latest_technical.get("atr_pct") / 100))
    t10.metric("Hist. vol 20D", _format_pct(latest_technical.get("historical_volatility_20") / 100))
    t11.metric(
        "52W high gap", _format_pct(latest_technical.get("distance_from_52w_high_pct") / 100)
    )
    t12.metric("MA stack", str(latest_technical.get("ma_stack_status", "mixed")).title())

with signal_tab:
    signal_rows = [
        {
            "Signal": signal.name,
            "Status": "Active" if signal.active else "Inactive",
            "Type": signal.strength,
            "Trigger": signal.trigger_price,
            "Entry low": signal.entry_zone_low,
            "Entry high": signal.entry_zone_high,
            "Stop-loss": signal.stop_loss,
            "Target": signal.target_price,
            "R/R": signal.risk_reward,
            "Confidence": signal.confidence,
            "Risk / share": _risk_per_share(signal),
            "Position size": _position_size(signal, portfolio_value),
            "Detail": signal.detail,
        }
        for signal in technical_signals
    ]
    st.dataframe(signal_rows, width="stretch", hide_index=True)
    active_count = sum(1 for signal in technical_signals if signal.active)
    st.caption(f"{active_count} of {len(technical_signals)} educational technical patterns active.")

    with st.expander("Signal history"):
        history = audits_to_frame(fetch_recent_signal_audits(symbol, limit=50))
        st.caption(f"Upserted {saved_signal_count} signal observations from this run.")
        if history.empty:
            st.info("No signal history saved yet.")
        else:
            st.dataframe(history, width="stretch", hide_index=True)
            export_frame = fetch_signal_event_export(symbol, active_only=False)
            if not export_frame.empty:
                st.download_button(
                    "Download signal history CSV",
                    data=export_frame.to_csv(index=False),
                    file_name=f"{symbol.lower().replace('.', '_')}_signal_events.csv",
                    mime="text/csv",
                )

with backtest_tab:
    st.caption(
        "Evaluates forward returns for signal observations saved in this session. "
        "Educational only — small sample sizes make these stats directional, not conclusive. "
        "Not investment advice."
    )

    holding_days = st.select_slider(
        "Holding period (trading days)",
        options=[5, 10, 20, 60],
        value=20,
    )

    bt_scope = st.radio(
        "Symbols to include",
        options=["Current symbol only", "All saved symbols"],
        horizontal=True,
    )

    if st.button("Run backtest on saved signals"):
        scope_symbol = symbol if bt_scope == "Current symbol only" else None
        events_df = fetch_signal_event_export(symbol=scope_symbol, active_only=True)

        if events_df.empty:
            st.info(
                "No active signal events saved yet. "
                "Scan a few stocks using the Signals tab first, then return here."
            )
        else:
            with st.spinner(
                f"Downloading price data for {len(events_df['symbol'].unique())} symbol(s)…"
            ):
                bt_provider = YahooFinanceProvider()
                trades, summaries = run_signal_backtest(
                    events_df,
                    price_provider=bt_provider,
                    holding_days=holding_days,
                )

            completed = [t for t in trades if t.return_pct is not None]
            pending = [t for t in trades if t.return_pct is None]

            st.markdown(
                f"**{len(completed)}** trade(s) with completed holding period  |  "
                f"**{len(pending)}** signal(s) too recent to evaluate  |  "
                f"**{holding_days}-day** holding period"
            )

            if summaries:
                st.markdown("#### Per-signal summary")
                summary_df = summaries_to_frame(summaries)
                st.dataframe(
                    summary_df,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "win_rate_pct": st.column_config.NumberColumn("Win rate %", format="%.1f"),
                        "avg_return_pct": st.column_config.NumberColumn(
                            "Avg return %", format="%.2f"
                        ),
                        "avg_win_pct": st.column_config.NumberColumn("Avg win %", format="%.2f"),
                        "avg_loss_pct": st.column_config.NumberColumn("Avg loss %", format="%.2f"),
                        "profit_factor": st.column_config.NumberColumn(
                            "Profit factor", format="%.2f"
                        ),
                        "best_trade_pct": st.column_config.NumberColumn(
                            "Best trade %", format="%.2f"
                        ),
                        "worst_trade_pct": st.column_config.NumberColumn(
                            "Worst trade %", format="%.2f"
                        ),
                    },
                )

            if completed:
                st.markdown("#### Portfolio-level diagnostics")
                portfolio_metrics = compute_portfolio_metrics(trades)
                metrics_df = portfolio_metrics_to_frame(portfolio_metrics)
                st.dataframe(metrics_df, width="stretch", hide_index=True)

                walk_forward_df = run_walk_forward_validation(
                    completed,
                    train_years=3,
                    validate_years=1,
                )
                if walk_forward_df.empty:
                    st.info(
                        "Walk-forward validation needs signals spread across more than "
                        "three years of completed trades. Keep scanning over time or import "
                        "historical signal events later."
                    )
                else:
                    st.markdown("#### Walk-forward validation")
                    st.dataframe(
                        walk_forward_df,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "train_avg_return_pct": st.column_config.NumberColumn(
                                "Train avg return %", format="%.2f"
                            ),
                            "train_win_rate_pct": st.column_config.NumberColumn(
                                "Train win rate %", format="%.1f"
                            ),
                            "validate_avg_return_pct": st.column_config.NumberColumn(
                                "Validate avg return %", format="%.2f"
                            ),
                            "validate_win_rate_pct": st.column_config.NumberColumn(
                                "Validate win rate %", format="%.1f"
                            ),
                            "performance_drift_pct": st.column_config.NumberColumn(
                                "Drift %", format="%.2f"
                            ),
                        },
                    )

                st.markdown("#### Individual trades")
                trades_df = trades_to_frame(completed)
                st.dataframe(
                    trades_df,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "return_pct": st.column_config.NumberColumn("Return %", format="%.2f"),
                        "mfe_pct": st.column_config.NumberColumn("MFE %", format="%.2f"),
                        "mae_pct": st.column_config.NumberColumn("MAE %", format="%.2f"),
                    },
                )
                st.download_button(
                    "Download backtest CSV",
                    data=trades_df.to_csv(index=False),
                    file_name=f"signal_backtest_{holding_days}d.csv",
                    mime="text/csv",
                )

            st.caption(
                "Profit factor = total wins / total losses. "
                "MFE = maximum favorable excursion (best unrealised gain). "
                "MAE = maximum adverse excursion (worst unrealised loss). "
                "Sample sizes here are very small — treat as directional only."
            )

# --------------------------------------------------------------------------- #
# Flows & Events panel  (Phase 3)
# --------------------------------------------------------------------------- #

st.subheader("Flows & Events")
st.caption(
    "Delivery % from NSE bhavcopy. Bulk/block deals and corporate actions from NSE / yfinance. "
    "NSE data fetches may take 10–20 s; results are shown when available. "
    "Not investment advice."
)

delivery_tab, deals_tab, actions_tab, holdings_tab = st.tabs(
    ["Delivery %", "Bulk / Block Deals", "Corporate Actions", "Holdings"]
)

# --- Delivery % ---
with delivery_tab:
    with st.spinner("Fetching NSE delivery data…"):
        raw_delivery = _load_delivery(symbol)

    if raw_delivery.empty:
        st.info(
            "NSE delivery data unavailable for this symbol. "
            "The NSE API may be temporarily unreachable, or the ticker may not trade on NSE."
        )
    else:
        enriched_delivery = compute_delivery_analytics(raw_delivery)
        stats = delivery_stats(raw_delivery)

        d1, d2, d3, d4 = st.columns(4)
        d1.metric(
            "Latest delivery %",
            f"{stats['latest_pct']}%" if stats["latest_pct"] is not None else "N/A",
        )
        d2.metric(
            "20-day avg %",
            f"{stats['ma20_pct']}%" if stats["ma20_pct"] is not None else "N/A",
        )
        d3.metric(
            "90-day avg %",
            f"{stats['avg_90d_pct']}%" if stats["avg_90d_pct"] is not None else "N/A",
        )
        d4.metric(
            "Trend",
            (stats["trend"] or "N/A").capitalize(),
            delta="Unusual spike today" if stats["unusual_today"] else None,
            delta_color="inverse" if stats["unusual_today"] else "normal",
        )

        delivery_fig = go.Figure()
        delivery_fig.add_trace(
            go.Bar(
                x=enriched_delivery["trade_date"],
                y=enriched_delivery["delivery_pct"],
                name="Delivery %",
                marker_color="steelblue",
                opacity=0.7,
            )
        )
        if "delivery_pct_ma20" in enriched_delivery.columns:
            delivery_fig.add_trace(
                go.Scatter(
                    x=enriched_delivery["trade_date"],
                    y=enriched_delivery["delivery_pct_ma20"],
                    mode="lines",
                    name="20-day MA",
                    line=dict(color="orange", width=2),
                )
            )
        unusual_rows = enriched_delivery[enriched_delivery.get("unusual_delivery", False)]
        if not unusual_rows.empty:
            delivery_fig.add_trace(
                go.Scatter(
                    x=unusual_rows["trade_date"],
                    y=unusual_rows["delivery_pct"],
                    mode="markers",
                    name="Unusual spike",
                    marker=dict(color="red", size=10, symbol="star"),
                )
            )
        delivery_fig.update_layout(
            title=f"{symbol} — Delivery % (NSE EQ series)",
            xaxis_title="Date",
            yaxis_title="Delivery %",
            height=380,
            margin=dict(l=20, r=20, t=50, b=20),
            legend=dict(orientation="h"),
        )
        st.plotly_chart(delivery_fig, width="stretch")

        unusual_deliveries = enriched_delivery[
            enriched_delivery.get("unusual_delivery", pd.Series(False))
        ]
        if not unusual_deliveries.empty:
            with st.expander(f"Unusual delivery spikes ({len(unusual_deliveries)} days)"):
                st.dataframe(
                    unusual_deliveries[
                        ["trade_date", "delivery_pct", "delivery_pct_ma20", "traded_qty"]
                    ],
                    hide_index=True,
                )

        st.caption(
            "High delivery % (>60%) = institutional / long-term buying conviction. "
            "Low (<25%) = largely intraday / speculative activity. "
            "Unusual spike = delivery % > 1.5× its 20-day average."
        )

# --- Bulk / Block Deals ---
with deals_tab:
    with st.spinner("Fetching NSE bulk/block deals…"):
        deals_df = _load_deals(symbol)

    if deals_df.empty:
        st.info(
            "Bulk / block deal data is currently unavailable. "
            "NSE's deal API (www.nseindia.com) requires a live browser session — "
            "automated HTTP clients are blocked by Akamai bot-protection on the main site. "
            "Delivery % data (previous tab) works fine via the public bhavcopy archive. "
            "Check NSE's website directly for recent bulk/block deals."
        )
    else:
        st.caption(f"{len(deals_df)} deal(s) found in the last 30 days.")
        st.dataframe(
            deals_df,
            hide_index=True,
            column_config={
                "deal_date": st.column_config.DateColumn("Date"),
                "quantity": st.column_config.NumberColumn("Quantity", format="%,.0f"),
                "price": st.column_config.NumberColumn("Price (₹)", format="%.2f"),
                "deal_type": "Type",
                "buy_sell": "Buy / Sell",
                "client_name": "Client",
            },
        )
        st.download_button(
            "Download deals CSV",
            data=deals_df.to_csv(index=False),
            file_name=f"{symbol.lower().replace('.', '_')}_bulk_block_deals.csv",
            mime="text/csv",
        )

    st.caption(
        "Bulk deal: single transaction ≥ 0.5% of equity shares outstanding. "
        "Block deal: negotiated transaction on a separate trading window. "
        "Source: NSE (last 30 calendar days)."
    )

# --- Corporate Actions ---
with actions_tab:
    with st.spinner("Loading corporate actions…"):
        div_df = _load_dividends(symbol)
        split_df = _load_splits(symbol)
        upcoming = _load_upcoming(symbol)
        earnings_hist = _load_earnings_hist(symbol)

    # Upcoming earnings callout
    days_left = days_to_next_earnings(upcoming)
    if upcoming is not None:
        e_date = upcoming["earnings_date"]
        e_eps = upcoming.get("eps_estimate")
        if days_left is not None and days_left <= 30:
            st.warning(
                f"Next earnings: **{e_date}** ({days_left} days away)"
                + (f" — EPS estimate: ₹{e_eps:.2f}" if e_eps else "")
            )
        elif upcoming is not None:
            st.info(
                f"Next earnings: **{e_date}**" + (f" — EPS estimate: ₹{e_eps:.2f}" if e_eps else "")
            )

    # Result volatility analysis
    if not earnings_hist.empty and not df.empty:
        hist_dates = list(earnings_hist["earnings_date"].dropna())
        rv = compute_result_volatility(df, hist_dates)
        if rv["volatility_multiple"] is not None:
            rv1, rv2, rv3 = st.columns(3)
            rv1.metric("Result-period ATR", f"₹{rv['event_atr']:.2f}")
            rv2.metric("Baseline ATR", f"₹{rv['baseline_atr']:.2f}")
            rv3.metric(
                "Volatility multiple",
                f"{rv['volatility_multiple']:.2f}×",
                help=(
                    "How much more volatile the stock is in the ±5-day window "
                    "around each earnings date vs. other periods. "
                    f"Based on {rv['events_found']} result event(s)."
                ),
            )

    # Dividend history
    act_col1, act_col2 = st.columns(2)

    with act_col1:
        st.markdown("##### Dividend history")
        if div_df.empty:
            st.info("No dividend data available from yfinance.")
        else:
            div_fig = go.Figure(
                go.Bar(
                    x=div_df["ex_date"],
                    y=div_df["amount"],
                    name="Dividend (₹)",
                    marker_color="seagreen",
                )
            )
            div_fig.update_layout(
                xaxis_title="Ex-date",
                yaxis_title="Amount (₹ per share)",
                height=300,
                margin=dict(l=10, r=10, t=30, b=20),
            )
            st.plotly_chart(div_fig, width="stretch")
            st.caption(f"Total dividends in dataset: ₹{div_df['amount'].sum():.2f} per share")

    with act_col2:
        st.markdown("##### Split / bonus history")
        if split_df.empty:
            st.info("No split data available from yfinance.")
        else:
            st.dataframe(
                split_df.rename(
                    columns={"ex_date": "Date", "ratio": "Split ratio", "source": "Source"}
                ),
                hide_index=True,
            )

    st.caption("Source: yfinance. Verify against NSE/BSE official records before any decision.")

# --- Holdings ---
with holdings_tab:
    st.markdown(
        "Ownership structure sourced from yfinance. "
        "Coverage for Indian .NS stocks varies; treat as indicative. "
        "For authoritative shareholding patterns, refer to NSE/BSE filings."
    )

    with st.spinner("Fetching holdings data…"):
        _h_summary = holdings_summary(symbol)
        _inst_df = get_institutional_holders(symbol)
        _mf_df = get_mutualfund_holders(symbol)
        _major_df = get_major_holders(symbol)

    if not _h_summary["data_available"]:
        st.info(
            "No holdings data available from yfinance for this symbol. "
            "This is common for Indian stocks where institutional data is limited."
        )
    else:
        h1, h2, h3 = st.columns(3)
        h1.metric(
            "Insider holding",
            f"{_h_summary['insider_pct']:.1f}%" if _h_summary["insider_pct"] is not None else "N/A",
        )
        h2.metric(
            "Institutional holding",
            f"{_h_summary['institution_pct']:.1f}%"
            if _h_summary["institution_pct"] is not None
            else "N/A",
        )
        h3.metric(
            "Float held by institutions",
            f"{_h_summary['float_pct']:.1f}%" if _h_summary["float_pct"] is not None else "N/A",
        )

        if _h_summary["top_holder"]:
            st.caption(
                f"Largest institutional holder: **{_h_summary['top_holder']}** "
                + (
                    f"({_h_summary['top_holder_pct']:.2f}% held)"
                    if _h_summary["top_holder_pct"]
                    else ""
                )
            )

        if not _major_df.empty:
            with st.expander("Major holders breakdown"):
                st.dataframe(_major_df, hide_index=True, width="stretch")

    col_inst, col_mf = st.columns(2)

    with col_inst:
        st.markdown("##### Top institutional holders")
        if _inst_df.empty:
            st.info("No institutional holder data from yfinance.")
        else:
            display_inst = _inst_df.copy()
            if "pct_held" in display_inst.columns:
                display_inst["pct_held"] = display_inst["pct_held"].apply(
                    lambda x: (
                        f"{x:.2f}%"
                        if x is not None and not (isinstance(x, float) and pd.isna(x))
                        else "N/A"
                    )
                )
            st.dataframe(display_inst, hide_index=True, width="stretch")

    with col_mf:
        st.markdown("##### Top mutual fund holders")
        if _mf_df.empty:
            st.info("No mutual fund holder data from yfinance for this symbol.")
        else:
            display_mf = _mf_df.copy()
            if "pct_held" in display_mf.columns:
                display_mf["pct_held"] = display_mf["pct_held"].apply(
                    lambda x: (
                        f"{x:.2f}%"
                        if x is not None and not (isinstance(x, float) and pd.isna(x))
                        else "N/A"
                    )
                )
            st.dataframe(display_mf, hide_index=True, width="stretch")

    st.caption(
        "Source: yfinance (institutional_holders, mutualfund_holders, major_holders). "
        "Verify against NSE/BSE quarterly shareholding disclosures."
    )

# --------------------------------------------------------------------------- #
# Raw data preview
# --------------------------------------------------------------------------- #

with st.expander("📄 Show recent rows"):
    st.dataframe(df.tail(30), width="stretch")

# --------------------------------------------------------------------------- #
# Footer
# --------------------------------------------------------------------------- #

st.markdown("---")
st.caption(
    "Source: yfinance (Yahoo Finance). Not for redistribution. "
    "Verify data against official NSE/BSE sources before any decision."
)

log.info("Rendered page for {} with {} rows.", symbol, len(df))
