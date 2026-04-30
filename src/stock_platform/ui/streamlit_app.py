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
    compute_extended_health,
    compute_multi_year_cagr,
    is_financial_sector,
)
from stock_platform.analytics.fundamentals.sector_ranking import sector_rank_summary  # noqa: E402
from stock_platform.analytics.fundamentals.summary import build_fundamentals_summary  # noqa: E402
from stock_platform.analytics.scanner import (  # noqa: E402
    add_symbols_to_watchlist,
    build_daily_research_brief,
    compare_latest_universe_scans,
    daily_brief_freshness,
    daily_brief_headline,
    daily_brief_table,
    enrich_watchlist_with_latest_scores,
    fetch_watchlist_items,
    list_available_universes,
    load_universe,
    save_universe_scan,
    scan_results_to_frame,
    scan_universe,
    update_watchlist_reviews,
    watchlist_to_frame,
)
from stock_platform.analytics.signals import scan_technical_signals  # noqa: E402
from stock_platform.analytics.signals.audit import (  # noqa: E402
    audits_to_frame,
    fetch_recent_signal_audits,
    fetch_signal_event_export,
    save_signal_audit,
)
from stock_platform.analytics.technicals import (  # noqa: E402
    add_technical_indicators,
    find_support_resistance_zones,
    latest_swing_levels,
)
from stock_platform.config import (  # noqa: E402
    ROOT_DIR,
    get_settings,
    get_thresholds_config,
    get_universe_config,
)
from stock_platform.data.providers import (  # noqa: E402
    CsvBankingFundamentalsProvider,
    CsvFundamentalsProvider,
    KiteProvider,
    KiteProviderError,
    MarketDataProvider,
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
    validate_banking_fundamentals,
    validate_ohlcv,
)
from stock_platform.ops import (  # noqa: E402
    build_data_trust_rows,
    build_provenance_rows,
    data_trust_level,
    data_trust_rows_to_frame,
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


@st.cache_data(ttl=3600, show_spinner=False)
def _load_banking_fundamentals(sym: str) -> pd.DataFrame:
    provider = CsvBankingFundamentalsProvider()
    return provider.get_banking_fundamentals(sym)


def _resolve_project_path(path_value: str) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else ROOT_DIR / path


def _format_pct(value: float | None) -> str:
    return "N/A" if value is None or pd.isna(value) else f"{value * 100:.1f}%"


def _format_pct_points(value: float | None) -> str:
    return "N/A" if value is None or pd.isna(value) else f"{value:.2f}%"


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


def _format_days(value: float | None) -> str:
    """Format a days metric that may be missing from live fundamentals data."""
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.0f}d"


def _unique_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for raw_symbol in symbols:
        normalized = raw_symbol.strip().upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique.append(normalized)
    return unique


def _normalize_user_symbol(raw_symbol: str) -> tuple[str, str | None]:
    """Normalize a user-entered Indian equity symbol for Yahoo Finance."""
    cleaned = raw_symbol.strip().upper()
    if not cleaned:
        return "", None
    if "." not in cleaned:
        return f"{cleaned}.NS", f"Using `{cleaned}.NS` because no exchange suffix was entered."
    return cleaned, None


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


def _universe_label(name: str) -> str:
    """Render a universe option with its size, e.g. 'Nifty 50 · 50 stocks'."""
    pretty = name.replace("_", " ").title()
    try:
        size = len(load_universe(name))
        return f"{pretty} · {size:,} stocks"
    except (FileNotFoundError, KeyError):
        return f"{pretty} · n/a"


def _research_pick_button(
    frame: pd.DataFrame,
    *,
    key: str,
    column_config: dict | None = None,
    hint: str = "Click any row above, then press **Research** to drill into that stock.",
) -> None:
    """Render a clickable dataframe + a Research-the-selected-symbol button.

    Sets ``st.session_state['research_symbol']`` and reruns when the button is
    clicked, which the sidebar reads to drive the per-stock detail view.
    """
    if frame.empty or "symbol" not in frame.columns:
        st.dataframe(frame, width="stretch", hide_index=True, column_config=column_config or {})
        return

    selection = st.dataframe(
        frame,
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key=key,
        column_config=column_config or {},
    )
    rows = selection.get("selection", {}).get("rows", []) if hasattr(selection, "get") else []
    if rows:
        picked = str(frame.iloc[rows[0]]["symbol"])
        if st.button(f"🔍 Research {picked}", type="primary", key=f"{key}_research_btn"):
            st.session_state["research_symbol"] = picked
            st.rerun()
    else:
        st.caption(hint)


def _research_universe_options(extra: list[str] | None = None) -> list[str]:
    """Return a sorted, deduped list of symbols suitable for the sidebar picker.

    Combines every small inline universe (Nifty 50 / Next 50 / Mid-cap select /
    Small-cap select). Excludes the huge ``all_nse_listed`` CSV universe — a
    2,000+ option selectbox is unusable. Any extras (a custom ticker the user
    just researched) are added to the front so they remain selectable.
    """
    symbols: set[str] = set()
    for universe_name in list_available_universes():
        if universe_name == "all_nse_listed":
            continue
        try:
            symbols.update(load_universe(universe_name))
        except (FileNotFoundError, KeyError):
            continue

    if extra:
        symbols.update(s for s in extra if s)

    return sorted(symbols)


GLOSSARY = {
    "RSI 14": "Relative Strength Index over 14 periods. Above 70 often means stretched momentum; below 30 often means oversold. It is context, not a standalone signal.",
    "MACD": "Moving Average Convergence Divergence. It compares short- and medium-term EMAs to show momentum shifts.",
    "ATR 14": "Average True Range over 14 periods. It estimates normal price movement and helps size stops or volatility risk.",
    "Relative volume": "Current volume divided by recent average volume. Above 1 means trading activity is higher than usual.",
    "20 EMA": "20-day Exponential Moving Average. A short-term trend line that reacts quickly to price changes.",
    "50 EMA": "50-day Exponential Moving Average. A medium-term trend line often used to judge trend health.",
    "100 EMA": "100-day Exponential Moving Average. A slower trend line between medium and long-term context.",
    "200 EMA": "200-day Exponential Moving Average. A long-term trend reference watched by many market participants.",
    "ATR %": "ATR as a percentage of price. Higher values mean the stock is more volatile relative to its price.",
    "Historical volatility": "Annualized 20-day realized volatility. It estimates how unstable recent returns have been.",
    "52W high gap": "How far the latest close is from the 52-week high. Negative values mean price is below that high.",
    "MA stack": "Moving-average alignment. Bullish means shorter averages are above longer averages; bearish is the reverse.",
    "Bollinger Bands": "A 20-period moving average plus/minus two standard deviations. Useful for volatility envelopes, not certainty.",
}


def _help(term: str) -> str:
    return GLOSSARY.get(term, "")


def _active_signal_names(signals) -> list[str]:
    return [signal.name for signal in signals if signal.active]


def _research_stance(composite, trust_level: str, active_signals: list[str]) -> tuple[str, str]:
    """Compliance-safe research stance. It is not a buy/sell instruction."""
    if trust_level == "Low":
        return "Verify first", "Data gaps are too large for a confident research conclusion."
    if composite.score >= 75 and len(active_signals) >= 2:
        return (
            "Accumulation watchlist candidate",
            "Strong score and multiple active signals; verify valuation, data quality, and risk before any action.",
        )
    if composite.score >= 60:
        return (
            "Watch / hold research candidate",
            "Score is constructive, but wait for stronger confirmation or cleaner data before upgrading.",
        )
    if composite.score <= 40:
        return (
            "Reduce / avoid-risk review",
            "Weak score suggests this belongs in a risk-review queue rather than an opportunity list.",
        )
    return (
        "Neutral watch",
        "Mixed evidence. Keep on watchlist only if there is a separate research reason.",
    )


def _pros_cons(
    composite, trust_rows: list[dict[str, object]], active_signals: list[str]
) -> tuple[list[str], list[str]]:
    pros = list(composite.reasons[:4])
    if active_signals:
        pros.append(f"Active technical signals: {', '.join(active_signals[:3])}.")
    if composite.score >= 60:
        pros.append(f"Composite score is constructive at {composite.score:.1f}/100.")

    cons = list(composite.risks[:4])
    if composite.missing_data:
        cons.append(f"Missing/provisional inputs: {', '.join(composite.missing_data[:4])}.")
    action_areas = [str(row.get("area")) for row in trust_rows if row.get("status") == "ACTION"]
    if action_areas:
        cons.append(f"Data Trust action areas: {', '.join(action_areas[:4])}.")
    if not active_signals:
        cons.append("No active technical signal fired in the current scan.")

    return pros[:6] or ["No clear positive driver yet."], cons[:6] or [
        "No major risk note surfaced by the MVP checks."
    ]


def _save_kite_access_token_to_env(access_token: str) -> None:
    """Save a generated Kite access token to local .env without displaying it."""
    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        raise FileNotFoundError(".env file was not found in the project root.")

    lines = env_path.read_text(encoding="utf-8").splitlines()
    updated = False
    new_lines: list[str] = []
    for line in lines:
        if line.startswith("KITE_ACCESS_TOKEN="):
            new_lines.append(f"KITE_ACCESS_TOKEN={access_token}")
            updated = True
        else:
            new_lines.append(line)
    if not updated:
        new_lines.append(f"KITE_ACCESS_TOKEN={access_token}")

    env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    log.info("Kite access token saved to local .env without displaying token.")


# --------------------------------------------------------------------------- #
# Page setup
# --------------------------------------------------------------------------- #

st.set_page_config(
    page_title="Indian Stock Research Platform",
    page_icon="chart_with_upwards_trend",
    layout="wide",
)

st.markdown(
    """
    <style>
    /* ---------- Layout polish ---------- */
    .block-container {
        padding-top: 1.0rem;
        padding-bottom: 2rem;
        max-width: 1280px;
    }
    h1, h2, h3, h4 { letter-spacing: -0.01em; }
    h2 {
        margin-top: 1.5rem !important;
        padding-bottom: 0.4rem;
        border-bottom: 1px solid #E2E8F0;
    }

    /* ---------- Metric cards ---------- */
    div[data-testid="stMetric"] {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 0.75rem 0.95rem;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        transition: border-color 0.15s ease;
    }
    div[data-testid="stMetric"]:hover { border-color: #CBD5E1; }
    div[data-testid="stMetricLabel"] {
        font-size: 0.78rem;
        color: #64748B;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        font-weight: 600;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.45rem;
        font-weight: 700;
        color: #0F172A;
    }

    /* ---------- Expanders, tabs, alerts ---------- */
    div[data-testid="stExpander"] {
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        background: #FFFFFF;
    }
    div[data-testid="stExpander"] summary {
        font-weight: 600;
        color: #1E293B;
    }
    button[role="tab"] {
        font-weight: 500 !important;
        color: #64748B !important;
    }
    button[role="tab"][aria-selected="true"] {
        color: #2563EB !important;
        font-weight: 700 !important;
    }
    div[data-testid="stAlert"] {
        border-radius: 10px;
        border-width: 1px;
        font-size: 0.9rem;
    }

    /* ---------- DataFrames ---------- */
    div[data-testid="stDataFrame"] {
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        overflow: hidden;
    }

    /* ---------- App header ---------- */
    .app-header {
        display: flex;
        align-items: flex-end;
        justify-content: space-between;
        gap: 1rem;
        padding: 0.4rem 0 1rem;
        margin-bottom: 1rem;
        border-bottom: 1px solid #E2E8F0;
    }
    .app-title-block { display: flex; flex-direction: column; }
    .app-title {
        font-size: 1.85rem;
        font-weight: 800;
        line-height: 1.1;
        color: #0F172A;
        margin: 0;
        letter-spacing: -0.02em;
    }
    .app-subtitle {
        color: #64748B;
        font-size: 0.92rem;
        margin-top: 0.3rem;
    }
    .phase-pill {
        display: inline-block;
        border: 1px solid #BFDBFE;
        background: #DBEAFE;
        color: #1E40AF;
        border-radius: 999px;
        padding: 0.3rem 0.75rem;
        font-size: 0.75rem;
        font-weight: 600;
        white-space: nowrap;
    }

    /* ---------- Score badges ---------- */
    .score-badge {
        display: inline-block;
        padding: 0.2rem 0.6rem;
        border-radius: 6px;
        font-size: 0.78rem;
        font-weight: 600;
        margin-left: 0.4rem;
    }
    .score-strong { background: #DCFCE7; color: #166534; border: 1px solid #BBF7D0; }
    .score-watch  { background: #FEF3C7; color: #92400E; border: 1px solid #FDE68A; }
    .score-weak   { background: #FEE2E2; color: #991B1B; border: 1px solid #FECACA; }

    /* ---------- Compact disclaimer ---------- */
    .disclaimer {
        border-left: 3px solid #F59E0B;
        background: #FFFBEB;
        color: #78350F;
        border-radius: 6px;
        padding: 0.55rem 0.8rem;
        margin-bottom: 1rem;
        font-size: 0.82rem;
        line-height: 1.45;
    }

    /* ---------- Sidebar polish ---------- */
    section[data-testid="stSidebar"] {
        background: #F8FAFC;
        border-right: 1px solid #E2E8F0;
    }
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 { color: #0F172A; }
    </style>
    """,
    unsafe_allow_html=True,
)

# --------------------------------------------------------------------------- #
# Disclaimer banner (always visible)
# --------------------------------------------------------------------------- #

st.markdown(
    """
    <div class="app-header">
      <div class="app-title-block">
        <div class="app-title">📈 Indian Stock Research Platform</div>
        <div class="app-subtitle">
          Fundamentals · Technicals · Flows · Composite scoring · Backtests · Universe scanner
        </div>
      </div>
      <span class="phase-pill">Phase 8.5 · 193 tests passing</span>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="disclaimer">
      <strong>Disclaimer:</strong> Personal research aid only. Not investment advice,
      not a SEBI-registered RA/RIA service, and not a guarantee of returns.
      Verify source data before any decision.
    </div>
    """,
    unsafe_allow_html=True,
)

# --------------------------------------------------------------------------- #
# Daily Research Brief (Phase 8.5)
# --------------------------------------------------------------------------- #

brief_universes = list_available_universes()
if brief_universes:
    st.subheader("Daily Research Brief")
    st.caption(
        "Built from the latest saved universe scan and your local shortlist. "
        "It does not run fresh network calls; run a universe scan below when you want new data."
    )
    brief_col1, brief_col2 = st.columns([2, 1])
    with brief_col1:
        brief_universe = st.selectbox(
            "Brief universe",
            options=brief_universes,
            format_func=_universe_label,
            help="Uses the latest saved scan for this universe.",
            key="daily_brief_universe",
        )
    with brief_col2:
        brief_min_score = st.slider(
            "Brief opportunity score",
            min_value=0,
            max_value=100,
            value=60,
            step=5,
            help="Minimum score for new opportunities in the daily brief.",
        )

    daily_brief = build_daily_research_brief(
        brief_universe,
        min_opportunity_score=float(brief_min_score),
    )

    if not daily_brief.has_latest_scan:
        st.info(
            "No saved scan exists yet for this universe. Open Top Opportunities below, "
            "run a small scan, and this brief will populate automatically."
        )
    else:
        # TL;DR headline
        st.markdown(f"**{daily_brief_headline(daily_brief)}**")

        # Freshness banner: green/amber/red based on age of latest saved scan
        freshness_status, freshness_age = daily_brief_freshness(daily_brief.latest_run_at)
        if freshness_status == "fresh":
            st.success(f"Latest scan is fresh — saved {freshness_age}.")
        elif freshness_status == "aging":
            st.warning(
                f"Latest scan is **{freshness_age}** — consider running a fresh scan "
                "before relying on these rows for new decisions."
            )
        elif freshness_status == "stale":
            st.error(
                f"Latest scan is **{freshness_age}** — STALE. "
                "Run a fresh scan in the Top Opportunities expander before using these rows."
            )
        else:
            st.caption(f"Scan freshness: {freshness_age}.")

        brief_metric_cols = st.columns(6)
        brief_metric_cols[0].metric("Latest run", f"#{daily_brief.latest_run_id}")
        brief_metric_cols[1].metric("Successful", daily_brief.successful_symbols)
        brief_metric_cols[2].metric("Failed", daily_brief.failed_symbols)
        brief_metric_cols[3].metric(
            "Average score",
            "N/A" if daily_brief.average_score is None else f"{daily_brief.average_score:.1f}",
        )
        brief_metric_cols[4].metric(
            "Top score",
            "N/A" if daily_brief.top_score is None else f"{daily_brief.top_score:.1f}",
        )
        brief_metric_cols[5].metric(
            "Action items",
            len(daily_brief.data_quality_actions) + len(daily_brief.shortlist_actions),
        )

        if daily_brief.latest_run_at:
            st.caption(
                f"Latest saved scan time: `{daily_brief.latest_run_at}`"
                + (
                    f" | Compared with scan #{daily_brief.previous_run_id}"
                    if daily_brief.previous_run_id
                    else " | No previous scan yet"
                )
            )

        brief_tabs = st.tabs(
            [
                "New opportunities",
                "Score movers",
                "New signals",
                "Action queue",
                "Shortlist follow-up",
            ]
        )

        _brief_columns = {
            "composite_score": st.column_config.NumberColumn("Score", format="%.1f"),
            "previous_score": st.column_config.NumberColumn("Prev score", format="%.1f"),
            "score_change": st.column_config.NumberColumn("Score Δ", format="%+.1f"),
            "active_signal_count": st.column_config.NumberColumn("Signals", format="%d"),
        }

        with brief_tabs[0]:
            new_opp_table = daily_brief_table(daily_brief.new_opportunities)
            if new_opp_table.empty:
                st.info("No new opportunity rows met the current brief score threshold.")
            else:
                _research_pick_button(
                    new_opp_table,
                    key="brief_new_opp_table",
                    column_config=_brief_columns,
                )

        with brief_tabs[1]:
            mover_col1, mover_col2 = st.columns(2)
            with mover_col1:
                st.markdown("##### Improved")
                improved_table = daily_brief_table(daily_brief.improved)
                if improved_table.empty:
                    st.info("No meaningful score improvers yet.")
                else:
                    _research_pick_button(
                        improved_table,
                        key="brief_improved_table",
                        column_config=_brief_columns,
                    )
            with mover_col2:
                st.markdown("##### Weakened")
                weakened_table = daily_brief_table(daily_brief.weakened)
                if weakened_table.empty:
                    st.info("No meaningful score weakeners yet.")
                else:
                    _research_pick_button(
                        weakened_table,
                        key="brief_weakened_table",
                        column_config=_brief_columns,
                    )

        with brief_tabs[2]:
            new_signal_table = daily_brief_table(daily_brief.new_signals)
            if new_signal_table.empty:
                st.info("No newly active signals versus the previous saved scan.")
            else:
                _research_pick_button(
                    new_signal_table,
                    key="brief_new_signals_table",
                    column_config=_brief_columns,
                )

        with brief_tabs[3]:
            action_table = daily_brief_table(daily_brief.data_quality_actions, limit=15)
            if action_table.empty:
                st.success("No saved-scan data-quality action rows in the latest scan.")
            else:
                st.dataframe(action_table, width="stretch", hide_index=True)
                st.caption(
                    "These rows need verification before relying on the score: missing data, "
                    "source warnings, or scan errors."
                )

        with brief_tabs[4]:
            shortlist_actions = daily_brief.shortlist_actions
            if shortlist_actions.empty:
                st.success("No active shortlist follow-up rows need review.")
            else:
                shortlist_columns = [
                    "symbol",
                    "review_status",
                    "tags",
                    "notes",
                    "latest_score",
                    "latest_band",
                    "latest_active_signals",
                    "updated_at",
                ]
                st.dataframe(
                    shortlist_actions[
                        [
                            column
                            for column in shortlist_columns
                            if column in shortlist_actions.columns
                        ]
                    ],
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "latest_score": st.column_config.NumberColumn(
                            "Latest score", format="%.1f"
                        ),
                        "latest_active_signals": st.column_config.NumberColumn(
                            "Signals", format="%d"
                        ),
                    },
                )

# --------------------------------------------------------------------------- #
# Top Opportunities scanner (Phase 8)
# --------------------------------------------------------------------------- #

st.markdown('<span id="top-opportunities-universe-scan"></span>', unsafe_allow_html=True)
with st.expander("🔭 Top Opportunities (universe scan)", expanded=False):
    st.caption(
        "Run the platform's per-stock pipeline (price → indicators → signals → "
        "composite score) across an entire index, then rank the results. "
        "This Phase 8 MVP scanner uses price/technical inputs first; fundamentals "
        "and flow inputs are still limited during universe-wide scans. "
        "Scans run sequentially for reliability, so a Nifty 50 scan can take a few minutes. "
        "Educational research support — not investment advice."
    )

    universes_available = list_available_universes()
    if not universes_available:
        st.warning("No universes configured. Add lists to `config/universes.yaml`.")
    else:
        scan_col1, scan_col2, scan_col3, scan_col4 = st.columns([2, 1, 1, 1])
        with scan_col1:
            chosen_universe = st.selectbox(
                "Universe",
                options=universes_available,
                format_func=_universe_label,
                help="Defined in config/universes.yaml. Use Mid-cap / Small-cap selects for broader research, all_nse_listed for the full ~2,000-stock NSE universe.",
            )
        with scan_col2:
            min_score = st.slider("Min composite score", 0, 100, 60, 5)
        with scan_col3:
            min_signals = st.slider("Min active signals", 0, 7, 1)

        try:
            tickers = load_universe(chosen_universe)
            universe_error = None
        except (FileNotFoundError, KeyError) as exc:
            tickers = []
            universe_error = str(exc)

        with scan_col4:
            max_symbols = st.number_input(
                "Max symbols",
                min_value=1,
                max_value=max(1, len(tickers)),
                value=min(25, max(1, len(tickers))),
                step=5,
                help="Keeps full-universe scans manageable. Increase gradually.",
                disabled=not tickers,
            )

        if universe_error:
            st.warning(universe_error)
            if chosen_universe == "all_nse_listed":
                st.code(
                    r"powershell -ExecutionPolicy Bypass -File .\scripts\update_nse_universe.ps1",
                    language="powershell",
                )
        else:
            st.caption(
                f"`{chosen_universe}` contains {len(tickers):,} symbol(s). "
                f"This run will scan the first {min(int(max_symbols), len(tickers)):,}."
            )

        scan_lookback_days = 400

        if st.button("Run universe scan", type="primary", disabled=not tickers):
            scan_tickers = tickers[: int(max_symbols)]
            progress = st.progress(0.0, text=f"Scanning 0/{len(scan_tickers)} symbols...")

            def _on_progress(done: int, total: int, sym: str) -> None:
                progress.progress(done / total, text=f"Scanned {done}/{total}: {sym}")

            with st.spinner(f"Running {chosen_universe} scan…"):
                scan_results = scan_universe(
                    scan_tickers,
                    lookback_days=scan_lookback_days,
                    max_workers=1,
                    progress_callback=_on_progress,
                )
            progress.empty()

            run_id = save_universe_scan(
                universe_name=chosen_universe,
                results=scan_results,
                lookback_days=scan_lookback_days,
                min_score_filter=float(min_score),
                min_signals_filter=int(min_signals),
                note=f"UI scan capped at {len(scan_tickers)} symbol(s).",
            )

            results_df = scan_results_to_frame(scan_results)
            results_df = results_df[results_df["error"].isna()]
            filtered = results_df[
                (results_df["composite_score"].fillna(0) >= min_score)
                & (results_df["active_signal_count"].fillna(0) >= min_signals)
            ]

            success_count = len(results_df)
            failed_count = len(scan_results) - success_count
            st.success(
                f"Scan #{run_id} saved: {success_count} successful, {failed_count} failed. "
                f"{len(filtered)} match your filters (score ≥ {min_score}, "
                f"≥ {min_signals} active signals)."
            )

            if filtered.empty:
                st.info(
                    "No stocks match the filters. Try lowering the composite score "
                    "or active-signal threshold."
                )
            else:
                display = filtered.copy()
                display = display[
                    [
                        "symbol",
                        "composite_score",
                        "band",
                        "active_signal_count",
                        "active_signals",
                        "fundamentals",
                        "technicals",
                        "flows",
                        "last_close",
                        "rsi_14",
                        "ma_stack",
                        "data_quality_warnings",
                    ]
                ]
                scanner_selection = st.dataframe(
                    display,
                    width="stretch",
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="scanner_results_table",
                    column_config={
                        "composite_score": st.column_config.NumberColumn("Score", format="%.1f"),
                        "active_signal_count": st.column_config.NumberColumn(
                            "Active signals", format="%d"
                        ),
                        "fundamentals": st.column_config.NumberColumn("Fund", format="%.1f"),
                        "technicals": st.column_config.NumberColumn("Tech", format="%.1f"),
                        "flows": st.column_config.NumberColumn("Flows", format="%.1f"),
                        "last_close": st.column_config.NumberColumn("Last close", format="₹%.2f"),
                        "rsi_14": st.column_config.NumberColumn("RSI 14", format="%.1f"),
                    },
                )

                _selected_rows = scanner_selection.get("selection", {}).get("rows", [])
                if _selected_rows:
                    _picked_symbol = str(display.iloc[_selected_rows[0]]["symbol"])
                    _btn_col, _hint_col = st.columns([1, 3])
                    with _btn_col:
                        if st.button(
                            f"🔍 Research {_picked_symbol}",
                            type="primary",
                            key="scanner_research_btn",
                        ):
                            st.session_state["research_symbol"] = _picked_symbol
                            st.rerun()
                    with _hint_col:
                        st.caption(
                            "Loads the full per-stock view (fundamentals, technicals, "
                            "signals, flows, holdings, composite score) for this symbol."
                        )
                else:
                    st.caption(
                        "Click any row above, then press **Research** to drill into that stock."
                    )

                st.download_button(
                    "Download scan results CSV",
                    data=display.to_csv(index=False),
                    file_name=f"{chosen_universe}_scan.csv",
                    mime="text/csv",
                )

        latest_run, previous_run, comparison_frame = compare_latest_universe_scans(chosen_universe)
        if latest_run is not None:
            st.markdown("#### Latest saved scan")
            latest_col1, latest_col2, latest_col3, latest_col4 = st.columns(4)
            latest_col1.metric("Run ID", latest_run.id)
            latest_col2.metric("Saved rows", latest_run.requested_symbols)
            latest_col3.metric("Successful", latest_run.successful_symbols)
            latest_col4.metric("Failed", latest_run.failed_symbols)

            latest_success = comparison_frame[comparison_frame["error"].isna()].copy()
            if not latest_success.empty:
                filter_col1, filter_col2, filter_col3 = st.columns(3)
                status_options = [
                    "All",
                    *sorted(latest_success["comparison_status"].dropna().unique()),
                ]
                with filter_col1:
                    comparison_status_filter = st.selectbox(
                        "Comparison status",
                        options=status_options,
                        help="Focus on improved, weakened, stable, or new symbols.",
                    )
                with filter_col2:
                    min_score_change_filter = st.number_input(
                        "Min score change",
                        value=-100.0,
                        min_value=-100.0,
                        max_value=100.0,
                        step=1.0,
                        help="Use 0 to show flat/improving rows; use 5 for meaningful improvement.",
                    )
                with filter_col3:
                    new_signals_only = st.checkbox(
                        "New signals only",
                        value=False,
                        help="Show only stocks with at least one newly active signal.",
                    )

                if comparison_status_filter != "All":
                    latest_success = latest_success[
                        latest_success["comparison_status"] == comparison_status_filter
                    ]
                latest_success = latest_success[
                    latest_success["score_change"].isna()
                    | (latest_success["score_change"] >= float(min_score_change_filter))
                ]
                if new_signals_only:
                    latest_success = latest_success[
                        latest_success["new_active_signals"].fillna("").astype(str).str.strip()
                        != ""
                    ]

            latest_display = latest_success.head(25)
            if latest_display.empty:
                st.info("The latest saved scan has no successful stock rows to display.")
            else:
                if previous_run is None:
                    st.caption("No previous saved scan exists yet for comparison.")
                else:
                    st.caption(
                        f"Compared against saved scan #{previous_run.id}. "
                        "Positive score change means the latest scan improved."
                    )
                _latest_display_cols = [
                    "symbol",
                    "composite_score",
                    "previous_score",
                    "score_change",
                    "comparison_status",
                    "band",
                    "active_signal_count",
                    "signal_count_change",
                    "new_active_signals",
                    "active_signals",
                    "last_close",
                    "rsi_14",
                    "ma_stack",
                    "data_quality_warnings",
                ]
                _latest_view = latest_display[_latest_display_cols]
                latest_selection = st.dataframe(
                    _latest_view,
                    width="stretch",
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="latest_saved_scan_table",
                    column_config={
                        "composite_score": st.column_config.NumberColumn("Score", format="%.1f"),
                        "previous_score": st.column_config.NumberColumn(
                            "Previous score", format="%.1f"
                        ),
                        "score_change": st.column_config.NumberColumn("Score Δ", format="%+.1f"),
                        "active_signal_count": st.column_config.NumberColumn(
                            "Active signals", format="%d"
                        ),
                        "signal_count_change": st.column_config.NumberColumn(
                            "Signal Δ", format="%+.0f"
                        ),
                        "last_close": st.column_config.NumberColumn("Last close", format="₹%.2f"),
                        "rsi_14": st.column_config.NumberColumn("RSI 14", format="%.1f"),
                    },
                )

                _saved_rows = latest_selection.get("selection", {}).get("rows", [])
                if _saved_rows:
                    _saved_pick = str(_latest_view.iloc[_saved_rows[0]]["symbol"])
                    if st.button(
                        f"🔍 Research {_saved_pick}",
                        type="primary",
                        key="saved_scan_research_btn",
                    ):
                        st.session_state["research_symbol"] = _saved_pick
                        st.rerun()
                else:
                    st.caption(
                        "Click any row above, then press **Research** to drill into that stock."
                    )

                shortlist_options = list(latest_success["symbol"].head(50))
                symbols_to_shortlist = st.multiselect(
                    "Add symbols from latest saved scan to research shortlist",
                    options=shortlist_options,
                    help="This only saves symbols for follow-up research. It is not a trade list.",
                )
                if st.button("Add selected to shortlist", disabled=not symbols_to_shortlist):
                    added_count = add_symbols_to_watchlist(
                        symbols_to_shortlist,
                        source_universe=chosen_universe,
                        source_run_id=int(latest_run.id),
                        reason="Selected from Phase 8 scanner comparison.",
                    )
                    st.success(f"Saved {added_count} symbol(s) to the local research shortlist.")

                st.download_button(
                    "Download latest saved scan CSV",
                    data=comparison_frame.to_csv(index=False),
                    file_name=f"{chosen_universe}_latest_saved_scan.csv",
                    mime="text/csv",
                )

        st.markdown("#### Local research shortlist")
        show_inactive_shortlist = st.checkbox(
            "Show inactive shortlist rows",
            value=False,
            help="Inactive rows stay saved for audit/history but are hidden by default.",
        )
        shortlist_frame = watchlist_to_frame(
            fetch_watchlist_items(active_only=not show_inactive_shortlist)
        )
        if shortlist_frame.empty:
            st.info("No shortlisted stocks yet. Add symbols from a saved scan above.")
        else:
            shortlist_frame = enrich_watchlist_with_latest_scores(shortlist_frame)
            shortlist_display = shortlist_frame[
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
            edited_shortlist = st.data_editor(
                shortlist_display,
                width="stretch",
                hide_index=True,
                column_config={
                    "symbol": st.column_config.TextColumn("Symbol"),
                    "review_status": st.column_config.SelectboxColumn(
                        "Review status",
                        options=["watch", "deep_dive", "avoid", "done"],
                        help="Simple research workflow status.",
                    ),
                    "tags": st.column_config.TextColumn(
                        "Tags",
                        help="Comma-separated labels such as bank, earnings, breakout, avoid.",
                    ),
                    "notes": st.column_config.TextColumn(
                        "Notes",
                        width="large",
                        help="Your local research notes. Keep them factual and non-advisory.",
                    ),
                    "active": st.column_config.CheckboxColumn(
                        "Active",
                        help="Untick to hide this row from the default shortlist view.",
                    ),
                    "latest_score": st.column_config.NumberColumn(
                        "Latest score",
                        format="%.1f",
                    ),
                    "latest_active_signals": st.column_config.NumberColumn(
                        "Signals",
                        format="%d",
                    ),
                    "latest_close": st.column_config.NumberColumn(
                        "Last close",
                        format="₹%.2f",
                    ),
                    "latest_run_id": st.column_config.NumberColumn(
                        "Scan run",
                        format="%d",
                    ),
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
                key=f"shortlist_editor_{chosen_universe}_{show_inactive_shortlist}",
            )
            save_col, export_col = st.columns([1, 2])
            with save_col:
                if st.button("Save shortlist review edits"):
                    updated_count = update_watchlist_reviews(
                        edited_shortlist.to_dict(orient="records")
                    )
                    st.success(f"Saved review edits for {updated_count} shortlist row(s).")
                    st.rerun()
            with export_col:
                st.download_button(
                    "Download shortlist CSV",
                    data=edited_shortlist.to_csv(index=False),
                    file_name="research_shortlist.csv",
                    mime="text/csv",
                )

# --------------------------------------------------------------------------- #
# Sidebar: inputs
# --------------------------------------------------------------------------- #

settings = get_settings()
universe = get_universe_config()
starter = universe.get("starter_watchlist", ["RELIANCE.NS"])

# Symbol pre-selected by clicking a row in the scanner / brief / shortlist
_jumped_symbol = st.session_state.get("research_symbol")

# Build the searchable picker list: every inline universe + the jumped symbol
_picker_options = _research_universe_options(extra=[_jumped_symbol] if _jumped_symbol else None)
if not _picker_options:
    _picker_options = list(starter)

# Default index: jumped symbol if set, else first starter, else first option
if _jumped_symbol and _jumped_symbol in _picker_options:
    _default_index = _picker_options.index(_jumped_symbol)
elif starter and starter[0] in _picker_options:
    _default_index = _picker_options.index(starter[0])
else:
    _default_index = 0

with st.sidebar:
    st.markdown("### Navigate")
    st.markdown(
        """
        - [Daily brief](#daily-research-brief)
        - [Universe scanner](#top-opportunities-universe-scan)
        - [Fundamentals](#fundamentals)
        - [Research guardrails](#research-guardrails)
        - [Composite score](#composite-score)
        - [Interactive chart](#interactive-chart)
        - [Technicals](#technicals)
        - [Flows & events](#flows-events)
        - [Zerodha API setup](#zerodha-api-setup)
        - [Operations](#operations-alerts)
        """
    )
    st.markdown("---")
    st.header("Research a stock")
    st.caption(
        f"Type to search across {len(_picker_options):,} curated NSE tickers. "
        "Click a row in the scanner or shortlist to jump straight here."
    )
    symbol = st.selectbox(
        "Ticker",
        options=_picker_options,
        index=_default_index,
        help="Yahoo Finance symbol. `.NS` for NSE, `.BO` for BSE. Type to filter.",
    )
    custom = st.text_input(
        "Or enter a ticker not in the list",
        value="",
        placeholder="e.g. RVNL or BHEL.NS",
        help=(
            "For NSE stocks, you can type either RVNL or RVNL.NS. "
            "The app will try the .NS suffix automatically when no suffix is provided."
        ),
    )
    if custom.strip():
        symbol, symbol_note = _normalize_user_symbol(custom)
        if symbol_note:
            st.caption(symbol_note)
        # Clear the jumped-symbol flag so manual entry takes precedence
        st.session_state.pop("research_symbol", None)

    today = date.today()
    default_start = today - timedelta(days=5 * 365)
    start = st.date_input("Start date", value=default_start, max_value=today - timedelta(days=1))
    end = st.date_input(
        "End date", value=today, min_value=start + timedelta(days=1), max_value=today
    )

    st.markdown("---")
    st.caption(f"Environment: `{settings.app_env}`")
    st.caption(f"Log level: `{settings.app_log_level}`")
    st.caption(f"Market data provider: `{settings.market_data_provider}`")
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

provider = MarketDataProvider()

with st.spinner(f"Downloading {symbol}…"):
    try:
        df = provider.get_ohlcv(symbol=symbol, start=start, end=end)
    except Exception as exc:  # noqa: BLE001
        log.exception("Download failed for {}: {}", symbol, exc)
        st.error(f"Could not fetch data for **{symbol}**: {exc}")
        st.stop()

if df.empty:
    st.error(f"No price data was returned for **{symbol}**.")
    st.info(
        "What this usually means: the symbol is typed incorrectly, the exchange suffix is wrong, "
        "the company symbol has changed, the stock is inactive/delisted, the selected date range "
        "has no trading days, Kite/yfinance is temporarily missing the data, or Kite could not map "
        "the instrument token. Try a known live symbol such as RELIANCE.NS or HDFCBANK.NS, then "
        "update the local universe list if this symbol is stale."
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
price_source = str(df.attrs.get("source") or provider.last_source or "unknown")
price_provider_label = str(df.attrs.get("provider_label") or price_source)
fallback_reason = str(df.attrs.get("fallback_reason") or provider.last_warning or "")
try:
    saved_signal_count = save_signal_audit(
        symbol,
        df,
        technical_signals,
        source=price_source,
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

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Symbol", symbol)
c2.metric("Last close", f"₹{latest['close']:.2f}", f"{pct:+.2f}% d/d")
c3.metric("Rows", f"{len(df):,}")
c4.metric("Last date", df.index[-1].strftime("%Y-%m-%d"))
c5.metric("Data source", price_provider_label)

if fallback_reason:
    st.warning(fallback_reason)
elif price_source == "kite":
    st.success("Data source: Zerodha Kite")
elif price_source == "yfinance":
    st.info("Data source: yfinance fallback")

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
fundamentals_frame = pd.DataFrame()
fundamentals_quality_errors: list[str] = []
fundamentals_quality_warnings: list[str] = []
banking_trust_frame = pd.DataFrame()
banking_quality_errors: list[str] = []
banking_quality_warnings: list[str] = []

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
        fundamentals_quality_errors = list(fundamentals_report.errors)
        fundamentals_quality_warnings = list(fundamentals_report.warnings)
        if fundamentals_report.errors:
            st.warning("Fundamentals data quality gaps:")
            for error in fundamentals_report.errors:
                st.markdown(f"- {error}")
        if fundamentals_report.warnings:
            st.warning("Fundamentals data quality warnings:")
            for warning in fundamentals_report.warnings:
                st.markdown(f"- {warning}")

        snapshots = fundamentals_provider.get_snapshots(symbol)
        latest_fundamentals = snapshots[-1]
        previous_fundamentals = snapshots[-2] if len(snapshots) > 1 else None
        selected_sector = (
            fundamentals_frame["sector"].dropna().iloc[-1]
            if "sector" in fundamentals_frame.columns
            and not fundamentals_frame["sector"].dropna().empty
            else None
        )
        selected_industry = (
            fundamentals_frame["industry"].dropna().iloc[-1]
            if "industry" in fundamentals_frame.columns
            and not fundamentals_frame["industry"].dropna().empty
            else None
        )
        financial_sector = is_financial_sector(
            symbol=symbol,
            sector=selected_sector,
            industry=selected_industry,
        )
        ratios = calculate_basic_ratios(latest_fundamentals)
        altman = None if financial_sector else calculate_altman_z_score(latest_fundamentals)
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
        f4.metric(
            "Altman Z-Score",
            "N/A for financials" if financial_sector else _format_number(altman.score),
        )

        r1, r2, r3, r4 = st.columns(4)
        r1.metric("ROA", _format_pct(ratios["return_on_assets"]))
        r2.metric("ROE", _format_pct(ratios["return_on_equity"]))
        r3.metric(
            "Debt / Equity",
            "N/A for financials" if financial_sector else _format_number(ratios["debt_to_equity"]),
        )
        r4.metric("Source", source)
        if financial_sector:
            st.info(
                "Financial-sector rules applied: Altman Z-Score, cash-conversion cycle, "
                "working-capital trend, and industrial debt/equity checks are not used for this stock."
            )

        # Multi-year CAGR (master prompt §4.1)
        st.markdown("##### Multi-year CAGR")
        cagr_all = compute_multi_year_cagr(snapshots)
        cagr_metrics = [
            ("revenue", "Revenue"),
            ("ebitda", "EBITDA"),
            ("net_income", "Net income"),
            ("eps", "EPS"),
            ("operating_cash_flow", "OCF"),
            ("free_cash_flow", "FCF"),
            ("book_value", "Book value"),
        ]
        cagr_rows = []
        for key, label in cagr_metrics:
            cagr_rows.append(
                {
                    "Metric": label,
                    "3Y CAGR": _format_pct(cagr_all.get(f"{key}_cagr_3y")),
                    "5Y CAGR": _format_pct(cagr_all.get(f"{key}_cagr_5y")),
                    "10Y CAGR": _format_pct(cagr_all.get(f"{key}_cagr_10y")),
                }
            )
        st.dataframe(cagr_rows, hide_index=True, width="stretch")
        st.caption(
            "CAGR returns 'N/A' when fewer than the required years of data are "
            "available, or when the start value is zero/negative."
        )

        if financial_sector:
            st.markdown("##### Bank / financial fundamentals")
            b1, b2, b3 = st.columns(3)
            b1.metric("ROA", _format_pct(ratios["return_on_assets"]))
            b2.metric("ROE", _format_pct(ratios["return_on_equity"]))
            b3.metric("Net income growth", _format_pct(growth.get("net_income_growth")))

            banking_frame = _load_banking_fundamentals(symbol)
            banking_trust_frame = banking_frame
            banking_template_path = "data/sample/banking_fundamentals_template.csv"
            if banking_frame.empty:
                st.info(
                    "No manual banking metrics found yet. Add audited rows to "
                    f"`{banking_template_path}` to show NIM, GNPA, NNPA, CASA, "
                    "credit/deposit growth, and capital adequacy here."
                )
                with st.expander("Banking metrics data-entry helper", expanded=False):
                    st.caption(
                        "Use audited annual reports, investor presentations, or exchange filings. "
                        "Leave unknown fields blank; the validator will warn instead of guessing."
                    )
                    st.dataframe(
                        [
                            {"Field": "nim_pct", "Meaning": "Net Interest Margin %"},
                            {"Field": "gnpa_pct", "Meaning": "Gross NPA %"},
                            {"Field": "nnpa_pct", "Meaning": "Net NPA %"},
                            {"Field": "casa_pct", "Meaning": "CASA ratio %"},
                            {"Field": "credit_growth_pct", "Meaning": "Loan/advances growth %"},
                            {"Field": "deposit_growth_pct", "Meaning": "Deposit growth %"},
                            {
                                "Field": "capital_adequacy_pct",
                                "Meaning": "Capital adequacy / CRAR %",
                            },
                        ],
                        hide_index=True,
                        width="stretch",
                    )
                    st.code(
                        "symbol,fiscal_year,nim_pct,gnpa_pct,nnpa_pct,casa_pct,"
                        "credit_growth_pct,deposit_growth_pct,capital_adequacy_pct,"
                        "source,source_url,last_updated\n"
                        f"{symbol},2025,,,,,,,,annual_report_2025,PASTE_SOURCE_URL_HERE,"
                        "2026-04-28",
                        language="csv",
                    )
                    st.caption("Detailed guide: `docs/banking_fundamentals_entry_guide.md`.")
            else:
                banking_report = validate_banking_fundamentals(
                    banking_frame,
                    symbol=symbol,
                    raise_on_error=False,
                )
                banking_quality_errors = list(banking_report.errors)
                banking_quality_warnings = list(banking_report.warnings)
                if banking_report.errors:
                    st.warning("Banking metrics data quality gaps:")
                    for error in banking_report.errors:
                        st.markdown(f"- {error}")
                if banking_report.warnings:
                    st.warning("Banking metrics data quality warnings:")
                    for warning in banking_report.warnings:
                        st.markdown(f"- {warning}")

                latest_bank = banking_frame.iloc[-1]
                bm1, bm2, bm3, bm4 = st.columns(4)
                bm1.metric("NIM", _format_pct_points(latest_bank.get("nim_pct")))
                bm2.metric("GNPA", _format_pct_points(latest_bank.get("gnpa_pct")))
                bm3.metric("NNPA", _format_pct_points(latest_bank.get("nnpa_pct")))
                bm4.metric("CASA", _format_pct_points(latest_bank.get("casa_pct")))
                bm5, bm6, bm7, bm8 = st.columns(4)
                bm5.metric(
                    "Credit growth",
                    _format_pct_points(latest_bank.get("credit_growth_pct")),
                )
                bm6.metric(
                    "Deposit growth",
                    _format_pct_points(latest_bank.get("deposit_growth_pct")),
                )
                bm7.metric(
                    "Capital adequacy",
                    _format_pct_points(latest_bank.get("capital_adequacy_pct")),
                )
                bm8.metric("Fiscal year", str(latest_bank.get("fiscal_year")))
                st.caption(
                    "Source: "
                    f"{latest_bank.get('source') or 'manual CSV'} | "
                    f"Last updated: {latest_bank.get('last_updated') or 'N/A'}"
                )
                st.dataframe(
                    banking_frame[
                        [
                            "fiscal_year",
                            "nim_pct",
                            "gnpa_pct",
                            "nnpa_pct",
                            "casa_pct",
                            "credit_growth_pct",
                            "deposit_growth_pct",
                            "capital_adequacy_pct",
                            "source",
                            "source_url",
                            "last_updated",
                        ]
                    ],
                    hide_index=True,
                    width="stretch",
                    column_config={
                        "nim_pct": st.column_config.NumberColumn("NIM %", format="%.2f"),
                        "gnpa_pct": st.column_config.NumberColumn("GNPA %", format="%.2f"),
                        "nnpa_pct": st.column_config.NumberColumn("NNPA %", format="%.2f"),
                        "casa_pct": st.column_config.NumberColumn("CASA %", format="%.2f"),
                        "credit_growth_pct": st.column_config.NumberColumn(
                            "Credit growth %", format="%.2f"
                        ),
                        "deposit_growth_pct": st.column_config.NumberColumn(
                            "Deposit growth %", format="%.2f"
                        ),
                        "capital_adequacy_pct": st.column_config.NumberColumn(
                            "Capital adequacy %", format="%.2f"
                        ),
                    },
                )
        else:
            # Extended balance-sheet health (master prompt §4.1)
            st.markdown("##### Balance-sheet health (extended)")
            ext = compute_extended_health(snapshots)
            e1, e2, e3, e4 = st.columns(4)
            e1.metric(
                "Interest coverage",
                f"{ext['interest_coverage']:.1f}×"
                if ext["interest_coverage"] is not None
                else "N/A",
                help="EBIT / |Interest expense|. >5× is healthy; <1.5× is a red flag.",
            )
            e2.metric(
                "Cash conv. cycle",
                f"{ext['ccc_days']:.0f} d" if ext["ccc_days"] is not None else "N/A",
                help="DSO + DIO − DPO (days). Lower is better; negative is excellent.",
            )
            e3.metric(
                "Working capital",
                f"₹{ext['working_capital_latest']:,.0f}"
                if ext["working_capital_latest"] is not None
                else "N/A",
            )
            e4.metric(
                "WC YoY change",
                _format_pct(ext["working_capital_yoy_change"]),
                help="Change in working capital vs. prior fiscal year.",
            )

            if any(ext.get(key) is not None for key in ("dso_days", "dio_days", "dpo_days")):
                st.caption(
                    f"DSO: {_format_days(ext['dso_days'])}  |  "
                    f"DIO: {_format_days(ext['dio_days'])}  |  "
                    f"DPO: {_format_days(ext['dpo_days'])}"
                )

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
    score_banking_frame = _load_banking_fundamentals(symbol)
    score_banking_row = (
        score_banking_frame.iloc[-1].to_dict() if not score_banking_frame.empty else None
    )
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
        banking_fundamentals=score_banking_row,
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

trust_rows = build_data_trust_rows(
    symbol=symbol,
    price_frame=df,
    price_source=settings.provider_price,
    price_warnings=report.warnings,
    price_errors=report.errors,
    fundamentals_frame=fundamentals_frame,
    fundamentals_source=_fundamentals_source_label,
    fundamentals_warnings=fundamentals_quality_warnings,
    fundamentals_errors=fundamentals_quality_errors,
    banking_frame=banking_trust_frame,
    banking_applicable=is_financial_sector(row=selected_summary) if selected_summary else False,
    banking_warnings=banking_quality_warnings,
    banking_errors=banking_quality_errors,
    composite_missing=composite.missing_data,
    composite_risks=composite.risks,
    active_signal_count=sum(1 for signal in technical_signals if signal.active),
    delivery_available=score_delivery_stats is not None,
    result_volatility_available=score_result_volatility is not None,
)
trust_level, trust_reason = data_trust_level(trust_rows)
trust_col_1, trust_col_2, trust_col_3 = st.columns([1, 2, 1])
trust_col_1.metric("Data trust", trust_level)
trust_col_2.info(trust_reason)
trust_col_3.metric(
    "Action items",
    sum(1 for row in trust_rows if row["status"] == "ACTION"),
)

st.subheader("Research Guardrails")
active_signal_names = _active_signal_names(technical_signals)
stance, stance_detail = _research_stance(composite, trust_level, active_signal_names)
pros, cons = _pros_cons(composite, trust_rows, active_signal_names)

stance_cols = st.columns([1.3, 2.4, 1.3])
stance_cols[0].metric("Research stance", stance)
stance_cols[1].info(stance_detail)
stance_cols[2].metric(
    "Active signals",
    len(active_signal_names),
    help="Number of educational technical patterns currently active. Not a trading instruction.",
)
st.caption(
    "Guardrail wording is deliberately non-advisory. Treat `accumulation`, `watch`, "
    "or `reduce-risk` as research queues, not direct buy/sell instructions."
)

pros_col, cons_col = st.columns(2)
with pros_col:
    st.markdown("##### Pros / supportive evidence")
    for item in pros:
        st.markdown(f"- {item}")
with cons_col:
    st.markdown("##### Cons / risk checks")
    for item in cons:
        st.markdown(f"- {item}")

with st.expander("Data Trust: source freshness, missing inputs, and score reliability"):
    st.dataframe(
        data_trust_rows_to_frame(trust_rows),
        width="stretch",
        hide_index=True,
        column_config={
            "status": st.column_config.TextColumn(
                "Status",
                help="OK means usable, PARTIAL means usable with caveats, ACTION means fix or verify.",
            ),
            "what_to_check": st.column_config.TextColumn(
                "What to check",
                width="large",
            ),
        },
    )
    st.caption(
        "Use this panel before trusting any score. It shows whether the current output is backed "
        "by loaded source data, partial fallback logic, or manual data that still needs verification."
    )

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
        price_provider=price_provider_label,
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

st.markdown('<span id="zerodha-api-setup"></span>', unsafe_allow_html=True)
st.subheader("Zerodha API Setup")
st.warning(
    "No portfolio, holdings, funds, margins, order, trade, order placement, "
    "order modification, or order cancellation APIs are enabled in this app."
)
st.caption(
    "Kite is used only for market data and instrument metadata in this phase. "
    "yfinance remains available as fallback."
)
st.info(
    f"Market data provider selected: `{settings.market_data_provider}`. "
    "Behavior: `kite` prefers Kite and falls back to yfinance; `yfinance` skips Kite; "
    "`auto` uses Kite only when credentials and access token exist."
)

kite_provider = KiteProvider(
    api_key=settings.kite_api_key,
    api_secret=settings.kite_api_secret,
    access_token=settings.kite_access_token,
)
kite_status_cols = st.columns(3)
kite_status_cols[0].metric(
    "KITE_API_KEY configured",
    "Yes" if bool(settings.kite_api_key.strip()) else "No",
)
kite_status_cols[1].metric(
    "KITE_API_SECRET configured",
    "Yes" if bool(settings.kite_api_secret.strip()) else "No",
)
kite_status_cols[2].metric(
    "KITE_ACCESS_TOKEN configured",
    "Yes" if bool(settings.kite_access_token.strip()) else "No",
)
kite_flags_cols = st.columns(3)
kite_flags_cols[0].metric(
    "Kite market data",
    "Enabled" if settings.enable_kite_market_data else "Disabled",
)
kite_flags_cols[1].metric(
    "Kite trading",
    "Disabled" if not settings.enable_kite_trading else "Blocked",
)
kite_flags_cols[2].metric(
    "Kite portfolio",
    "Disabled" if not settings.enable_kite_portfolio else "Blocked",
)

if st.button("Generate Zerodha Login URL"):
    try:
        login_url = kite_provider.get_login_url()
        st.success("Login URL generated. Open it, complete Zerodha login, then copy request_token.")
        st.link_button("Open Zerodha login", login_url)
        st.code(login_url, language="text")
    except KiteProviderError as exc:
        st.error(str(exc))

request_token = st.text_input(
    "Paste temporary request_token",
    type="password",
    help=(
        "After Zerodha redirects to localhost, copy only the value after "
        "`request_token=` from the browser address bar."
    ),
)
if st.button("Generate Access Token"):
    try:
        result = kite_provider.generate_session(request_token)
        st.session_state["kite_generated_access_token"] = result["access_token"]
        st.success(
            "Access token generated and kept only in this local Streamlit session. "
            "The full token is not displayed."
        )
        st.info(
            "To persist it for local development, save it to `.env` as "
            "`KITE_ACCESS_TOKEN`. Use the button below to save locally without "
            "showing the token."
        )
    except KiteProviderError as exc:
        st.error(str(exc))
    except Exception:
        log.warning("Kite access token generation failed.")
        st.error(
            "Could not generate access token. The request_token may be expired, "
            "already used, or the API secret may be incorrect."
        )

generated_token = st.session_state.get("kite_generated_access_token")
if generated_token and st.button("Save generated token to local .env"):
    try:
        _save_kite_access_token_to_env(str(generated_token))
        st.success(
            "Saved KITE_ACCESS_TOKEN to local `.env` without displaying it. "
            "Restart Streamlit so settings reload the token."
        )
    except Exception as exc:
        st.error(f"Could not save token locally: {exc}")

if st.button("Test Kite Market Data Connection"):
    result = kite_provider.connection_test()
    if result["ok"]:
        st.success(result["message"])
    else:
        st.warning(result["message"])

kite_test_cols = st.columns(2)
with kite_test_cols[0]:
    if st.button("Test RELIANCE LTP from Kite"):
        try:
            ltp_frame = kite_provider.get_ltp(["RELIANCE"])
            if ltp_frame.empty:
                st.warning("Kite returned no LTP rows for RELIANCE.")
            else:
                safe_cols = [
                    col for col in ["symbol", "exchange", "ltp", "source"] if col in ltp_frame
                ]
                st.dataframe(ltp_frame[safe_cols], width="stretch", hide_index=True)
        except KiteProviderError as exc:
            st.warning(str(exc))
        except Exception:
            log.warning("Kite RELIANCE LTP test failed.")
            st.warning("Kite LTP test failed. Regenerate token or check Kite subscription.")
with kite_test_cols[1]:
    if st.button("Test RELIANCE historical candles from Kite"):
        try:
            candle_frame = kite_provider.get_historical_candles(
                "RELIANCE",
                from_date=start,
                to_date=end,
                interval="day",
            )
            if candle_frame.empty:
                st.warning("Kite returned no candle rows for RELIANCE.")
            else:
                st.success(
                    f"Kite returned {len(candle_frame):,} candle rows from "
                    f"{candle_frame.index.min().date()} to {candle_frame.index.max().date()}."
                )
        except KiteProviderError as exc:
            st.warning(str(exc))
        except Exception:
            log.warning("Kite RELIANCE candle test failed.")
            st.warning("Kite candle test failed. Regenerate token or check instrument access.")

st.markdown("##### Documentation")
st.code(r"docs\ZERODHA_KITE_SETUP.md", language="text")
st.caption(
    "Security note: the app never displays API secret, access token, profile details, "
    "holdings, positions, funds, margins, orders, trades, or request token."
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

st.subheader("Interactive Chart")
chart_col1, chart_col2 = st.columns([2, 1])
with chart_col1:
    chart_overlays = st.multiselect(
        "Additional overlays",
        options=["Bollinger Bands", "52W high / low"],
        default=[],
        help=(
            "The chart is interactive: zoom, drag, use the range buttons, and click legend "
            "items to hide or isolate series."
        ),
    )
with chart_col2:
    show_volume_overlay = st.checkbox(
        "Volume overlay",
        value=False,
        help="Adds volume bars on a secondary axis. Use this to confirm participation behind price moves.",
    )

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
    xaxis_rangeslider_visible=True,
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
if "Bollinger Bands" in chart_overlays:
    fig.add_trace(
        go.Scatter(
            x=technical_df.index,
            y=technical_df["bb_upper"],
            mode="lines",
            name="BB upper",
            line=dict(width=1, dash="dot"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=technical_df.index,
            y=technical_df["bb_lower"],
            mode="lines",
            name="BB lower",
            line=dict(width=1, dash="dot"),
            fill="tonexty",
            fillcolor="rgba(100,116,139,0.08)",
        )
    )
if "52W high / low" in chart_overlays:
    fig.add_trace(
        go.Scatter(
            x=technical_df.index,
            y=technical_df["high_52w"],
            mode="lines",
            name="52W high",
            line=dict(width=1, dash="dash"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=technical_df.index,
            y=technical_df["low_52w"],
            mode="lines",
            name="52W low",
            line=dict(width=1, dash="dash"),
        )
    )
if show_volume_overlay:
    fig.add_trace(
        go.Bar(
            x=technical_df.index,
            y=technical_df["volume"],
            name="Volume",
            yaxis="y2",
            marker_color="rgba(100,116,139,0.25)",
            hovertemplate="Volume: %{y:,.0f}<extra></extra>",
        )
    )
    fig.update_layout(
        yaxis2=dict(
            title="Volume",
            overlaying="y",
            side="right",
            showgrid=False,
        )
    )
fig.update_layout(
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
)
fig.update_xaxes(
    rangeselector=dict(
        buttons=[
            dict(count=1, label="1M", step="month", stepmode="backward"),
            dict(count=3, label="3M", step="month", stepmode="backward"),
            dict(count=6, label="6M", step="month", stepmode="backward"),
            dict(count=1, label="1Y", step="year", stepmode="backward"),
            dict(step="all", label="All"),
        ]
    )
)
st.plotly_chart(fig, width="stretch")

# --------------------------------------------------------------------------- #
# Technicals panel
# --------------------------------------------------------------------------- #

st.subheader("Technicals")
st.caption("Educational pattern observations only; not investment advice.")

tech_tab, structure_tab, signal_tab, backtest_tab = st.tabs(
    ["Indicators", "Price structure", "Signals", "Signal backtest"]
)

with tech_tab:
    t1, t2, t3, t4 = st.columns(4)
    t1.metric("RSI 14", _format_number(latest_technical.get("rsi_14")), help=_help("RSI 14"))
    t2.metric("MACD", _format_number(latest_technical.get("macd")), help=_help("MACD"))
    t3.metric("ATR 14", _format_currency(latest_technical.get("atr_14")), help=_help("ATR 14"))
    t4.metric(
        "Relative volume",
        _format_number(latest_technical.get("relative_volume")),
        help=_help("Relative volume"),
    )

    t5, t6, t7, t8 = st.columns(4)
    t5.metric("20 EMA", _format_currency(latest_technical.get("ema_20")), help=_help("20 EMA"))
    t6.metric("50 EMA", _format_currency(latest_technical.get("ema_50")), help=_help("50 EMA"))
    t7.metric("100 EMA", _format_currency(latest_technical.get("ema_100")), help=_help("100 EMA"))
    t8.metric("200 EMA", _format_currency(latest_technical.get("ema_200")), help=_help("200 EMA"))

    t9, t10, t11, t12 = st.columns(4)
    t9.metric("ATR %", _format_pct(latest_technical.get("atr_pct") / 100), help=_help("ATR %"))
    t10.metric(
        "Hist. vol 20D",
        _format_pct(latest_technical.get("historical_volatility_20") / 100),
        help=_help("Historical volatility"),
    )
    t11.metric(
        "52W high gap",
        _format_pct(latest_technical.get("distance_from_52w_high_pct") / 100),
        help=_help("52W high gap"),
    )
    t12.metric(
        "MA stack",
        str(latest_technical.get("ma_stack_status", "mixed")).title(),
        help=_help("MA stack"),
    )

    with st.expander("Indicator definitions"):
        glossary_rows = [{"Term": term, "Definition": text} for term, text in GLOSSARY.items()]
        st.dataframe(glossary_rows, width="stretch", hide_index=True)

with structure_tab:
    st.caption(
        "Swing pivots and clustered support/resistance zones from the last "
        "year of price history. Educational only — confirm with your own chart reading."
    )
    last_swings = latest_swing_levels(df, window=5)
    sr_zones = find_support_resistance_zones(df, window=5, lookback=252, max_zones=4)

    sw1, sw2, sw3 = st.columns(3)
    sw1.metric(
        "Last swing high",
        _format_currency(last_swings["last_swing_high"]),
    )
    sw2.metric(
        "Last swing low",
        _format_currency(last_swings["last_swing_low"]),
    )
    sw3.metric(
        "Current close",
        _format_currency(float(df["close"].iloc[-1]) if not df.empty else None),
    )

    sr_col1, sr_col2 = st.columns(2)
    with sr_col1:
        st.markdown("##### Resistance zones (above)")
        if not sr_zones["resistance"]:
            st.info("No resistance zones detected in the lookback window.")
        else:
            st.dataframe(
                [{"Level (₹)": round(p, 2)} for p in sr_zones["resistance"]],
                hide_index=True,
                width="stretch",
            )
    with sr_col2:
        st.markdown("##### Support zones (below)")
        if not sr_zones["support"]:
            st.info("No support zones detected in the lookback window.")
        else:
            st.dataframe(
                [{"Level (₹)": round(p, 2)} for p in sr_zones["support"]],
                hide_index=True,
                width="stretch",
            )

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
    f"Source: {price_provider_label}. Not for redistribution. "
    "Verify data against official NSE/BSE sources before any decision."
)

log.info("Rendered page for {} with {} rows.", symbol, len(df))
