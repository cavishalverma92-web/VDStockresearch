"""Small reusable UI helpers for the Streamlit pages."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from stock_platform.analytics.scanner import list_available_universes, load_universe
from stock_platform.auth import save_kite_access_token
from stock_platform.config import ROOT_DIR, get_thresholds_config, get_universe_config

GLOSSARY = {
    "RSI 14": "Relative Strength Index over 14 periods. Above 70 is often stretched; below 30 is often oversold.",
    "MACD": "Moving Average Convergence Divergence. It compares short- and medium-term EMAs to show momentum shifts.",
    "ATR 14": "Average True Range over 14 periods. It estimates normal price movement.",
    "Relative volume": "Current volume divided by recent average volume. Above 1 means activity is higher than usual.",
    "20 EMA": "20-day Exponential Moving Average. A short-term trend line.",
    "50 EMA": "50-day Exponential Moving Average. A medium-term trend line.",
    "200 EMA": "200-day Exponential Moving Average. A long-term trend reference.",
    "MA stack": "Moving-average alignment. Bullish means shorter averages sit above longer averages.",
    "Bollinger Bands": "A 20-period moving average plus/minus two standard deviations.",
}


def help_text(term: str) -> str:
    return GLOSSARY.get(term, "")


def format_pct(value: float | None) -> str:
    return "N/A" if value is None or pd.isna(value) else f"{value * 100:.1f}%"


def format_pct_points(value: float | None) -> str:
    return "N/A" if value is None or pd.isna(value) else f"{value:.2f}%"


def format_number(value: float | None) -> str:
    return "N/A" if value is None or pd.isna(value) else f"{value:.2f}"


def format_currency(value: float | None) -> str:
    return "N/A" if value is None or pd.isna(value) else f"INR {value:.2f}"


def format_score(value: float | None, max_score: int) -> str:
    return "N/A" if value is None else f"{value:.0f}/{max_score}"


def normalize_user_symbol(raw_symbol: str) -> tuple[str, str | None]:
    cleaned = raw_symbol.strip().upper()
    if not cleaned:
        return "", None
    if "." not in cleaned:
        return f"{cleaned}.NS", f"Using `{cleaned}.NS` because no exchange suffix was entered."
    return cleaned, None


def resolve_project_path(path_value: str) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else ROOT_DIR / path


def unique_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for raw_symbol in symbols:
        normalized = str(raw_symbol).strip().upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique.append(normalized)
    return unique


def universe_label(name: str) -> str:
    return name.replace("_", " ").title()


def research_universe_options(extra: list[str] | None = None) -> list[str]:
    config = get_universe_config()
    symbols: set[str] = set(config.get("starter_watchlist", []))
    for universe in list_available_universes():
        try:
            symbols.update(load_universe(universe))
        except (FileNotFoundError, KeyError):
            continue
    if extra:
        symbols.update(item for item in extra if item)
    return sorted(symbols)


def active_signal_names(signals: list[Any]) -> list[str]:
    return [signal.name for signal in signals if getattr(signal, "active", False)]


def risk_per_share(signal: Any) -> float | None:
    if signal.trigger_price is None or signal.stop_loss is None:
        return None
    risk = signal.trigger_price - signal.stop_loss
    return round(risk, 2) if risk > 0 else None


def position_size(signal: Any, portfolio_value: float) -> int | None:
    risk = risk_per_share(signal)
    if risk is None or portfolio_value <= 0:
        return None
    max_risk_pct = float(
        get_thresholds_config().get("risk", {}).get("max_portfolio_risk_per_trade_pct", 1.0)
    )
    return int((portfolio_value * (max_risk_pct / 100)) // risk)


def research_stance(composite: Any, trust_level: str, active_signals: list[str]) -> tuple[str, str]:
    if trust_level == "Low":
        return "Verify first", "Data gaps are too large for a confident research conclusion."
    if composite.score >= 75 and len(active_signals) >= 2:
        return (
            "Accumulation watchlist candidate",
            "Strong score and multiple active signals; verify risk first.",
        )
    if composite.score >= 60:
        return (
            "Watch / hold research candidate",
            "Score is constructive, but confirmation still matters.",
        )
    if composite.score <= 40:
        return (
            "Reduce / avoid-risk review",
            "Weak score suggests a risk-review queue, not an opportunity list.",
        )
    return (
        "Neutral watch",
        "Mixed evidence. Keep on watchlist only with a separate research reason.",
    )


def pros_cons(
    composite: Any,
    trust_rows: list[dict[str, object]],
    active_signals: list[str],
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


def _score_color(score: float) -> str:
    if score >= 75:
        return "#10B981"  # green
    if score >= 60:
        return "#3B82F6"  # blue
    if score >= 40:
        return "#F59E0B"  # amber
    return "#EF4444"  # red


def _trust_chip_class(trust_level: str) -> str:
    level = trust_level.lower()
    if level == "high":
        return "chip-green"
    if level == "medium":
        return "chip-amber"
    return "chip-red"


def render_verdict_card(
    *,
    stance: str,
    detail: str,
    score: float,
    band: str,
    trust_level: str,
    active_signal_count: int,
) -> None:
    """One hero card with the research verdict — replaces the old 6-metric strip."""
    color = _score_color(score)
    pct = max(0.0, min(100.0, score))
    trust_class = _trust_chip_class(trust_level)
    st.markdown(
        f"""
        <div class="verdict-card">
          <div class="verdict-stance">{stance}</div>
          <div class="verdict-detail">{detail}</div>
          <div class="verdict-row">
            <div style="min-width:120px;">
              <div style="font-size:0.72rem;color:var(--muted);">RESEARCH SCORE</div>
              <div style="font-size:1.45rem;font-weight:700;color:{color};">
                {score:.1f}<span style="color:var(--muted);font-size:0.95rem;font-weight:500;"> / 100</span>
              </div>
            </div>
            <div class="score-bar-wrap">
              <div class="score-bar-fill" style="width:{pct:.1f}%;background:{color};"></div>
            </div>
            <div>
              <span class="chip">{band}</span>
              <span class="chip {trust_class}">Trust: {trust_level}</span>
              <span class="chip">{active_signal_count} active signal{'s' if active_signal_count != 1 else ''}</span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def research_pick_button(frame: pd.DataFrame, *, key: str) -> None:
    if frame.empty or "symbol" not in frame.columns:
        st.dataframe(frame, width="stretch", hide_index=True)
        return
    selection = st.dataframe(
        frame,
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key=key,
    )
    rows = selection.get("selection", {}).get("rows", [])
    if not rows:
        st.caption("Click a row, then press Research to open that symbol.")
        return
    symbol = str(frame.iloc[rows[0]]["symbol"])
    if st.button(f"Research {symbol}", type="primary", key=f"{key}_research"):
        st.session_state["research_symbol"] = symbol
        st.switch_page("pages/10_stock_research.py")


def save_kite_access_token_locally(access_token: str) -> Path:
    return save_kite_access_token(access_token)


def date_range_caption(start: date, end: date) -> str:
    return f"{start.isoformat()} to {end.isoformat()}"
