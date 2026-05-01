"""Shared page layout for the Streamlit app."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st


def bootstrap_path() -> None:
    src = Path(__file__).resolve().parents[3]
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


def apply_page_config() -> None:
    st.set_page_config(
        page_title="Indian Stock Research Platform",
        page_icon="chart_with_upwards_trend",
        layout="wide",
        initial_sidebar_state="expanded",
    )


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        .block-container { max-width: 1180px; padding-top: 1.4rem; }
        h1, h2, h3 { letter-spacing: 0; }
        div[data-testid="stMetric"] {
            border: 1px solid #E2E8F0;
            border-radius: 8px;
            padding: 0.8rem 0.9rem;
            background: #FFFFFF;
        }
        .app-header {
            display: flex;
            align-items: flex-start;
            justify-content: space-between;
            gap: 1rem;
            border-bottom: 1px solid #E2E8F0;
            padding-bottom: 1rem;
            margin-bottom: 0.8rem;
        }
        .app-title { font-size: 1.6rem; font-weight: 750; color: #0F172A; }
        .app-subtitle { color: #475569; font-size: 0.88rem; }
        .phase-pill {
            background: #EFF6FF;
            border: 1px solid #BFDBFE;
            color: #1E40AF;
            border-radius: 999px;
            padding: 0.3rem 0.75rem;
            font-size: 0.75rem;
            font-weight: 600;
            white-space: nowrap;
        }
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
        section[data-testid="stSidebar"] {
            background: #F8FAFC;
            border-right: 1px solid #E2E8F0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header(*, subtitle: str | None = None, badge: str = "Phase 8.9") -> None:
    subtitle_text = subtitle or (
        "Fundamentals · Technicals · Flows · Composite scoring · Backtests · Data health"
    )
    st.markdown(
        f"""
        <div class="app-header">
          <div>
            <div class="app-title">Indian Stock Research Platform</div>
            <div class="app-subtitle">{subtitle_text}</div>
          </div>
          <span class="phase-pill">{badge}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_disclaimer() -> None:
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


def render_page_shell(title: str, caption: str | None = None) -> None:
    render_header()
    render_disclaimer()
    st.title(title)
    if caption:
        st.caption(caption)
