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
        :root {
            --accent: #2563EB;
            --border: #E2E8F0;
            --muted: #64748B;
            --ink: #0F172A;
        }
        .block-container { max-width: 1180px; padding-top: 1.2rem; }
        h1 { font-size: 1.55rem; font-weight: 700; letter-spacing: 0; margin-bottom: 0.1rem; }
        h2 { font-size: 1.1rem; font-weight: 650; }
        h3 { font-size: 0.98rem; font-weight: 650; }
        div[data-testid="stMetric"] {
            border-bottom: 1px solid var(--border);
            border-radius: 0;
            padding: 0.4rem 0.2rem 0.6rem 0.2rem;
            background: transparent;
        }
        div[data-testid="stMetric"] label { color: var(--muted); font-size: 0.78rem; }
        .verdict-card {
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 1rem 1.2rem;
            background: #FFFFFF;
            margin-bottom: 0.8rem;
        }
        .verdict-stance { font-size: 1.25rem; font-weight: 700; color: var(--ink); }
        .verdict-detail { color: var(--muted); font-size: 0.85rem; margin-top: 0.15rem; }
        .verdict-row {
            display: flex; gap: 1.5rem; align-items: center;
            margin-top: 0.7rem; flex-wrap: wrap;
        }
        .score-bar-wrap {
            flex: 1; min-width: 240px;
            background: #F1F5F9; border-radius: 999px; height: 10px; overflow: hidden;
        }
        .score-bar-fill { height: 100%; border-radius: 999px; }
        .chip {
            display: inline-block; padding: 0.18rem 0.6rem;
            border-radius: 999px; font-size: 0.75rem; font-weight: 600;
            border: 1px solid var(--border); background: #F8FAFC; color: var(--ink);
        }
        .chip-green { background: #ECFDF5; border-color: #A7F3D0; color: #065F46; }
        .chip-amber { background: #FFFBEB; border-color: #FCD34D; color: #78350F; }
        .chip-red   { background: #FEF2F2; border-color: #FECACA; color: #991B1B; }
        .sidebar-footer {
            font-size: 0.72rem; color: var(--muted); line-height: 1.4;
            border-top: 1px solid var(--border); padding-top: 0.6rem; margin-top: 0.8rem;
        }
        section[data-testid="stSidebar"] {
            background: #F8FAFC;
            border-right: 1px solid var(--border);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_footer() -> None:
    """Render the disclaimer + build pill once, in the sidebar."""
    st.sidebar.markdown(
        """
        <div class="sidebar-footer">
          <strong>Research aid only.</strong> Not investment advice, not a SEBI-registered
          RA/RIA service. Verify source data before any decision.
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_page_shell(title: str, caption: str | None = None) -> None:
    """Render a lightweight page header. Disclaimer is shown once in the sidebar."""
    st.title(title)
    if caption:
        st.caption(caption)
    render_sidebar_footer()
