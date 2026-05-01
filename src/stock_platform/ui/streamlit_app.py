"""Streamlit entrypoint for the multi-page Indian Stock Research Platform.

Run with:
    streamlit run src/stock_platform/ui/streamlit_app.py

This file intentionally stays small. Page-level code lives in
``src/stock_platform/ui/pages`` and shared UI helpers live in
``src/stock_platform/ui/components``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# Keep direct `streamlit run <file>` working without PYTHONPATH tweaks.
SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from stock_platform.ui.components.layout import apply_page_config, apply_theme  # noqa: E402


def main() -> None:
    """Render the Streamlit navigation shell."""
    apply_page_config()
    apply_theme()

    pages_dir = Path(__file__).resolve().parent / "pages"
    pages = {
        "Research Desk": [
            st.Page(
                str(pages_dir / "00_market_today.py"),
                title="Market Today",
                icon=":material/dashboard:",
                default=True,
            ),
            st.Page(
                str(pages_dir / "10_stock_research.py"),
                title="Stock Research",
                icon=":material/query_stats:",
            ),
            st.Page(
                str(pages_dir / "20_top_opportunities.py"),
                title="Top Opportunities",
                icon=":material/travel_explore:",
            ),
            st.Page(
                str(pages_dir / "30_watchlist.py"),
                title="Watchlist",
                icon=":material/bookmark:",
            ),
            st.Page(
                str(pages_dir / "40_backtests.py"),
                title="Backtests",
                icon=":material/history:",
            ),
        ],
        "Operations": [
            st.Page(
                str(pages_dir / "80_data_health.py"),
                title="Data Health",
                icon=":material/health_and_safety:",
            ),
            st.Page(
                str(pages_dir / "90_settings.py"),
                title="Settings",
                icon=":material/settings:",
            ),
        ],
    }

    navigation = st.navigation(pages, position="sidebar")
    navigation.run()


if __name__ == "__main__":
    main()
