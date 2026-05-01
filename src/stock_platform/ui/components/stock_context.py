"""Shared stock input, fetch, validation, and scoring context."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import pandas as pd
import streamlit as st

from stock_platform.analytics.signals import scan_technical_signals
from stock_platform.analytics.signals.audit import save_signal_audit
from stock_platform.analytics.technicals import add_technical_indicators
from stock_platform.config import get_settings, get_universe_config
from stock_platform.data.providers import MarketDataProvider
from stock_platform.data.validators import OHLCVValidationError, validate_ohlcv
from stock_platform.data.validators.ohlcv_validator import ValidationReport
from stock_platform.ui.components.common import (
    normalize_user_symbol,
    research_universe_options,
)
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class StockInputs:
    symbol: str
    start: date
    end: date
    portfolio_value: float


@dataclass(frozen=True)
class StockContext:
    inputs: StockInputs
    df: pd.DataFrame
    report: ValidationReport
    technical_df: pd.DataFrame
    latest_technical: pd.Series
    signals: list[Any]
    price_source: str
    price_provider_label: str
    fallback_reason: str
    saved_signal_count: int


def render_stock_sidebar() -> StockInputs:
    settings = get_settings()
    universe = get_universe_config()
    starter = universe.get("starter_watchlist", ["RELIANCE.NS"])
    jumped = st.session_state.get("research_symbol")
    options = research_universe_options(extra=[jumped] if jumped else None) or list(starter)
    if jumped in options:
        default_index = options.index(jumped)
    elif starter and starter[0] in options:
        default_index = options.index(starter[0])
    else:
        default_index = 0

    with st.sidebar:
        st.header("Research a stock")
        symbol = st.selectbox(
            "Ticker",
            options=options,
            index=default_index,
            help="Use .NS for NSE stocks. Type to filter.",
        )
        custom = st.text_input(
            "Or enter a ticker not in the list",
            value="",
            placeholder="e.g. RVNL or BHEL.NS",
        )
        if custom.strip():
            symbol, note = normalize_user_symbol(custom)
            if note:
                st.caption(note)
            st.session_state.pop("research_symbol", None)

        today = date.today()
        start = st.date_input("Start date", value=today - timedelta(days=5 * 365), max_value=today)
        end = st.date_input(
            "End date", value=today, min_value=start + timedelta(days=1), max_value=today
        )
        st.markdown("---")
        st.caption(f"Market data provider: `{settings.market_data_provider}`")
        st.caption(f"Fundamentals provider: `{settings.provider_fundamentals}`")
        portfolio_value = st.number_input(
            "Portfolio value for position sizing",
            min_value=0.0,
            value=1_000_000.0,
            step=50_000.0,
            help="Educational risk sizing only.",
        )

    return StockInputs(symbol=symbol, start=start, end=end, portfolio_value=portfolio_value)


def load_stock_context(inputs: StockInputs) -> StockContext:
    provider = MarketDataProvider()
    with st.spinner(f"Loading {inputs.symbol}..."):
        try:
            df = provider.get_ohlcv(symbol=inputs.symbol, start=inputs.start, end=inputs.end)
        except Exception as exc:  # noqa: BLE001
            log.exception("Download failed for {}: {}", inputs.symbol, exc)
            st.error(f"Could not fetch data for **{inputs.symbol}**: {exc}")
            st.stop()

    if df.empty:
        st.error(f"No price data was returned for **{inputs.symbol}**.")
        st.info(
            "The symbol may be stale, the date range may have no trading days, "
            "or the provider may not have data for it."
        )
        st.stop()

    try:
        report = validate_ohlcv(df, symbol=inputs.symbol, raise_on_error=False)
    except OHLCVValidationError as exc:
        st.error(f"Data quality failure: {exc}")
        st.stop()

    technical_df = add_technical_indicators(df)
    latest = technical_df.iloc[-1]
    signals = scan_technical_signals(df)
    price_source = str(df.attrs.get("source") or provider.last_source or "unknown")
    label = str(df.attrs.get("provider_label") or price_source)
    warning = str(df.attrs.get("fallback_reason") or provider.last_warning or "")

    try:
        saved = save_signal_audit(inputs.symbol, df, signals, source=price_source)
    except Exception as exc:  # noqa: BLE001
        saved = 0
        log.exception("Could not save signal audit for {}: {}", inputs.symbol, exc)

    return StockContext(
        inputs=inputs,
        df=df,
        report=report,
        technical_df=technical_df,
        latest_technical=latest,
        signals=signals,
        price_source=price_source,
        price_provider_label=label,
        fallback_reason=warning,
        saved_signal_count=saved,
    )
