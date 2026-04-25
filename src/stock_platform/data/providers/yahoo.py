"""
yfinance implementation of PriceDataProvider.

Phase 0 MVP source. Good enough to get charts rendering locally. Verify
coverage and adjusted prices carefully before relying on for research.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import yfinance as yf

from stock_platform.data.providers.base import PriceDataProvider
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


class YahooFinanceProvider(PriceDataProvider):
    """Wraps `yfinance` behind the PriceDataProvider interface."""

    name = "yfinance"

    def get_ohlcv(
        self,
        symbol: str,
        start: date,
        end: date,
        interval: str = "1d",
    ) -> pd.DataFrame:
        log.info("yfinance: downloading {} {} → {} ({})", symbol, start, end, interval)

        raw = yf.download(
            tickers=symbol,
            start=str(start),
            end=str(end),
            interval=interval,
            auto_adjust=False,
            progress=False,
            threads=False,
        )

        if raw is None or raw.empty:
            log.warning("yfinance returned empty dataframe for {}", symbol)
            return pd.DataFrame(columns=["open", "high", "low", "close", "adj_close", "volume"])

        # yfinance sometimes returns a multi-index column frame when multiple tickers.
        # Flatten defensively.
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = [c[0] for c in raw.columns]

        df = raw.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume",
            }
        )

        # Keep only the expected columns in a stable order
        expected = ["open", "high", "low", "close", "adj_close", "volume"]
        missing = [c for c in expected if c not in df.columns]
        if missing:
            log.error("yfinance response missing columns {} for {}", missing, symbol)
            raise RuntimeError(f"yfinance response missing columns: {missing}")

        df = df[expected].copy()
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df.index.name = "date"

        log.info("yfinance: got {} rows for {}", len(df), symbol)
        return df
