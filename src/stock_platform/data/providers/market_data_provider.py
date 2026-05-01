"""Market-data provider router.

Kite is preferred for market data when configured. yfinance remains the fallback
and can also be selected explicitly.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from stock_platform.auth import load_kite_access_token
from stock_platform.config import get_settings
from stock_platform.data.providers.kite_provider import KiteProvider
from stock_platform.data.providers.yahoo import YahooFinanceProvider
from stock_platform.data.validators import validate_ohlcv
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)


class MarketDataProvider:
    """Route market-data requests to Kite first, then yfinance fallback."""

    def __init__(
        self,
        provider_name: str | None = None,
        kite_provider: KiteProvider | None = None,
        yfinance_provider: YahooFinanceProvider | None = None,
        enable_kite_market_data: bool | None = None,
    ) -> None:
        settings = get_settings()
        self.provider_name = (provider_name or settings.market_data_provider or "kite").lower()
        self.enable_kite_market_data = (
            settings.enable_kite_market_data
            if enable_kite_market_data is None
            else bool(enable_kite_market_data)
        )
        self.kite = kite_provider or KiteProvider(
            api_key=settings.kite_api_key,
            api_secret=settings.kite_api_secret,
            access_token=load_kite_access_token() or "",
        )
        self.yfinance = yfinance_provider or YahooFinanceProvider()
        self.last_source = ""
        self.last_warning = ""
        log.info(
            "Market data provider selected: {}, kite_enabled={}",
            self.provider_name,
            self.enable_kite_market_data,
        )

    def get_ohlcv(
        self,
        symbol: str,
        start: date,
        end: date,
        interval: str = "1d",
    ) -> pd.DataFrame:
        """Return validated OHLCV with Kite preferred and yfinance fallback."""
        self.last_warning = ""
        if self.provider_name == "yfinance":
            return self._from_yfinance(symbol, start, end, interval, label="yfinance")

        if self.provider_name not in {"kite", "auto"}:
            self.last_warning = (
                f"Unknown MARKET_DATA_PROVIDER={self.provider_name}; using yfinance fallback."
            )
            log.warning(self.last_warning)
            return self._from_yfinance(symbol, start, end, interval)

        kite_blocker = self._kite_unavailable_reason()
        if not kite_blocker:
            try:
                frame = self.kite.get_historical_candles(
                    symbol=symbol,
                    from_date=start,
                    to_date=end,
                    interval=interval,
                )
                validate_ohlcv(frame, symbol=symbol, raise_on_error=True)
                self.last_source = "kite"
                frame.attrs["source"] = "kite"
                frame.attrs["provider_label"] = "Zerodha Kite"
                frame.attrs["fallback_reason"] = ""
                return frame
            except Exception as exc:
                self.last_warning = (
                    "Kite data unavailable; using yfinance fallback. "
                    f"Reason: {self._safe_exception_message(exc)}"
                )
                log.warning(
                    "Fallback to yfinance triggered for {}: {}",
                    symbol,
                    self._safe_exception_message(exc),
                )
        else:
            self.last_warning = f"Kite data unavailable; using yfinance fallback. {kite_blocker}"
            log.warning(
                "Fallback to yfinance triggered for {}: {}",
                symbol,
                kite_blocker,
            )

        return self._from_yfinance(symbol, start, end, interval)

    def get_ltp(self, symbols: list[str]) -> pd.DataFrame:
        """Get LTP from Kite when possible, otherwise approximate via latest yfinance close."""
        if self._can_try_kite():
            try:
                return self.kite.get_ltp(symbols)
            except Exception as exc:
                log.warning("Kite LTP failed; falling back to yfinance: {}", type(exc).__name__)
        rows = []
        today = date.today()
        start = today.replace(year=today.year - 1)
        for symbol in symbols:
            frame = self.yfinance.get_ohlcv(symbol, start=start, end=today)
            rows.append(
                {
                    "symbol": symbol,
                    "exchange": "NSE" if symbol.upper().endswith(".NS") else "",
                    "ltp": None if frame.empty else float(frame.iloc[-1]["close"]),
                    "instrument_token": None,
                    "source": "yfinance",
                }
            )
        return pd.DataFrame(rows)

    def get_ohlc(self, symbols: list[str]) -> pd.DataFrame:
        if self._can_try_kite():
            try:
                return self.kite.get_ohlc(symbols)
            except Exception as exc:
                log.warning("Kite OHLC failed; returning empty fallback: {}", type(exc).__name__)
        return pd.DataFrame()

    def get_quote(self, symbols: list[str]) -> pd.DataFrame:
        if self._can_try_kite():
            try:
                return self.kite.get_quote(symbols)
            except Exception as exc:
                log.warning("Kite quote failed; returning empty fallback: {}", type(exc).__name__)
        return pd.DataFrame()

    def _from_yfinance(
        self,
        symbol: str,
        start: date,
        end: date,
        interval: str,
        label: str = "yfinance fallback",
    ) -> pd.DataFrame:
        frame = self.yfinance.get_ohlcv(symbol=symbol, start=start, end=end, interval=interval)
        if not frame.empty:
            validate_ohlcv(frame, symbol=symbol, raise_on_error=True)
        self.last_source = "yfinance"
        frame.attrs["source"] = "yfinance"
        frame.attrs["provider_label"] = label
        frame.attrs["fallback_reason"] = self.last_warning
        return frame

    def _can_try_kite(self) -> bool:
        return (
            self.provider_name in {"kite", "auto"}
            and self.enable_kite_market_data
            and self.kite.is_configured()
            and self.kite.has_access_token()
        )

    def _kite_unavailable_reason(self) -> str:
        if not self.enable_kite_market_data:
            return "ENABLE_KITE_MARKET_DATA is false."
        if not self.kite.is_configured():
            return "KITE_API_KEY or KITE_API_SECRET is missing."
        if not self.kite.has_access_token():
            return "KITE_ACCESS_TOKEN is missing or expired. Generate a fresh access token in Zerodha API Setup."
        return ""

    @staticmethod
    def _safe_exception_message(exc: Exception) -> str:
        message = str(exc).strip()
        if message and len(message) <= 180:
            return message
        return type(exc).__name__

    def status(self) -> dict[str, Any]:
        return {
            "selected": self.provider_name,
            "kite_enabled": self.enable_kite_market_data,
            "kite_configured": self.kite.is_configured(),
            "kite_access_token": self.kite.has_access_token(),
            "last_source": self.last_source,
            "last_warning": self.last_warning,
        }
