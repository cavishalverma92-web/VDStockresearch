"""Safe Zerodha Kite Connect market-data provider.

Allowed scope:
- Authentication/login URL.
- Instrument master and instrument-token lookup.
- Historical candles, LTP, OHLC, and quote data.

Blocked by design:
- Profile display, holdings, positions, funds, margins, orders, trades, and
  any order placement/modification/cancellation.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd

from stock_platform.utils.logging import get_logger

try:  # pragma: no cover - tests monkeypatch this when needed
    from kiteconnect import KiteConnect
except Exception:  # pragma: no cover
    KiteConnect = None  # type: ignore[assignment]


log = get_logger(__name__)

BLOCKED_METHODS = {
    "holdings",
    "positions",
    "margins",
    "funds",
    "orders",
    "trades",
    "place_order",
    "modify_order",
    "cancel_order",
    "exit_order",
    "convert_position",
}


class KiteProviderError(RuntimeError):
    """Raised when Kite Connect setup or requests cannot be completed safely."""


class KiteSecurityError(NotImplementedError):
    """Raised when blocked portfolio/trading methods are attempted."""


def _clean(value: str | None) -> str:
    return (value or "").strip()


def _mask(value: str | None) -> str:
    cleaned = _clean(value)
    if not cleaned:
        return "<empty>"
    if len(cleaned) <= 10:
        return "<provided>"
    return f"{cleaned[:4]}...{cleaned[-4:]}"


def kite_trading_symbol(symbol: str) -> str:
    """Convert app symbols like RELIANCE.NS into Kite trading symbols like RELIANCE."""
    cleaned = _clean(symbol).upper()
    for suffix in (".NS", ".BO"):
        if cleaned.endswith(suffix):
            return cleaned[: -len(suffix)]
    if ":" in cleaned:
        return cleaned.split(":", 1)[1]
    return cleaned


def kite_symbol_key(symbol: str, exchange: str = "NSE") -> str:
    """Return Kite's quote key format, e.g. NSE:RELIANCE."""
    return f"{exchange.upper()}:{kite_trading_symbol(symbol)}"


class KiteProvider:
    """Safety-scoped wrapper around Zerodha Kite Connect market-data APIs."""

    name = "kite"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        access_token: str | None = None,
    ) -> None:
        self.api_key = _clean(api_key)
        self.api_secret = _clean(api_secret)
        self.access_token = _clean(access_token) or None
        self._instrument_cache: dict[str, pd.DataFrame] = {}
        log.info(
            "KiteProvider initialized: api_key_configured={}, api_secret_configured={}, "
            "access_token_configured={}",
            bool(self.api_key),
            bool(self.api_secret),
            bool(self.access_token),
        )

    def __getattr__(self, name: str):
        if name in BLOCKED_METHODS:
            raise KiteSecurityError(
                f"Kite method `{name}` is intentionally disabled. This app only uses "
                "Kite for market data and instrument metadata."
            )
        raise AttributeError(name)

    def is_configured(self) -> bool:
        """Return True when API key and API secret are present."""
        configured = bool(self.api_key and self.api_secret)
        if not configured:
            log.warning(
                "KiteProvider missing credentials: api_key_configured={}, api_secret_configured={}",
                bool(self.api_key),
                bool(self.api_secret),
            )
        return configured

    def has_access_token(self) -> bool:
        """Return True when an access token is available."""
        ok = bool(self.access_token)
        if not ok:
            log.warning("Kite access token missing.")
        return ok

    def _client(self):
        if KiteConnect is None:
            raise KiteProviderError(
                "kiteconnect package is not installed. Run: pip install -r requirements.txt"
            )
        if not self.api_key:
            raise KiteProviderError("KITE_API_KEY is missing.")
        client = KiteConnect(api_key=self.api_key)
        if self.access_token:
            client.set_access_token(self.access_token)
        return client

    def _authenticated_client(self):
        if not self.is_configured():
            raise KiteProviderError("KITE_API_KEY and KITE_API_SECRET are required.")
        if not self.has_access_token():
            raise KiteProviderError("KITE_ACCESS_TOKEN is missing or expired.")
        return self._client()

    def get_login_url(self) -> str:
        """Return the Zerodha login URL for the configured API key."""
        log.info("Kite login URL requested: api_key_configured={}", bool(self.api_key))
        if not self.api_key:
            raise KiteProviderError("KITE_API_KEY is missing. Add it to .env first.")
        login_url = self._client().login_url()
        log.info("Kite login URL generated.")
        return login_url

    def generate_session(self, request_token: str) -> dict[str, Any]:
        """Exchange a temporary request_token for an access_token."""
        token = _clean(request_token)
        if not self.is_configured():
            raise KiteProviderError("KITE_API_KEY and KITE_API_SECRET are required.")
        if not token:
            raise KiteProviderError("request_token is missing.")

        log.info("Kite request token exchange attempted: request_token={}", _mask(token))
        try:
            session = self._client().generate_session(token, api_secret=self.api_secret)
        except Exception as exc:
            log.warning("Kite request token exchange failed: {}", type(exc).__name__)
            raise KiteProviderError(
                "Could not generate access token. The request_token may be expired, "
                "already used, or the API secret may be incorrect."
            ) from exc

        access_token = _clean(session.get("access_token") if isinstance(session, dict) else None)
        if not access_token:
            log.warning("Kite session generated without an access_token.")
            raise KiteProviderError("Zerodha did not return an access_token.")
        self.set_access_token(access_token)
        log.info("Kite access token generated successfully: access_token={}", _mask(access_token))
        return {
            "ok": True,
            "message": "Zerodha access token generated",
            "access_token": access_token,
        }

    def set_access_token(self, access_token: str) -> None:
        """Set an access token on this provider instance."""
        self.access_token = _clean(access_token) or None
        log.info("Kite access token set on provider: access_token={}", _mask(self.access_token))

    def connection_test(self) -> dict[str, Any]:
        """Confirm authenticated market-data access without returning personal details."""
        if not self.is_configured():
            return {
                "ok": False,
                "message": "KITE_API_KEY and KITE_API_SECRET are not configured.",
                "provider": self.name,
            }
        if not self.access_token:
            log.warning("Kite connection test skipped: missing access token.")
            return {
                "ok": False,
                "message": "KITE_ACCESS_TOKEN is missing. Generate a token first.",
                "provider": self.name,
            }

        try:
            frame = self.get_ltp(["INFY"])
            ok = not frame.empty and "ltp" in frame.columns
            if ok:
                log.info("Kite market-data connection test success.")
                return {
                    "ok": True,
                    "message": "Zerodha market-data connection is working",
                    "provider": self.name,
                }
            return {
                "ok": False,
                "message": "Zerodha market-data test returned no LTP rows.",
                "provider": self.name,
            }
        except Exception as exc:
            log.warning("Kite market-data connection test failed: {}", type(exc).__name__)
            return {
                "ok": False,
                "message": "Zerodha market-data connection failed. Regenerate token or check credentials.",
                "provider": self.name,
            }

    def get_instruments(self, exchange: str = "NSE") -> pd.DataFrame:
        """Fetch instrument master for an exchange."""
        normalized_exchange = exchange.upper()
        if normalized_exchange in self._instrument_cache:
            return self._instrument_cache[normalized_exchange].copy()

        log.info("Kite instrument sync attempted: exchange={}", normalized_exchange)
        rows = self._authenticated_client().instruments(normalized_exchange)
        frame = pd.DataFrame(rows)
        columns = [
            "instrument_token",
            "exchange_token",
            "tradingsymbol",
            "name",
            "exchange",
            "segment",
            "instrument_type",
            "tick_size",
            "lot_size",
            "expiry",
            "strike",
        ]
        for column in columns:
            if column not in frame.columns:
                frame[column] = pd.NA
        frame = frame[columns].copy()
        log.info("Kite instruments fetched: exchange={}, rows={}", normalized_exchange, len(frame))
        self._instrument_cache[normalized_exchange] = frame
        return frame.copy()

    def find_instrument_token(self, symbol: str, exchange: str = "NSE") -> int | None:
        """Find Kite instrument token for a trading symbol."""
        trading_symbol = kite_trading_symbol(symbol)
        instruments = self.get_instruments(exchange)
        if instruments.empty:
            return None
        matches = instruments[
            (instruments["tradingsymbol"].astype(str).str.upper() == trading_symbol)
            & (instruments["exchange"].astype(str).str.upper() == exchange.upper())
        ]
        if matches.empty:
            log.warning(
                "Kite instrument token unavailable: symbol={}, exchange={}",
                trading_symbol,
                exchange,
            )
            return None
        return int(matches.iloc[0]["instrument_token"])

    def get_historical_candles(
        self,
        symbol: str,
        from_date: date | datetime | str,
        to_date: date | datetime | str,
        interval: str = "day",
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """Fetch historical OHLCV candles by symbol."""
        kite_interval = "day" if interval in {"1d", "day"} else interval
        token = self.find_instrument_token(symbol, exchange=exchange)
        if token is None:
            raise KiteProviderError(f"Kite instrument token was not found for {symbol}.")

        log.info(
            "Kite historical candles requested: symbol={}, exchange={}, interval={}",
            kite_trading_symbol(symbol),
            exchange,
            kite_interval,
        )
        try:
            candles = self._authenticated_client().historical_data(
                instrument_token=token,
                from_date=from_date,
                to_date=to_date,
                interval=kite_interval,
            )
        except Exception as exc:
            log.warning("Kite historical candles failed: {}", type(exc).__name__)
            raise KiteProviderError(
                "Could not fetch Zerodha historical candles. Check access token, "
                "instrument token, date range, and Kite Connect subscription."
            ) from exc

        frame = pd.DataFrame(candles)
        expected = ["date", "open", "high", "low", "close", "volume"]
        if frame.empty:
            return pd.DataFrame(columns=[*expected, "adj_close", "source", "symbol", "exchange"])
        missing = [column for column in expected if column not in frame.columns]
        if missing:
            raise KiteProviderError(
                f"Zerodha historical candles response is missing columns: {', '.join(missing)}"
            )
        frame = frame[expected].copy()
        frame["date"] = pd.to_datetime(frame["date"]).dt.tz_localize(None)
        frame = frame.sort_values("date").set_index("date")
        frame.index.name = "date"
        frame["adj_close"] = frame["close"]
        frame["source"] = self.name
        frame["symbol"] = symbol
        frame["exchange"] = exchange.upper()
        frame.attrs["source"] = self.name
        frame.attrs["provider_label"] = "Zerodha Kite"
        log.info("Kite historical candles fetched: symbol={}, rows={}", symbol, len(frame))
        return frame

    def get_ltp(self, symbols: list[str], exchange: str = "NSE") -> pd.DataFrame:
        """Fetch last traded price for symbols."""
        keys = [kite_symbol_key(symbol, exchange) for symbol in symbols]
        log.info("Kite LTP requested: count={}, exchange={}", len(keys), exchange)
        raw = self._authenticated_client().ltp(keys)
        rows = []
        for key, payload in (raw or {}).items():
            exch, trading_symbol = key.split(":", 1)
            rows.append(
                {
                    "symbol": trading_symbol,
                    "exchange": exch,
                    "ltp": payload.get("last_price"),
                    "instrument_token": payload.get("instrument_token"),
                    "source": self.name,
                }
            )
        log.info("Kite LTP fetched: rows={}", len(rows))
        return pd.DataFrame(
            rows, columns=["symbol", "exchange", "ltp", "instrument_token", "source"]
        )

    def get_ohlc(self, symbols: list[str], exchange: str = "NSE") -> pd.DataFrame:
        """Fetch OHLC quote snapshot for symbols."""
        keys = [kite_symbol_key(symbol, exchange) for symbol in symbols]
        log.info("Kite OHLC requested: count={}, exchange={}", len(keys), exchange)
        raw = self._authenticated_client().ohlc(keys)
        rows = []
        for key, payload in (raw or {}).items():
            exch, trading_symbol = key.split(":", 1)
            ohlc = payload.get("ohlc", {}) or {}
            rows.append(
                {
                    "symbol": trading_symbol,
                    "exchange": exch,
                    "open": ohlc.get("open"),
                    "high": ohlc.get("high"),
                    "low": ohlc.get("low"),
                    "close": ohlc.get("close"),
                    "ltp": payload.get("last_price"),
                    "instrument_token": payload.get("instrument_token"),
                    "source": self.name,
                }
            )
        log.info("Kite OHLC fetched: rows={}", len(rows))
        return pd.DataFrame(rows)

    def get_quote(self, symbols: list[str], exchange: str = "NSE") -> pd.DataFrame:
        """Fetch quote snapshot for symbols."""
        keys = [kite_symbol_key(symbol, exchange) for symbol in symbols]
        log.info("Kite quote requested: count={}, exchange={}", len(keys), exchange)
        raw = self._authenticated_client().quote(keys)
        rows = []
        for key, payload in (raw or {}).items():
            exch, trading_symbol = key.split(":", 1)
            ohlc = payload.get("ohlc", {}) or {}
            rows.append(
                {
                    "symbol": trading_symbol,
                    "exchange": exch,
                    "last_price": payload.get("last_price"),
                    "volume": payload.get("volume"),
                    "average_price": payload.get("average_price"),
                    "open": ohlc.get("open"),
                    "high": ohlc.get("high"),
                    "low": ohlc.get("low"),
                    "close": ohlc.get("close"),
                    "source": self.name,
                }
            )
        log.info("Kite quote fetched: rows={}", len(rows))
        return pd.DataFrame(rows)
