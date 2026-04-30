from __future__ import annotations

import pandas as pd
import pytest

from stock_platform.data.providers import kite_provider
from stock_platform.data.providers.kite_provider import KiteProvider, KiteSecurityError


class _FakeKiteConnect:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.access_token = None

    def login_url(self) -> str:
        return f"https://kite.zerodha.com/connect/login?v=3&api_key={self.api_key}"

    def set_access_token(self, access_token: str) -> None:
        self.access_token = access_token

    def profile(self) -> dict[str, str]:
        return {"user_id": "DO_NOT_RETURN"}

    def ltp(self, symbols: list[str]) -> dict[str, dict[str, float]]:
        return {
            symbol: {
                "instrument_token": 123,
                "last_price": 1500.5,
            }
            for symbol in symbols
        }

    def instruments(self, exchange: str) -> list[dict[str, object]]:
        return [
            {
                "instrument_token": 111,
                "exchange_token": "500325",
                "tradingsymbol": "RELIANCE",
                "name": "RELIANCE INDUSTRIES",
                "exchange": exchange,
                "segment": f"{exchange}-EQ",
                "instrument_type": "EQ",
                "tick_size": 0.05,
                "lot_size": 1,
                "expiry": None,
                "strike": 0,
            }
        ]

    def historical_data(
        self, instrument_token: int, from_date, to_date, interval: str
    ) -> list[dict[str, object]]:
        return [
            {
                "date": pd.Timestamp("2026-01-01"),
                "open": 100.0,
                "high": 105.0,
                "low": 99.0,
                "close": 104.0,
                "volume": 1000,
            }
        ]


def test_missing_credentials_returns_not_configured() -> None:
    provider = KiteProvider(api_key="", api_secret="")

    assert provider.is_configured() is False


def test_configured_when_api_key_and_secret_exist() -> None:
    provider = KiteProvider(api_key="test_key", api_secret="test_secret")

    assert provider.is_configured() is True


def test_login_url_generation_works_when_api_key_exists(monkeypatch) -> None:
    monkeypatch.setattr(kite_provider, "KiteConnect", _FakeKiteConnect)
    provider = KiteProvider(api_key="test_key", api_secret="")

    assert provider.get_login_url() == "https://kite.zerodha.com/connect/login?v=3&api_key=test_key"


def test_connection_test_handles_missing_access_token_gracefully(monkeypatch) -> None:
    monkeypatch.setattr(kite_provider, "KiteConnect", _FakeKiteConnect)
    provider = KiteProvider(api_key="test_key", api_secret="test_secret")

    result = provider.connection_test()

    assert result == {
        "ok": False,
        "message": "KITE_ACCESS_TOKEN is missing. Generate a token first.",
        "provider": "kite",
    }


def test_connection_test_returns_only_safe_status(monkeypatch) -> None:
    monkeypatch.setattr(kite_provider, "KiteConnect", _FakeKiteConnect)
    provider = KiteProvider(
        api_key="test_key",
        api_secret="test_secret",
        access_token="test_access_token",
    )

    result = provider.connection_test()

    assert result == {
        "ok": True,
        "message": "Zerodha market-data connection is working",
        "provider": "kite",
    }


def test_historical_candles_normalize_expected_columns(monkeypatch) -> None:
    monkeypatch.setattr(kite_provider, "KiteConnect", _FakeKiteConnect)
    provider = KiteProvider(
        api_key="test_key",
        api_secret="test_secret",
        access_token="test_access_token",
    )

    frame = provider.get_historical_candles(
        "RELIANCE.NS",
        from_date="2026-01-01",
        to_date="2026-01-02",
    )

    assert list(frame.columns) == [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adj_close",
        "source",
        "symbol",
        "exchange",
    ]
    assert frame.index.name == "date"
    assert frame["source"].iloc[0] == "kite"
    assert frame["symbol"].iloc[0] == "RELIANCE.NS"


def test_blocked_portfolio_and_trading_methods_raise_security_error() -> None:
    provider = KiteProvider(api_key="test_key", api_secret="test_secret")

    with pytest.raises(KiteSecurityError):
        provider.positions()
