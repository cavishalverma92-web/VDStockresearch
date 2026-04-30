from __future__ import annotations

from datetime import date

import pandas as pd

from stock_platform.data.providers.market_data_provider import MarketDataProvider


def _price_frame(source: str = "fake") -> pd.DataFrame:
    idx = pd.date_range("2026-01-01", periods=3, freq="B")
    frame = pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "adj_close": [100.5, 101.5, 102.5],
            "volume": [1000, 1100, 1200],
            "source": [source, source, source],
            "symbol": ["RELIANCE.NS", "RELIANCE.NS", "RELIANCE.NS"],
        },
        index=idx,
    )
    frame.index.name = "date"
    frame.attrs["source"] = source
    return frame


class _FakeYfinanceProvider:
    name = "yfinance"

    def __init__(self) -> None:
        self.called = False

    def get_ohlcv(self, symbol: str, start: date, end: date, interval: str = "1d") -> pd.DataFrame:
        self.called = True
        return _price_frame("yfinance")


class _FakeKiteProvider:
    def __init__(self, *, configured: bool = True, token: bool = True, fail: bool = False) -> None:
        self.configured = configured
        self.token = token
        self.fail = fail
        self.called = False

    def is_configured(self) -> bool:
        return self.configured

    def has_access_token(self) -> bool:
        return self.token

    def get_historical_candles(
        self,
        symbol: str,
        from_date,
        to_date,
        interval: str = "day",
    ) -> pd.DataFrame:
        self.called = True
        if self.fail:
            raise RuntimeError("kite offline")
        return _price_frame("kite")

    def get_ltp(self, symbols: list[str]) -> pd.DataFrame:
        return pd.DataFrame({"symbol": symbols, "ltp": [100.0] * len(symbols), "source": "kite"})


def test_market_data_provider_uses_yfinance_when_selected() -> None:
    kite = _FakeKiteProvider()
    yfinance = _FakeYfinanceProvider()
    provider = MarketDataProvider(
        provider_name="yfinance",
        kite_provider=kite,
        yfinance_provider=yfinance,
    )

    frame = provider.get_ohlcv("RELIANCE.NS", date(2026, 1, 1), date(2026, 1, 10))

    assert yfinance.called is True
    assert kite.called is False
    assert frame.attrs["source"] == "yfinance"


def test_market_data_provider_falls_back_when_kite_not_configured() -> None:
    kite = _FakeKiteProvider(configured=False)
    yfinance = _FakeYfinanceProvider()
    provider = MarketDataProvider(
        provider_name="kite",
        kite_provider=kite,
        yfinance_provider=yfinance,
    )

    frame = provider.get_ohlcv("RELIANCE.NS", date(2026, 1, 1), date(2026, 1, 10))

    assert yfinance.called is True
    assert frame.attrs["source"] == "yfinance"
    assert "fallback" in frame.attrs["provider_label"]


def test_market_data_provider_falls_back_when_kite_call_fails() -> None:
    kite = _FakeKiteProvider(fail=True)
    yfinance = _FakeYfinanceProvider()
    provider = MarketDataProvider(
        provider_name="kite",
        kite_provider=kite,
        yfinance_provider=yfinance,
    )

    frame = provider.get_ohlcv("RELIANCE.NS", date(2026, 1, 1), date(2026, 1, 10))

    assert kite.called is True
    assert yfinance.called is True
    assert frame.attrs["source"] == "yfinance"
    assert "Kite data unavailable" in frame.attrs["fallback_reason"]
    assert "kite offline" in frame.attrs["fallback_reason"]


def test_market_data_provider_explains_missing_kite_access_token() -> None:
    kite = _FakeKiteProvider(configured=True, token=False)
    yfinance = _FakeYfinanceProvider()
    provider = MarketDataProvider(
        provider_name="kite",
        kite_provider=kite,
        yfinance_provider=yfinance,
    )

    frame = provider.get_ohlcv("RELIANCE.NS", date(2026, 1, 1), date(2026, 1, 10))

    assert kite.called is False
    assert yfinance.called is True
    assert frame.attrs["source"] == "yfinance"
    assert "KITE_ACCESS_TOKEN is missing or expired" in frame.attrs["fallback_reason"]


def test_market_data_provider_uses_kite_when_available() -> None:
    kite = _FakeKiteProvider()
    yfinance = _FakeYfinanceProvider()
    provider = MarketDataProvider(
        provider_name="kite",
        kite_provider=kite,
        yfinance_provider=yfinance,
    )

    frame = provider.get_ohlcv("RELIANCE.NS", date(2026, 1, 1), date(2026, 1, 10))

    assert kite.called is True
    assert yfinance.called is False
    assert frame.attrs["source"] == "kite"
