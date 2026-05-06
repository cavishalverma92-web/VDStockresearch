from __future__ import annotations

from stock_platform.config import get_settings
from stock_platform.ui.components.common import is_hosted_demo_mode


def _clear_settings_cache() -> None:
    get_settings.cache_clear()


def test_is_hosted_demo_mode_for_free_render_defaults(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///data/stock_platform.db")
    monkeypatch.setenv("MARKET_DATA_PROVIDER", "yfinance")
    monkeypatch.setenv("ENABLE_KITE_MARKET_DATA", "false")
    _clear_settings_cache()

    try:
        assert is_hosted_demo_mode() is True
    finally:
        _clear_settings_cache()


def test_is_hosted_demo_mode_false_when_kite_market_data_enabled(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///data/stock_platform.db")
    monkeypatch.setenv("MARKET_DATA_PROVIDER", "yfinance")
    monkeypatch.setenv("ENABLE_KITE_MARKET_DATA", "true")
    _clear_settings_cache()

    try:
        assert is_hosted_demo_mode() is False
    finally:
        _clear_settings_cache()


def test_is_hosted_demo_mode_false_for_local_development(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///data/stock_platform.db")
    monkeypatch.setenv("MARKET_DATA_PROVIDER", "yfinance")
    monkeypatch.setenv("ENABLE_KITE_MARKET_DATA", "false")
    _clear_settings_cache()

    try:
        assert is_hosted_demo_mode() is False
    finally:
        _clear_settings_cache()
