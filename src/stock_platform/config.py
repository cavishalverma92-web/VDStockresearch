"""
Central configuration loader.

Reads:
- Environment variables (via python-dotenv from .env at project root)
- YAML files in config/

Everything in the app should import settings/config from here — never read
files or env vars directly from feature code.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #

# This file lives at: <repo>/src/stock_platform/config.py
# So repo root is two parents up.
ROOT_DIR: Path = Path(__file__).resolve().parents[2]
CONFIG_DIR: Path = ROOT_DIR / "config"
DATA_DIR: Path = ROOT_DIR / "data"
LOGS_DIR: Path = ROOT_DIR / "logs"

load_dotenv(ROOT_DIR / ".env")


# --------------------------------------------------------------------------- #
# Environment settings (from .env)
# --------------------------------------------------------------------------- #


class Settings(BaseSettings):
    """Typed environment settings. Override via .env."""

    # App
    app_env: str = "development"
    app_log_level: str = "INFO"
    app_timezone: str = "Asia/Kolkata"

    # Streamlit
    streamlit_server_port: int = 8501

    # Database
    database_url: str = "sqlite:///data/stock_platform.db"

    # Providers
    provider_price: str = "yfinance"
    market_data_provider: str = "kite"
    provider_fundamentals: str = "local_csv"
    fundamentals_csv_path: str = str(DATA_DIR / "sample/fundamentals_annual_sample.csv")

    # Paths (overridable via env for testing)
    cache_dir: str = str(DATA_DIR / "cache")
    raw_dir: str = str(DATA_DIR / "raw")
    processed_dir: str = str(DATA_DIR / "processed")

    # HTTP
    http_rate_limit_per_sec: float = 2.0
    http_timeout_sec: int = 30

    # Keys (may be empty in Phase 0)
    kite_api_key: str = ""
    kite_api_secret: str = ""
    kite_access_token: str = ""
    enable_kite_market_data: bool = True
    enable_kite_trading: bool = False
    enable_kite_portfolio: bool = False
    anthropic_api_key: str = ""
    openai_api_key: str = ""

    model_config = SettingsConfigDict(
        env_file=ROOT_DIR / ".env",
        case_sensitive=False,
        extra="ignore",
    )


# --------------------------------------------------------------------------- #
# YAML config loader
# --------------------------------------------------------------------------- #


def _load_yaml(filename: str) -> dict[str, Any]:
    """Load a YAML file from the config/ directory."""
    path = CONFIG_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found: {path}\n"
            f"Expected a file at: {path}\n"
            f"Check that you are running from the project root."
        )
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return environment settings (cached)."""
    return Settings()


@lru_cache(maxsize=1)
def get_scoring_weights() -> dict[str, Any]:
    """Return the scoring weights config."""
    return _load_yaml("scoring_weights.yaml")


@lru_cache(maxsize=1)
def get_universe_config() -> dict[str, Any]:
    """Return the universe config."""
    return _load_yaml("universe.yaml")


@lru_cache(maxsize=1)
def get_data_sources_config() -> dict[str, Any]:
    """Return the data sources config."""
    return _load_yaml("data_sources.yaml")


@lru_cache(maxsize=1)
def get_thresholds_config() -> dict[str, Any]:
    """Return the thresholds config."""
    return _load_yaml("thresholds.yaml")


@lru_cache(maxsize=1)
def get_universes_config() -> dict[str, Any]:
    """Return the index-universe lists for the Phase 8 scanner."""
    return _load_yaml("universes.yaml")
