"""
Abstract base classes for data providers.

Rule: application code MUST depend on these abstractions, not on any specific
vendor library. This lets us swap free sources for paid ones without rewrites.

See `config/data_sources.yaml` for the provider registry.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

import pandas as pd


class PriceDataProvider(ABC):
    """Provider of OHLCV bars for a symbol over a date range."""

    name: str = "base"

    @abstractmethod
    def get_ohlcv(
        self,
        symbol: str,
        start: date,
        end: date,
        interval: str = "1d",
    ) -> pd.DataFrame:
        """
        Return a DataFrame indexed by date, with columns:
            open, high, low, close, adj_close, volume

        Raises:
            ValueError: if symbol / range / interval is invalid.
            RuntimeError: if the upstream source fails.
        """
        ...


class FundamentalsDataProvider(ABC):
    """Provider of company fundamentals (income statement, balance sheet, etc.)."""

    name: str = "base"

    @abstractmethod
    def get_income_statement(self, symbol: str) -> pd.DataFrame: ...

    @abstractmethod
    def get_balance_sheet(self, symbol: str) -> pd.DataFrame: ...

    @abstractmethod
    def get_cash_flow(self, symbol: str) -> pd.DataFrame: ...


class CorporateActionsProvider(ABC):
    """Splits, bonuses, dividends, buybacks, rights."""

    name: str = "base"

    @abstractmethod
    def get_actions(self, symbol: str, start: date, end: date) -> pd.DataFrame: ...


class HoldingsDataProvider(ABC):
    """Promoter / FII / DII / MF holdings over time."""

    name: str = "base"

    @abstractmethod
    def get_shareholding_pattern(self, symbol: str) -> pd.DataFrame: ...


class InsiderTradesProvider(ABC):
    """SEBI PIT disclosures."""

    name: str = "base"

    @abstractmethod
    def get_insider_trades(self, symbol: str, start: date, end: date) -> pd.DataFrame: ...


class EventsProvider(ABC):
    """Earnings calendar, corporate actions calendar, rating actions."""

    name: str = "base"

    @abstractmethod
    def get_earnings_calendar(self, start: date, end: date) -> pd.DataFrame: ...
