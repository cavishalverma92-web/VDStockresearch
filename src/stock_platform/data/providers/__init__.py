"""Data provider implementations.

All application code must go through the abstract base classes defined here.
Never import yfinance / NSE / Screener directly in feature modules.
"""

from stock_platform.data.providers.base import FundamentalsDataProvider, PriceDataProvider
from stock_platform.data.providers.corporate_actions import (
    get_dividends,
    get_earnings_history,
    get_splits,
    get_upcoming_earnings,
)
from stock_platform.data.providers.csv_fundamentals import CsvFundamentalsProvider
from stock_platform.data.providers.institutional_holdings import (
    get_institutional_holders,
    get_major_holders,
    get_mutualfund_holders,
    holdings_summary,
)
from stock_platform.data.providers.nse import (
    fetch_bulk_deals,
    fetch_deals_for_symbol,
    fetch_delivery_data,
)
from stock_platform.data.providers.yahoo import YahooFinanceProvider
from stock_platform.data.providers.yfinance_fundamentals import YFinanceFundamentalsProvider

__all__ = [
    "CsvFundamentalsProvider",
    "FundamentalsDataProvider",
    "PriceDataProvider",
    "YahooFinanceProvider",
    "YFinanceFundamentalsProvider",
    "fetch_bulk_deals",
    "fetch_deals_for_symbol",
    "fetch_delivery_data",
    "get_dividends",
    "get_earnings_history",
    "get_institutional_holders",
    "get_major_holders",
    "get_mutualfund_holders",
    "get_splits",
    "get_upcoming_earnings",
    "holdings_summary",
]
