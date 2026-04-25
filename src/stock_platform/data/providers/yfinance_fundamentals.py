"""yfinance-backed fundamentals provider.

Reads annual income statement, balance sheet, and cash flow directly from
yfinance.  Falls back gracefully when columns are missing — yfinance coverage
varies by stock and changes across library versions.

Values are in the currency reported by yfinance (INR for .NS symbols).
Indian fiscal year ends 31 March, so a date column of 2025-03-31 maps to
fiscal_year 2025.
"""

from __future__ import annotations

import pandas as pd
import yfinance as yf

from stock_platform.analytics.fundamentals.schema import FundamentalSnapshot
from stock_platform.data.providers.base import FundamentalsDataProvider
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Column name maps: yfinance row label → our schema field
# ---------------------------------------------------------------------------

_INCOME_MAP: dict[str, str] = {
    "Total Revenue": "revenue",
    "Gross Profit": "gross_profit",
    "EBITDA": "ebitda",
    "Operating Income": "ebit",
    "Net Income": "net_income",
    "Diluted EPS": "eps",
    "Basic EPS": "eps",  # fallback when diluted is absent
}

_BALANCE_MAP: dict[str, str] = {
    "Total Assets": "total_assets",
    "Total Liabilities Net Minority Interest": "total_liabilities",
    "Current Assets": "current_assets",
    "Current Liabilities": "current_liabilities",
    "Retained Earnings": "retained_earnings",
    "Ordinary Shares Number": "shares_outstanding",
    "Total Debt": "debt",
    "Cash And Cash Equivalents": "cash_and_equivalents",
    "Common Stock Equity": "book_value",
}

_CASHFLOW_MAP: dict[str, str] = {
    "Operating Cash Flow": "operating_cash_flow",
    "Capital Expenditure": "capital_expenditure",
    "Free Cash Flow": "free_cash_flow",
}

# Columns returned by get_annual_fundamentals / get_all_annual_fundamentals
_ANNUAL_COLUMNS = [
    "symbol",
    "fiscal_year",
    "revenue",
    "gross_profit",
    "ebitda",
    "ebit",
    "net_income",
    "eps",
    "book_value",
    "operating_cash_flow",
    "capital_expenditure",
    "free_cash_flow",
    "debt",
    "net_debt",
    "cash_and_equivalents",
    "total_assets",
    "total_liabilities",
    "current_assets",
    "current_liabilities",
    "retained_earnings",
    "shares_outstanding",
    "market_cap",
    "enterprise_value",
    "sector",
    "industry",
    "market_cap_bucket",
    "source",
    "source_url",
]


class YFinanceFundamentalsProvider(FundamentalsDataProvider):
    """Annual fundamentals provider backed by yfinance financial statements."""

    name = "yfinance"

    # ------------------------------------------------------------------
    # Abstract method implementations
    # ------------------------------------------------------------------

    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        frame = self.get_annual_fundamentals(symbol)
        cols = [
            c
            for c in [
                "symbol",
                "fiscal_year",
                "revenue",
                "gross_profit",
                "ebitda",
                "ebit",
                "net_income",
                "eps",
            ]
            if c in frame.columns
        ]
        return frame[cols]

    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        frame = self.get_annual_fundamentals(symbol)
        cols = [
            c
            for c in [
                "symbol",
                "fiscal_year",
                "total_assets",
                "total_liabilities",
                "current_assets",
                "current_liabilities",
                "retained_earnings",
                "book_value",
                "debt",
                "net_debt",
                "cash_and_equivalents",
                "shares_outstanding",
                "market_cap",
                "enterprise_value",
            ]
            if c in frame.columns
        ]
        return frame[cols]

    def get_cash_flow(self, symbol: str) -> pd.DataFrame:
        frame = self.get_annual_fundamentals(symbol)
        cols = [
            c
            for c in [
                "symbol",
                "fiscal_year",
                "operating_cash_flow",
                "capital_expenditure",
                "free_cash_flow",
            ]
            if c in frame.columns
        ]
        return frame[cols]

    # ------------------------------------------------------------------
    # Core method — mirrors CsvFundamentalsProvider API so summary.py
    # and the Streamlit app can use either provider interchangeably.
    # ------------------------------------------------------------------

    def get_annual_fundamentals(self, symbol: str) -> pd.DataFrame:
        """Fetch annual fundamentals for one symbol from yfinance."""
        try:
            ticker = yf.Ticker(symbol)
            rows = self._build_annual_rows(ticker, symbol)
            if not rows:
                log.warning("yfinance returned no usable fundamentals rows for %s", symbol)
                return pd.DataFrame(columns=_ANNUAL_COLUMNS)

            frame = pd.DataFrame(rows)
            frame["symbol"] = symbol.upper()
            frame["source"] = "yfinance"
            frame["source_url"] = f"https://finance.yahoo.com/quote/{symbol}"

            # Sector / industry / market_cap_bucket from ticker info
            info = _safe_info(ticker)
            frame["sector"] = info.get("sector") or info.get("sectorKey") or None
            frame["industry"] = info.get("industry") or info.get("industryKey") or None
            frame["market_cap_bucket"] = _market_cap_bucket(info.get("marketCap"))

            return frame.sort_values("fiscal_year").reset_index(drop=True)

        except Exception as exc:
            log.warning("yfinance fundamentals failed for %s: %s", symbol, exc)
            return pd.DataFrame(columns=_ANNUAL_COLUMNS)

    def get_all_annual_fundamentals(self) -> pd.DataFrame:
        """Not supported for the yfinance provider (no universe list here)."""
        return pd.DataFrame(columns=_ANNUAL_COLUMNS)

    def get_snapshots(self, symbol: str) -> list[FundamentalSnapshot]:
        """Return annual fundamentals as typed FundamentalSnapshot objects."""
        frame = self.get_annual_fundamentals(symbol)
        snapshots: list[FundamentalSnapshot] = []
        for row in frame.to_dict(orient="records"):
            snapshots.append(
                FundamentalSnapshot(
                    symbol=str(row["symbol"]),
                    fiscal_year=int(row["fiscal_year"]),
                    revenue=_f(row.get("revenue")),
                    gross_profit=_f(row.get("gross_profit")),
                    ebitda=_f(row.get("ebitda")),
                    ebit=_f(row.get("ebit")),
                    net_income=_f(row.get("net_income")),
                    eps=_f(row.get("eps")),
                    book_value=_f(row.get("book_value")),
                    operating_cash_flow=_f(row.get("operating_cash_flow")),
                    capital_expenditure=_f(row.get("capital_expenditure")),
                    free_cash_flow=_f(row.get("free_cash_flow")),
                    debt=_f(row.get("debt")),
                    net_debt=_f(row.get("net_debt")),
                    cash_and_equivalents=_f(row.get("cash_and_equivalents")),
                    total_assets=_f(row.get("total_assets")),
                    total_liabilities=_f(row.get("total_liabilities")),
                    current_assets=_f(row.get("current_assets")),
                    current_liabilities=_f(row.get("current_liabilities")),
                    retained_earnings=_f(row.get("retained_earnings")),
                    shares_outstanding=_f(row.get("shares_outstanding")),
                    market_cap=_f(row.get("market_cap")),
                    enterprise_value=_f(row.get("enterprise_value")),
                )
            )
        return snapshots

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_annual_rows(self, ticker: yf.Ticker, symbol: str) -> list[dict[str, object]]:
        income = _transpose_statements(ticker.income_stmt)
        balance = _transpose_statements(ticker.balance_sheet)
        cashflow = _transpose_statements(ticker.cashflow)

        if income.empty and balance.empty:
            return []

        # Use income stmt dates as the primary index; fall back to balance sheet
        all_dates = sorted(set(income.index) | set(balance.index) | set(cashflow.index))

        # Current market cap (point-in-time MC not available per period — use current)
        info = _safe_info(ticker)
        current_mc = _f(info.get("marketCap"))

        rows: list[dict[str, object]] = []
        for dt in all_dates:
            fy = _fiscal_year(dt)
            row: dict[str, object] = {"fiscal_year": fy}

            # Income statement
            inc_row = income.loc[dt] if dt in income.index else pd.Series(dtype=float)
            _map_row(inc_row, _INCOME_MAP, row)

            # Balance sheet
            bal_row = balance.loc[dt] if dt in balance.index else pd.Series(dtype=float)
            _map_row(bal_row, _BALANCE_MAP, row)

            # Cash flow
            cf_row = cashflow.loc[dt] if dt in cashflow.index else pd.Series(dtype=float)
            _map_row(cf_row, _CASHFLOW_MAP, row)

            # Derived fields
            cash = _f(row.get("cash_and_equivalents"))
            debt = _f(row.get("debt"))
            if cash is not None and debt is not None:
                row["net_debt"] = debt - cash

            # Only attach market cap to the most-recent period
            row["market_cap"] = current_mc if dt == max(all_dates) else None
            row["enterprise_value"] = (
                _enterprise_value(current_mc, row.get("net_debt")) if dt == max(all_dates) else None
            )

            rows.append(row)

        log.info("yfinance fundamentals: %d annual rows for %s", len(rows), symbol)
        return rows


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _transpose_statements(stmt: pd.DataFrame) -> pd.DataFrame:
    """Transpose yfinance statement so rows = periods, columns = metrics."""
    if stmt is None or stmt.empty:
        return pd.DataFrame()
    transposed = stmt.T
    transposed.index = pd.to_datetime(transposed.index)
    return transposed


def _map_row(
    source_row: pd.Series,
    mapping: dict[str, str],
    target: dict[str, object],
) -> None:
    """Copy mapped values from source_row into target dict; skip NaN."""
    for src_col, dst_col in mapping.items():
        if dst_col in target:
            # Already set by a higher-priority mapping (e.g. diluted EPS before basic)
            continue
        if src_col in source_row.index:
            val = source_row[src_col]
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                target[dst_col] = float(val)


def _fiscal_year(dt: pd.Timestamp) -> int:
    """For Indian fiscal years (Apr–Mar), year ending Mar 31 → that calendar year."""
    if dt.month <= 3:
        return dt.year
    # April–December end date → next year's FY label (e.g. 2024-12-31 → FY2025)
    return dt.year + 1


def _enterprise_value(market_cap: float | None, net_debt: object) -> float | None:
    if market_cap is None:
        return None
    nd = _f(net_debt)
    if nd is None:
        return market_cap
    return market_cap + nd


def _market_cap_bucket(mc: float | None) -> str | None:
    if mc is None:
        return None
    cr = mc / 1e7  # convert rupees to crores
    if cr >= 20_000:
        return "large_cap"
    if cr >= 5_000:
        return "mid_cap"
    return "small_cap"


def _safe_info(ticker: yf.Ticker) -> dict:
    try:
        return ticker.info or {}
    except Exception:
        return {}


def _f(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
