"""Screener.in fundamentals provider.

Screener publishes per-company pages with stable HTML section ids:
- ``#profit-loss``    — annual P&L (last ~12 years)
- ``#balance-sheet``  — annual balance sheet
- ``#cash-flow``      — annual cash flow
- ``#quarters``       — last ~12 quarterly results

We fetch the consolidated page (``/company/<id>/consolidated/``) and parse
those tables into the same schema as ``YFinanceFundamentalsProvider`` so
the rest of the platform can use either source interchangeably.

Notes & caveats:
- This is a public web page; we send a polite UA, cache aggressively at the
  job layer (run nightly), and never hammer the site. Review Screener's
  ToS before scaled use.
- The mapping from screener row labels → our schema is defensive: missing
  rows are simply omitted. Different companies (banks, NBFCs) use different
  templates and not every field is present everywhere.
- Screener stores rupee values in **crores** by default. We multiply by
  1e7 to normalize to rupees, matching the yfinance convention used elsewhere.
- Uses ``httpx`` (already a dep) and the stdlib HTML parser (no new deps).
"""

from __future__ import annotations

import re
from html.parser import HTMLParser

import httpx
import pandas as pd

from stock_platform.analytics.fundamentals.schema import (
    FundamentalSnapshot,
    QuarterlyFundamentalSnapshot,
)
from stock_platform.data.providers.base import FundamentalsDataProvider
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)

_BASE = "https://www.screener.in"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
}
_TIMEOUT = httpx.Timeout(15.0, connect=10.0)
_CRORE_TO_RUPEES = 1e7

# Screener row label → our schema field. Match is case-insensitive and
# accepts trailing footnote markers Screener uses (e.g. "Sales +").
_PL_MAP: dict[str, str] = {
    "sales": "revenue",
    "revenue": "revenue",
    "operating profit": "ebitda",  # Screener's OP includes D&A back-out variants; close enough for ranking
    "profit before tax": "ebit",
    "net profit": "net_income",
    "eps in rs": "eps",
    "interest": "interest_expense",
    "raw material cost": "cost_of_revenue",
}

_BS_MAP: dict[str, str] = {
    "equity capital": "shares_outstanding",  # face-value shares, not float — best Screener exposes
    "reserves": "retained_earnings",
    "borrowings": "debt",
    "total liabilities": "total_liabilities",
    "total assets": "total_assets",
    "fixed assets": None,  # ignored — placeholder showing what we considered
    "investments": None,
    "other assets": None,
}
_BS_MAP = {k: v for k, v in _BS_MAP.items() if v is not None}

_CF_MAP: dict[str, str] = {
    "cash from operating activity": "operating_cash_flow",
    "cash from investing activity": "capital_expenditure",  # used as proxy when capex line absent
}

_QUARTERS_MAP: dict[str, str] = {
    "sales": "revenue",
    "revenue": "revenue",
    "operating profit": "ebitda",
    "profit before tax": "ebit",
    "net profit": "net_income",
    "eps in rs": "eps",
}


# ---------------------------------------------------------------------------
# HTML parsing — minimal, dependency-free
# ---------------------------------------------------------------------------


class _SectionTableParser(HTMLParser):
    """Pull all ``<table>`` rows from inside the section with ``id=section_id``.

    We don't need a full DOM; we just track when we're inside the target
    section, inside a table, inside a row, inside a cell.
    """

    def __init__(self, section_id: str) -> None:
        super().__init__()
        self._target = section_id
        self._depth_in_target = 0
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._cell_buf: list[str] = []
        self._row_buf: list[str] = []
        self.headers: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_dict = {k: v for k, v in attrs}
        if tag == "section" and attr_dict.get("id") == self._target:
            self._depth_in_target = 1
            return
        if self._depth_in_target == 0:
            return
        if tag == "section":
            self._depth_in_target += 1
            return
        if tag == "table":
            self._in_table = True
            return
        if not self._in_table:
            return
        if tag == "tr":
            self._in_row = True
            self._row_buf = []
            return
        if tag in ("td", "th"):
            self._in_cell = True
            self._cell_buf = []

    def handle_endtag(self, tag: str) -> None:
        if self._depth_in_target == 0:
            return
        if tag == "section":
            self._depth_in_target -= 1
            return
        if not self._in_table:
            return
        if tag == "table":
            self._in_table = False
            return
        if tag == "tr":
            if self._row_buf:
                if not self.headers:
                    self.headers = self._row_buf
                else:
                    self.rows.append(self._row_buf)
            self._in_row = False
            return
        if tag in ("td", "th") and self._in_cell:
            self._row_buf.append("".join(self._cell_buf).strip())
            self._in_cell = False

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_buf.append(data)


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------


class ScreenerFundamentalsProvider(FundamentalsDataProvider):
    """Annual + quarterly fundamentals from the public Screener.in pages.

    Pass ``html_fetcher`` for tests — it should be a callable
    ``url -> str`` returning the raw page HTML.
    """

    name = "screener"

    def __init__(self, html_fetcher: callable | None = None) -> None:  # type: ignore[name-defined]
        self._fetch_html = html_fetcher or self._default_fetch

    # ------------------------------------------------------------------
    # FundamentalsDataProvider interface
    # ------------------------------------------------------------------

    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        frame = self.get_annual_fundamentals(symbol)
        keep = [
            c
            for c in ("symbol", "fiscal_year", "revenue", "ebitda", "ebit", "net_income", "eps")
            if c in frame.columns
        ]
        return frame[keep] if keep else frame

    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        frame = self.get_annual_fundamentals(symbol)
        keep = [
            c
            for c in (
                "symbol",
                "fiscal_year",
                "total_assets",
                "total_liabilities",
                "debt",
                "shares_outstanding",
                "retained_earnings",
            )
            if c in frame.columns
        ]
        return frame[keep] if keep else frame

    def get_cash_flow(self, symbol: str) -> pd.DataFrame:
        frame = self.get_annual_fundamentals(symbol)
        keep = [
            c
            for c in ("symbol", "fiscal_year", "operating_cash_flow", "capital_expenditure")
            if c in frame.columns
        ]
        return frame[keep] if keep else frame

    # ------------------------------------------------------------------

    def get_annual_fundamentals(self, symbol: str) -> pd.DataFrame:
        try:
            html = self._fetch_html(self._page_url(symbol))
        except Exception as exc:
            log.warning("screener fetch failed for {}: {}", symbol, exc)
            return pd.DataFrame()
        if not html:
            return pd.DataFrame()

        pl = _parse_section(html, "profit-loss", _PL_MAP)
        bs = _parse_section(html, "balance-sheet", _BS_MAP)
        cf = _parse_section(html, "cash-flow", _CF_MAP)

        merged = _merge_annual_sections([pl, bs, cf])
        if merged.empty:
            return merged

        merged["symbol"] = symbol.upper()
        merged["source"] = "screener"
        merged["source_url"] = self._page_url(symbol)
        return merged.sort_values("fiscal_year").reset_index(drop=True)

    def get_quarterly_fundamentals(self, symbol: str) -> pd.DataFrame:
        try:
            html = self._fetch_html(self._page_url(symbol))
        except Exception as exc:
            log.warning("screener fetch failed for {}: {}", symbol, exc)
            return pd.DataFrame()
        if not html:
            return pd.DataFrame()

        frame = _parse_section(html, "quarters", _QUARTERS_MAP, period_kind="quarter")
        if frame.empty:
            return frame
        frame["symbol"] = symbol.upper()
        frame["source"] = "screener"
        frame["source_url"] = self._page_url(symbol)
        return frame.sort_values(["fiscal_year", "fiscal_quarter"]).reset_index(drop=True)

    def get_snapshots(self, symbol: str) -> list[FundamentalSnapshot]:
        frame = self.get_annual_fundamentals(symbol)
        out: list[FundamentalSnapshot] = []
        for row in frame.to_dict(orient="records"):
            out.append(
                FundamentalSnapshot(
                    symbol=str(row["symbol"]),
                    fiscal_year=int(row["fiscal_year"]),
                    revenue=_f(row.get("revenue")),
                    ebitda=_f(row.get("ebitda")),
                    ebit=_f(row.get("ebit")),
                    net_income=_f(row.get("net_income")),
                    eps=_f(row.get("eps")),
                    operating_cash_flow=_f(row.get("operating_cash_flow")),
                    capital_expenditure=_f(row.get("capital_expenditure")),
                    debt=_f(row.get("debt")),
                    total_assets=_f(row.get("total_assets")),
                    total_liabilities=_f(row.get("total_liabilities")),
                    retained_earnings=_f(row.get("retained_earnings")),
                    shares_outstanding=_f(row.get("shares_outstanding")),
                    interest_expense=_f(row.get("interest_expense")),
                    cost_of_revenue=_f(row.get("cost_of_revenue")),
                )
            )
        return out

    def get_quarterly_snapshots(self, symbol: str) -> list[QuarterlyFundamentalSnapshot]:
        frame = self.get_quarterly_fundamentals(symbol)
        out: list[QuarterlyFundamentalSnapshot] = []
        for row in frame.to_dict(orient="records"):
            out.append(
                QuarterlyFundamentalSnapshot(
                    symbol=str(row["symbol"]),
                    fiscal_year=int(row["fiscal_year"]),
                    fiscal_quarter=int(row["fiscal_quarter"]),
                    revenue=_f(row.get("revenue")),
                    ebitda=_f(row.get("ebitda")),
                    ebit=_f(row.get("ebit")),
                    net_income=_f(row.get("net_income")),
                    eps=_f(row.get("eps")),
                )
            )
        return out

    # ------------------------------------------------------------------

    @staticmethod
    def _page_url(symbol: str) -> str:
        # Screener uses the bare NSE symbol (no .NS suffix)
        clean = symbol.upper().replace(".NS", "").replace(".BO", "")
        return f"{_BASE}/company/{clean}/consolidated/"

    def _default_fetch(self, url: str) -> str:
        with httpx.Client(headers=_HEADERS, timeout=_TIMEOUT, follow_redirects=True) as client:
            response = client.get(url)
            if response.status_code == 404:
                # Fall back to standalone page if consolidated doesn't exist
                fallback = url.replace("/consolidated/", "/")
                response = client.get(fallback)
            response.raise_for_status()
            return response.text


# ---------------------------------------------------------------------------
# Section parsing
# ---------------------------------------------------------------------------


def _parse_section(
    html: str,
    section_id: str,
    label_map: dict[str, str],
    *,
    period_kind: str = "annual",
) -> pd.DataFrame:
    """Parse one ``<section id=...>`` table into a long DataFrame.

    Returns columns: ``fiscal_year`` (and ``fiscal_quarter`` for quarter
    sections), plus one column per mapped metric. Values are floats in
    rupees.
    """
    parser = _SectionTableParser(section_id)
    parser.feed(html)
    if not parser.headers or not parser.rows:
        return pd.DataFrame()

    period_headers = parser.headers[1:]
    if period_kind == "quarter":
        periods = [_parse_quarter_header(h) for h in period_headers]
    else:
        periods = [_parse_year_header(h) for h in period_headers]

    valid_idx = [i for i, p in enumerate(periods) if p is not None]
    if not valid_idx:
        return pd.DataFrame()

    period_cols: dict[tuple[int, int | None], dict[str, float]] = {
        periods[i]: {}
        for i in valid_idx  # type: ignore[index]
    }

    for raw_row in parser.rows:
        if not raw_row:
            continue
        label = _normalize_label(raw_row[0])
        target = _match_label(label, label_map)
        if target is None:
            continue
        cells = raw_row[1:]
        for i in valid_idx:
            if i >= len(cells):
                continue
            value = _parse_number(cells[i])
            if value is None:
                continue
            period_cols[periods[i]][target] = value * _CRORE_TO_RUPEES  # type: ignore[index]

    records = []
    for period, metrics in period_cols.items():
        if period is None:
            continue
        record: dict[str, object] = {"fiscal_year": period[0]}
        if period_kind == "quarter":
            record["fiscal_quarter"] = period[1]
        record.update(metrics)
        records.append(record)
    return pd.DataFrame(records)


def _merge_annual_sections(frames: list[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        return pd.DataFrame()
    merged = non_empty[0]
    for other in non_empty[1:]:
        merged = merged.merge(other, on="fiscal_year", how="outer", suffixes=("", "_dup"))
        for col in list(merged.columns):
            if col.endswith("_dup"):
                base = col[:-4]
                merged[base] = merged[base].fillna(merged[col])
                merged = merged.drop(columns=[col])
    return merged


# ---------------------------------------------------------------------------
# Cell + header parsing
# ---------------------------------------------------------------------------

_NUMBER_RE = re.compile(r"^-?[\d,]+(?:\.\d+)?$")
_QUARTER_RE = re.compile(r"^([A-Za-z]{3})\s*(\d{4})$")
_MONTH_TO_QUARTER = {
    "Jan": 4,
    "Feb": 4,
    "Mar": 4,
    "Apr": 1,
    "May": 1,
    "Jun": 1,
    "Jul": 2,
    "Aug": 2,
    "Sep": 2,
    "Oct": 3,
    "Nov": 3,
    "Dec": 3,
}


def _normalize_label(text: str) -> str:
    return re.sub(r"[^a-z0-9 ]", "", text.lower()).strip()


def _match_label(label: str, label_map: dict[str, str]) -> str | None:
    for key, target in label_map.items():
        if label.startswith(key):
            return target
    return None


def _parse_number(text: str) -> float | None:
    cleaned = text.strip().replace(",", "").replace("₹", "").replace("%", "").strip()
    cleaned = cleaned.replace(" ", "")
    if not cleaned or not _NUMBER_RE.match(cleaned):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_year_header(text: str) -> tuple[int, None] | None:
    """Screener annual headers look like 'Mar 2025' — fiscal_year = 2025."""
    match = _QUARTER_RE.match(text.strip())
    if match is None:
        # Sometimes just "2025"
        digits = re.findall(r"\d{4}", text)
        return (int(digits[-1]), None) if digits else None
    return (int(match.group(2)), None)


def _parse_quarter_header(text: str) -> tuple[int, int] | None:
    """Quarterly headers look like 'Mar 2025'. Indian FY: Mar 2025 → FY2025/Q4."""
    match = _QUARTER_RE.match(text.strip())
    if match is None:
        return None
    month_abbr = match.group(1).title()
    year = int(match.group(2))
    quarter = _MONTH_TO_QUARTER.get(month_abbr)
    if quarter is None:
        return None
    fiscal_year = year if month_abbr in ("Jan", "Feb", "Mar") else year + 1
    return (fiscal_year, quarter)


def _f(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
