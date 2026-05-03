"""Tests for the Screener.in fundamentals provider (offline, fixture HTML)."""

from __future__ import annotations

from stock_platform.data.providers.screener_fundamentals import (
    ScreenerFundamentalsProvider,
    _parse_number,
    _parse_quarter_header,
    _parse_year_header,
)

_FIXTURE_HTML = """
<html><body>
<section id="profit-loss">
  <table>
    <tr><th></th><th>Mar 2024</th><th>Mar 2025</th></tr>
    <tr><td>Sales</td><td>1,000</td><td>1,200</td></tr>
    <tr><td>Net Profit</td><td>100</td><td>150</td></tr>
    <tr><td>EPS in Rs</td><td>10.00</td><td>15.00</td></tr>
  </table>
</section>
<section id="balance-sheet">
  <table>
    <tr><th></th><th>Mar 2024</th><th>Mar 2025</th></tr>
    <tr><td>Total Assets</td><td>5,000</td><td>5,500</td></tr>
    <tr><td>Borrowings</td><td>800</td><td>700</td></tr>
  </table>
</section>
<section id="cash-flow">
  <table>
    <tr><th></th><th>Mar 2024</th><th>Mar 2025</th></tr>
    <tr><td>Cash from Operating Activity</td><td>120</td><td>180</td></tr>
  </table>
</section>
<section id="quarters">
  <table>
    <tr><th></th><th>Jun 2024</th><th>Sep 2024</th><th>Dec 2024</th><th>Mar 2025</th></tr>
    <tr><td>Sales</td><td>250</td><td>290</td><td>320</td><td>340</td></tr>
    <tr><td>Net Profit</td><td>20</td><td>30</td><td>45</td><td>55</td></tr>
  </table>
</section>
</body></html>
"""


def test_parse_year_header():
    assert _parse_year_header("Mar 2025") == (2025, None)
    assert _parse_year_header("2024") == (2024, None)
    assert _parse_year_header("not a year") is None


def test_parse_quarter_header():
    assert _parse_quarter_header("Mar 2025") == (2025, 4)  # FY2025 Q4
    assert _parse_quarter_header("Jun 2024") == (2025, 1)  # FY2025 Q1
    assert _parse_quarter_header("Sep 2024") == (2025, 2)
    assert _parse_quarter_header("Dec 2024") == (2025, 3)


def test_parse_number_handles_commas_and_signs():
    assert _parse_number("1,200") == 1200.0
    assert _parse_number("-50") == -50.0
    assert _parse_number("3.14") == 3.14
    assert _parse_number("--") is None
    assert _parse_number("N/A") is None


def test_screener_annual_parses_fixture():
    provider = ScreenerFundamentalsProvider(html_fetcher=lambda url: _FIXTURE_HTML)
    frame = provider.get_annual_fundamentals("RELIANCE")
    assert not frame.empty
    assert sorted(frame["fiscal_year"].tolist()) == [2024, 2025]
    fy25 = frame[frame["fiscal_year"] == 2025].iloc[0]
    assert fy25["revenue"] == 1200 * 1e7
    assert fy25["net_income"] == 150 * 1e7
    assert fy25["total_assets"] == 5500 * 1e7
    assert fy25["operating_cash_flow"] == 180 * 1e7
    assert frame["source"].iloc[0] == "screener"


def test_screener_quarterly_parses_fixture():
    provider = ScreenerFundamentalsProvider(html_fetcher=lambda url: _FIXTURE_HTML)
    frame = provider.get_quarterly_fundamentals("RELIANCE")
    assert not frame.empty
    assert len(frame) == 4
    # Mar 2025 → FY2025 Q4
    q4 = frame[(frame["fiscal_year"] == 2025) & (frame["fiscal_quarter"] == 4)].iloc[0]
    assert q4["revenue"] == 340 * 1e7
    assert q4["net_income"] == 55 * 1e7


def test_screener_get_snapshots_typed():
    provider = ScreenerFundamentalsProvider(html_fetcher=lambda url: _FIXTURE_HTML)
    snaps = provider.get_snapshots("RELIANCE")
    assert len(snaps) == 2
    latest = snaps[-1]
    assert latest.fiscal_year == 2025
    assert latest.revenue == 1200 * 1e7


def test_screener_empty_html_returns_empty_frame():
    provider = ScreenerFundamentalsProvider(html_fetcher=lambda url: "")
    assert provider.get_annual_fundamentals("X").empty
    assert provider.get_quarterly_fundamentals("X").empty


def test_screener_fetch_failure_swallowed():
    def raises(_url: str) -> str:
        raise RuntimeError("boom")

    provider = ScreenerFundamentalsProvider(html_fetcher=raises)
    assert provider.get_annual_fundamentals("X").empty


def test_page_url_strips_suffix():
    assert ScreenerFundamentalsProvider._page_url("RELIANCE.NS").endswith("/RELIANCE/consolidated/")
    assert ScreenerFundamentalsProvider._page_url("INFY").endswith("/INFY/consolidated/")
