"""Tests for cross-source fundamentals comparison + data_trust integration."""

from __future__ import annotations

import pandas as pd

from stock_platform.analytics.fundamentals.cross_source import (
    compare_fundamentals_sources,
)
from stock_platform.ops.data_trust import build_data_trust_rows


def _two_source_frame(yf_revenue: float, screener_revenue: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fiscal_year": 2025,
                "source": "yfinance",
                "revenue": yf_revenue,
                "net_income": 100.0,
                "ebitda": 200.0,
                "eps": 10.0,
                "operating_cash_flow": 120.0,
                "total_assets": 5000.0,
            },
            {
                "fiscal_year": 2025,
                "source": "screener",
                "revenue": screener_revenue,
                "net_income": 100.0,
                "ebitda": 200.0,
                "eps": 10.0,
                "operating_cash_flow": 120.0,
                "total_assets": 5000.0,
            },
        ]
    )


def test_compare_returns_no_disagreements_when_close() -> None:
    frame = _two_source_frame(1000.0, 1010.0)  # 1% diff < 5% tolerance
    report = compare_fundamentals_sources(frame, "X")
    assert report.has_disagreements is False
    assert report.sources == ("screener", "yfinance")


def test_compare_flags_disagreement_above_tolerance() -> None:
    frame = _two_source_frame(1000.0, 1200.0)  # 20% diff
    report = compare_fundamentals_sources(frame, "X", tolerance=0.05)
    assert report.has_disagreements is True
    revenue_disagreements = [d for d in report.disagreements if d.field == "revenue"]
    assert len(revenue_disagreements) == 1
    d = revenue_disagreements[0]
    assert d.fiscal_year == 2025
    assert d.values == {"yfinance": 1000.0, "screener": 1200.0}
    assert d.relative_diff > 0.05


def test_compare_single_source_returns_empty_report() -> None:
    frame = pd.DataFrame([{"fiscal_year": 2025, "source": "yfinance", "revenue": 1000.0}])
    report = compare_fundamentals_sources(frame, "X")
    assert report.sources == ("yfinance",)
    assert report.has_disagreements is False


def test_compare_handles_missing_field_gracefully() -> None:
    """When one source has a value and another is null, that field is skipped."""
    frame = pd.DataFrame(
        [
            {"fiscal_year": 2025, "source": "yfinance", "revenue": 1000.0, "eps": 10.0},
            {"fiscal_year": 2025, "source": "screener", "revenue": 1500.0, "eps": None},
        ]
    )
    report = compare_fundamentals_sources(frame, "X", tolerance=0.05)
    fields_flagged = {d.field for d in report.disagreements}
    assert "revenue" in fields_flagged
    assert "eps" not in fields_flagged


def test_compare_empty_frame_returns_empty_report() -> None:
    report = compare_fundamentals_sources(pd.DataFrame(), "X")
    assert report.sources == ()
    assert report.has_disagreements is False


def test_compare_disagreements_sorted_by_severity() -> None:
    """The most-different field should appear first."""
    frame = pd.DataFrame(
        [
            {
                "fiscal_year": 2025,
                "source": "yfinance",
                "revenue": 1000.0,
                "net_income": 100.0,
            },
            {
                "fiscal_year": 2025,
                "source": "screener",
                "revenue": 1100.0,  # 10% diff
                "net_income": 200.0,  # 100% diff
            },
        ]
    )
    report = compare_fundamentals_sources(frame, "X", tolerance=0.05)
    assert len(report.disagreements) >= 2
    assert report.disagreements[0].field == "net_income"


def test_summary_text_with_no_disagreements() -> None:
    frame = _two_source_frame(1000.0, 1000.0)
    report = compare_fundamentals_sources(frame, "X")
    assert "agree" in report.summary_text().lower()


def test_summary_text_truncates_to_max_items() -> None:
    rows = []
    for year in (2022, 2023, 2024, 2025):
        rows.append({"fiscal_year": year, "source": "yfinance", "revenue": 1000.0})
        rows.append({"fiscal_year": year, "source": "screener", "revenue": 2000.0})
    report = compare_fundamentals_sources(pd.DataFrame(rows), "X", tolerance=0.05)
    assert len(report.disagreements) == 4
    summary = report.summary_text(max_items=2)
    assert "+2 more" in summary


# ---------------------------------------------------------------------------
# data_trust integration
# ---------------------------------------------------------------------------


def test_data_trust_includes_cross_source_row_when_no_report() -> None:
    rows = build_data_trust_rows(
        symbol="X",
        price_frame=pd.DataFrame({"close": [100.0]}, index=pd.to_datetime(["2026-01-01"])),
        price_source="kite",
    )
    areas = [row["area"] for row in rows]
    assert "Fundamentals cross-check" in areas
    cross = next(r for r in rows if r["area"] == "Fundamentals cross-check")
    assert cross["status"] == "PARTIAL"


def test_data_trust_cross_source_ok_when_sources_agree() -> None:
    frame = _two_source_frame(1000.0, 1010.0)
    report = compare_fundamentals_sources(frame, "X")
    rows = build_data_trust_rows(
        symbol="X",
        price_frame=pd.DataFrame({"close": [100.0]}, index=pd.to_datetime(["2026-01-01"])),
        price_source="kite",
        cross_source_report=report,
    )
    cross = next(r for r in rows if r["area"] == "Fundamentals cross-check")
    assert cross["status"] == "OK"
    assert "yfinance" in cross["source"] and "screener" in cross["source"]


def test_data_trust_cross_source_partial_when_disagreement() -> None:
    frame = _two_source_frame(1000.0, 1500.0)
    report = compare_fundamentals_sources(frame, "X")
    rows = build_data_trust_rows(
        symbol="X",
        price_frame=pd.DataFrame({"close": [100.0]}, index=pd.to_datetime(["2026-01-01"])),
        price_source="kite",
        cross_source_report=report,
    )
    cross = next(r for r in rows if r["area"] == "Fundamentals cross-check")
    assert cross["status"] == "PARTIAL"
    assert "revenue" in cross["what_to_check"].lower()
