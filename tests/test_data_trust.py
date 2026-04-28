from __future__ import annotations

import pandas as pd

from stock_platform.ops import (
    build_data_trust_rows,
    data_trust_level,
    data_trust_rows_to_frame,
)


def test_data_trust_rows_flag_missing_banking_metrics() -> None:
    price_frame = pd.DataFrame(
        {"close": [100.0, 101.0]},
        index=pd.to_datetime(["2026-04-27", "2026-04-28"]),
    )
    fundamentals_frame = pd.DataFrame({"fiscal_year": [2025], "source": ["yfinance"]})

    rows = build_data_trust_rows(
        symbol="HDFCBANK.NS",
        price_frame=price_frame,
        price_source="yfinance",
        fundamentals_frame=fundamentals_frame,
        fundamentals_source="yfinance (live)",
        banking_frame=pd.DataFrame(),
        composite_missing=["manual banking metrics"],
        active_signal_count=2,
    )

    banking_row = next(row for row in rows if row["area"] == "Banking metrics")
    assert banking_row["status"] == "ACTION"
    assert "audited nim" in banking_row["what_to_check"].lower()


def test_data_trust_level_medium_for_single_partial() -> None:
    rows = [
        {"status": "OK"},
        {"status": "PARTIAL"},
        {"status": "OK"},
    ]

    level, reason = data_trust_level(rows)

    assert level == "Medium"
    assert "partial" in reason.lower() or "provisional" in reason.lower()


def test_data_trust_frame_has_stable_columns_for_empty_state() -> None:
    frame = data_trust_rows_to_frame([])

    assert list(frame.columns) == [
        "area",
        "status",
        "source",
        "freshness",
        "coverage",
        "what_to_check",
        "symbol",
    ]
