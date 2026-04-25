import pandas as pd

from stock_platform.ops import build_provenance_rows, provenance_rows_to_frame


def test_build_provenance_rows_separates_sources_from_derived_outputs():
    price_frame = pd.DataFrame(
        {"close": [100.0]},
        index=pd.to_datetime(["2026-04-24"]),
    )

    rows = build_provenance_rows(
        symbol="reliance.ns",
        price_provider="yfinance",
        fundamentals_provider="local_csv",
        price_frame=price_frame,
        fundamentals_source="sample_placeholder",
        delivery_available=True,
        deals_available=None,
    )

    by_area = {row["area"]: row for row in rows}
    assert by_area["Price / OHLCV"]["freshness"] == "2026-04-24"
    assert by_area["Fundamentals"]["status"] == "sample"
    assert by_area["Delivery %"]["status"] == "active"
    assert by_area["Bulk / block deals"]["status"] == "manual_check"
    assert by_area["Signals / score / backtest"]["status"] == "derived"


def test_provenance_rows_to_frame_has_stable_columns_for_empty_state():
    frame = provenance_rows_to_frame([])

    assert list(frame.columns) == [
        "area",
        "source",
        "symbol",
        "freshness",
        "status",
        "caveat",
        "generated_at_utc",
    ]
    assert frame.empty
