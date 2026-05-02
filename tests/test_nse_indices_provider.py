from __future__ import annotations

import pytest

from stock_platform.data.providers.nse_indices import (
    IndexCsvSource,
    NseIndexProviderError,
    parse_index_constituents_csv,
)


def test_parse_index_constituents_csv_normalizes_eq_symbols():
    source = IndexCsvSource(
        universe_name="nifty_50",
        display_name="Nifty 50",
        url="https://example.test/nifty50.csv",
    )
    csv_text = (
        "Company Name,Industry,Symbol,Series,ISIN Code\n"
        "Reliance Industries Ltd.,Oil Gas,RELIANCE,EQ,INE002A01018\n"
        "Test Debt,Debt,TEST,N1,INE000000000\n"
        "HDFC Bank Ltd.,Financial Services,HDFCBANK,EQ,INE040A01034\n"
    )

    frame = parse_index_constituents_csv(csv_text, source=source)

    assert frame["yfinance_symbol"].tolist() == ["RELIANCE.NS", "HDFCBANK.NS"]
    assert frame["source"].unique().tolist() == ["nse_index_csv"]
    assert frame["universe_name"].unique().tolist() == ["nifty_50"]


def test_parse_index_constituents_csv_requires_expected_columns():
    source = IndexCsvSource(
        universe_name="nifty_50",
        display_name="Nifty 50",
        url="https://example.test/nifty50.csv",
    )

    with pytest.raises(NseIndexProviderError, match="missing columns"):
        parse_index_constituents_csv("Symbol,Series\nRELIANCE,EQ\n", source=source)
