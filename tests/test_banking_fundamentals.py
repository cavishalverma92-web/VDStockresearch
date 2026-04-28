"""Tests for manual banking fundamentals provider and validator."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from stock_platform.data.providers import CsvBankingFundamentalsProvider
from stock_platform.data.providers.banking_fundamentals import BANKING_FUNDAMENTAL_COLUMNS
from stock_platform.data.validators import validate_banking_fundamentals


def test_banking_provider_filters_symbol_and_sorts_years(tmp_path: Path) -> None:
    path = tmp_path / "banking.csv"
    pd.DataFrame(
        [
            {
                "symbol": "HDFCBANK.NS",
                "fiscal_year": 2025,
                "nim_pct": 3.4,
                "gnpa_pct": 1.2,
                "nnpa_pct": 0.3,
                "casa_pct": 38.0,
                "credit_growth_pct": 12.0,
                "deposit_growth_pct": 10.0,
                "capital_adequacy_pct": 18.5,
                "source": "annual_report",
                "source_url": "https://example.com/hdfc-annual-report",
                "last_updated": "2026-04-28",
            },
            {
                "symbol": "HDFCBANK.NS",
                "fiscal_year": 2024,
                "nim_pct": 3.3,
                "gnpa_pct": 1.3,
                "nnpa_pct": 0.4,
                "casa_pct": 39.0,
                "credit_growth_pct": 11.0,
                "deposit_growth_pct": 9.0,
                "capital_adequacy_pct": 18.0,
                "source": "annual_report",
                "source_url": "https://example.com/hdfc-annual-report",
                "last_updated": "2026-04-28",
            },
            {
                "symbol": "ICICIBANK.NS",
                "fiscal_year": 2025,
                "nim_pct": 4.0,
            },
        ]
    ).to_csv(path, index=False)

    provider = CsvBankingFundamentalsProvider(path)
    frame = provider.get_banking_fundamentals("hdfcbank.ns")

    assert list(frame["fiscal_year"]) == [2024, 2025]
    assert set(frame["symbol"]) == {"HDFCBANK.NS"}


def test_banking_provider_returns_typed_snapshots(tmp_path: Path) -> None:
    path = tmp_path / "banking.csv"
    pd.DataFrame(
        [
            {
                "symbol": "HDFCBANK.NS",
                "fiscal_year": 2025,
                "nim_pct": 3.4,
                "gnpa_pct": 1.2,
                "nnpa_pct": 0.3,
                "casa_pct": 38.0,
                "credit_growth_pct": 12.0,
                "deposit_growth_pct": 10.0,
                "capital_adequacy_pct": 18.5,
                "source": "annual_report",
                "source_url": "https://example.com/hdfc-annual-report",
                "last_updated": "2026-04-28",
            }
        ]
    ).to_csv(path, index=False)

    snapshots = CsvBankingFundamentalsProvider(path).get_snapshots("HDFCBANK.NS")

    assert len(snapshots) == 1
    assert snapshots[0].symbol == "HDFCBANK.NS"
    assert snapshots[0].nim_pct == 3.4
    assert snapshots[0].capital_adequacy_pct == 18.5


def test_banking_validator_passes_valid_rows(tmp_path: Path) -> None:
    path = tmp_path / "banking.csv"
    pd.DataFrame(
        [
            {
                "symbol": "HDFCBANK.NS",
                "fiscal_year": 2025,
                "nim_pct": 3.4,
                "gnpa_pct": 1.2,
                "nnpa_pct": 0.3,
                "casa_pct": 38.0,
                "credit_growth_pct": 12.0,
                "deposit_growth_pct": 10.0,
                "capital_adequacy_pct": 18.5,
                "source": "annual_report",
                "source_url": "https://example.com/hdfc-annual-report",
                "last_updated": "2026-04-28",
            }
        ]
    ).to_csv(path, index=False)

    frame = CsvBankingFundamentalsProvider(path).get_banking_fundamentals("HDFCBANK.NS")
    report = validate_banking_fundamentals(frame, "HDFCBANK.NS", raise_on_error=False)

    assert report.ok
    assert report.errors == []


def test_banking_validator_flags_out_of_range_percentages() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol": "HDFCBANK.NS",
                "fiscal_year": 2025,
                "nim_pct": 101.0,
                "gnpa_pct": 1.2,
                "nnpa_pct": 0.3,
                "casa_pct": 38.0,
                "credit_growth_pct": 12.0,
                "deposit_growth_pct": 10.0,
                "capital_adequacy_pct": 18.5,
                "source": "annual_report",
                "source_url": "https://example.com/hdfc-annual-report",
                "last_updated": "2026-04-28",
            }
        ],
        columns=BANKING_FUNDAMENTAL_COLUMNS,
    )

    report = validate_banking_fundamentals(frame, "HDFCBANK.NS", raise_on_error=False)

    assert "percentage_out_of_range_nim_pct" in report.errors


def test_banking_validator_empty_frame_is_warning_not_error() -> None:
    frame = pd.DataFrame(columns=BANKING_FUNDAMENTAL_COLUMNS)

    report = validate_banking_fundamentals(frame, "HDFCBANK.NS", raise_on_error=False)

    assert report.ok
    assert report.warnings == ["banking_fundamentals_empty"]
