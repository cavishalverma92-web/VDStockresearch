"""Manual CSV provider for bank-specific fundamentals.

This keeps banking metrics auditable while source terms and official provider
options are reviewed. Values are percentages unless noted otherwise.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from stock_platform.analytics.fundamentals.schema import BankingFundamentalSnapshot
from stock_platform.config import ROOT_DIR

BANKING_FUNDAMENTAL_COLUMNS = [
    "symbol",
    "fiscal_year",
    "nim_pct",
    "gnpa_pct",
    "nnpa_pct",
    "casa_pct",
    "credit_growth_pct",
    "deposit_growth_pct",
    "capital_adequacy_pct",
    "source",
    "source_url",
    "last_updated",
]


class CsvBankingFundamentalsProvider:
    """Provider for bank metrics stored in a local CSV file."""

    name = "local_csv_banking"

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path or ROOT_DIR / "data/sample/banking_fundamentals_template.csv")

    def get_all_banking_fundamentals(self) -> pd.DataFrame:
        """Return all locally saved bank metrics."""
        if not self.path.exists():
            return pd.DataFrame(columns=BANKING_FUNDAMENTAL_COLUMNS)

        frame = pd.read_csv(self.path)
        if "symbol" not in frame.columns:
            return pd.DataFrame(columns=BANKING_FUNDAMENTAL_COLUMNS)
        return _ensure_columns(frame)

    def get_banking_fundamentals(self, symbol: str) -> pd.DataFrame:
        """Return bank metrics rows for one symbol, sorted by fiscal year."""
        frame = self.get_all_banking_fundamentals()
        if frame.empty:
            return pd.DataFrame(columns=BANKING_FUNDAMENTAL_COLUMNS)

        filtered = frame[frame["symbol"].astype(str).str.upper() == symbol.upper()].copy()
        if filtered.empty:
            return pd.DataFrame(columns=BANKING_FUNDAMENTAL_COLUMNS)

        filtered["fiscal_year"] = pd.to_numeric(filtered["fiscal_year"], errors="coerce")
        filtered = filtered.dropna(subset=["fiscal_year"])
        filtered["fiscal_year"] = filtered["fiscal_year"].astype(int)
        return filtered.sort_values("fiscal_year").reset_index(drop=True)

    def get_snapshots(self, symbol: str) -> list[BankingFundamentalSnapshot]:
        """Return bank metrics as typed snapshots."""
        frame = self.get_banking_fundamentals(symbol)
        return [
            BankingFundamentalSnapshot(
                symbol=str(row["symbol"]),
                fiscal_year=int(row["fiscal_year"]),
                nim_pct=_optional_float(row.get("nim_pct")),
                gnpa_pct=_optional_float(row.get("gnpa_pct")),
                nnpa_pct=_optional_float(row.get("nnpa_pct")),
                casa_pct=_optional_float(row.get("casa_pct")),
                credit_growth_pct=_optional_float(row.get("credit_growth_pct")),
                deposit_growth_pct=_optional_float(row.get("deposit_growth_pct")),
                capital_adequacy_pct=_optional_float(row.get("capital_adequacy_pct")),
                source=_optional_text(row.get("source")),
                source_url=_optional_text(row.get("source_url")),
                last_updated=_optional_text(row.get("last_updated")),
            )
            for row in frame.to_dict(orient="records")
        ]


def _ensure_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    for column in BANKING_FUNDAMENTAL_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    return normalized[BANKING_FUNDAMENTAL_COLUMNS]


def _optional_float(value: object) -> float | None:
    if value is None or pd.isna(value) or value == "":
        return None
    return float(value)


def _optional_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None
