"""Data quality checks for manual banking fundamentals."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from stock_platform.data.providers.banking_fundamentals import BANKING_FUNDAMENTAL_COLUMNS
from stock_platform.utils.logging import get_dq_logger

log = get_dq_logger(__name__)


class BankingFundamentalsValidationError(Exception):
    """Raised when bank-specific fundamentals fail critical checks."""


@dataclass
class BankingFundamentalsValidationReport:
    symbol: str
    rows: int
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def validate_banking_fundamentals(
    frame: pd.DataFrame,
    symbol: str,
    *,
    raise_on_error: bool = True,
) -> BankingFundamentalsValidationReport:
    """Validate manual bank metrics for one symbol."""
    report = BankingFundamentalsValidationReport(symbol=symbol, rows=len(frame))

    if frame.empty:
        report.warnings.append("banking_fundamentals_empty")
        return _finalize(report, raise_on_error)

    missing_columns = set(BANKING_FUNDAMENTAL_COLUMNS) - set(frame.columns)
    if missing_columns:
        report.errors.append(f"missing_columns: {sorted(missing_columns)}")
        return _finalize(report, raise_on_error)

    if frame["fiscal_year"].isna().any():
        report.errors.append("missing_fiscal_year")
    if frame["fiscal_year"].duplicated().any():
        report.errors.append("duplicate_fiscal_year")

    percent_columns = [
        "nim_pct",
        "gnpa_pct",
        "nnpa_pct",
        "casa_pct",
        "capital_adequacy_pct",
    ]
    growth_columns = ["credit_growth_pct", "deposit_growth_pct"]
    for column in [*percent_columns, *growth_columns]:
        values = pd.to_numeric(frame[column], errors="coerce")
        non_empty = frame[column].notna() & (frame[column].astype(str).str.strip() != "")
        if values[non_empty].isna().any():
            report.errors.append(f"non_numeric_{column}")

    for column in percent_columns:
        values = pd.to_numeric(frame[column], errors="coerce").dropna()
        if ((values < 0) | (values > 100)).any():
            report.errors.append(f"percentage_out_of_range_{column}")

    for column in growth_columns:
        values = pd.to_numeric(frame[column], errors="coerce").dropna()
        if ((values < -100) | (values > 200)).any():
            report.warnings.append(f"growth_outlier_{column}")

    missing_metric_columns = [
        column
        for column in [
            "nim_pct",
            "gnpa_pct",
            "nnpa_pct",
            "casa_pct",
            "credit_growth_pct",
            "deposit_growth_pct",
            "capital_adequacy_pct",
        ]
        if frame[column].isna().any()
    ]
    if missing_metric_columns:
        report.warnings.append(f"missing_banking_metrics: {missing_metric_columns}")

    if frame["source"].fillna("").astype(str).str.strip().eq("").any():
        report.warnings.append("missing_source")
    if frame["source_url"].fillna("").astype(str).str.strip().eq("").any():
        report.warnings.append("missing_source_url")
    if pd.to_datetime(frame["last_updated"], errors="coerce").isna().any():
        report.warnings.append("invalid_or_missing_last_updated")

    return _finalize(report, raise_on_error)


def _finalize(
    report: BankingFundamentalsValidationReport,
    raise_on_error: bool,
) -> BankingFundamentalsValidationReport:
    for warning in report.warnings:
        log.warning("Banking fundamentals DQ warning [{}]: {}", report.symbol, warning)
    for error in report.errors:
        log.error("Banking fundamentals DQ error   [{}]: {}", report.symbol, error)

    if report.errors and raise_on_error:
        raise BankingFundamentalsValidationError(
            f"Banking fundamentals validation failed for {report.symbol}: {report.errors}"
        )
    return report
