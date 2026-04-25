"""Data quality checks for annual fundamentals CSV data."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from stock_platform.utils.logging import get_dq_logger

log = get_dq_logger(__name__)

# Columns that must be present for financial calculations and quality scores.
# Metadata columns (sector, industry, market_cap_bucket) are optional and
# intentionally excluded here — they enrich rankings but do not block validation.
_REQUIRED_COLUMNS = [
    "symbol",
    "fiscal_year",
    "revenue",
    "gross_profit",
    "ebit",
    "net_income",
    "operating_cash_flow",
    "total_assets",
    "total_liabilities",
    "current_assets",
    "current_liabilities",
    "retained_earnings",
    "shares_outstanding",
    "market_cap",
    "source",
]


class FundamentalsValidationError(Exception):
    """Raised when fundamentals data fails critical validation checks."""


@dataclass
class FundamentalsValidationReport:
    symbol: str
    rows: int
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def validate_annual_fundamentals(
    frame: pd.DataFrame,
    symbol: str,
    *,
    raise_on_error: bool = True,
) -> FundamentalsValidationReport:
    """Validate annual fundamentals rows for one symbol."""
    report = FundamentalsValidationReport(symbol=symbol, rows=len(frame))

    if frame.empty:
        report.warnings.append("fundamentals_empty")
        return _finalize(report, raise_on_error)

    missing_columns = set(_REQUIRED_COLUMNS) - set(frame.columns)
    if missing_columns:
        report.errors.append(f"missing_columns: {sorted(missing_columns)}")
        return _finalize(report, raise_on_error)

    if frame["fiscal_year"].isna().any():
        report.errors.append("missing_fiscal_year")
    if frame["fiscal_year"].duplicated().any():
        report.errors.append("duplicate_fiscal_year")

    required_for_scores = {
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
    }
    missing_score_inputs = sorted(
        column
        for column in required_for_scores
        if column not in frame.columns or frame[column].isna().any()
    )
    if missing_score_inputs:
        report.warnings.append(f"missing_score_inputs: {missing_score_inputs}")

    numeric_columns = required_for_scores | {"fiscal_year"}
    for column in numeric_columns:
        if column not in frame.columns:
            continue
        coerced = pd.to_numeric(frame[column], errors="coerce")
        if coerced.isna().any() and not frame[column].isna().any():
            report.errors.append(f"non_numeric_{column}")

    for column in ("revenue", "total_assets", "shares_outstanding"):
        values = pd.to_numeric(frame[column], errors="coerce")
        if (values.dropna() <= 0).any():
            report.errors.append(f"non_positive_{column}")

    source_values = frame["source"].fillna("").astype(str).str.strip()
    if (source_values == "").any():
        report.warnings.append("missing_source")
    if source_values.str.contains("sample", case=False, na=False).any():
        report.warnings.append("sample_data_source")

    return _finalize(report, raise_on_error)


def _finalize(
    report: FundamentalsValidationReport,
    raise_on_error: bool,
) -> FundamentalsValidationReport:
    for warning in report.warnings:
        log.warning("Fundamentals DQ warning [{}]: {}", report.symbol, warning)
    for error in report.errors:
        log.error("Fundamentals DQ error   [{}]: {}", report.symbol, error)

    if report.errors and raise_on_error:
        raise FundamentalsValidationError(
            f"Fundamentals validation failed for {report.symbol}: {report.errors}"
        )
    return report
