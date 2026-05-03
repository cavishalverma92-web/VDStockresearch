"""Data quality checks for quarterly fundamentals."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from stock_platform.utils.logging import get_dq_logger

log = get_dq_logger(__name__)


REQUIRED_QUARTERLY_COLUMNS = (
    "symbol",
    "fiscal_year",
    "fiscal_quarter",
    "source",
)


class QuarterlyFundamentalsValidationError(Exception):
    """Raised when quarterly fundamentals fail critical checks."""


@dataclass
class QuarterlyFundamentalsValidationReport:
    symbol: str
    rows: int
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def validate_quarterly_fundamentals(
    frame: pd.DataFrame,
    symbol: str,
    *,
    raise_on_error: bool = True,
) -> QuarterlyFundamentalsValidationReport:
    """Validate quarterly rows for one symbol."""
    report = QuarterlyFundamentalsValidationReport(symbol=symbol, rows=len(frame))

    if frame.empty:
        report.warnings.append("quarterly_empty")
        return _finalize(report, raise_on_error)

    missing = set(REQUIRED_QUARTERLY_COLUMNS) - set(frame.columns)
    if missing:
        report.errors.append(f"missing_columns: {sorted(missing)}")
        return _finalize(report, raise_on_error)

    if frame["fiscal_year"].isna().any() or frame["fiscal_quarter"].isna().any():
        report.errors.append("missing_fiscal_period")

    quarters = pd.to_numeric(frame["fiscal_quarter"], errors="coerce")
    if ((quarters < 1) | (quarters > 4)).any():
        report.errors.append("fiscal_quarter_out_of_range")

    period_pairs = list(zip(frame["fiscal_year"], frame["fiscal_quarter"], strict=False))
    if len(period_pairs) != len(set(period_pairs)):
        report.errors.append("duplicate_period")

    for col in ("revenue", "net_income"):
        if col in frame.columns and frame[col].isna().all():
            report.warnings.append(f"all_null_{col}")

    return _finalize(report, raise_on_error)


def _finalize(
    report: QuarterlyFundamentalsValidationReport,
    raise_on_error: bool,
) -> QuarterlyFundamentalsValidationReport:
    for warning in report.warnings:
        log.warning("Quarterly DQ warning [{}]: {}", report.symbol, warning)
    for error in report.errors:
        log.error("Quarterly DQ error   [{}]: {}", report.symbol, error)
    if report.errors and raise_on_error:
        raise QuarterlyFundamentalsValidationError(
            f"Quarterly fundamentals validation failed for {report.symbol}: {report.errors}"
        )
    return report
