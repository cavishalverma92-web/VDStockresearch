"""Cross-source fundamentals comparison.

When more than one provider has populated the fundamentals tables for a
symbol (e.g. yfinance + screener), this module compares them on key fields
and surfaces disagreements above a configurable tolerance. Disagreements
feed the data-trust panel so users see a "two sources differ on revenue
for FY2025" warning instead of silently picking one.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

import pandas as pd

# Fields worth comparing — restricted to "headline" lines that should agree
# across sources within a tight tolerance. Balance-sheet items vary more
# (consolidated vs standalone, restatements) so they're excluded by default.
DEFAULT_COMPARED_FIELDS = (
    "revenue",
    "net_income",
    "ebitda",
    "eps",
    "operating_cash_flow",
    "total_assets",
)


@dataclass(frozen=True)
class FieldDisagreement:
    """One field on one fiscal period where sources differ above tolerance."""

    fiscal_year: int
    field: str
    values: dict[str, float]  # source → value
    relative_diff: float  # max diff / median, as a fraction


@dataclass(frozen=True)
class CrossSourceReport:
    """Aggregate cross-source comparison for one symbol."""

    symbol: str
    sources: tuple[str, ...]
    fields_compared: tuple[str, ...]
    disagreements: list[FieldDisagreement] = field(default_factory=list)

    @property
    def has_disagreements(self) -> bool:
        return bool(self.disagreements)

    def summary_text(self, max_items: int = 3) -> str:
        if not self.has_disagreements:
            return "Sources agree on compared fields within tolerance."
        parts: list[str] = []
        for d in self.disagreements[:max_items]:
            parts.append(
                f"{d.field} FY{d.fiscal_year} differs by "
                f"{d.relative_diff * 100:.1f}% across sources"
            )
        if len(self.disagreements) > max_items:
            parts.append(f"+{len(self.disagreements) - max_items} more")
        return "; ".join(parts)


def compare_fundamentals_sources(
    frame: pd.DataFrame,
    symbol: str,
    *,
    tolerance: float = 0.05,
    fields: Iterable[str] | None = None,
) -> CrossSourceReport:
    """Compare per-source rows for the same fiscal year.

    Expects a frame with at least ``fiscal_year`` and ``source`` columns plus
    one column per metric to compare. Returns a CrossSourceReport — empty
    disagreements list when the frame has fewer than two sources for any year
    or all values agree within tolerance.

    The ``tolerance`` is a fraction (0.05 = 5%). For each (year, field) where
    multiple sources report a non-null value, we compute
    ``(max - min) / median`` and flag when that exceeds the tolerance.
    """
    fields_to_check = tuple(fields or DEFAULT_COMPARED_FIELDS)
    cleaned_symbol = str(symbol or "").strip().upper()

    if (
        frame is None
        or frame.empty
        or "fiscal_year" not in frame.columns
        or "source" not in frame.columns
    ):
        return CrossSourceReport(
            symbol=cleaned_symbol,
            sources=(),
            fields_compared=fields_to_check,
        )

    sources = tuple(sorted({str(s) for s in frame["source"].dropna().unique()}))
    if len(sources) < 2:
        return CrossSourceReport(
            symbol=cleaned_symbol,
            sources=sources,
            fields_compared=fields_to_check,
        )

    disagreements: list[FieldDisagreement] = []
    for fiscal_year, group in frame.groupby("fiscal_year"):
        if group["source"].nunique() < 2:
            continue
        try:
            year = int(fiscal_year)
        except (TypeError, ValueError):
            continue

        for col in fields_to_check:
            if col not in group.columns:
                continue
            per_source: dict[str, float] = {}
            for _, row in group.iterrows():
                value = row.get(col)
                if value is None or pd.isna(value):
                    continue
                try:
                    per_source[str(row["source"])] = float(value)
                except (TypeError, ValueError):
                    continue
            if len(per_source) < 2:
                continue

            values = list(per_source.values())
            spread = max(values) - min(values)
            median = sorted(values)[len(values) // 2]
            if median == 0:
                # Use mean of absolute values as a fallback denominator
                denom = sum(abs(v) for v in values) / len(values)
                if denom == 0:
                    continue
            else:
                denom = abs(median)
            relative = spread / denom
            if relative > tolerance:
                disagreements.append(
                    FieldDisagreement(
                        fiscal_year=year,
                        field=col,
                        values=per_source,
                        relative_diff=relative,
                    )
                )

    disagreements.sort(key=lambda d: (-d.relative_diff, d.fiscal_year, d.field))
    return CrossSourceReport(
        symbol=cleaned_symbol,
        sources=sources,
        fields_compared=fields_to_check,
        disagreements=disagreements,
    )
