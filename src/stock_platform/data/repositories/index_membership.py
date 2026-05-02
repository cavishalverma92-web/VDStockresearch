"""Repository helpers for point-in-time index membership history."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from stock_platform.db.models import IndexMembershipHistory, utc_now


@dataclass(frozen=True)
class IndexMembershipSyncSummary:
    """Counts from one official index membership snapshot sync."""

    index_name: str
    effective_date: date
    current_symbols: int
    inserted: int
    updated: int
    closed: int


def sync_index_membership_snapshot(
    session: Session,
    *,
    index_name: str,
    constituents: pd.DataFrame,
    effective_date: date,
    source: str = "nse_index_csv",
    source_url: str | None = None,
) -> IndexMembershipSyncSummary:
    """Upsert current index constituents as open-ended membership periods.

    If a symbol is present in the new snapshot and has no active row, a new
    membership period starts on ``effective_date``. If a previously active
    symbol is missing from the snapshot, its period is closed the day before
    ``effective_date``.
    """
    if constituents is None or constituents.empty:
        raise ValueError("Cannot sync index membership from an empty constituent list.")

    rows = _constituent_rows(constituents, source_url=source_url)
    current_symbols = set(rows)
    active_rows = _active_rows_for_index(session, index_name=index_name, source=source)

    inserted = 0
    updated = 0
    now = utc_now()

    for symbol, payload in rows.items():
        existing = active_rows.get(symbol)
        if existing is None:
            session.add(
                IndexMembershipHistory(
                    index_name=index_name,
                    symbol=symbol,
                    company_name=payload.get("company_name"),
                    industry=payload.get("industry"),
                    isin=payload.get("isin"),
                    from_date=effective_date,
                    to_date=None,
                    active=True,
                    source=source,
                    source_url=payload.get("source_url"),
                    observed_at=now,
                    updated_at=now,
                )
            )
            inserted += 1
            continue

        existing.company_name = payload.get("company_name")
        existing.industry = payload.get("industry")
        existing.isin = payload.get("isin")
        existing.source_url = payload.get("source_url")
        existing.observed_at = now
        existing.updated_at = now
        updated += 1

    close_date = effective_date - timedelta(days=1)
    closed = 0
    for symbol, existing in active_rows.items():
        if symbol in current_symbols:
            continue
        existing.active = False
        existing.to_date = close_date
        existing.updated_at = now
        closed += 1

    session.flush()
    return IndexMembershipSyncSummary(
        index_name=index_name,
        effective_date=effective_date,
        current_symbols=len(current_symbols),
        inserted=inserted,
        updated=updated,
        closed=closed,
    )


def list_index_members_on(
    session: Session,
    *,
    index_name: str,
    on_date: date,
    source: str = "nse_index_csv",
) -> list[str]:
    """Return symbols that belonged to an index on ``on_date``."""
    stmt = (
        select(IndexMembershipHistory.symbol)
        .where(
            IndexMembershipHistory.index_name == index_name,
            IndexMembershipHistory.source == source,
            IndexMembershipHistory.from_date <= on_date,
            or_(
                IndexMembershipHistory.to_date.is_(None),
                IndexMembershipHistory.to_date >= on_date,
            ),
        )
        .order_by(IndexMembershipHistory.symbol)
    )
    return list(session.scalars(stmt).all())


def was_index_member_on(
    session: Session,
    *,
    index_name: str,
    symbol: str,
    on_date: date,
    source: str = "nse_index_csv",
) -> bool:
    """Return True if ``symbol`` belonged to ``index_name`` on ``on_date``."""
    normalized_symbol = _normalize_symbol(symbol)
    stmt = (
        select(IndexMembershipHistory.id)
        .where(
            IndexMembershipHistory.index_name == index_name,
            IndexMembershipHistory.symbol == normalized_symbol,
            IndexMembershipHistory.source == source,
            IndexMembershipHistory.from_date <= on_date,
            or_(
                IndexMembershipHistory.to_date.is_(None),
                IndexMembershipHistory.to_date >= on_date,
            ),
        )
        .limit(1)
    )
    return session.scalar(stmt) is not None


def _active_rows_for_index(
    session: Session,
    *,
    index_name: str,
    source: str,
) -> dict[str, IndexMembershipHistory]:
    stmt = select(IndexMembershipHistory).where(
        and_(
            IndexMembershipHistory.index_name == index_name,
            IndexMembershipHistory.source == source,
            IndexMembershipHistory.active.is_(True),
            IndexMembershipHistory.to_date.is_(None),
        )
    )
    return {row.symbol: row for row in session.scalars(stmt).all()}


def _constituent_rows(
    constituents: pd.DataFrame,
    *,
    source_url: str | None,
) -> dict[str, dict[str, str | None]]:
    frame = constituents.copy()
    if "yfinance_symbol" not in frame.columns and "Symbol" not in frame.columns:
        raise KeyError("Constituent data must include 'yfinance_symbol' or 'Symbol'.")

    rows: dict[str, dict[str, str | None]] = {}
    for _, row in frame.iterrows():
        raw_symbol = row.get("yfinance_symbol", row.get("Symbol"))
        symbol = _normalize_symbol(raw_symbol)
        if not symbol:
            continue
        rows[symbol] = {
            "company_name": _optional_string(row.get("Company Name", row.get("company_name"))),
            "industry": _optional_string(row.get("Industry", row.get("industry"))),
            "isin": _optional_string(row.get("ISIN Code", row.get("isin"))),
            "source_url": _optional_string(row.get("source_url")) or source_url,
        }
    if not rows:
        raise ValueError("Constituent data did not contain any usable symbols.")
    return rows


def _normalize_symbol(value: object) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        return ""
    return symbol if "." in symbol else f"{symbol}.NS"


def _optional_string(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None
