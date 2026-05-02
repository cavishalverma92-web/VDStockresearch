from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from stock_platform.data.repositories.index_membership import (
    list_index_members_on,
    sync_index_membership_snapshot,
    was_index_member_on,
)
from stock_platform.db.models import Base, IndexMembershipHistory


def _constituents(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Company Name": [symbol.replace(".NS", " Ltd.") for symbol in symbols],
            "Industry": ["Test Industry"] * len(symbols),
            "Symbol": [symbol.replace(".NS", "") for symbol in symbols],
            "Series": ["EQ"] * len(symbols),
            "ISIN Code": [f"INE{i:09d}" for i, _ in enumerate(symbols)],
            "source_url": ["https://example.test/index.csv"] * len(symbols),
        }
    )


def test_sync_index_membership_snapshot_inserts_open_periods():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        summary = sync_index_membership_snapshot(
            session,
            index_name="Nifty 50",
            constituents=_constituents(["RELIANCE.NS", "HDFCBANK.NS"]),
            effective_date=date(2026, 5, 3),
        )
        session.commit()

    with Session(engine) as session:
        rows = session.query(IndexMembershipHistory).all()
        assert summary.inserted == 2
        assert summary.updated == 0
        assert summary.closed == 0
        assert len(rows) == 2
        assert all(row.active for row in rows)
        assert was_index_member_on(
            session,
            index_name="Nifty 50",
            symbol="RELIANCE.NS",
            on_date=date(2026, 5, 3),
        )


def test_sync_index_membership_snapshot_closes_removed_symbols():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        sync_index_membership_snapshot(
            session,
            index_name="Nifty 50",
            constituents=_constituents(["RELIANCE.NS", "HDFCBANK.NS"]),
            effective_date=date(2026, 5, 1),
        )
        sync_index_membership_snapshot(
            session,
            index_name="Nifty 50",
            constituents=_constituents(["RELIANCE.NS", "TMPV.NS"]),
            effective_date=date(2026, 5, 3),
        )
        session.commit()

    with Session(engine) as session:
        members = list_index_members_on(
            session,
            index_name="Nifty 50",
            on_date=date(2026, 5, 3),
        )
        assert members == ["RELIANCE.NS", "TMPV.NS"]
        assert was_index_member_on(
            session,
            index_name="Nifty 50",
            symbol="HDFCBANK.NS",
            on_date=date(2026, 5, 2),
        )
        assert not was_index_member_on(
            session,
            index_name="Nifty 50",
            symbol="HDFCBANK.NS",
            on_date=date(2026, 5, 3),
        )
        closed = (
            session.query(IndexMembershipHistory)
            .filter(IndexMembershipHistory.symbol == "HDFCBANK.NS")
            .one()
        )
        assert closed.active is False
        assert closed.to_date == date(2026, 5, 2)
