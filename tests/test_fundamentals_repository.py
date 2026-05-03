"""Tests for the fundamentals repository (annual + quarterly upsert/fetch)."""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from stock_platform.data.repositories.fundamentals import (
    fetch_fundamentals_annual,
    latest_fundamentals_period,
    upsert_fundamentals_annual,
    upsert_fundamentals_quarterly,
)
from stock_platform.db.models import Base


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


def _annual_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "fiscal_year": [2024, 2025],
            "revenue": [1000.0, 1200.0],
            "net_income": [100.0, 150.0],
            "total_assets": [5000.0, 5500.0],
            "shares_outstanding": [10.0, 10.0],
        }
    )


def _quarterly_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "fiscal_year": [2025, 2025, 2025, 2025],
            "fiscal_quarter": [1, 2, 3, 4],
            "revenue": [300.0, 310.0, 320.0, 330.0],
            "net_income": [30.0, 32.0, 34.0, 40.0],
        }
    )


def test_upsert_annual_inserts_then_updates(engine) -> None:
    frame = _annual_frame()
    with Session(engine) as session:
        s1 = upsert_fundamentals_annual(session, "RELIANCE", frame, source="yfinance")
        session.commit()
    assert s1.inserted == 2
    assert s1.updated == 0

    revised = frame.copy()
    revised.loc[1, "revenue"] = 1300.0
    with Session(engine) as session:
        s2 = upsert_fundamentals_annual(session, "RELIANCE", revised, source="yfinance")
        session.commit()
    assert s2.inserted == 0
    assert s2.updated == 2

    with Session(engine) as session:
        out = fetch_fundamentals_annual(session, "RELIANCE", source="yfinance")
    assert list(out["fiscal_year"]) == [2024, 2025]
    assert out.loc[out["fiscal_year"] == 2025, "revenue"].iloc[0] == 1300.0


def test_upsert_annual_two_sources_coexist(engine) -> None:
    frame = _annual_frame()
    with Session(engine) as session:
        upsert_fundamentals_annual(session, "RELIANCE", frame, source="yfinance")
        upsert_fundamentals_annual(session, "RELIANCE", frame, source="screener")
        session.commit()
    with Session(engine) as session:
        all_rows = fetch_fundamentals_annual(session, "RELIANCE")
    assert len(all_rows) == 4  # 2 years × 2 sources
    assert set(all_rows["source"]) == {"yfinance", "screener"}


def test_upsert_quarterly_and_latest_period(engine) -> None:
    with Session(engine) as session:
        s = upsert_fundamentals_quarterly(session, "TCS", _quarterly_frame(), source="yfinance")
        session.commit()
    assert s.inserted == 4

    with Session(engine) as session:
        latest = latest_fundamentals_period(session, "TCS", source="yfinance")
    assert latest == (2025, 4)


def test_upsert_quarterly_skips_invalid_quarter(engine) -> None:
    bad = pd.DataFrame(
        {
            "fiscal_year": [2025, 2025],
            "fiscal_quarter": [5, 2],  # 5 is invalid
            "revenue": [100.0, 200.0],
        }
    )
    with Session(engine) as session:
        s = upsert_fundamentals_quarterly(session, "X", bad, source="yfinance")
        session.commit()
    assert s.inserted == 1
    assert s.skipped == 1


def test_fetch_empty_returns_empty_frame(engine) -> None:
    with Session(engine) as session:
        out = fetch_fundamentals_annual(session, "MISSING")
    assert out.empty


def test_upsert_annual_empty_is_noop(engine) -> None:
    with Session(engine) as session:
        s = upsert_fundamentals_annual(session, "X", pd.DataFrame(), source="yfinance")
    assert s.inserted == 0
    assert s.updated == 0
