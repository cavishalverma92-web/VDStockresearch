"""Tests for historical market-cap reconstruction."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine

from stock_platform.analytics.fundamentals.market_cap import (
    reconstruct_historical_market_cap,
)
from stock_platform.data.providers.db_fundamentals import DbFundamentalsProvider
from stock_platform.data.repositories.fundamentals import upsert_fundamentals_annual
from stock_platform.data.repositories.price_daily import upsert_price_daily
from stock_platform.db.models import Base


def _price_history(closes_by_date: dict[str, float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": list(closes_by_date.values()),
            "high": [c + 1 for c in closes_by_date.values()],
            "low": [c - 1 for c in closes_by_date.values()],
            "close": list(closes_by_date.values()),
            "volume": [1000.0] * len(closes_by_date),
        },
        index=pd.to_datetime(list(closes_by_date.keys())),
    )


def test_reconstruct_fills_missing_market_cap_on_period_end() -> None:
    annual = pd.DataFrame(
        {
            "fiscal_year": [2024, 2025],
            "period_end": [date(2024, 3, 31), date(2025, 3, 31)],
            "shares_outstanding": [1_000_000.0, 1_000_000.0],
            "net_debt": [50_000.0, 60_000.0],
            "market_cap": [None, None],
            "enterprise_value": [None, None],
        }
    )
    prices = _price_history(
        {
            "2024-03-29": 100.0,  # Fri (Mar 31 was a Sunday)
            "2025-03-31": 150.0,
        }
    )
    out = reconstruct_historical_market_cap(annual, prices)
    fy24 = out[out["fiscal_year"] == 2024].iloc[0]
    fy25 = out[out["fiscal_year"] == 2025].iloc[0]
    assert fy24["market_cap"] == 100_000_000.0  # 1M shares × 100
    assert fy24["enterprise_value"] == 100_050_000.0
    assert fy25["market_cap"] == 150_000_000.0
    assert fy25["enterprise_value"] == 150_060_000.0


def test_reconstruct_does_not_overwrite_existing_unless_asked() -> None:
    annual = pd.DataFrame(
        {
            "fiscal_year": [2025],
            "period_end": [date(2025, 3, 31)],
            "shares_outstanding": [1_000_000.0],
            "net_debt": [0.0],
            "market_cap": [999.0],  # already set
            "enterprise_value": [999.0],
        }
    )
    prices = _price_history({"2025-03-31": 150.0})
    out = reconstruct_historical_market_cap(annual, prices)
    assert out.iloc[0]["market_cap"] == 999.0  # untouched

    overwritten = reconstruct_historical_market_cap(annual, prices, overwrite=True)
    assert overwritten.iloc[0]["market_cap"] == 150_000_000.0


def test_reconstruct_skips_when_no_period_end() -> None:
    annual = pd.DataFrame(
        {
            "fiscal_year": [2025],
            "period_end": [None],
            "shares_outstanding": [1_000_000.0],
            "market_cap": [None],
        }
    )
    prices = _price_history({"2025-03-31": 150.0})
    out = reconstruct_historical_market_cap(annual, prices)
    assert pd.isna(out.iloc[0]["market_cap"])


def test_reconstruct_skips_when_shares_outstanding_missing() -> None:
    annual = pd.DataFrame(
        {
            "fiscal_year": [2025],
            "period_end": [date(2025, 3, 31)],
            "shares_outstanding": [None],
            "market_cap": [None],
        }
    )
    prices = _price_history({"2025-03-31": 150.0})
    out = reconstruct_historical_market_cap(annual, prices)
    assert pd.isna(out.iloc[0]["market_cap"])


def test_reconstruct_returns_input_when_price_history_empty() -> None:
    annual = pd.DataFrame(
        {
            "fiscal_year": [2025],
            "period_end": [date(2025, 3, 31)],
            "shares_outstanding": [1_000_000.0],
            "market_cap": [None],
        }
    )
    out = reconstruct_historical_market_cap(annual, pd.DataFrame())
    assert pd.isna(out.iloc[0]["market_cap"])


def test_reconstruct_uses_lookback_for_non_trading_days() -> None:
    """When period_end falls on a Sunday, walk back to the prior trading day."""
    annual = pd.DataFrame(
        {
            "fiscal_year": [2025],
            "period_end": [date(2025, 3, 30)],  # Sunday
            "shares_outstanding": [1_000_000.0],
            "market_cap": [None],
        }
    )
    prices = _price_history(
        {
            "2025-03-28": 145.0,  # Friday — closest preceding trading day
            "2025-04-01": 152.0,  # after period_end, must be ignored
        }
    )
    out = reconstruct_historical_market_cap(annual, prices)
    assert out.iloc[0]["market_cap"] == 145_000_000.0


def test_reconstruct_gives_up_beyond_lookback_window() -> None:
    annual = pd.DataFrame(
        {
            "fiscal_year": [2025],
            "period_end": [date(2025, 3, 31)],
            "shares_outstanding": [1_000_000.0],
            "market_cap": [None],
        }
    )
    prices = _price_history(
        {
            "2025-02-28": 100.0,  # >14 days before period_end → outside default window
        }
    )
    out = reconstruct_historical_market_cap(annual, prices, max_lookback_days=14)
    assert pd.isna(out.iloc[0]["market_cap"])


def test_reconstruct_empty_input_returns_empty() -> None:
    out = reconstruct_historical_market_cap(pd.DataFrame(), pd.DataFrame())
    assert out.empty


# ---------------------------------------------------------------------------
# DbFundamentalsProvider integration
# ---------------------------------------------------------------------------


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


def test_db_provider_enriches_market_cap_when_opted_in(engine) -> None:
    from sqlalchemy.orm import Session

    with Session(engine) as session:
        upsert_fundamentals_annual(
            session,
            "RELIANCE.NS",
            pd.DataFrame(
                {
                    "fiscal_year": [2024, 2025],
                    "period_end": [date(2024, 3, 31), date(2025, 3, 31)],
                    "shares_outstanding": [1_000_000.0, 1_000_000.0],
                    "net_debt": [0.0, 0.0],
                }
            ),
            source="yfinance",
        )
        # Need price rows on those exact dates
        prices = _price_history(
            {"2024-03-29": 100.0, "2025-03-31": 150.0}  # Mar 31 2024 is a Sunday
        )
        upsert_price_daily(session, "RELIANCE.NS", prices, source="kite")
        session.commit()

    enriched = DbFundamentalsProvider(engine=engine, enrich_market_cap=True)
    frame = enriched.get_annual_fundamentals("RELIANCE.NS")
    assert frame.loc[frame["fiscal_year"] == 2024, "market_cap"].iloc[0] == 100_000_000.0
    assert frame.loc[frame["fiscal_year"] == 2025, "market_cap"].iloc[0] == 150_000_000.0

    plain = DbFundamentalsProvider(engine=engine, enrich_market_cap=False)
    frame_plain = plain.get_annual_fundamentals("RELIANCE.NS")
    # Without enrichment, market_cap stays whatever the DB has (None here).
    assert frame_plain.loc[frame_plain["fiscal_year"] == 2025, "market_cap"].iloc[0] is None
