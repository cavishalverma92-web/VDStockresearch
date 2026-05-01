"""Tests for the persisted Data Health report."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from stock_platform.data.repositories.composite_scores import upsert_composite_score
from stock_platform.data.repositories.instruments import upsert_instruments
from stock_platform.data.repositories.price_daily import upsert_price_daily
from stock_platform.data.repositories.refresh_runs import (
    complete_refresh_run,
    start_refresh_run,
)
from stock_platform.db.models import Base
from stock_platform.ops import build_data_health_report
from stock_platform.scoring import CompositeScore


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def settings_stub(monkeypatch):
    """Isolate token-presence checks from the developer's real .env."""
    from stock_platform.auth import kite_token_store
    from stock_platform.ops import data_health

    stub = SimpleNamespace(
        kite_api_key="",
        kite_api_secret="",
        kite_access_token="",
    )
    monkeypatch.setattr(kite_token_store, "get_settings", lambda: stub)
    monkeypatch.setattr(data_health, "get_settings", lambda: stub)
    return stub


def _composite(symbol: str, score: float = 70.0) -> CompositeScore:
    return CompositeScore(
        symbol=symbol,
        score=score,
        band="watch",
        sub_scores={"fundamentals": 0.0, "technicals": score, "flows": 0.0},
        reasons=[],
        risks=[],
        missing_data=["fundamentals"],
    )


def _seed_prices(
    engine,
    symbol: str,
    *,
    dates: list[str],
    source: str = "kite",
    base_close: float = 100.0,
) -> None:
    frame = pd.DataFrame(
        {
            "open": [base_close + i for i in range(len(dates))],
            "high": [base_close + i + 1 for i in range(len(dates))],
            "low": [base_close + i - 1 for i in range(len(dates))],
            "close": [base_close + i for i in range(len(dates))],
            "volume": [1000.0 + i * 10 for i in range(len(dates))],
        },
        index=pd.to_datetime(dates),
    )
    with Session(engine) as session:
        upsert_price_daily(session, symbol, frame, source=source)
        session.commit()


def test_report_on_empty_database(engine, settings_stub) -> None:
    report = build_data_health_report(engine=engine, today=date(2026, 5, 1))

    assert report.kite_token.configured is False
    assert report.kite_token.source == "missing"
    assert report.recent_refresh_runs == []
    assert report.price_coverage is not None
    assert report.price_coverage.distinct_symbols == 0
    assert report.price_coverage.total_rows == 0
    assert report.composite_score_coverage.distinct_symbols == 0
    assert report.instrument_coverage.total == 0
    assert report.stale_symbols == []


def test_token_source_reflects_env_when_no_store(engine, settings_stub) -> None:
    settings_stub.kite_access_token = "from-env"
    settings_stub.kite_api_key = "key"
    settings_stub.kite_api_secret = "secret"

    report = build_data_health_report(engine=engine, today=date(2026, 5, 1))

    assert report.kite_token.configured is True
    assert report.kite_token.source == "env"
    assert report.kite_token.api_key_configured is True
    assert report.kite_token.api_secret_configured is True


def test_token_source_reflects_store_when_present(
    engine, settings_stub, tmp_path, monkeypatch
) -> None:
    from stock_platform.auth import kite_token_store
    from stock_platform.ops import data_health

    monkeypatch.setattr(data_health, "kite_token_path", lambda: tmp_path / "kite_token.json")
    kite_token_store.save_kite_access_token("xyz", data_dir=tmp_path)
    # save_kite_access_token writes under tmp_path/secure/kite_token.json — point at it
    monkeypatch.setattr(
        data_health, "kite_token_path", lambda: tmp_path / "secure" / "kite_token.json"
    )

    settings_stub.kite_access_token = "also-in-env"

    report = build_data_health_report(engine=engine, today=date(2026, 5, 1))

    # Store wins over env.
    assert report.kite_token.source == "store"
    assert report.kite_token.store_path is not None


def test_price_coverage_aggregates_by_source(engine, settings_stub) -> None:
    _seed_prices(
        engine,
        "RELIANCE.NS",
        dates=["2026-04-28", "2026-04-29", "2026-04-30"],
        source="kite",
    )
    _seed_prices(engine, "INFY.NS", dates=["2026-04-30"], source="yfinance")

    report = build_data_health_report(engine=engine, today=date(2026, 5, 1))
    coverage = report.price_coverage

    assert coverage is not None
    assert coverage.distinct_symbols == 2
    assert coverage.total_rows == 4
    assert coverage.oldest_trade_date == date(2026, 4, 28)
    assert coverage.newest_trade_date == date(2026, 4, 30)
    assert coverage.by_source == {"kite": 3, "yfinance": 1}


def test_stale_symbols_flagged_beyond_threshold(engine, settings_stub) -> None:
    _seed_prices(engine, "FRESH.NS", dates=["2026-04-30"])
    _seed_prices(engine, "OLD.NS", dates=["2026-04-15"])
    _seed_prices(engine, "ANCIENT.NS", dates=["2025-06-01"])

    report = build_data_health_report(
        engine=engine,
        today=date(2026, 5, 1),
        stale_threshold_days=5,
    )

    stale_symbols = {row.symbol for row in report.stale_symbols}
    assert "FRESH.NS" not in stale_symbols
    assert {"OLD.NS", "ANCIENT.NS"}.issubset(stale_symbols)
    by_symbol = {row.symbol: row.days_stale for row in report.stale_symbols}
    assert by_symbol["OLD.NS"] == 16
    assert by_symbol["ANCIENT.NS"] >= 300


def test_composite_score_coverage_counts_recent_rows(engine, settings_stub) -> None:
    today = date(2026, 5, 1)
    with Session(engine) as session:
        upsert_composite_score(
            session,
            symbol="RELIANCE.NS",
            as_of_date=date(2026, 4, 30),
            composite=_composite("RELIANCE.NS", 75.0),
            source="kite",
        )
        upsert_composite_score(
            session,
            symbol="RELIANCE.NS",
            as_of_date=date(2026, 4, 1),
            composite=_composite("RELIANCE.NS", 60.0),
            source="kite",
        )
        upsert_composite_score(
            session,
            symbol="INFY.NS",
            as_of_date=date(2026, 4, 28),
            composite=_composite("INFY.NS", 65.0),
            source="kite",
        )
        session.commit()

    report = build_data_health_report(engine=engine, today=today)
    coverage = report.composite_score_coverage

    assert coverage is not None
    assert coverage.distinct_symbols == 2
    assert coverage.total_rows == 3
    assert coverage.latest_as_of_date == date(2026, 4, 30)
    # Last 7 days (Apr 24..May 1): RELIANCE Apr 30 + INFY Apr 28 = 2.
    assert coverage.rows_last_7_days == 2


def test_instrument_coverage_aggregates_by_exchange(engine, settings_stub) -> None:
    nse_rows = pd.DataFrame(
        [
            {"instrument_token": 1, "tradingsymbol": "RELIANCE", "exchange": "NSE"},
            {"instrument_token": 2, "tradingsymbol": "INFY", "exchange": "NSE"},
        ]
    )
    bse_rows = pd.DataFrame(
        [{"instrument_token": 3, "tradingsymbol": "TATAMOTORS", "exchange": "BSE"}]
    )
    with Session(engine) as session:
        upsert_instruments(session, nse_rows)
        upsert_instruments(session, bse_rows)
        session.commit()

    report = build_data_health_report(engine=engine, today=date(2026, 5, 1))
    coverage = report.instrument_coverage

    assert coverage.total == 3
    assert coverage.by_exchange == {"NSE": 2, "BSE": 1}


def test_recent_refresh_runs_returns_newest_first(engine, settings_stub) -> None:
    with Session(engine) as session:
        run_a = start_refresh_run(
            session, universe_name="nifty_50", requested_symbols=10, source="kite"
        )
        complete_refresh_run(
            session,
            run_a,
            successful_symbols=10,
            failed_symbols=0,
            price_rows_upserted=2500,
            technical_rows_upserted=2500,
            status="completed",
            duration_seconds=12.0,
            finished_at=datetime(2026, 4, 30, 14, 0, tzinfo=UTC),
        )
        run_b = start_refresh_run(
            session, universe_name="nifty_50", requested_symbols=10, source="kite"
        )
        complete_refresh_run(
            session,
            run_b,
            successful_symbols=8,
            failed_symbols=2,
            price_rows_upserted=2000,
            technical_rows_upserted=2000,
            status="completed_with_errors",
            duration_seconds=14.0,
            finished_at=datetime(2026, 5, 1, 14, 0, tzinfo=UTC),
        )
        session.commit()

    report = build_data_health_report(engine=engine, today=date(2026, 5, 1))

    assert [run.run_id for run in report.recent_refresh_runs] == [run_b, run_a]
    latest = report.recent_refresh_runs[0]
    assert latest.status == "completed_with_errors"
    assert latest.failed_symbols == 2
    # age_seconds should be a non-negative number when finished_at is set.
    assert latest.age_seconds is not None
    assert latest.age_seconds >= 0.0


def test_recent_refresh_runs_age_handles_naive_finished_at(engine, settings_stub) -> None:
    with Session(engine) as session:
        run_id = start_refresh_run(
            session, universe_name="nifty_50", requested_symbols=1, source="kite"
        )
        complete_refresh_run(
            session,
            run_id,
            successful_symbols=1,
            failed_symbols=0,
            price_rows_upserted=10,
            technical_rows_upserted=10,
            status="completed",
            duration_seconds=1.0,
            finished_at=datetime.now() - timedelta(hours=1),  # naive datetime
        )
        session.commit()

    report = build_data_health_report(engine=engine, today=date(2026, 5, 1))

    latest = report.recent_refresh_runs[0]
    # Naive datetimes are interpreted as UTC; the resulting age depends on the
    # local timezone offset. We only assert it is a non-negative float — the
    # important behaviour is that the helper does not crash on naive datetimes.
    assert latest.age_seconds is not None
    assert latest.age_seconds >= 0.0
