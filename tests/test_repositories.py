"""Tests for the persisted-data repositories layer."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from stock_platform.data.repositories.composite_scores import (
    fetch_composite_scores,
    latest_composite_score,
    upsert_composite_score,
)
from stock_platform.data.repositories.corporate_actions import (
    fetch_corporate_actions,
    upsert_corporate_actions,
)
from stock_platform.data.repositories.instruments import (
    count_instruments,
    find_instrument_token,
    upsert_instruments,
)
from stock_platform.data.repositories.price_daily import (
    fetch_price_daily,
    latest_trade_date,
    upsert_price_daily,
)
from stock_platform.data.repositories.refresh_runs import (
    complete_refresh_run,
    start_refresh_run,
)
from stock_platform.data.repositories.technical_snapshots import upsert_technical_snapshots
from stock_platform.db.models import (
    Base,
    CompositeScoreSnapshot,
    CorporateAction,
    DailyRefreshRun,
    InstrumentMaster,
    PriceDaily,
    TechnicalSnapshot,
)
from stock_platform.scoring import CompositeScore


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


def _price_frame(dates: list[str], *, closes: list[float] | None = None) -> pd.DataFrame:
    n = len(dates)
    closes = closes if closes is not None else [100.5 + i for i in range(n)]
    return pd.DataFrame(
        {
            "open": [c - 0.5 for c in closes],
            "high": [c + 0.5 for c in closes],
            "low": [c - 1.0 for c in closes],
            "close": closes,
            "volume": [1000.0 + i * 10 for i in range(n)],
        },
        index=pd.to_datetime(dates),
    )


# ---------------------------------------------------------------------------
# Price daily
# ---------------------------------------------------------------------------


def test_upsert_price_daily_inserts_new_rows(engine) -> None:
    frame = _price_frame(["2026-01-05", "2026-01-06", "2026-01-07"])
    with Session(engine) as session:
        summary = upsert_price_daily(session, "RELIANCE.NS", frame, source="kite")
        session.commit()
    assert summary.inserted == 3
    assert summary.updated == 0
    assert summary.skipped == 0


def test_upsert_price_daily_updates_existing_dates(engine) -> None:
    initial = _price_frame(["2026-01-05", "2026-01-06"], closes=[100.0, 101.0])
    with Session(engine) as session:
        upsert_price_daily(session, "RELIANCE.NS", initial, source="kite")
        session.commit()

    revised = _price_frame(
        ["2026-01-05", "2026-01-06", "2026-01-07"],
        closes=[100.5, 101.5, 102.5],
    )
    with Session(engine) as session:
        summary = upsert_price_daily(session, "RELIANCE.NS", revised, source="kite")
        session.commit()

    assert summary.inserted == 1
    assert summary.updated == 2

    with Session(engine) as session:
        rows = (
            session.query(PriceDaily)
            .filter(PriceDaily.symbol == "RELIANCE.NS")
            .order_by(PriceDaily.trade_date)
            .all()
        )
        assert [round(row.close, 2) for row in rows] == [100.5, 101.5, 102.5]


def test_upsert_price_daily_skips_rows_with_missing_ohlc(engine) -> None:
    frame = _price_frame(["2026-01-05", "2026-01-06"], closes=[100.0, float("nan")])
    with Session(engine) as session:
        summary = upsert_price_daily(session, "RELIANCE.NS", frame, source="kite")
        session.commit()
    assert summary.inserted == 1
    assert summary.skipped == 1


def test_upsert_price_daily_empty_frame(engine) -> None:
    with Session(engine) as session:
        summary = upsert_price_daily(session, "X.NS", pd.DataFrame(), source="kite")
    assert summary.inserted == 0
    assert summary.updated == 0


def test_latest_trade_date_returns_max(engine) -> None:
    frame = _price_frame(["2026-01-05", "2026-01-08", "2026-01-10"])
    with Session(engine) as session:
        upsert_price_daily(session, "RELIANCE.NS", frame, source="kite")
        session.commit()
    with Session(engine) as session:
        assert latest_trade_date(session, "RELIANCE.NS") == date(2026, 1, 10)
        assert latest_trade_date(session, "RELIANCE.NS", source="kite") == date(2026, 1, 10)
        assert latest_trade_date(session, "MISSING.NS") is None


def test_latest_trade_date_filters_by_source(engine) -> None:
    with Session(engine) as session:
        upsert_price_daily(
            session,
            "RELIANCE.NS",
            _price_frame(["2026-01-05", "2026-01-08"]),
            source="kite",
        )
        upsert_price_daily(
            session,
            "RELIANCE.NS",
            _price_frame(["2026-01-12"]),
            source="yfinance",
        )
        session.commit()
    with Session(engine) as session:
        assert latest_trade_date(session, "RELIANCE.NS", source="kite") == date(2026, 1, 8)
        assert latest_trade_date(session, "RELIANCE.NS", source="yfinance") == date(2026, 1, 12)


def test_fetch_price_daily_returns_indexed_frame(engine) -> None:
    frame = _price_frame(["2026-01-05", "2026-01-06"])
    with Session(engine) as session:
        upsert_price_daily(session, "RELIANCE.NS", frame, source="kite")
        session.commit()
    with Session(engine) as session:
        history = fetch_price_daily(session, "RELIANCE.NS")
    assert len(history) == 2
    assert {"open", "high", "low", "close", "volume", "adj_close"}.issubset(history.columns)
    assert history.index.name == "date"


def test_fetch_price_daily_empty_when_no_rows(engine) -> None:
    with Session(engine) as session:
        history = fetch_price_daily(session, "MISSING.NS")
    assert history.empty
    assert history.index.name == "date"


# ---------------------------------------------------------------------------
# Instruments
# ---------------------------------------------------------------------------


def test_upsert_instruments_inserts_then_updates(engine) -> None:
    frame = pd.DataFrame(
        [
            {
                "instrument_token": 738561,
                "exchange_token": 2885,
                "tradingsymbol": "RELIANCE",
                "name": "Reliance Industries",
                "exchange": "NSE",
                "segment": "NSE",
                "instrument_type": "EQ",
                "tick_size": 0.05,
                "lot_size": 1,
                "expiry": pd.NaT,
                "strike": 0.0,
            }
        ]
    )

    with Session(engine) as session:
        s1 = upsert_instruments(session, frame)
        session.commit()

    assert s1.inserted == 1
    assert s1.updated == 0

    revised = frame.copy()
    revised.loc[0, "name"] = "Reliance Industries Limited"
    with Session(engine) as session:
        s2 = upsert_instruments(session, revised)
        session.commit()

    assert s2.inserted == 0
    assert s2.updated == 1

    with Session(engine) as session:
        row = session.query(InstrumentMaster).one()
        assert row.name == "Reliance Industries Limited"


def test_upsert_instruments_skips_rows_missing_token_or_symbol(engine) -> None:
    frame = pd.DataFrame(
        [
            {"instrument_token": 1, "tradingsymbol": "OK", "exchange": "NSE"},
            {"instrument_token": None, "tradingsymbol": "BAD", "exchange": "NSE"},
            {"instrument_token": 2, "tradingsymbol": "", "exchange": "NSE"},
        ]
    )
    with Session(engine) as session:
        summary = upsert_instruments(session, frame)
        session.commit()
    assert summary.inserted == 1
    assert summary.skipped == 2


def test_find_instrument_token_handles_case_and_missing(engine) -> None:
    frame = pd.DataFrame(
        [
            {
                "instrument_token": 738561,
                "tradingsymbol": "RELIANCE",
                "exchange": "NSE",
                "segment": "NSE",
                "instrument_type": "EQ",
            }
        ]
    )
    with Session(engine) as session:
        upsert_instruments(session, frame)
        session.commit()
    with Session(engine) as session:
        assert find_instrument_token(session, "RELIANCE") == 738561
        assert find_instrument_token(session, "reliance") == 738561
        assert find_instrument_token(session, "MISSING") is None
        assert count_instruments(session) == 1
        assert count_instruments(session, exchange="BSE") == 0


# ---------------------------------------------------------------------------
# Technical snapshots
# ---------------------------------------------------------------------------


def test_upsert_technical_snapshots_inserts_only_rows_with_indicators(engine) -> None:
    idx = pd.date_range("2026-01-01", periods=5, freq="B")
    frame = pd.DataFrame(
        {
            "close": [100.0, 101.0, 102.0, 103.0, 104.0],
            "rsi_14": [None, None, 55.0, 56.0, 57.0],
            "ma_stack_status": ["mixed"] * 5,
        },
        index=idx,
    )

    with Session(engine) as session:
        summary = upsert_technical_snapshots(session, "RELIANCE.NS", frame, source="kite")
        session.commit()

    assert summary.inserted == 3
    assert summary.skipped == 2

    with Session(engine) as session:
        rows = session.query(TechnicalSnapshot).order_by(TechnicalSnapshot.as_of_date).all()
        assert [round(r.rsi_14, 1) for r in rows] == [55.0, 56.0, 57.0]
        assert all(r.ma_stack_status == "mixed" for r in rows)


def test_upsert_technical_snapshots_only_after(engine) -> None:
    idx = pd.date_range("2026-01-01", periods=4, freq="B")
    frame = pd.DataFrame(
        {
            "close": [100.0, 101.0, 102.0, 103.0],
            "rsi_14": [50.0, 51.0, 52.0, 53.0],
            "ma_stack_status": ["mixed"] * 4,
        },
        index=idx,
    )

    with Session(engine) as session:
        summary = upsert_technical_snapshots(
            session,
            "RELIANCE.NS",
            frame,
            source="kite",
            only_after=date(2026, 1, 2),
        )
        session.commit()

    # 2026-01-01 is a Thursday; bdate_range yields Jan 1, 2, 5, 6 — three are >= Jan 2.
    assert summary.inserted == 3


def test_upsert_technical_snapshots_updates_existing(engine) -> None:
    idx = pd.date_range("2026-01-01", periods=2, freq="B")
    frame = pd.DataFrame(
        {"close": [100.0, 101.0], "rsi_14": [50.0, 51.0], "ma_stack_status": ["mixed"] * 2},
        index=idx,
    )
    with Session(engine) as session:
        upsert_technical_snapshots(session, "RELIANCE.NS", frame, source="kite")
        session.commit()

    revised = frame.copy()
    revised["rsi_14"] = [55.0, 56.0]
    with Session(engine) as session:
        summary = upsert_technical_snapshots(session, "RELIANCE.NS", revised, source="kite")
        session.commit()

    assert summary.inserted == 0
    assert summary.updated == 2


# ---------------------------------------------------------------------------
# Corporate actions
# ---------------------------------------------------------------------------


def test_upsert_corporate_actions_inserts_then_updates(engine) -> None:
    frame = pd.DataFrame(
        {
            "ex_date": [date(2024, 8, 12), date(2025, 3, 4)],
            "value": [2.0, 5.0],
        }
    )
    with Session(engine) as session:
        s1 = upsert_corporate_actions(
            session,
            "RELIANCE.NS",
            frame,
            action_type="split",
            source="yfinance",
        )
        session.commit()

    assert s1.inserted == 2
    assert s1.updated == 0

    revised = frame.copy()
    revised.loc[1, "value"] = 4.0
    with Session(engine) as session:
        s2 = upsert_corporate_actions(
            session,
            "RELIANCE.NS",
            revised,
            action_type="split",
            source="yfinance",
        )
        session.commit()

    assert s2.inserted == 0
    assert s2.updated == 2

    with Session(engine) as session:
        rows = session.query(CorporateAction).order_by(CorporateAction.ex_date).all()
        assert [round(row.value, 2) for row in rows] == [2.0, 4.0]


def test_upsert_corporate_actions_skips_invalid_rows(engine) -> None:
    frame = pd.DataFrame(
        {
            "ex_date": [date(2024, 8, 12), None, date(2025, 3, 4)],
            "value": [2.0, 3.0, None],
        }
    )
    with Session(engine) as session:
        summary = upsert_corporate_actions(
            session,
            "RELIANCE.NS",
            frame,
            action_type="split",
            source="yfinance",
        )
        session.commit()

    assert summary.inserted == 1
    assert summary.skipped == 2


def test_fetch_corporate_actions_filters_by_action_type(engine) -> None:
    splits = pd.DataFrame({"ex_date": [date(2024, 8, 12)], "value": [2.0]})
    dividends = pd.DataFrame({"ex_date": [date(2024, 9, 1)], "value": [10.0]})
    with Session(engine) as session:
        upsert_corporate_actions(
            session, "RELIANCE.NS", splits, action_type="split", source="yfinance"
        )
        upsert_corporate_actions(
            session, "RELIANCE.NS", dividends, action_type="dividend", source="yfinance"
        )
        session.commit()
    with Session(engine) as session:
        only_splits = fetch_corporate_actions(session, "RELIANCE.NS", action_type="split")
        all_actions = fetch_corporate_actions(session, "RELIANCE.NS")

    assert len(only_splits) == 1
    assert only_splits.iloc[0]["action_type"] == "split"
    assert len(all_actions) == 2


# ---------------------------------------------------------------------------
# Composite scores
# ---------------------------------------------------------------------------


def _composite(symbol: str, score: float = 72.5) -> CompositeScore:
    return CompositeScore(
        symbol=symbol,
        score=score,
        band="watch",
        sub_scores={
            "fundamentals": 50.0,
            "technicals": 80.0,
            "flows": 0.0,
            "events_quality": 0.0,
            "macro_sector": 0.0,
        },
        reasons=["EMA stack bullish", "RSI 60 momentum"],
        risks=["Fundamentals missing"],
        missing_data=["fundamentals"],
    )


def test_upsert_composite_score_inserts_and_updates(engine) -> None:
    with Session(engine) as session:
        s1 = upsert_composite_score(
            session,
            symbol="RELIANCE.NS",
            as_of_date=date(2026, 4, 30),
            composite=_composite("RELIANCE.NS", 70.0),
            source="kite",
        )
        session.commit()
    assert s1.inserted == 1
    assert s1.updated == 0

    with Session(engine) as session:
        s2 = upsert_composite_score(
            session,
            symbol="RELIANCE.NS",
            as_of_date=date(2026, 4, 30),
            composite=_composite("RELIANCE.NS", 75.0),
            source="kite",
        )
        session.commit()
    assert s2.inserted == 0
    assert s2.updated == 1

    with Session(engine) as session:
        rows = session.query(CompositeScoreSnapshot).all()
        assert len(rows) == 1
        assert rows[0].score == 75.0
        assert rows[0].band == "watch"
        assert rows[0].fundamentals_score == 50.0


def test_upsert_composite_score_separates_sources(engine) -> None:
    with Session(engine) as session:
        upsert_composite_score(
            session,
            symbol="RELIANCE.NS",
            as_of_date=date(2026, 4, 30),
            composite=_composite("RELIANCE.NS", 70.0),
            source="kite",
        )
        upsert_composite_score(
            session,
            symbol="RELIANCE.NS",
            as_of_date=date(2026, 4, 30),
            composite=_composite("RELIANCE.NS", 65.0),
            source="yfinance",
        )
        session.commit()

    with Session(engine) as session:
        rows = session.query(CompositeScoreSnapshot).all()
        assert len(rows) == 2
        assert {row.source for row in rows} == {"kite", "yfinance"}


def test_latest_composite_score_returns_most_recent(engine) -> None:
    with Session(engine) as session:
        upsert_composite_score(
            session,
            symbol="RELIANCE.NS",
            as_of_date=date(2026, 4, 28),
            composite=_composite("RELIANCE.NS", 60.0),
            source="kite",
        )
        upsert_composite_score(
            session,
            symbol="RELIANCE.NS",
            as_of_date=date(2026, 4, 30),
            composite=_composite("RELIANCE.NS", 75.0),
            source="kite",
        )
        session.commit()
    with Session(engine) as session:
        latest = latest_composite_score(session, "RELIANCE.NS")
        assert latest is not None
        assert latest.as_of_date == date(2026, 4, 30)
        assert latest.score == 75.0


def test_fetch_composite_scores_returns_series(engine) -> None:
    with Session(engine) as session:
        for offset, score in enumerate([60.0, 65.0, 72.0]):
            upsert_composite_score(
                session,
                symbol="RELIANCE.NS",
                as_of_date=date(2026, 4, 26 + offset),
                composite=_composite("RELIANCE.NS", score),
                source="kite",
            )
        session.commit()
    with Session(engine) as session:
        frame = fetch_composite_scores(session, "RELIANCE.NS")
    assert len(frame) == 3
    assert list(frame["score"]) == [60.0, 65.0, 72.0]
    assert frame.index.name == "as_of_date"


def test_fetch_composite_scores_empty_for_missing_symbol(engine) -> None:
    with Session(engine) as session:
        frame = fetch_composite_scores(session, "MISSING.NS")
    assert frame.empty
    assert frame.index.name == "as_of_date"


# ---------------------------------------------------------------------------
# Refresh runs
# ---------------------------------------------------------------------------


def test_start_and_complete_refresh_run(engine) -> None:
    with Session(engine) as session:
        run_id = start_refresh_run(
            session,
            universe_name="nifty_50",
            requested_symbols=10,
            source="kite",
            note="smoke test",
        )
        session.commit()

    with Session(engine) as session:
        run = session.get(DailyRefreshRun, run_id)
        assert run is not None
        assert run.status == "started"
        assert run.requested_symbols == 10

    with Session(engine) as session:
        complete_refresh_run(
            session,
            run_id,
            successful_symbols=8,
            failed_symbols=2,
            price_rows_upserted=2400,
            technical_rows_upserted=2400,
            status="completed_with_errors",
            duration_seconds=12.3,
        )
        session.commit()

    with Session(engine) as session:
        run = session.get(DailyRefreshRun, run_id)
        assert run is not None
        assert run.status == "completed_with_errors"
        assert run.successful_symbols == 8
        assert run.failed_symbols == 2
        assert run.duration_seconds == 12.3
        assert run.finished_at is not None
