from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from stock_platform.auth import save_kite_access_token
from stock_platform.data.repositories.composite_scores import upsert_composite_score
from stock_platform.data.repositories.corporate_actions import upsert_corporate_actions
from stock_platform.data.repositories.price_daily import upsert_price_daily
from stock_platform.data.repositories.refresh_runs import complete_refresh_run, start_refresh_run
from stock_platform.db.models import Base
from stock_platform.ops import market_today
from stock_platform.ops.market_today import build_market_today_summary
from stock_platform.scoring import CompositeScore


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def settings_stub(monkeypatch, tmp_path):
    from stock_platform.auth import kite_token_store
    from stock_platform.ops import data_health

    stub = SimpleNamespace(
        kite_api_key="",
        kite_api_secret="",
        kite_access_token="",
    )
    monkeypatch.setattr(kite_token_store, "get_settings", lambda: stub)
    monkeypatch.setattr(data_health, "get_settings", lambda: stub)
    monkeypatch.setattr(market_today, "get_settings", lambda: stub)
    monkeypatch.setattr(
        data_health, "kite_token_path", lambda: tmp_path / "secure" / "kite_token.json"
    )
    monkeypatch.setattr(
        market_today, "kite_token_path", lambda: tmp_path / "secure" / "kite_token.json"
    )
    return stub, tmp_path


def _seed_prices(engine, symbol: str, closes: list[float]) -> None:
    dates = pd.date_range("2026-04-28", periods=len(closes), freq="D")
    frame = pd.DataFrame(
        {
            "open": closes,
            "high": [value + 1 for value in closes],
            "low": [value - 1 for value in closes],
            "close": closes,
            "volume": [1000.0] * len(closes),
        },
        index=dates,
    )
    with Session(engine) as session:
        upsert_price_daily(session, symbol, frame, source="kite")
        session.commit()


def _score(symbol: str, score: float, *, risk: str = "Review risk") -> CompositeScore:
    return CompositeScore(
        symbol=symbol,
        score=score,
        band="watch",
        sub_scores={"fundamentals": 40.0, "technicals": score, "flows": 0.0},
        reasons=["Constructive setup"],
        risks=[risk],
        missing_data=[],
    )


def _seed_score(engine, symbol: str, as_of: date, score: float) -> None:
    with Session(engine) as session:
        upsert_composite_score(
            session,
            symbol=symbol,
            as_of_date=as_of,
            composite=_score(symbol, score, risk=f"{symbol} risk"),
            source="kite",
        )
        session.commit()


def test_market_today_summarizes_breadth_score_movers_and_events(engine, settings_stub) -> None:
    _seed_prices(engine, "UP.NS", [100, 105])
    _seed_prices(engine, "DOWN.NS", [100, 95])
    _seed_prices(engine, "FLAT.NS", [100, 100])

    _seed_score(engine, "UP.NS", date(2026, 4, 29), 55.0)
    _seed_score(engine, "UP.NS", date(2026, 4, 30), 72.0)
    _seed_score(engine, "DOWN.NS", date(2026, 4, 29), 75.0)
    _seed_score(engine, "DOWN.NS", date(2026, 4, 30), 60.0)

    with Session(engine) as session:
        run_id = start_refresh_run(
            session, universe_name="nifty_50", requested_symbols=3, source="kite"
        )
        complete_refresh_run(
            session,
            run_id,
            successful_symbols=3,
            failed_symbols=0,
            price_rows_upserted=6,
            technical_rows_upserted=6,
            status="completed",
            duration_seconds=2.0,
            finished_at=datetime(2026, 4, 30, 12, 0, tzinfo=UTC),
        )
        upsert_corporate_actions(
            session,
            "UP.NS",
            pd.DataFrame([{"ex_date": "2026-05-04", "value": 1.0, "detail": "Result date"}]),
            action_type="earnings",
            source="unit",
            detail_column="detail",
        )
        session.commit()

    summary = build_market_today_summary(
        engine=engine,
        today=date(2026, 5, 1),
        now=datetime(2026, 5, 1, 10, 0, tzinfo=UTC),
    )

    assert summary.provider_health.label == "Healthy"
    assert summary.breadth.advances == 1
    assert summary.breadth.declines == 1
    assert summary.breadth.unchanged == 1
    assert set(summary.score_movers["symbol"]) == {"UP.NS", "DOWN.NS"}
    assert list(summary.top_attention["symbol"]) == ["UP.NS", "DOWN.NS"]
    assert list(summary.upcoming_events["symbol"]) == ["UP.NS"]


def test_market_today_provider_health_partial_when_refresh_has_failures(
    engine, settings_stub
) -> None:
    with Session(engine) as session:
        run_id = start_refresh_run(
            session, universe_name="nifty_50", requested_symbols=3, source="kite"
        )
        complete_refresh_run(
            session,
            run_id,
            successful_symbols=2,
            failed_symbols=1,
            price_rows_upserted=4,
            technical_rows_upserted=4,
            status="completed_with_errors",
            duration_seconds=2.0,
            finished_at=datetime(2026, 4, 30, 12, 0, tzinfo=UTC),
        )
        session.commit()

    summary = build_market_today_summary(engine=engine, today=date(2026, 5, 1))

    assert summary.provider_health.label == "Partial"
    assert summary.provider_health.color == "amber"


def test_kite_token_countdown_uses_secure_store_saved_at(engine, settings_stub) -> None:
    _, tmp_path = settings_stub
    save_kite_access_token("safe-test-token", data_dir=tmp_path)

    summary = build_market_today_summary(
        engine=engine,
        today=date(2026, 5, 1),
        now=datetime(2026, 5, 1, 20, 0, tzinfo=UTC),
    )

    assert summary.kite_token.configured is True
    assert summary.kite_token.source == "store"
    assert summary.kite_token.expires_at is not None
    assert summary.kite_token.status in {"ok", "warning", "expired"}


def test_kite_token_countdown_handles_env_without_saved_time(engine, settings_stub) -> None:
    stub, _ = settings_stub
    stub.kite_access_token = "env-token"

    summary = build_market_today_summary(
        engine=engine,
        today=date(2026, 5, 1),
        now=datetime(2026, 5, 1, 20, 0, tzinfo=UTC),
    )

    assert summary.kite_token.configured is True
    assert summary.kite_token.source == "env"
    assert summary.kite_token.status == "unknown"
