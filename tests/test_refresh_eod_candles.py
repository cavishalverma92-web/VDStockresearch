"""Tests for the EOD refresh job."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from stock_platform.db.models import (
    Base,
    CompositeScoreSnapshot,
    CorporateAction,
    DailyRefreshRun,
    PriceDaily,
    TechnicalSnapshot,
)
from stock_platform.jobs.refresh_eod_candles import refresh_eod_candles


def _no_splits(_symbol: str) -> pd.DataFrame:
    """Default splits fetcher for tests that do not exercise corp-action sync."""
    return pd.DataFrame(columns=["ex_date", "ratio"])


class _FakeRouter:
    """Duck-typed stand-in for ``MarketDataProvider``.

    Returns a deterministic frame so indicator computation is reproducible.
    """

    provider_name = "kite"

    def __init__(
        self,
        *,
        source: str = "kite",
        fail_for: set[str] | None = None,
        empty_for: set[str] | None = None,
    ) -> None:
        self.source = source
        self.fail_for = fail_for or set()
        self.empty_for = empty_for or set()
        self.calls: list[tuple[str, date, date]] = []

    def get_ohlcv(self, symbol: str, start: date, end: date, interval: str = "1d") -> pd.DataFrame:
        self.calls.append((symbol, start, end))
        if symbol in self.fail_for:
            raise RuntimeError(f"fake failure for {symbol}")
        if symbol in self.empty_for:
            empty = pd.DataFrame(columns=["open", "high", "low", "close", "adj_close", "volume"])
            empty.attrs["source"] = self.source
            return empty
        return self._candles(symbol, start, end)

    def _candles(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        idx = pd.bdate_range(start=start, end=end)
        if len(idx) == 0:
            empty = pd.DataFrame(columns=["open", "high", "low", "close", "adj_close", "volume"])
            empty.attrs["source"] = self.source
            return empty
        n = len(idx)
        prices = [100.0 + i * 0.5 for i in range(n)]
        frame = pd.DataFrame(
            {
                "open": prices,
                "high": [p + 1.0 for p in prices],
                "low": [p - 1.0 for p in prices],
                "close": prices,
                "adj_close": prices,
                "volume": [1000.0 + i * 5 for i in range(n)],
                "source": [self.source] * n,
                "symbol": [symbol] * n,
            },
            index=idx,
        )
        frame.index.name = "date"
        frame.attrs["source"] = self.source
        frame.attrs["provider_label"] = self.source
        return frame


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


def test_refresh_inserts_price_and_technicals_on_first_run(engine) -> None:
    router = _FakeRouter()

    summary = refresh_eod_candles(
        universe=["RELIANCE.NS", "INFY.NS"],
        market_data_provider=router,
        engine=engine,
        end_date=date(2026, 5, 1),
        initial_history_days=120,
        splits_fetcher=_no_splits,
    )

    assert summary.successful_symbols == 2
    assert summary.failed_symbols == 0
    assert summary.run_id is not None
    assert summary.price_rows_upserted > 0
    assert summary.technical_rows_upserted > 0
    assert summary.dry_run is False

    with Session(engine) as session:
        prices = session.query(PriceDaily).all()
        assert len(prices) > 0
        assert {p.source for p in prices} == {"kite"}
        snapshots = session.query(TechnicalSnapshot).all()
        assert len(snapshots) > 0
        runs = session.query(DailyRefreshRun).all()
        assert len(runs) == 1
        assert runs[0].status == "completed"
        assert runs[0].successful_symbols == 2


def test_refresh_dry_run_does_not_write(engine) -> None:
    router = _FakeRouter()

    summary = refresh_eod_candles(
        universe=["RELIANCE.NS"],
        market_data_provider=router,
        engine=engine,
        end_date=date(2026, 5, 1),
        initial_history_days=60,
        dry_run=True,
        splits_fetcher=_no_splits,
    )

    assert summary.dry_run is True
    assert summary.run_id is None
    assert summary.outcomes[0].skipped_reason == "dry-run"

    with Session(engine) as session:
        assert session.query(PriceDaily).count() == 0
        assert session.query(TechnicalSnapshot).count() == 0
        assert session.query(DailyRefreshRun).count() == 0


def test_refresh_isolates_per_symbol_failures(engine) -> None:
    router = _FakeRouter(fail_for={"BAD.NS"})

    summary = refresh_eod_candles(
        universe=["RELIANCE.NS", "BAD.NS", "INFY.NS"],
        market_data_provider=router,
        engine=engine,
        end_date=date(2026, 5, 1),
        initial_history_days=120,
        splits_fetcher=_no_splits,
    )

    assert summary.successful_symbols == 2
    assert summary.failed_symbols == 1
    assert any(o.error and "BAD.NS" in o.symbol for o in summary.outcomes)

    with Session(engine) as session:
        run = session.query(DailyRefreshRun).one()
        assert run.status == "completed_with_errors"
        symbols_with_prices = {
            row.symbol for row in session.query(PriceDaily.symbol).distinct().all()
        }
        assert symbols_with_prices == {"RELIANCE.NS", "INFY.NS"}


def test_refresh_records_no_rows_returned_as_error(engine) -> None:
    router = _FakeRouter(empty_for={"NODATA.NS"})

    summary = refresh_eod_candles(
        universe=["NODATA.NS"],
        market_data_provider=router,
        engine=engine,
        end_date=date(2026, 5, 1),
        initial_history_days=60,
        splits_fetcher=_no_splits,
    )

    assert summary.successful_symbols == 0
    assert summary.failed_symbols == 1
    assert "no rows" in (summary.outcomes[0].error or "")


def test_refresh_uses_overlap_window_on_second_run(engine) -> None:
    first_router = _FakeRouter()
    refresh_eod_candles(
        universe=["RELIANCE.NS"],
        market_data_provider=first_router,
        engine=engine,
        end_date=date(2026, 5, 1),
        initial_history_days=60,
        splits_fetcher=_no_splits,
    )

    second_router = _FakeRouter()
    summary = refresh_eod_candles(
        universe=["RELIANCE.NS"],
        market_data_provider=second_router,
        engine=engine,
        end_date=date(2026, 5, 8),
        initial_history_days=60,
        incremental_overlap_days=3,
        splits_fetcher=_no_splits,
    )

    # The second call should request from (latest_persisted - overlap) to end_date.
    assert len(second_router.calls) == 1
    _, start, end = second_router.calls[0]
    assert end == date(2026, 5, 8)
    # Latest persisted close should be the last business day on/before May 1.
    assert start <= date(2026, 5, 1)
    assert start >= date(2026, 4, 26)
    # Window is much smaller than initial backfill — proves we did not refetch 60 days.
    assert (end - start).days <= 14

    # Second run should have produced both inserted (new) and updated (overlapped) rows.
    outcome = summary.outcomes[0]
    assert outcome.error is None
    assert outcome.price_rows_inserted > 0


def test_refresh_skips_when_window_already_covered(engine) -> None:
    router = _FakeRouter()
    refresh_eod_candles(
        universe=["RELIANCE.NS"],
        market_data_provider=router,
        engine=engine,
        end_date=date(2026, 5, 1),
        initial_history_days=60,
        splits_fetcher=_no_splits,
    )

    second_router = _FakeRouter()
    summary = refresh_eod_candles(
        universe=["RELIANCE.NS"],
        market_data_provider=second_router,
        engine=engine,
        end_date=date(2026, 4, 25),  # earlier than persisted latest
        initial_history_days=60,
        incremental_overlap_days=0,
        splits_fetcher=_no_splits,
    )

    assert summary.skipped_symbols == 1
    assert summary.successful_symbols == 0
    assert summary.failed_symbols == 0
    assert summary.outcomes[0].skipped_reason == "already up to date"
    assert second_router.calls == []  # no provider call when already up to date


def test_refresh_max_symbols_caps_universe(engine) -> None:
    router = _FakeRouter()

    summary = refresh_eod_candles(
        universe=["A.NS", "B.NS", "C.NS", "D.NS"],
        market_data_provider=router,
        engine=engine,
        end_date=date(2026, 5, 1),
        initial_history_days=60,
        max_symbols=2,
        splits_fetcher=_no_splits,
    )

    assert summary.requested_symbols == 2
    assert {sym for sym, _, _ in router.calls} == {"A.NS", "B.NS"}


# ---------------------------------------------------------------------------
# Corporate-action integration
# ---------------------------------------------------------------------------


def test_refresh_syncs_splits_and_adjusts_indicators(engine) -> None:
    """A 2:1 split mid-history should:
    - persist a row in ``corporate_actions`` (splits_upserted == 1)
    - leave ``price_daily`` rows as-traded (no in-place division)
    - feed the indicator computation a continuous, split-adjusted close so
      indicators are not corrupted by the discontinuity.
    """
    router = _FakeRouter()
    end = date(2026, 5, 1)
    # Split halfway through the fetched window — exact ex_date depends on the
    # business-day calendar, but anywhere mid-window is fine for this test.
    split_date = date(2026, 3, 16)

    def fake_splits(_symbol: str) -> pd.DataFrame:
        return pd.DataFrame({"ex_date": [split_date], "ratio": [2.0]})

    summary = refresh_eod_candles(
        universe=["RELIANCE.NS"],
        market_data_provider=router,
        engine=engine,
        end_date=end,
        initial_history_days=180,
        splits_fetcher=fake_splits,
    )

    outcome = summary.outcomes[0]
    assert outcome.error is None
    assert outcome.splits_upserted == 1
    assert summary.successful_symbols == 1

    with Session(engine) as session:
        actions = session.query(CorporateAction).all()
        assert len(actions) == 1
        assert actions[0].action_type == "split"
        assert actions[0].value == 2.0

        # price_daily must remain as-traded — the fake router emits a smooth
        # ramp where consecutive closes differ by 0.5; if we had divided
        # pre-split rows in place, we would see a jump there.
        prices = (
            session.query(PriceDaily)
            .filter(PriceDaily.symbol == "RELIANCE.NS")
            .order_by(PriceDaily.trade_date)
            .all()
        )
        diffs = [round(prices[i + 1].close - prices[i].close, 4) for i in range(len(prices) - 1)]
        assert all(diff == 0.5 for diff in diffs), "price_daily should stay as-traded"

        # Indicator close in the snapshot table should reflect the
        # split-adjusted series. The most recent snapshot is post-split,
        # so its close equals the as-traded close on that date.
        snapshots = (
            session.query(TechnicalSnapshot)
            .filter(TechnicalSnapshot.symbol == "RELIANCE.NS")
            .order_by(TechnicalSnapshot.as_of_date.desc())
            .all()
        )
        assert snapshots, "expected at least one indicator snapshot"
        latest = snapshots[0]
        # Latest bar is on/after split → adjustment factor is 1.
        latest_price_row = (
            session.query(PriceDaily)
            .filter(
                PriceDaily.symbol == "RELIANCE.NS",
                PriceDaily.trade_date == latest.as_of_date,
            )
            .one()
        )
        assert round(latest.close or 0.0, 4) == round(latest_price_row.close, 4)


def test_refresh_skips_split_sync_when_fetcher_is_none(engine) -> None:
    router = _FakeRouter()
    summary = refresh_eod_candles(
        universe=["RELIANCE.NS"],
        market_data_provider=router,
        engine=engine,
        end_date=date(2026, 5, 1),
        initial_history_days=60,
        splits_fetcher=None,
    )

    assert summary.successful_symbols == 1
    assert summary.outcomes[0].splits_upserted == 0
    with Session(engine) as session:
        assert session.query(CorporateAction).count() == 0


def test_refresh_tolerates_splits_fetcher_failure(engine) -> None:
    router = _FakeRouter()

    def boom(_symbol: str) -> pd.DataFrame:
        raise RuntimeError("yfinance corp-action endpoint down")

    summary = refresh_eod_candles(
        universe=["RELIANCE.NS"],
        market_data_provider=router,
        engine=engine,
        end_date=date(2026, 5, 1),
        initial_history_days=60,
        splits_fetcher=boom,
    )

    # The split sync failure must not poison the run — prices and indicators
    # should still land.
    assert summary.successful_symbols == 1
    assert summary.outcomes[0].splits_upserted == 0
    with Session(engine) as session:
        assert session.query(PriceDaily).count() > 0
        assert session.query(TechnicalSnapshot).count() > 0


# ---------------------------------------------------------------------------
# Composite score persistence
# ---------------------------------------------------------------------------


def test_refresh_persists_composite_score_for_latest_bar(engine) -> None:
    router = _FakeRouter()
    end = date(2026, 5, 1)

    summary = refresh_eod_candles(
        universe=["RELIANCE.NS"],
        market_data_provider=router,
        engine=engine,
        end_date=end,
        initial_history_days=180,
        splits_fetcher=_no_splits,
    )

    outcome = summary.outcomes[0]
    assert outcome.error is None
    assert outcome.composite_score_saved is True
    assert outcome.composite_score is not None
    assert 0.0 <= outcome.composite_score <= 100.0
    assert summary.composite_scores_saved == 1

    with Session(engine) as session:
        rows = (
            session.query(CompositeScoreSnapshot)
            .filter(CompositeScoreSnapshot.symbol == "RELIANCE.NS")
            .all()
        )
        assert len(rows) == 1
        row = rows[0]
        assert row.source == "kite"
        assert row.score == outcome.composite_score
        # Score is built with fundamentals=None, so the missing list must
        # surface that fact for the Data Trust panel.
        assert "fundamentals" in row.missing_data_json


def test_refresh_upserts_composite_score_when_run_repeats(engine) -> None:
    router = _FakeRouter()
    refresh_eod_candles(
        universe=["RELIANCE.NS"],
        market_data_provider=router,
        engine=engine,
        end_date=date(2026, 5, 1),
        initial_history_days=180,
        splits_fetcher=_no_splits,
    )
    refresh_eod_candles(
        universe=["RELIANCE.NS"],
        market_data_provider=_FakeRouter(),
        engine=engine,
        end_date=date(2026, 5, 1),
        initial_history_days=180,
        splits_fetcher=_no_splits,
    )

    with Session(engine) as session:
        rows = session.query(CompositeScoreSnapshot).all()
    # Same (symbol, as_of_date, source) → upsert, not duplicate.
    assert len(rows) == 1


def test_refresh_skips_composite_score_when_history_too_short(engine) -> None:
    """Short history (< 30 bars) cannot produce stable indicators; score must skip."""
    router = _FakeRouter()
    summary = refresh_eod_candles(
        universe=["TINY.NS"],
        market_data_provider=router,
        engine=engine,
        end_date=date(2026, 1, 15),
        initial_history_days=10,
        splits_fetcher=_no_splits,
    )

    outcome = summary.outcomes[0]
    assert outcome.error is None
    assert outcome.composite_score_saved is False
    assert outcome.composite_score is None
    assert summary.composite_scores_saved == 0

    with Session(engine) as session:
        assert session.query(CompositeScoreSnapshot).count() == 0


def test_refresh_dry_run_does_not_persist_composite_score(engine) -> None:
    router = _FakeRouter()
    summary = refresh_eod_candles(
        universe=["RELIANCE.NS"],
        market_data_provider=router,
        engine=engine,
        end_date=date(2026, 5, 1),
        initial_history_days=180,
        dry_run=True,
        splits_fetcher=_no_splits,
    )

    assert summary.composite_scores_saved == 0
    assert summary.outcomes[0].composite_score_saved is False
    with Session(engine) as session:
        assert session.query(CompositeScoreSnapshot).count() == 0
