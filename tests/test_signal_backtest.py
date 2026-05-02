"""Tests for the signal event backtest runner."""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from stock_platform.analytics.backtest.signal_backtest import (
    BacktestSummary,
    TradeResult,
    _compute_summaries,
    compute_portfolio_metrics,
    portfolio_metrics_to_frame,
    run_signal_backtest,
    run_walk_forward_validation,
    summaries_to_frame,
    trades_to_frame,
)
from stock_platform.data.repositories.index_membership import sync_index_membership_snapshot
from stock_platform.db.models import Base

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_events(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _make_price_frame(
    dates: pd.DatetimeIndex,
    closes: list[float],
    highs: list[float] | None = None,
    lows: list[float] | None = None,
) -> pd.DataFrame:
    n = len(dates)
    return pd.DataFrame(
        {
            "open": closes,
            "high": highs or [c * 1.02 for c in closes],
            "low": lows or [c * 0.98 for c in closes],
            "close": closes,
            "adj_close": closes,
            "volume": [1_000_000] * n,
        },
        index=dates,
    )


# ---------------------------------------------------------------------------
# Trivial / boundary cases
# ---------------------------------------------------------------------------


def test_empty_events_returns_empty_lists():
    trades, summaries = run_signal_backtest(pd.DataFrame())
    assert trades == []
    assert summaries == []


def test_none_events_returns_empty_lists():
    trades, summaries = run_signal_backtest(None)  # type: ignore[arg-type]
    assert trades == []
    assert summaries == []


# ---------------------------------------------------------------------------
# Winning trade
# ---------------------------------------------------------------------------


def test_single_winning_trade_return():
    entry_date = date(2024, 3, 1)
    entry_price = 100.0

    dates = pd.date_range(start="2024-03-01", periods=25, freq="B")
    closes = [100.0] + [112.0] * 24

    mock_provider = MagicMock()
    mock_provider.get_ohlcv.return_value = _make_price_frame(dates, closes)

    events = _make_events(
        [
            {
                "symbol": "WIN.NS",
                "signal": "200 EMA Pullback",
                "event_date": entry_date,
                "active": True,
                "close": entry_price,
            }
        ]
    )

    trades, summaries = run_signal_backtest(events, mock_provider, holding_days=20)

    assert len(trades) == 1
    assert trades[0].return_pct == pytest.approx(12.0, abs=0.1)
    assert trades[0].exit_price == pytest.approx(112.0, abs=0.1)
    assert len(summaries) == 1
    assert summaries[0].win_count == 1
    assert summaries[0].loss_count == 0
    assert summaries[0].win_rate_pct == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Losing trade
# ---------------------------------------------------------------------------


def test_single_losing_trade_return():
    entry_date = date(2024, 3, 1)
    entry_price = 100.0

    dates = pd.date_range(start="2024-03-01", periods=25, freq="B")
    closes = [100.0] + [90.0] * 24

    mock_provider = MagicMock()
    mock_provider.get_ohlcv.return_value = _make_price_frame(dates, closes)

    events = _make_events(
        [
            {
                "symbol": "LOSE.NS",
                "signal": "MA Stack",
                "event_date": entry_date,
                "active": True,
                "close": entry_price,
            }
        ]
    )

    trades, summaries = run_signal_backtest(events, mock_provider, holding_days=20)

    assert trades[0].return_pct == pytest.approx(-10.0, abs=0.1)
    assert summaries[0].win_count == 0
    assert summaries[0].loss_count == 1
    assert summaries[0].win_rate_pct == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# No future price data
# ---------------------------------------------------------------------------


def test_no_future_bars_gives_none_return():
    entry_date = date.today() - timedelta(days=1)

    mock_provider = MagicMock()
    mock_provider.get_ohlcv.return_value = pd.DataFrame()

    events = _make_events(
        [
            {
                "symbol": "NODATA.NS",
                "signal": "RSI 60 Momentum",
                "event_date": entry_date,
                "active": True,
                "close": 200.0,
            }
        ]
    )

    trades, _ = run_signal_backtest(events, mock_provider, holding_days=20)

    assert len(trades) == 1
    assert trades[0].return_pct is None
    assert trades[0].exit_price is None


def test_signal_backtest_can_filter_by_point_in_time_index_membership():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    events = _make_events(
        [
            {
                "symbol": "RELIANCE.NS",
                "signal": "MA Stack",
                "event_date": date(2026, 5, 3),
                "active": True,
                "close": 100.0,
            },
            {
                "symbol": "OLD.NS",
                "signal": "MA Stack",
                "event_date": date(2026, 5, 3),
                "active": True,
                "close": 100.0,
            },
        ]
    )
    dates = pd.date_range(start="2026-05-03", periods=25, freq="B")
    mock_provider = MagicMock()
    mock_provider.get_ohlcv.return_value = _make_price_frame(dates, [100.0] + [105.0] * 24)

    with Session(engine) as session:
        sync_index_membership_snapshot(
            session,
            index_name="Nifty 50",
            constituents=pd.DataFrame({"Symbol": ["RELIANCE"], "Series": ["EQ"]}),
            effective_date=date(2026, 5, 3),
        )
        session.commit()

        trades, summaries = run_signal_backtest(
            events,
            mock_provider,
            holding_days=20,
            membership_session=session,
            index_name="Nifty 50",
        )

    assert [trade.symbol for trade in trades] == ["RELIANCE.NS"]
    assert summaries[0].total_trades == 1


# ---------------------------------------------------------------------------
# Missing entry price
# ---------------------------------------------------------------------------


def test_missing_entry_price_gives_none_return():
    mock_provider = MagicMock()
    mock_provider.get_ohlcv.return_value = pd.DataFrame()

    events = _make_events(
        [
            {
                "symbol": "NOPRICE.NS",
                "signal": "MA Stack",
                "event_date": date(2024, 1, 5),
                "active": True,
                "close": None,
            }
        ]
    )

    trades, _ = run_signal_backtest(events, mock_provider, holding_days=20)
    assert trades[0].return_pct is None


# ---------------------------------------------------------------------------
# MFE / MAE
# ---------------------------------------------------------------------------


def test_mfe_mae_computed_correctly():
    entry_date = date(2024, 4, 1)
    entry_price = 100.0

    dates = pd.date_range(start="2024-04-01", periods=25, freq="B")
    closes = [100.0] + [105.0] * 24
    highs = [100.0] + [115.0] * 24  # max favorable: 15%
    lows = [100.0] + [95.0] * 24  # max adverse: -5%

    mock_provider = MagicMock()
    mock_provider.get_ohlcv.return_value = _make_price_frame(dates, closes, highs, lows)

    events = _make_events(
        [
            {
                "symbol": "MFE.NS",
                "signal": "Breakout With Volume",
                "event_date": entry_date,
                "active": True,
                "close": entry_price,
            }
        ]
    )

    trades, _ = run_signal_backtest(events, mock_provider, holding_days=20)
    assert trades[0].mfe_pct == pytest.approx(15.0, abs=0.1)
    assert trades[0].mae_pct == pytest.approx(-5.0, abs=0.1)


# ---------------------------------------------------------------------------
# Profit factor
# ---------------------------------------------------------------------------


def test_profit_factor_computed():
    # 3 wins of 10%, 2 losses of 5%
    wins = [
        TradeResult("A", "sig", date(2024, 1, i), 100.0, None, None, 10.0, 12.0, -2.0, 20)
        for i in range(1, 4)
    ]
    losses = [
        TradeResult("A", "sig", date(2024, 2, i), 100.0, None, None, -5.0, 2.0, -6.0, 20)
        for i in range(1, 3)
    ]
    summaries = _compute_summaries(wins + losses)
    assert len(summaries) == 1
    s = summaries[0]
    assert s.win_count == 3
    assert s.loss_count == 2
    # profit_factor = (3 * 10) / (2 * 5) = 3.0
    assert s.profit_factor == pytest.approx(3.0, abs=0.01)


# ---------------------------------------------------------------------------
# Frame conversion helpers
# ---------------------------------------------------------------------------


def test_trades_to_frame_has_expected_columns():
    t = TradeResult(
        symbol="TEST.NS",
        signal="MA Stack",
        entry_date=date(2024, 1, 10),
        entry_price=500.0,
        exit_date=date(2024, 2, 7),
        exit_price=530.0,
        return_pct=6.0,
        mfe_pct=8.0,
        mae_pct=-2.0,
        holding_days=20,
    )
    frame = trades_to_frame([t])
    assert list(frame.columns) == [
        "symbol",
        "signal",
        "entry_date",
        "entry_price",
        "exit_date",
        "exit_price",
        "return_pct",
        "mfe_pct",
        "mae_pct",
        "holding_days",
    ]
    assert frame["return_pct"][0] == pytest.approx(6.0)


def test_summaries_to_frame_has_expected_columns():
    s = BacktestSummary(
        signal="RSI 60 Momentum",
        total_trades=5,
        win_count=3,
        loss_count=2,
        win_rate_pct=60.0,
        avg_return_pct=3.5,
        avg_win_pct=8.0,
        avg_loss_pct=-3.0,
        profit_factor=2.0,
        best_trade_pct=15.0,
        worst_trade_pct=-5.0,
    )
    frame = summaries_to_frame([s])
    assert "win_rate_pct" in frame.columns
    assert frame["win_rate_pct"][0] == pytest.approx(60.0)


def test_empty_trades_returns_empty_frame():
    frame = trades_to_frame([])
    assert frame.empty


def test_empty_summaries_returns_empty_frame():
    frame = summaries_to_frame([])
    assert frame.empty


def test_portfolio_metrics_compute_core_risk_stats():
    trades = [
        TradeResult("A.NS", "sig", date(2020, 1, 1), 100, date(2020, 1, 21), 110, 10, 12, -2, 20),
        TradeResult("B.NS", "sig", date(2020, 2, 1), 100, date(2020, 2, 21), 95, -5, 4, -7, 20),
        TradeResult("A.NS", "sig", date(2020, 3, 1), 100, None, None, None, None, None, 20),
    ]

    metrics = compute_portfolio_metrics(trades)
    frame = portfolio_metrics_to_frame(metrics)

    assert metrics.total_signals == 3
    assert metrics.completed_trades == 2
    assert metrics.pending_trades == 1
    assert metrics.absolute_return_pct == pytest.approx(4.5, abs=0.1)
    assert metrics.win_rate_pct == pytest.approx(50)
    assert metrics.max_drawdown_pct is not None
    assert metrics.turnover == 2
    assert metrics.unique_symbols == 2
    assert "Max drawdown %" in set(frame["metric"])


def test_walk_forward_validation_compares_train_and_validation_windows():
    trades = [
        TradeResult("A.NS", "sig", date(2020, 1, 1), 100, date(2020, 1, 21), 110, 10, 12, -2, 20),
        TradeResult("A.NS", "sig", date(2021, 1, 1), 100, date(2021, 1, 21), 108, 8, 10, -2, 20),
        TradeResult("A.NS", "sig", date(2022, 1, 1), 100, date(2022, 1, 21), 106, 6, 9, -2, 20),
        TradeResult("A.NS", "sig", date(2023, 2, 1), 100, date(2023, 2, 21), 102, 2, 5, -3, 20),
        TradeResult("B.NS", "other", date(2023, 3, 1), 100, date(2023, 3, 21), 90, -10, 2, -12, 20),
    ]

    frame = run_walk_forward_validation(trades, train_years=3, validate_years=1)

    assert not frame.empty
    sig_row = frame[frame["signal"] == "sig"].iloc[0]
    assert sig_row["train_trades"] == 3
    assert sig_row["validate_trades"] == 1
    assert sig_row["performance_drift_pct"] < 0
