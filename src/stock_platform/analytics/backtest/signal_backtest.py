"""Simple signal event backtest runner for Phase 2.

Evaluates forward returns for signal observations saved in the signal_audit
table.  For each saved event the runner:

1. Uses the stored close price as the entry price (no look-ahead).
2. Looks at OHLCV bars strictly *after* the signal date.
3. Records exit price at a fixed holding-day horizon.
4. Computes Maximum Favorable Excursion (MFE) and Maximum Adverse Excursion
   (MAE) over the holding window.
5. Aggregates win rate, profit factor, and other stats per signal type.

This is an educational analysis tool.  The sample set is small (only stocks
the user has scanned) so treat the stats as directional, not conclusive.
No forward-looking data is used in the return calculation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from math import prod

import pandas as pd
from sqlalchemy.orm import Session

from stock_platform.data.providers.yahoo import YahooFinanceProvider
from stock_platform.data.repositories import was_index_member_on
from stock_platform.utils.logging import get_backtest_logger

log = get_backtest_logger(__name__)


@dataclass
class TradeResult:
    """Forward-return outcome for one signal observation."""

    symbol: str
    signal: str
    entry_date: date
    entry_price: float | None
    exit_date: date | None
    exit_price: float | None
    return_pct: float | None
    mfe_pct: float | None
    mae_pct: float | None
    holding_days: int


@dataclass
class BacktestSummary:
    """Aggregate statistics for one signal type."""

    signal: str
    total_trades: int
    win_count: int
    loss_count: int
    win_rate_pct: float | None
    avg_return_pct: float | None
    avg_win_pct: float | None
    avg_loss_pct: float | None
    profit_factor: float | None
    best_trade_pct: float | None
    worst_trade_pct: float | None


@dataclass
class PortfolioBacktestMetrics:
    """Portfolio-level diagnostics for completed signal trades."""

    total_signals: int
    completed_trades: int
    pending_trades: int
    absolute_return_pct: float | None
    cagr_pct: float | None
    win_rate_pct: float | None
    avg_return_pct: float | None
    max_drawdown_pct: float | None
    sharpe_ratio: float | None
    sortino_ratio: float | None
    exposure_time_pct: float | None
    turnover: int
    avg_holding_period_days: float | None
    unique_symbols: int
    max_symbol_concentration_pct: float | None


def run_signal_backtest(
    events: pd.DataFrame,
    price_provider: YahooFinanceProvider | None = None,
    holding_days: int = 20,
    membership_session: Session | None = None,
    index_name: str | None = None,
) -> tuple[list[TradeResult], list[BacktestSummary]]:
    """Evaluate forward returns for saved signal events.

    Args:
        events: DataFrame from ``fetch_signal_event_export()`` with at minimum
                columns: event_date, symbol, signal, active, close.
        price_provider: OHLCV provider. Defaults to ``YahooFinanceProvider()``.
        holding_days: Number of trading days to hold after the signal date.
        membership_session: optional SQLAlchemy session used to filter events
            by point-in-time index membership.
        index_name: optional index name such as ``"Nifty 50"``. When provided
            with ``membership_session``, events are only backtested if the
            symbol belonged to that index on the event date.

    Returns:
        ``(trade_results, per_signal_summaries)``
    """
    if events is None or events.empty:
        return [], []

    events = filter_events_by_index_membership(events, membership_session, index_name)
    if events.empty:
        return [], []

    provider = price_provider or YahooFinanceProvider()

    # Download price history once per symbol (covers all its signal dates).
    price_cache: dict[str, pd.DataFrame] = {}
    for symbol in events["symbol"].unique():
        earliest = _earliest_date_for_symbol(events, symbol)
        download_start = earliest - timedelta(days=10)
        try:
            frame = provider.get_ohlcv(
                symbol=symbol,
                start=download_start,
                end=date.today(),
            )
            if not frame.empty:
                price_cache[symbol] = frame.sort_index()
                log.info("Backtest: cached {} rows for {}", len(frame), symbol)
        except Exception as exc:  # noqa: BLE001
            log.warning("Backtest: could not download {} — {}", symbol, exc)

    trades: list[TradeResult] = []
    for _, row in events.iterrows():
        symbol = str(row["symbol"])
        signal = str(row["signal"])
        entry_date = _as_date(row["event_date"])
        entry_price = _optional_float(row.get("close"))

        if entry_price is None or symbol not in price_cache:
            trades.append(
                TradeResult(
                    symbol=symbol,
                    signal=signal,
                    entry_date=entry_date,
                    entry_price=entry_price,
                    exit_date=None,
                    exit_price=None,
                    return_pct=None,
                    mfe_pct=None,
                    mae_pct=None,
                    holding_days=holding_days,
                )
            )
            continue

        prices = price_cache[symbol]
        # Only look at bars strictly after the signal date (no look-ahead).
        future_mask = pd.Series(prices.index).apply(lambda t, d=entry_date: t.date() > d).values
        future = prices.iloc[future_mask]

        if future.empty:
            trades.append(
                TradeResult(
                    symbol=symbol,
                    signal=signal,
                    entry_date=entry_date,
                    entry_price=entry_price,
                    exit_date=None,
                    exit_price=None,
                    return_pct=None,
                    mfe_pct=None,
                    mae_pct=None,
                    holding_days=holding_days,
                )
            )
            continue

        window = future.head(holding_days)
        exit_row = window.iloc[-1]
        exit_dt = exit_row.name
        exit_date_val = exit_dt.date() if hasattr(exit_dt, "date") else None
        exit_price = float(exit_row["close"])
        return_pct = (exit_price - entry_price) / entry_price * 100
        mfe_pct = (float(window["high"].max()) - entry_price) / entry_price * 100
        mae_pct = (float(window["low"].min()) - entry_price) / entry_price * 100

        trades.append(
            TradeResult(
                symbol=symbol,
                signal=signal,
                entry_date=entry_date,
                entry_price=entry_price,
                exit_date=exit_date_val,
                exit_price=exit_price,
                return_pct=return_pct,
                mfe_pct=mfe_pct,
                mae_pct=mae_pct,
                holding_days=holding_days,
            )
        )

    summaries = _compute_summaries(trades)
    return trades, summaries


def trades_to_frame(trades: list[TradeResult]) -> pd.DataFrame:
    """Convert trade results to a display-friendly DataFrame."""
    columns = [
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
    if not trades:
        return pd.DataFrame(columns=columns)

    return pd.DataFrame(
        [
            {
                "symbol": t.symbol,
                "signal": t.signal,
                "entry_date": t.entry_date,
                "entry_price": t.entry_price,
                "exit_date": t.exit_date,
                "exit_price": t.exit_price,
                "return_pct": _round2(t.return_pct),
                "mfe_pct": _round2(t.mfe_pct),
                "mae_pct": _round2(t.mae_pct),
                "holding_days": t.holding_days,
            }
            for t in trades
        ]
    )


def summaries_to_frame(summaries: list[BacktestSummary]) -> pd.DataFrame:
    """Convert backtest summaries to a display-friendly DataFrame."""
    columns = [
        "signal",
        "total_trades",
        "win_count",
        "loss_count",
        "win_rate_pct",
        "avg_return_pct",
        "avg_win_pct",
        "avg_loss_pct",
        "profit_factor",
        "best_trade_pct",
        "worst_trade_pct",
    ]
    if not summaries:
        return pd.DataFrame(columns=columns)

    return pd.DataFrame(
        [
            {
                "signal": s.signal,
                "total_trades": s.total_trades,
                "win_count": s.win_count,
                "loss_count": s.loss_count,
                "win_rate_pct": _round2(s.win_rate_pct),
                "avg_return_pct": _round2(s.avg_return_pct),
                "avg_win_pct": _round2(s.avg_win_pct),
                "avg_loss_pct": _round2(s.avg_loss_pct),
                "profit_factor": _round2(s.profit_factor),
                "best_trade_pct": _round2(s.best_trade_pct),
                "worst_trade_pct": _round2(s.worst_trade_pct),
            }
            for s in summaries
        ]
    )


def compute_portfolio_metrics(trades: list[TradeResult]) -> PortfolioBacktestMetrics:
    """Compute broad backtest diagnostics from completed trade outcomes."""
    total = len(trades)
    completed = [trade for trade in trades if trade.return_pct is not None]
    pending = total - len(completed)

    if not completed:
        return PortfolioBacktestMetrics(
            total_signals=total,
            completed_trades=0,
            pending_trades=pending,
            absolute_return_pct=None,
            cagr_pct=None,
            win_rate_pct=None,
            avg_return_pct=None,
            max_drawdown_pct=None,
            sharpe_ratio=None,
            sortino_ratio=None,
            exposure_time_pct=None,
            turnover=0,
            avg_holding_period_days=None,
            unique_symbols=0,
            max_symbol_concentration_pct=None,
        )

    ordered = sorted(
        completed, key=lambda trade: (trade.exit_date or trade.entry_date, trade.symbol)
    )
    returns = pd.Series([trade.return_pct / 100 for trade in ordered], dtype=float)
    compounded = prod(1 + value for value in returns) - 1
    win_rate = float((returns > 0).mean() * 100)
    avg_return = float(returns.mean() * 100)

    start = min(trade.entry_date for trade in ordered)
    end_candidates = [trade.exit_date for trade in ordered if trade.exit_date is not None]
    end = max(end_candidates) if end_candidates else max(trade.entry_date for trade in ordered)
    span_days = max((end - start).days, 1)
    cagr = ((1 + compounded) ** (365 / span_days) - 1) * 100 if span_days >= 30 else None

    equity = (1 + returns).cumprod()
    drawdown = (equity / equity.cummax()) - 1
    max_drawdown = float(drawdown.min() * 100)

    periods_per_year = 252 / max(ordered[0].holding_days, 1)
    return_std = float(returns.std(ddof=1)) if len(returns) > 1 else 0.0
    sharpe = (float(returns.mean()) / return_std * (periods_per_year**0.5)) if return_std else None
    downside = returns[returns < 0]
    downside_std = float(downside.std(ddof=1)) if len(downside) > 1 else 0.0
    sortino = (
        float(returns.mean()) / downside_std * (periods_per_year**0.5) if downside_std else None
    )

    total_holding_calendar_days = sum(
        max(((trade.exit_date or trade.entry_date) - trade.entry_date).days, 1) for trade in ordered
    )
    exposure = min(100.0, total_holding_calendar_days / span_days * 100)
    symbol_counts = pd.Series([trade.symbol for trade in ordered]).value_counts()
    concentration = float(symbol_counts.iloc[0] / len(ordered) * 100)

    return PortfolioBacktestMetrics(
        total_signals=total,
        completed_trades=len(ordered),
        pending_trades=pending,
        absolute_return_pct=compounded * 100,
        cagr_pct=cagr,
        win_rate_pct=win_rate,
        avg_return_pct=avg_return,
        max_drawdown_pct=max_drawdown,
        sharpe_ratio=sharpe,
        sortino_ratio=sortino,
        exposure_time_pct=exposure,
        turnover=len(ordered),
        avg_holding_period_days=float(sum(trade.holding_days for trade in ordered) / len(ordered)),
        unique_symbols=int(symbol_counts.size),
        max_symbol_concentration_pct=concentration,
    )


def portfolio_metrics_to_frame(metrics: PortfolioBacktestMetrics) -> pd.DataFrame:
    """Return portfolio metrics as a compact two-column table."""
    return pd.DataFrame(
        [
            {"metric": "Total signals", "value": metrics.total_signals},
            {"metric": "Completed trades", "value": metrics.completed_trades},
            {"metric": "Pending trades", "value": metrics.pending_trades},
            {"metric": "Absolute return %", "value": _round2(metrics.absolute_return_pct)},
            {"metric": "CAGR %", "value": _round2(metrics.cagr_pct)},
            {"metric": "Win rate %", "value": _round2(metrics.win_rate_pct)},
            {"metric": "Average return %", "value": _round2(metrics.avg_return_pct)},
            {"metric": "Max drawdown %", "value": _round2(metrics.max_drawdown_pct)},
            {"metric": "Sharpe ratio", "value": _round2(metrics.sharpe_ratio)},
            {"metric": "Sortino ratio", "value": _round2(metrics.sortino_ratio)},
            {"metric": "Exposure time %", "value": _round2(metrics.exposure_time_pct)},
            {"metric": "Turnover", "value": metrics.turnover},
            {
                "metric": "Average holding period",
                "value": _round2(metrics.avg_holding_period_days),
            },
            {"metric": "Unique symbols", "value": metrics.unique_symbols},
            {
                "metric": "Max symbol concentration %",
                "value": _round2(metrics.max_symbol_concentration_pct),
            },
        ]
    )


def filter_events_by_index_membership(
    events: pd.DataFrame,
    membership_session: Session | None,
    index_name: str | None,
) -> pd.DataFrame:
    """Return only events whose symbol belonged to ``index_name`` on event date."""
    if membership_session is None or not index_name:
        return events

    keep: list[bool] = []
    for _, row in events.iterrows():
        keep.append(
            was_index_member_on(
                membership_session,
                index_name=index_name,
                symbol=str(row["symbol"]),
                on_date=_as_date(row["event_date"]),
            )
        )
    return events.loc[keep].reset_index(drop=True)


def run_walk_forward_validation(
    trades: list[TradeResult],
    *,
    train_years: int = 3,
    validate_years: int = 1,
) -> pd.DataFrame:
    """Compare train-window and next-window performance by signal type."""
    completed = sorted(
        [trade for trade in trades if trade.return_pct is not None],
        key=lambda trade: trade.entry_date,
    )
    columns = [
        "signal",
        "train_start",
        "train_end",
        "validate_start",
        "validate_end",
        "train_trades",
        "train_avg_return_pct",
        "train_win_rate_pct",
        "validate_trades",
        "validate_avg_return_pct",
        "validate_win_rate_pct",
        "performance_drift_pct",
    ]
    if not completed:
        return pd.DataFrame(columns=columns)

    min_date = min(trade.entry_date for trade in completed)
    max_date = max(trade.entry_date for trade in completed)
    rows: list[dict[str, object]] = []
    train_days = train_years * 365
    validate_days = validate_years * 365
    window_start = min_date

    while window_start + timedelta(days=train_days) < max_date:
        train_start = window_start
        train_end = train_start + timedelta(days=train_days)
        validate_start = train_end + timedelta(days=1)
        validate_end = validate_start + timedelta(days=validate_days)
        train_trades = [
            trade for trade in completed if train_start <= trade.entry_date <= train_end
        ]
        validate_trades = [
            trade for trade in completed if validate_start <= trade.entry_date <= validate_end
        ]

        for signal in sorted({trade.signal for trade in [*train_trades, *validate_trades]}):
            train_signal = [trade for trade in train_trades if trade.signal == signal]
            validate_signal = [trade for trade in validate_trades if trade.signal == signal]
            train_avg = _avg_return(train_signal)
            validate_avg = _avg_return(validate_signal)
            drift = None if train_avg is None or validate_avg is None else validate_avg - train_avg
            rows.append(
                {
                    "signal": signal,
                    "train_start": train_start,
                    "train_end": train_end,
                    "validate_start": validate_start,
                    "validate_end": validate_end,
                    "train_trades": len(train_signal),
                    "train_avg_return_pct": _round2(train_avg),
                    "train_win_rate_pct": _round2(_win_rate(train_signal)),
                    "validate_trades": len(validate_signal),
                    "validate_avg_return_pct": _round2(validate_avg),
                    "validate_win_rate_pct": _round2(_win_rate(validate_signal)),
                    "performance_drift_pct": _round2(drift),
                }
            )

        window_start = window_start + timedelta(days=validate_days)

    return pd.DataFrame(rows, columns=columns)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _compute_summaries(trades: list[TradeResult]) -> list[BacktestSummary]:
    signal_names = sorted({t.signal for t in trades})
    summaries: list[BacktestSummary] = []

    for signal in signal_names:
        completed = [t for t in trades if t.signal == signal and t.return_pct is not None]
        if not completed:
            summaries.append(
                BacktestSummary(
                    signal=signal,
                    total_trades=0,
                    win_count=0,
                    loss_count=0,
                    win_rate_pct=None,
                    avg_return_pct=None,
                    avg_win_pct=None,
                    avg_loss_pct=None,
                    profit_factor=None,
                    best_trade_pct=None,
                    worst_trade_pct=None,
                )
            )
            continue

        returns = [t.return_pct for t in completed]  # type: ignore[misc]
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r <= 0]

        win_rate = len(wins) / len(returns) * 100 if returns else None
        avg_win = sum(wins) / len(wins) if wins else None
        avg_loss = sum(losses) / len(losses) if losses else None

        profit_factor: float | None = None
        if wins and losses:
            total_loss_abs = abs(sum(losses))
            if total_loss_abs > 0:
                profit_factor = sum(wins) / total_loss_abs

        summaries.append(
            BacktestSummary(
                signal=signal,
                total_trades=len(completed),
                win_count=len(wins),
                loss_count=len(losses),
                win_rate_pct=win_rate,
                avg_return_pct=sum(returns) / len(returns),
                avg_win_pct=avg_win,
                avg_loss_pct=avg_loss,
                profit_factor=profit_factor,
                best_trade_pct=max(returns),
                worst_trade_pct=min(returns),
            )
        )

    return summaries


def _earliest_date_for_symbol(events: pd.DataFrame, symbol: str) -> date:
    symbol_events = events[events["symbol"] == symbol]
    dates = pd.to_datetime(symbol_events["event_date"]).dt.date
    return min(dates)


def _as_date(value: object) -> date:
    if isinstance(value, date) and not isinstance(value, pd.Timestamp):
        return value
    return pd.Timestamp(value).date()


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def _round2(value: float | None) -> float | None:
    return None if value is None else round(value, 2)


def _avg_return(trades: list[TradeResult]) -> float | None:
    returns = [trade.return_pct for trade in trades if trade.return_pct is not None]
    return None if not returns else float(sum(returns) / len(returns))


def _win_rate(trades: list[TradeResult]) -> float | None:
    returns = [trade.return_pct for trade in trades if trade.return_pct is not None]
    return (
        None
        if not returns
        else float(sum(1 for value in returns if value > 0) / len(returns) * 100)
    )
