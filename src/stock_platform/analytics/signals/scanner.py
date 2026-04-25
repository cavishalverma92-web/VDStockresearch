"""Educational technical pattern scanner."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from stock_platform.analytics.technicals import add_technical_indicators
from stock_platform.config import get_thresholds_config


@dataclass(frozen=True)
class SignalResult:
    """One technical pattern observation."""

    name: str
    active: bool
    detail: str
    strength: str = "info"
    trigger_price: float | None = None
    entry_zone_low: float | None = None
    entry_zone_high: float | None = None
    stop_loss: float | None = None
    target_price: float | None = None
    risk_reward: float | None = None
    confidence: float | None = None
    data_used: tuple[str, ...] = ()


def scan_technical_signals(
    frame: pd.DataFrame,
    thresholds: dict[str, Any] | None = None,
) -> list[SignalResult]:
    """Evaluate the Phase 2 MVP signals on the latest row."""
    if frame.empty or len(frame) < 2:
        return []

    df = add_technical_indicators(frame)
    signals_cfg = thresholds or get_thresholds_config().get("signals", {})
    latest = df.iloc[-1]
    previous = df.iloc[-2]

    return [
        _ema_200_pullback(latest, signals_cfg.get("ema_200_pullback", {})),
        _rsi_60_momentum(latest, signals_cfg.get("rsi_60_momentum", {})),
        _breakout_with_volume(df, signals_cfg.get("breakout_with_volume", {})),
        _darvas_base_breakout(df, signals_cfg.get("darvas_base_breakout", {})),
        _mean_reversion_oversold(latest, signals_cfg.get("mean_reversion_oversold", {})),
        _ma_stack(latest),
        _golden_death_cross(latest, previous),
    ]


def _ema_200_pullback(row: pd.Series, cfg: dict[str, Any]) -> SignalResult:
    max_distance_pct = float(cfg.get("max_distance_pct", 3.0))
    rsi_low = float(cfg.get("rsi_low", 40))
    rsi_high = float(cfg.get("rsi_high", 50))
    distance_pct = _pct_distance(row.get("close"), row.get("ema_200"))
    active = (
        _all_present(row, ["close", "ema_200", "rsi_14"])
        and row["close"] > row["ema_200"]
        and abs(distance_pct) <= max_distance_pct
        and rsi_low <= row["rsi_14"] <= rsi_high
    )
    return _with_trade_plan(
        SignalResult(
            name="200 EMA Pullback",
            active=bool(active),
            detail=f"Close is {distance_pct:.1f}% from 200 EMA; RSI is {_fmt(row.get('rsi_14'))}.",
            strength="trend",
            data_used=("close", "ema_200", "rsi_14", "atr_14"),
        ),
        row,
    )


def _rsi_60_momentum(row: pd.Series, cfg: dict[str, Any]) -> SignalResult:
    rsi_floor = float(cfg.get("rsi_floor", 60))
    active = (
        _all_present(row, ["close", "ema_50", "rsi_14"])
        and row["close"] > row["ema_50"]
        and row["rsi_14"] >= rsi_floor
    )
    return _with_trade_plan(
        SignalResult(
            name="RSI 60 Momentum",
            active=bool(active),
            detail=f"RSI is {_fmt(row.get('rsi_14'))}; close is above 50 EMA.",
            strength="momentum",
            data_used=("close", "ema_50", "rsi_14", "macd", "atr_14"),
        ),
        row,
    )


def _breakout_with_volume(df: pd.DataFrame, cfg: dict[str, Any]) -> SignalResult:
    lookback = int(cfg.get("lookback_days_for_high", 252))
    volume_multiple = float(cfg.get("volume_multiple", 2.0))
    latest = df.iloc[-1]
    prior_high = df["high"].iloc[:-1].tail(lookback).max()
    active = (
        pd.notna(prior_high)
        and _all_present(latest, ["close", "relative_volume"])
        and latest["close"] > prior_high
        and latest["relative_volume"] >= volume_multiple
    )
    return _with_trade_plan(
        SignalResult(
            name="Breakout With Volume",
            active=bool(active),
            detail=(
                f"Close {_fmt(latest.get('close'))} vs prior high {_fmt(prior_high)}; "
                f"relative volume {_fmt(latest.get('relative_volume'))}x."
            ),
            strength="breakout",
            data_used=("close", "high", "relative_volume", "avg_volume_20", "atr_14"),
        ),
        latest,
    )


def _darvas_base_breakout(df: pd.DataFrame, cfg: dict[str, Any]) -> SignalResult:
    days = int(cfg.get("min_consolidation_days", 20))
    max_range_pct = float(cfg.get("max_range_pct", 10.0))
    if len(df) <= days:
        return SignalResult(
            "Darvas Base Breakout",
            False,
            "Not enough history.",
            "structure",
            data_used=("high", "low", "close", "atr_14"),
        )

    base = df.iloc[-days - 1 : -1]
    base_high = base["high"].max()
    base_low = base["low"].min()
    range_pct = ((base_high - base_low) / base_low) * 100 if base_low else float("nan")
    latest_close = df.iloc[-1]["close"]
    active = pd.notna(range_pct) and range_pct <= max_range_pct and latest_close > base_high
    return _with_trade_plan(
        SignalResult(
            name="Darvas Base Breakout",
            active=bool(active),
            detail=f"Prior {days}-day base range was {range_pct:.1f}%; latest close vs base high.",
            strength="structure",
            data_used=("high", "low", "close", "atr_14"),
        ),
        df.iloc[-1],
    )


def _mean_reversion_oversold(row: pd.Series, cfg: dict[str, Any]) -> SignalResult:
    rsi_threshold = float(cfg.get("rsi_threshold", 30))
    active = (
        _all_present(row, ["close", "bb_lower", "rsi_14"])
        and row["close"] < row["bb_lower"]
        and row["rsi_14"] < rsi_threshold
    )
    return _with_trade_plan(
        SignalResult(
            name="Mean-Reversion Oversold",
            active=bool(active),
            detail=f"RSI is {_fmt(row.get('rsi_14'))}; close vs lower Bollinger band.",
            strength="mean_reversion",
            data_used=("close", "bb_lower", "rsi_14", "atr_14"),
        ),
        row,
    )


def _ma_stack(row: pd.Series) -> SignalResult:
    active = (
        _all_present(row, ["close", "ema_20", "ema_50", "ema_200"])
        and row["close"] > row["ema_20"] > row["ema_50"] > row["ema_200"]
    )
    return _with_trade_plan(
        SignalResult(
            name="MA Stack",
            active=bool(active),
            detail="Close, 20 EMA, 50 EMA, and 200 EMA are checked for bullish order.",
            strength="trend",
            data_used=("close", "ema_20", "ema_50", "ema_200", "atr_14"),
        ),
        row,
    )


def _golden_death_cross(latest: pd.Series, previous: pd.Series) -> SignalResult:
    if not _all_present(latest, ["sma_50", "sma_200"]) or not _all_present(
        previous, ["sma_50", "sma_200"]
    ):
        return SignalResult(
            "Golden/Death Cross",
            False,
            "Not enough SMA history.",
            "trend",
            data_used=("sma_50", "sma_200"),
        )

    golden = latest["sma_50"] > latest["sma_200"] and previous["sma_50"] <= previous["sma_200"]
    death = latest["sma_50"] < latest["sma_200"] and previous["sma_50"] >= previous["sma_200"]
    detail = "No latest 50/200 SMA crossover."
    if golden:
        detail = "50 SMA crossed above 200 SMA on the latest bar."
    elif death:
        detail = "50 SMA crossed below 200 SMA on the latest bar."
    return _with_trade_plan(
        SignalResult(
            "Golden/Death Cross",
            bool(golden or death),
            detail,
            "trend",
            data_used=("sma_50", "sma_200", "close", "atr_14"),
        ),
        latest,
    )


def _with_trade_plan(signal: SignalResult, row: pd.Series) -> SignalResult:
    """Add educational entry/exit levels for active signals."""
    if not signal.active:
        return signal
    close = _optional_float(row.get("close"))
    atr = _optional_float(row.get("atr_14"))
    if close is None or atr is None or atr <= 0:
        return signal

    entry_low = close - (0.5 * atr)
    entry_high = close + (0.25 * atr)
    stop_loss = close - (2.0 * atr)
    risk = close - stop_loss
    target = close + (2.5 * risk)
    return SignalResult(
        name=signal.name,
        active=signal.active,
        detail=signal.detail,
        strength=signal.strength,
        trigger_price=round(close, 2),
        entry_zone_low=round(entry_low, 2),
        entry_zone_high=round(entry_high, 2),
        stop_loss=round(stop_loss, 2),
        target_price=round(target, 2),
        risk_reward=2.5,
        confidence=_confidence(row),
        data_used=signal.data_used,
    )


def _all_present(row: pd.Series, columns: list[str]) -> bool:
    return all(column in row and pd.notna(row[column]) for column in columns)


def _pct_distance(left: float | None, right: float | None) -> float:
    if left is None or right is None or pd.isna(left) or pd.isna(right) or right == 0:
        return float("nan")
    return ((left - right) / right) * 100


def _fmt(value: object) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.2f}"


def _optional_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _confidence(row: pd.Series) -> float:
    score = 55.0
    rsi = _optional_float(row.get("rsi_14"))
    relative_volume = _optional_float(row.get("relative_volume"))
    if rsi is not None and 45 <= rsi <= 70:
        score += 10
    if relative_volume is not None and relative_volume >= 1.5:
        score += 10
    if (
        _all_present(row, ["close", "ema_50", "ema_200"])
        and row["close"] > row["ema_50"] > row["ema_200"]
    ):
        score += 10
    return round(min(score, 90.0), 1)
