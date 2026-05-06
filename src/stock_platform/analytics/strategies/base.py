"""Shared helpers for strategy scanner rules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import pandas as pd

from stock_platform.analytics.scanner.result_schema import StrategyScanResult
from stock_platform.analytics.technicals import add_technical_indicators
from stock_platform.config import get_thresholds_config, get_universe_config


@dataclass(frozen=True)
class StrategyContext:
    """Inputs available to every strategy rule."""

    symbol: str
    frame: pd.DataFrame
    technical_frame: pd.DataFrame
    data_source: str
    data_freshness: str
    warnings: tuple[str, ...] = ()
    provider_fallback_reason: str = ""
    company_name: str | None = None
    sector: str | None = None
    market_cap_bucket: str | None = None

    @property
    def latest(self) -> pd.Series:
        return self.technical_frame.iloc[-1]


class StrategyDefinition(Protocol):
    """Protocol implemented by concrete strategy rules."""

    name: str
    setup_type: str

    def evaluate(self, context: StrategyContext) -> StrategyScanResult | None:
        """Return a result when the strategy is triggered, else ``None``."""


def build_strategy_context(
    *,
    symbol: str,
    frame: pd.DataFrame,
    data_source: str,
    warnings: list[str] | tuple[str, ...] | None = None,
    provider_fallback_reason: str = "",
) -> StrategyContext:
    """Build a validated strategy context from OHLCV data."""
    technical_frame = add_technical_indicators(frame)
    latest_date = technical_frame.index[-1]
    if isinstance(latest_date, pd.Timestamp):
        freshness = latest_date.date().isoformat()
    else:
        freshness = str(latest_date)
    return StrategyContext(
        symbol=symbol.upper(),
        frame=frame,
        technical_frame=technical_frame,
        data_source=data_source,
        data_freshness=freshness,
        warnings=tuple(warnings or ()),
        provider_fallback_reason=provider_fallback_reason,
    )


def evaluate_default_strategies(context: StrategyContext) -> list[StrategyScanResult]:
    """Evaluate the first MVP strategy set for one symbol."""
    from stock_platform.analytics.strategies.breakout import BreakoutWithVolumeStrategy
    from stock_platform.analytics.strategies.ema_pullback import EmaPullbackStrategy
    from stock_platform.analytics.strategies.ema_stack import EmaStackStrategy
    from stock_platform.analytics.strategies.rsi_momentum import RsiMomentumStrategy

    strategies: list[StrategyDefinition] = [
        EmaStackStrategy(),
        RsiMomentumStrategy(),
        EmaPullbackStrategy(),
        BreakoutWithVolumeStrategy(),
    ]
    results: list[StrategyScanResult] = []
    for strategy in strategies:
        result = strategy.evaluate(context)
        if result is not None:
            results.append(result)
    return results


def all_present(row: pd.Series, columns: tuple[str, ...]) -> bool:
    return all(column in row and pd.notna(row[column]) for column in columns)


def safe_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def latest_signal_date(context: StrategyContext):
    index_value = context.technical_frame.index[-1]
    if isinstance(index_value, pd.Timestamp):
        return index_value.date()
    if hasattr(index_value, "date"):
        return index_value.date()
    return pd.Timestamp(index_value).date()


def avg_traded_value_cr(context: StrategyContext, window: int = 20) -> float | None:
    frame = context.technical_frame
    if "close" not in frame.columns or "volume" not in frame.columns:
        return None
    traded_value = pd.to_numeric(frame["close"], errors="coerce") * pd.to_numeric(
        frame["volume"], errors="coerce"
    )
    value = traded_value.tail(window).mean()
    if pd.isna(value):
        return None
    return round(float(value) / 10_000_000, 2)


def liquidity_status(context: StrategyContext) -> tuple[str, float | None, list[str]]:
    """Return liquidity label, average traded value, and warnings."""
    value_cr = avg_traded_value_cr(context)
    liquidity_cfg = get_universe_config().get("liquidity", {})
    pass_cr = float(liquidity_cfg.get("min_median_daily_turnover_cr", 5))
    warn_cr = float(liquidity_cfg.get("flag_below_cr", 3))
    if value_cr is None:
        return "Unknown", None, ["Average traded value is unavailable."]
    if value_cr >= pass_cr:
        return "Pass", value_cr, []
    if value_cr >= warn_cr:
        return "Watch", value_cr, [f"Average traded value is below INR {pass_cr:.0f} crore."]
    return "Low", value_cr, [f"Average traded value is below INR {warn_cr:.0f} crore."]


def data_trust_status(
    context: StrategyContext,
    *,
    liquidity: str,
    avg_value_cr: float | None,
) -> tuple[str, list[str]]:
    """Classify whether a strategy signal is usable, risky, or not trustworthy."""
    row = context.latest
    warnings: list[str] = []
    critical: list[str] = []
    thresholds = get_thresholds_config()
    stale_days = int(thresholds.get("data_quality", {}).get("stale_price_days", 5))
    risk_cfg = thresholds.get("scanner_risk", {})
    high_atr_pct = float(risk_cfg.get("high_atr_pct", 8.0))
    extreme_atr_pct = float(risk_cfg.get("extreme_atr_pct", 12.0))
    min_price = float(risk_cfg.get("min_price", 20.0))

    close = safe_float(row.get("close"))
    atr_pct = safe_float(row.get("atr_pct"))
    latest_date = latest_signal_date(context)
    age_days = (datetime.now(UTC).date() - latest_date).days

    if age_days > stale_days:
        critical.append(f"Persisted price data is stale by {age_days} day(s).")
    if liquidity == "Low":
        critical.append("Liquidity is below the scanner trust floor.")
    elif liquidity == "Unknown":
        warnings.append("Liquidity could not be verified.")
    elif liquidity == "Watch":
        warnings.append("Liquidity is acceptable only for cautious review.")
    if avg_value_cr is None:
        warnings.append("Average traded value is unavailable.")

    if close is not None and close < min_price:
        critical.append(
            f"Close price is below INR {min_price:.0f}, which raises manipulation risk."
        )
    if atr_pct is not None:
        if atr_pct > extreme_atr_pct:
            critical.append(f"ATR is extremely high at {atr_pct:.1f}% of price.")
        elif atr_pct > high_atr_pct:
            warnings.append(f"ATR is high at {atr_pct:.1f}% of price.")

    for warning in context.warnings:
        lowered = warning.lower()
        if "stale_data" in lowered:
            critical.append(warning)
        elif (
            "suspicious_price_moves" in lowered
            or "zero_volume_rows" in lowered
            or "mixed persisted price sources" in lowered
        ):
            warnings.append(warning)
        else:
            warnings.append(warning)

    if critical:
        return "Do not trust signal", [*critical, *warnings]
    if warnings:
        return "Warning", warnings
    return "Good data", []


def risk_plan(row: pd.Series) -> tuple[float | None, float | None, float | None, float | None]:
    """Educational ATR-based entry/stop/target plan."""
    close = safe_float(row.get("close"))
    atr = safe_float(row.get("atr_14"))
    if close is None or atr is None or atr <= 0:
        return None, None, None, None
    entry_low = close - (0.5 * atr)
    entry_high = close + (0.25 * atr)
    stop_loss = close - (2.0 * atr)
    risk = close - stop_loss
    target = close + (2.5 * risk)
    return round(entry_low, 2), round(entry_high, 2), round(stop_loss, 2), round(target, 2)


def common_result_kwargs(context: StrategyContext) -> dict[str, object]:
    """Shared result fields computed from the latest row."""
    row = context.latest
    liquidity, traded_value_cr, liquidity_warnings = liquidity_status(context)
    trust_label, trust_warnings = data_trust_status(
        context,
        liquidity=liquidity,
        avg_value_cr=traded_value_cr,
    )
    entry_low, entry_high, stop_loss, target = risk_plan(row)
    return {
        "symbol": context.symbol,
        "signal_date": latest_signal_date(context),
        "close": round(float(row["close"]), 2),
        "entry_zone_low": entry_low,
        "entry_zone_high": entry_high,
        "stop_loss": stop_loss,
        "target_price": target,
        "risk_reward": 2.5 if stop_loss is not None and target is not None else None,
        "rsi": _round_optional(row.get("rsi_14")),
        "trend_status": str(row.get("ma_stack_status", "mixed")),
        "relative_volume": _round_optional(row.get("relative_volume")),
        "atr_pct": _round_optional(row.get("atr_pct")),
        "liquidity_status": liquidity,
        "data_source": context.data_source,
        "data_freshness": context.data_freshness,
        "data_trust": trust_label,
        "company_name": context.company_name,
        "sector": context.sector,
        "market_cap_bucket": context.market_cap_bucket,
        "ema_20": _round_optional(row.get("ema_20")),
        "ema_50": _round_optional(row.get("ema_50")),
        "ema_100": _round_optional(row.get("ema_100")),
        "ema_200": _round_optional(row.get("ema_200")),
        "avg_traded_value_cr": traded_value_cr,
        "warnings": tuple(dict.fromkeys([*liquidity_warnings, *trust_warnings])),
        "provider_fallback_reason": context.provider_fallback_reason,
    }


def confidence_from_context(context: StrategyContext, base: float) -> float:
    """Simple transparent confidence score, not a backtested probability."""
    row = context.latest
    score = base
    relative_volume = safe_float(row.get("relative_volume"))
    atr_pct = safe_float(row.get("atr_pct"))
    liquidity, _, _ = liquidity_status(context)
    trust, _ = data_trust_status(
        context, liquidity=liquidity, avg_value_cr=avg_traded_value_cr(context)
    )
    if relative_volume is not None and relative_volume >= 1.5:
        score += 5
    if atr_pct is not None and 1 <= atr_pct <= 5:
        score += 4
    if liquidity == "Pass":
        score += 5
    elif liquidity == "Low":
        score -= 8
    if trust == "Warning":
        score -= 6
    elif trust == "Do not trust signal":
        score -= 18
    if context.warnings:
        score -= 6
    return round(max(0.0, min(score, 90.0)), 1)


def _round_optional(value: object, digits: int = 2) -> float | None:
    number = safe_float(value)
    return None if number is None else round(number, digits)
