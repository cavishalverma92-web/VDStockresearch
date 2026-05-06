"""Strategy scanner result schema.

The strategy scanner is intentionally separate from the composite-score scanner:
one row means one strategy setup was observed for one symbol on one date.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

DEFAULT_STRATEGY_SCAN_COLUMNS = [
    "symbol",
    "company_name",
    "sector",
    "strategy",
    "setup_type",
    "signal_date",
    "close",
    "entry_zone",
    "stop_loss",
    "target_zone",
    "risk_reward",
    "rsi",
    "trend_status",
    "relative_volume",
    "liquidity_status",
    "data_trust",
    "data_freshness",
    "confidence_score",
    "why_this_appeared",
    "key_risk",
]


ADVANCED_STRATEGY_SCAN_COLUMNS = [
    "atr_pct",
    "data_source",
    "market_cap_bucket",
    "ema_20",
    "ema_50",
    "ema_100",
    "ema_200",
    "breakout_level",
    "avg_traded_value_cr",
    "warnings",
    "provider_fallback_reason",
]


@dataclass(frozen=True)
class StrategyScanResult:
    """One strategy scanner hit for display, persistence, and later backtests."""

    symbol: str
    strategy: str
    setup_type: str
    signal_date: date
    close: float
    entry_zone_low: float | None
    entry_zone_high: float | None
    stop_loss: float | None
    target_price: float | None
    risk_reward: float | None
    rsi: float | None
    trend_status: str
    relative_volume: float | None
    atr_pct: float | None
    liquidity_status: str
    data_source: str
    data_freshness: str
    confidence_score: float
    why_this_appeared: str
    key_risk: str
    data_trust: str = "Good data"
    company_name: str | None = None
    sector: str | None = None
    market_cap_bucket: str | None = None
    ema_20: float | None = None
    ema_50: float | None = None
    ema_100: float | None = None
    ema_200: float | None = None
    breakout_level: float | None = None
    avg_traded_value_cr: float | None = None
    warnings: tuple[str, ...] = ()
    provider_fallback_reason: str = ""

    @property
    def entry_zone(self) -> str:
        """Human-readable educational entry zone."""
        if self.entry_zone_low is None or self.entry_zone_high is None:
            return "N/A"
        return f"INR {self.entry_zone_low:.2f} - {self.entry_zone_high:.2f}"

    @property
    def target_zone(self) -> str:
        """Human-readable educational target level."""
        if self.target_price is None:
            return "N/A"
        return f"INR {self.target_price:.2f}"

    def to_row(self) -> dict[str, object]:
        """Return a stable UI-ready row."""
        return {
            "symbol": self.symbol,
            "company_name": self.company_name or "",
            "sector": self.sector or "",
            "strategy": self.strategy,
            "setup_type": self.setup_type,
            "signal_date": self.signal_date,
            "close": self.close,
            "entry_zone": self.entry_zone,
            "stop_loss": self.stop_loss,
            "target_zone": self.target_zone,
            "risk_reward": self.risk_reward,
            "rsi": self.rsi,
            "trend_status": self.trend_status,
            "relative_volume": self.relative_volume,
            "liquidity_status": self.liquidity_status,
            "data_freshness": self.data_freshness,
            "confidence_score": self.confidence_score,
            "why_this_appeared": self.why_this_appeared,
            "key_risk": self.key_risk,
            "atr_pct": self.atr_pct,
            "data_source": self.data_source,
            "market_cap_bucket": self.market_cap_bucket or "",
            "ema_20": self.ema_20,
            "ema_50": self.ema_50,
            "ema_100": self.ema_100,
            "ema_200": self.ema_200,
            "breakout_level": self.breakout_level,
            "avg_traded_value_cr": self.avg_traded_value_cr,
            "data_trust": self.data_trust,
            "warnings": "; ".join(self.warnings),
            "provider_fallback_reason": self.provider_fallback_reason,
        }


@dataclass(frozen=True)
class StrategyScanFrameSummary:
    """Compact counts for scanner dashboard cards."""

    total_setups: int
    unique_symbols: int
    clean_setups: int
    warning_setups: int
    untrusted_setups: int
    breakout_setups: int
    top_strategy: str
    top_strategy_count: int


def strategy_results_to_frame(
    results: list[StrategyScanResult],
    *,
    include_advanced: bool = True,
) -> pd.DataFrame:
    """Convert strategy results to a stable DataFrame."""
    columns = list(DEFAULT_STRATEGY_SCAN_COLUMNS)
    if include_advanced:
        columns.extend(ADVANCED_STRATEGY_SCAN_COLUMNS)
    if not results:
        return pd.DataFrame(columns=columns)
    frame = pd.DataFrame([result.to_row() for result in results])
    return (
        frame[[column for column in columns if column in frame.columns]]
        .sort_values(
            by=["confidence_score", "risk_reward", "symbol", "strategy"],
            ascending=[False, False, True, True],
            na_position="last",
        )
        .reset_index(drop=True)
    )


def summarize_strategy_scan_frame(frame: pd.DataFrame) -> StrategyScanFrameSummary:
    """Summarize a strategy scan result frame for high-signal UI cards."""
    if frame is None or frame.empty:
        return StrategyScanFrameSummary(
            total_setups=0,
            unique_symbols=0,
            clean_setups=0,
            warning_setups=0,
            untrusted_setups=0,
            breakout_setups=0,
            top_strategy="None",
            top_strategy_count=0,
        )

    total = len(frame)
    unique_symbols = int(frame["symbol"].nunique()) if "symbol" in frame.columns else 0
    trust = frame.get("data_trust", pd.Series(dtype="object")).fillna("").astype(str)
    clean = int((trust == "Good data").sum())
    warning = int((trust == "Warning").sum())
    untrusted = int((trust == "Do not trust signal").sum())

    setup_type = frame.get("setup_type", pd.Series(dtype="object")).fillna("").astype(str)
    strategy = frame.get("strategy", pd.Series(dtype="object")).fillna("").astype(str)
    breakout = int(
        (
            (setup_type.str.lower() == "breakout") | strategy.str.contains("breakout", case=False)
        ).sum()
    )

    counts = strategy[strategy != ""].value_counts()
    if counts.empty:
        top_strategy = "None"
        top_count = 0
    else:
        top_strategy = str(counts.index[0])
        top_count = int(counts.iloc[0])

    return StrategyScanFrameSummary(
        total_setups=total,
        unique_symbols=unique_symbols,
        clean_setups=clean,
        warning_setups=warning,
        untrusted_setups=untrusted,
        breakout_setups=breakout,
        top_strategy=top_strategy,
        top_strategy_count=top_count,
    )
