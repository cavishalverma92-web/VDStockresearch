"""Compliance-safe alert candidate generation.

Phase 6 does not send Telegram/email alerts yet. It only prepares preview
messages so the user can see what would be sent after credentials, rate limits,
and compliance wording are reviewed.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from stock_platform.analytics.signals import SignalResult
from stock_platform.scoring import CompositeScore


@dataclass(frozen=True)
class AlertCandidate:
    """One alert preview row."""

    symbol: str
    severity: str
    title: str
    message: str
    source: str


def build_alert_candidates(
    *,
    symbol: str,
    composite: CompositeScore,
    signals: list[SignalResult],
    data_warnings: list[str] | None = None,
) -> list[AlertCandidate]:
    """Return alert previews with explicit research-aid wording."""
    candidates: list[AlertCandidate] = []
    active_signals = [signal for signal in signals if signal.active]

    if composite.score >= 80 and active_signals:
        candidates.append(
            AlertCandidate(
                symbol=symbol.upper(),
                severity="research_candidate",
                title=f"{symbol.upper()} crossed strong research score threshold",
                message=(
                    f"{symbol.upper()} has a composite research score of "
                    f"{composite.score:.1f}/100 and {len(active_signals)} active technical "
                    "signal(s). Review the dashboard, data freshness, and risk notes before "
                    "taking any action. This is not investment advice."
                ),
                source="composite_score",
            )
        )

    for signal in active_signals[:3]:
        candidates.append(
            AlertCandidate(
                symbol=symbol.upper(),
                severity="signal_active",
                title=f"{symbol.upper()} active signal: {signal.name}",
                message=_signal_message(symbol, signal),
                source="technical_signal",
            )
        )

    warnings = [warning for warning in data_warnings or [] if warning]
    if warnings:
        candidates.append(
            AlertCandidate(
                symbol=symbol.upper(),
                severity="data_quality",
                title=f"{symbol.upper()} has data-quality warning(s)",
                message=(
                    f"{symbol.upper()} has data-quality warning(s): "
                    f"{'; '.join(warnings[:3])}. Verify source data before relying on outputs."
                ),
                source="data_quality",
            )
        )

    return candidates


def alert_candidates_to_frame(candidates: list[AlertCandidate]) -> pd.DataFrame:
    """Convert alert candidates to a display table."""
    columns = ["symbol", "severity", "title", "message", "source"]
    if not candidates:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame([candidate.__dict__ for candidate in candidates], columns=columns)


def _signal_message(symbol: str, signal: SignalResult) -> str:
    parts = [
        f"{symbol.upper()} has an active {signal.name} observation.",
        signal.detail,
    ]
    if signal.trigger_price is not None:
        parts.append(f"Trigger: {signal.trigger_price:.2f}.")
    if signal.entry_zone_low is not None and signal.entry_zone_high is not None:
        parts.append(f"Entry zone: {signal.entry_zone_low:.2f}-{signal.entry_zone_high:.2f}.")
    if signal.stop_loss is not None:
        parts.append(f"Invalidation/stop-loss area: {signal.stop_loss:.2f}.")
    if signal.risk_reward is not None:
        parts.append(f"Educational risk/reward: {signal.risk_reward:.1f}.")
    parts.append("Research aid only; not investment advice.")
    return " ".join(parts)
