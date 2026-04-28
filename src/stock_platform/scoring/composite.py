"""Explainable composite scoring for Phase 4.

The score is deliberately simple for the MVP: it combines the data we already
trust locally, records missing inputs, and avoids turning the result into advice.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd

from stock_platform.analytics.fundamentals.sector_policy import is_financial_sector
from stock_platform.analytics.signals import SignalResult
from stock_platform.config import get_scoring_weights


@dataclass(frozen=True)
class CompositeScore:
    """One explainable 0-100 score for a stock."""

    symbol: str
    score: float
    band: str
    sub_scores: dict[str, float]
    reasons: list[str]
    risks: list[str]
    missing_data: list[str]


def score_stock(
    *,
    symbol: str,
    fundamentals: Mapping[str, Any] | None,
    banking_fundamentals: Mapping[str, Any] | None = None,
    technicals: Mapping[str, Any] | pd.Series | None,
    signals: list[SignalResult],
    delivery: Mapping[str, Any] | None = None,
    result_volatility: Mapping[str, Any] | None = None,
    weights: Mapping[str, Any] | None = None,
) -> CompositeScore:
    """Build an explainable composite score from available MVP inputs."""
    config = dict(weights or get_scoring_weights())
    bucket_weights = _normalise_weights(config.get("buckets", {}))
    missing: list[str] = []
    reasons: list[str] = []
    risks: list[str] = []

    sub_scores = {
        "fundamentals": _fundamentals_score(
            fundamentals,
            banking_fundamentals,
            reasons,
            risks,
            missing,
        ),
        "technicals": _technicals_score(technicals, signals, reasons, risks, missing),
        "flows": _flows_score(delivery, reasons, risks, missing),
        "events_quality": _events_score(result_volatility, reasons, risks, missing),
        "macro_sector": _macro_sector_score(fundamentals, reasons, risks, missing),
    }
    composite = sum(sub_scores[key] * bucket_weights.get(key, 0.0) for key in sub_scores)

    return CompositeScore(
        symbol=symbol.upper(),
        score=round(composite, 1),
        band=_score_band(composite, config.get("score_bands", {})),
        sub_scores={key: round(value, 1) for key, value in sub_scores.items()},
        reasons=reasons[:6],
        risks=risks[:6],
        missing_data=sorted(set(missing)),
    )


def composite_scores_to_frame(scores: list[CompositeScore]) -> pd.DataFrame:
    """Convert scores to a UI-friendly table."""
    rows = []
    for item in scores:
        rows.append(
            {
                "symbol": item.symbol,
                "composite_score": item.score,
                "band": item.band,
                "fundamentals": item.sub_scores.get("fundamentals"),
                "technicals": item.sub_scores.get("technicals"),
                "flows": item.sub_scores.get("flows"),
                "events_quality": item.sub_scores.get("events_quality"),
                "macro_sector": item.sub_scores.get("macro_sector"),
                "missing_data": "; ".join(item.missing_data),
            }
        )
    return pd.DataFrame(rows)


def _fundamentals_score(
    row: Mapping[str, Any] | None,
    banking_row: Mapping[str, Any] | None,
    reasons: list[str],
    risks: list[str],
    missing: list[str],
) -> float:
    if not row:
        missing.append("fundamentals")
        return 50.0

    financial_sector = is_financial_sector(row=row)
    parts: list[float] = []
    parts.append(_scale(_num(row.get("piotroski_f_score")), low=0, high=9))
    if financial_sector:
        reasons.append("Using financial-sector fundamental scoring.")
    else:
        parts.append(_altman_score(_num(row.get("altman_z_score"))))
    parts.append(_scale(_first_num(row, ["roe_pct_sector_rank", "roe_pct"]), low=0, high=100))
    parts.append(_scale(_first_num(row, ["roa_pct_sector_rank", "roa_pct"]), low=0, high=100))
    parts.append(
        _scale(_first_num(row, ["revenue_growth_pct_sector_rank", "revenue_growth_pct"]), 0, 100)
    )
    if financial_sector:
        banking_score = _banking_metrics_score(banking_row, reasons, risks, missing)
        if banking_score is not None:
            parts.append(banking_score)
            parts.append(banking_score)
        else:
            reasons.append("Banking metrics missing; score uses general financial-sector fallback.")
    if not financial_sector:
        debt_rank = _first_num(row, ["debt_to_equity_sector_rank"])
        debt_score = (
            debt_rank
            if debt_rank is not None
            else _inverse_scale(_num(row.get("debt_to_equity")), 0, 2)
        )
        parts.append(debt_score)

    valid = [value for value in parts if value is not None]
    if not valid:
        missing.append("fundamental metrics")
        return 50.0

    piotroski = _num(row.get("piotroski_f_score"))
    if piotroski is not None and piotroski >= 7:
        reasons.append(f"Strong Piotroski F-Score ({piotroski:.0f}/9).")
    altman = _num(row.get("altman_z_score"))
    if not financial_sector and altman is not None and altman < 1.8:
        risks.append(f"Low Altman Z-Score ({altman:.2f}) signals balance-sheet caution.")
    if str(row.get("status", "")).lower() == "sample":
        missing.append("verified fundamentals source")

    return sum(valid) / len(valid)


def _banking_metrics_score(
    row: Mapping[str, Any] | None,
    reasons: list[str],
    risks: list[str],
    missing: list[str],
) -> float | None:
    """Score bank-specific metrics from audited/manual CSV rows."""
    if not row:
        missing.append("manual banking metrics")
        return None

    parts: list[float] = []
    source = str(row.get("source") or "").strip()
    last_updated = str(row.get("last_updated") or "").strip()
    if source:
        reasons.append(
            "Manual banking metrics source: "
            + source
            + (f" (updated {last_updated})." if last_updated else ".")
        )
    else:
        missing.append("banking metrics source")

    nim = _num(row.get("nim_pct"))
    if nim is None:
        missing.append("NIM")
    else:
        parts.append(_scale(nim, 2.5, 5.0))
        if nim >= 3.5:
            reasons.append(f"Banking NIM is healthy at {nim:.2f}%.")

    gnpa = _num(row.get("gnpa_pct"))
    if gnpa is None:
        missing.append("GNPA")
    else:
        parts.append(_inverse_scale(gnpa, 0.0, 8.0))
        if gnpa <= 2.0:
            reasons.append(f"GNPA is contained at {gnpa:.2f}%.")
        elif gnpa >= 5.0:
            risks.append(f"GNPA is elevated at {gnpa:.2f}%.")

    nnpa = _num(row.get("nnpa_pct"))
    if nnpa is None:
        missing.append("NNPA")
    else:
        parts.append(_inverse_scale(nnpa, 0.0, 4.0))
        if nnpa <= 1.0:
            reasons.append(f"NNPA is contained at {nnpa:.2f}%.")
        elif nnpa >= 2.0:
            risks.append(f"NNPA is elevated at {nnpa:.2f}%.")

    casa = _num(row.get("casa_pct"))
    if casa is None:
        missing.append("CASA")
    else:
        parts.append(_scale(casa, 25.0, 50.0))
        if casa >= 35.0:
            reasons.append(f"CASA ratio is supportive at {casa:.2f}%.")

    capital = _num(row.get("capital_adequacy_pct"))
    if capital is None:
        missing.append("capital adequacy")
    else:
        parts.append(_scale(capital, 12.0, 22.0))
        if capital >= 15.0:
            reasons.append(f"Capital adequacy is comfortable at {capital:.2f}%.")
        elif capital < 13.0:
            risks.append(f"Capital adequacy is thin at {capital:.2f}%.")

    credit_growth = _num(row.get("credit_growth_pct"))
    deposit_growth = _num(row.get("deposit_growth_pct"))
    growth_score = _bank_growth_balance_score(credit_growth, deposit_growth)
    if growth_score is None:
        missing.append("credit/deposit growth")
    else:
        parts.append(growth_score)
        if (
            credit_growth is not None
            and deposit_growth is not None
            and credit_growth > deposit_growth + 8.0
        ):
            risks.append(
                "Credit growth is materially faster than deposit growth; funding mix needs review."
            )
        elif growth_score >= 70:
            reasons.append("Credit and deposit growth are reasonably balanced.")

    valid = [value for value in parts if value is not None]
    return sum(valid) / len(valid) if valid else None


def _bank_growth_balance_score(
    credit_growth: float | None,
    deposit_growth: float | None,
) -> float | None:
    if credit_growth is None or deposit_growth is None:
        return None
    credit_score = _scale(credit_growth, -5.0, 25.0)
    deposit_score = _scale(deposit_growth, -5.0, 25.0)
    if credit_score is None or deposit_score is None:
        return None
    gap_penalty = min(35.0, abs(credit_growth - deposit_growth) * 3.0)
    return max(0.0, ((credit_score + deposit_score) / 2.0) - gap_penalty)


def _technicals_score(
    row: Mapping[str, Any] | pd.Series | None,
    signals: list[SignalResult],
    reasons: list[str],
    risks: list[str],
    missing: list[str],
) -> float:
    parts: list[float] = []
    active = [signal for signal in signals if signal.active]
    parts.append(min(100.0, 40.0 + len(active) * 12.0))

    if active:
        reasons.append(
            "Active technical signals: " + ", ".join(signal.name for signal in active[:3]) + "."
        )
    else:
        risks.append("No prebuilt technical signal is currently active.")

    rsi = _num(_get(row, "rsi_14"))
    if rsi is None:
        missing.append("RSI")
    else:
        parts.append(_rsi_score(rsi))

    close = _num(_get(row, "close"))
    ema_50 = _num(_get(row, "ema_50"))
    ema_200 = _num(_get(row, "ema_200"))
    if close is None or ema_50 is None or ema_200 is None:
        missing.append("moving average context")
    elif close > ema_50 > ema_200:
        parts.append(85.0)
        reasons.append("Price is above 50 EMA and 200 EMA.")
    elif close > ema_200:
        parts.append(65.0)
    else:
        parts.append(35.0)
        risks.append("Price is below long-term moving-average support.")

    rel_volume = _num(_get(row, "relative_volume"))
    if rel_volume is not None:
        parts.append(_scale(rel_volume, 0.5, 2.0))

    return sum(parts) / len(parts) if parts else 50.0


def _flows_score(
    delivery: Mapping[str, Any] | None,
    reasons: list[str],
    risks: list[str],
    missing: list[str],
) -> float:
    if not delivery:
        missing.append("delivery and institutional flow data")
        return 50.0

    latest = _num(delivery.get("latest_pct"))
    ma20 = _num(delivery.get("ma20_pct"))
    trend = delivery.get("trend")
    parts = []
    if latest is None:
        missing.append("latest delivery percentage")
    else:
        parts.append(_scale(latest, 20, 65))
        if latest >= 50:
            reasons.append(f"Latest delivery percentage is elevated at {latest:.1f}%.")
    if ma20 is not None:
        parts.append(_scale(ma20, 20, 60))
    if trend == "rising":
        parts.append(75.0)
        reasons.append("Delivery trend is rising.")
    elif trend == "falling":
        parts.append(35.0)
        risks.append("Delivery trend is falling.")
    elif trend == "flat":
        parts.append(55.0)

    if delivery.get("unusual_today"):
        reasons.append("Unusual delivery spike detected today.")
    if not parts:
        return 50.0
    return sum(parts) / len(parts)


def _events_score(
    result_volatility: Mapping[str, Any] | None,
    reasons: list[str],
    risks: list[str],
    missing: list[str],
) -> float:
    if not result_volatility:
        missing.append("event-risk data")
        return 50.0

    multiple = _num(result_volatility.get("volatility_multiple"))
    if multiple is None:
        missing.append("result volatility multiple")
        return 50.0
    if multiple <= 1.1:
        reasons.append("Result-period volatility is close to baseline.")
        return 80.0
    if multiple <= 1.5:
        return 60.0
    risks.append(f"Result-period volatility is elevated at {multiple:.2f}x baseline.")
    return 35.0


def _macro_sector_score(
    row: Mapping[str, Any] | None,
    reasons: list[str],
    risks: list[str],
    missing: list[str],
) -> float:
    if not row:
        missing.append("sector context")
        return 50.0
    candidates = [
        _num(row.get("roe_pct_sector_rank")),
        _num(row.get("revenue_growth_pct_sector_rank")),
        _num(row.get("piotroski_f_score_sector_rank")),
    ]
    valid = [value for value in candidates if value is not None]
    if not valid:
        missing.append("sector relative strength context")
        return 50.0
    score = sum(valid) / len(valid)
    if score >= 70:
        reasons.append("Fundamental ranks are strong versus available peers.")
    elif score <= 35:
        risks.append("Fundamental ranks are weak versus available peers.")
    return score


def _score_band(score: float, bands: Mapping[str, Any]) -> str:
    strong = float(bands.get("strong_candidate", 80))
    watchlist = float(bands.get("watchlist", 60))
    neutral = float(bands.get("neutral", 40))
    if score >= strong:
        return "Strong research candidate"
    if score >= watchlist:
        return "Watchlist candidate"
    if score >= neutral:
        return "Neutral / needs more evidence"
    return "High caution"


def _normalise_weights(raw: Mapping[str, Any]) -> dict[str, float]:
    defaults = {
        "fundamentals": 0.35,
        "technicals": 0.30,
        "flows": 0.17,
        "events_quality": 0.12,
        "macro_sector": 0.06,
    }
    weights = {key: float(raw.get(key, defaults[key])) for key in defaults}
    total = sum(weights.values())
    return {key: value / total for key, value in weights.items()} if total else defaults


def _altman_score(value: float | None) -> float | None:
    if value is None:
        return None
    if value >= 3.0:
        return 100.0
    if value >= 1.8:
        return 55.0 + ((value - 1.8) / 1.2 * 35.0)
    return max(0.0, value / 1.8 * 45.0)


def _rsi_score(value: float) -> float:
    if 45 <= value <= 65:
        return 85.0
    if 35 <= value < 45 or 65 < value <= 75:
        return 65.0
    if value < 30 or value > 80:
        return 35.0
    return 50.0


def _scale(value: float | None, low: float, high: float) -> float | None:
    if value is None:
        return None
    if high == low:
        return 50.0
    return max(0.0, min(100.0, (value - low) / (high - low) * 100.0))


def _inverse_scale(value: float | None, low: float, high: float) -> float | None:
    scaled = _scale(value, low, high)
    return None if scaled is None else 100.0 - scaled


def _first_num(row: Mapping[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        value = _num(row.get(key))
        if value is not None:
            return value
    return None


def _get(row: Mapping[str, Any] | pd.Series | None, key: str) -> object:
    if row is None:
        return None
    return row.get(key) if hasattr(row, "get") else None


def _num(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(number) else number
