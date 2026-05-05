"""Persistence helpers for read-only strategy scanner results."""

from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import Engine, desc, select
from sqlalchemy.orm import selectinload

from stock_platform.analytics.scanner.result_schema import (
    StrategyScanResult,
    strategy_results_to_frame,
)
from stock_platform.analytics.scanner.strategy_scanner import StrategyScanSummary
from stock_platform.db import create_all_tables, get_engine, get_session
from stock_platform.db.models import StrategyScanResult as StrategyScanResultModel
from stock_platform.db.models import StrategyScanRun


def save_strategy_scan(
    *,
    universe_name: str,
    summary: StrategyScanSummary,
    min_confidence_filter: float | None = None,
    min_rr_filter: float | None = None,
    source: str = "persisted_eod",
    note: str | None = None,
    engine: Engine | None = None,
) -> int:
    """Persist one strategy scan run and all matching setup rows."""
    active_engine = engine or get_engine()
    create_all_tables(active_engine)

    with get_session(active_engine) as session:
        run = StrategyScanRun(
            universe_name=universe_name,
            requested_symbols=summary.requested_symbols,
            scanned_symbols=summary.scanned_symbols,
            failed_symbols=summary.failed_symbols,
            result_count=len(summary.results),
            min_confidence_filter=min_confidence_filter,
            min_rr_filter=min_rr_filter,
            source=source,
            note=note,
            errors_json=json.dumps(summary.errors),
        )
        session.add(run)
        session.flush()

        for result in summary.results:
            session.add(_result_to_model(run.id, result))

        return int(run.id)


def fetch_latest_strategy_scan(
    universe_name: str | None = None,
    *,
    engine: Engine | None = None,
) -> StrategyScanRun | None:
    """Return the latest saved strategy scan run."""
    active_engine = engine or get_engine()
    create_all_tables(active_engine)

    statement = (
        select(StrategyScanRun)
        .options(selectinload(StrategyScanRun.results))
        .order_by(desc(StrategyScanRun.created_at), desc(StrategyScanRun.id))
        .limit(1)
    )
    if universe_name:
        statement = statement.where(StrategyScanRun.universe_name == universe_name)

    with get_session(active_engine) as session:
        run = session.scalar(statement)
        if run is None:
            return None
        _ = list(run.results)
        session.expunge_all()
        return run


def strategy_scan_storage_to_frame(run: StrategyScanRun | None) -> pd.DataFrame:
    """Convert a saved strategy scan run into a UI-ready DataFrame."""
    if run is None or not run.results:
        return strategy_results_to_frame([])

    results = [_model_to_result(row) for row in run.results]
    return strategy_results_to_frame(results)


def strategy_scan_errors(run: StrategyScanRun | None) -> dict[str, str]:
    """Return saved per-symbol scan errors."""
    if run is None or not run.errors_json:
        return {}
    try:
        parsed = json.loads(run.errors_json)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {str(key): str(value) for key, value in parsed.items()}


def _result_to_model(run_id: int, result: StrategyScanResult) -> StrategyScanResultModel:
    return StrategyScanResultModel(
        run_id=run_id,
        symbol=result.symbol,
        company_name=result.company_name,
        sector=result.sector,
        strategy=result.strategy,
        setup_type=result.setup_type,
        signal_date=result.signal_date,
        close=result.close,
        entry_zone_low=result.entry_zone_low,
        entry_zone_high=result.entry_zone_high,
        stop_loss=result.stop_loss,
        target_price=result.target_price,
        risk_reward=result.risk_reward,
        rsi=result.rsi,
        trend_status=result.trend_status,
        relative_volume=result.relative_volume,
        atr_pct=result.atr_pct,
        liquidity_status=result.liquidity_status,
        data_source=result.data_source,
        data_freshness=result.data_freshness,
        confidence_score=result.confidence_score,
        why_this_appeared=result.why_this_appeared,
        key_risk=result.key_risk,
        data_trust=result.data_trust,
        market_cap_bucket=result.market_cap_bucket,
        ema_20=result.ema_20,
        ema_50=result.ema_50,
        ema_100=result.ema_100,
        ema_200=result.ema_200,
        breakout_level=result.breakout_level,
        avg_traded_value_cr=result.avg_traded_value_cr,
        warnings_json=json.dumps(list(result.warnings)),
        provider_fallback_reason=result.provider_fallback_reason,
    )


def _model_to_result(row: StrategyScanResultModel) -> StrategyScanResult:
    return StrategyScanResult(
        symbol=row.symbol,
        strategy=row.strategy,
        setup_type=row.setup_type,
        signal_date=row.signal_date,
        close=row.close,
        entry_zone_low=row.entry_zone_low,
        entry_zone_high=row.entry_zone_high,
        stop_loss=row.stop_loss,
        target_price=row.target_price,
        risk_reward=row.risk_reward,
        rsi=row.rsi,
        trend_status=row.trend_status or "",
        relative_volume=row.relative_volume,
        atr_pct=row.atr_pct,
        liquidity_status=row.liquidity_status or "",
        data_source=row.data_source,
        data_freshness=row.data_freshness or "",
        confidence_score=row.confidence_score,
        why_this_appeared=row.why_this_appeared,
        key_risk=row.key_risk,
        data_trust=row.data_trust,
        company_name=row.company_name,
        sector=row.sector,
        market_cap_bucket=row.market_cap_bucket,
        ema_20=row.ema_20,
        ema_50=row.ema_50,
        ema_100=row.ema_100,
        ema_200=row.ema_200,
        breakout_level=row.breakout_level,
        avg_traded_value_cr=row.avg_traded_value_cr,
        warnings=tuple(_json_list(row.warnings_json)),
        provider_fallback_reason=row.provider_fallback_reason or "",
    )


def _json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]
