"""Persistence helpers for technical signal observations."""

from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import Engine, desc, inspect, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from stock_platform.analytics.signals import SignalResult
from stock_platform.analytics.technicals import add_technical_indicators
from stock_platform.db import create_all_tables, get_engine, get_session
from stock_platform.db.models import SignalAudit


def save_signal_audit(
    symbol: str,
    price_frame: pd.DataFrame,
    signals: list[SignalResult],
    *,
    source: str = "yfinance",
    engine: Engine | None = None,
) -> int:
    """Persist one audit row per signal result for the latest price bar."""
    if price_frame.empty or not signals:
        return 0

    active_engine = engine or get_engine()
    create_all_tables(active_engine)
    _ensure_signal_audit_schema(active_engine)
    technical_frame = add_technical_indicators(price_frame)
    latest = technical_frame.iloc[-1]
    as_of = _index_to_date(technical_frame.index[-1])

    with get_session(active_engine) as session:
        for signal in signals:
            existing = session.scalar(
                select(SignalAudit).where(
                    SignalAudit.symbol == symbol.upper(),
                    SignalAudit.as_of_date == as_of,
                    SignalAudit.signal_name == signal.name,
                    SignalAudit.source == source,
                )
            )
            if existing is None:
                session.add(
                    SignalAudit(
                        symbol=symbol.upper(),
                        as_of_date=as_of,
                        signal_name=signal.name,
                        active=signal.active,
                        strength=signal.strength,
                        detail=signal.detail,
                        trigger_price=signal.trigger_price,
                        entry_zone_low=signal.entry_zone_low,
                        entry_zone_high=signal.entry_zone_high,
                        stop_loss=signal.stop_loss,
                        target_price=signal.target_price,
                        risk_reward=signal.risk_reward,
                        confidence=signal.confidence,
                        close=_optional_float(latest.get("close")),
                        rsi_14=_optional_float(latest.get("rsi_14")),
                        ema_20=_optional_float(latest.get("ema_20")),
                        ema_50=_optional_float(latest.get("ema_50")),
                        ema_200=_optional_float(latest.get("ema_200")),
                        relative_volume=_optional_float(latest.get("relative_volume")),
                        source=source,
                        scan_count=1,
                    )
                )
                continue

            existing.active = signal.active
            existing.strength = signal.strength
            existing.detail = signal.detail
            existing.trigger_price = signal.trigger_price
            existing.entry_zone_low = signal.entry_zone_low
            existing.entry_zone_high = signal.entry_zone_high
            existing.stop_loss = signal.stop_loss
            existing.target_price = signal.target_price
            existing.risk_reward = signal.risk_reward
            existing.confidence = signal.confidence
            existing.close = _optional_float(latest.get("close"))
            existing.rsi_14 = _optional_float(latest.get("rsi_14"))
            existing.ema_20 = _optional_float(latest.get("ema_20"))
            existing.ema_50 = _optional_float(latest.get("ema_50"))
            existing.ema_200 = _optional_float(latest.get("ema_200"))
            existing.relative_volume = _optional_float(latest.get("relative_volume"))
            existing.scan_count += 1
    return len(signals)


def fetch_recent_signal_audits(
    symbol: str,
    *,
    limit: int = 50,
    engine: Engine | None = None,
) -> list[SignalAudit]:
    """Return recent signal audit rows for one symbol."""
    active_engine = engine or get_engine()
    create_all_tables(active_engine)
    _ensure_signal_audit_schema(active_engine)
    with Session(active_engine) as session:
        statement = (
            select(SignalAudit)
            .where(SignalAudit.symbol == symbol.upper())
            .order_by(desc(SignalAudit.created_at), desc(SignalAudit.id))
            .limit(limit)
        )
        return list(session.scalars(statement).all())


def fetch_signal_event_export(
    symbol: str | None = None,
    *,
    active_only: bool = True,
    limit: int = 1_000,
    engine: Engine | None = None,
) -> pd.DataFrame:
    """Return saved signal observations in a backtest-ready event table."""
    active_engine = engine or get_engine()
    create_all_tables(active_engine)
    _ensure_signal_audit_schema(active_engine)

    statement = select(SignalAudit).order_by(
        desc(SignalAudit.as_of_date),
        SignalAudit.symbol,
        SignalAudit.signal_name,
    )
    if symbol:
        statement = statement.where(SignalAudit.symbol == symbol.upper())
    if active_only:
        statement = statement.where(SignalAudit.active.is_(True))
    statement = statement.limit(limit)

    with Session(active_engine) as session:
        return signal_events_to_frame(list(session.scalars(statement).all()))


def audits_to_frame(rows: list[SignalAudit]) -> pd.DataFrame:
    """Convert audit rows to a UI-friendly DataFrame."""
    return pd.DataFrame(
        [
            {
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "scan_count": row.scan_count,
                "as_of_date": row.as_of_date,
                "symbol": row.symbol,
                "signal": row.signal_name,
                "status": "Active" if row.active else "Inactive",
                "type": row.strength,
                "close": row.close,
                "trigger_price": row.trigger_price,
                "entry_zone_low": row.entry_zone_low,
                "entry_zone_high": row.entry_zone_high,
                "stop_loss": row.stop_loss,
                "target_price": row.target_price,
                "risk_reward": row.risk_reward,
                "confidence": row.confidence,
                "rsi_14": row.rsi_14,
                "relative_volume": row.relative_volume,
                "detail": row.detail,
                "source": row.source,
            }
            for row in rows
        ]
    )


def signal_events_to_frame(rows: list[SignalAudit]) -> pd.DataFrame:
    """Convert audit rows to a stable CSV shape for later backtesting."""
    columns = [
        "event_date",
        "symbol",
        "signal",
        "active",
        "strength",
        "close",
        "trigger_price",
        "entry_zone_low",
        "entry_zone_high",
        "stop_loss",
        "target_price",
        "risk_reward",
        "confidence",
        "rsi_14",
        "ema_20",
        "ema_50",
        "ema_200",
        "relative_volume",
        "source",
        "scan_count",
        "observed_at",
        "detail",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)

    frame = pd.DataFrame(
        [
            {
                "event_date": row.as_of_date,
                "symbol": row.symbol,
                "signal": row.signal_name,
                "active": row.active,
                "strength": row.strength,
                "close": row.close,
                "trigger_price": row.trigger_price,
                "entry_zone_low": row.entry_zone_low,
                "entry_zone_high": row.entry_zone_high,
                "stop_loss": row.stop_loss,
                "target_price": row.target_price,
                "risk_reward": row.risk_reward,
                "confidence": row.confidence,
                "rsi_14": row.rsi_14,
                "ema_20": row.ema_20,
                "ema_50": row.ema_50,
                "ema_200": row.ema_200,
                "relative_volume": row.relative_volume,
                "source": row.source,
                "scan_count": row.scan_count,
                "observed_at": row.updated_at or row.created_at,
                "detail": row.detail,
            }
            for row in rows
        ],
        columns=columns,
    )
    return frame.sort_values(["event_date", "symbol", "signal"]).reset_index(drop=True)


def _index_to_date(value: object) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if hasattr(value, "date"):
        return value.date()
    raise TypeError(f"Unsupported index value for signal audit date: {value!r}")


def _optional_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _ensure_signal_audit_schema(engine: Engine) -> None:
    """Apply lightweight SQLite-compatible migrations for local MVP use."""
    inspector = inspect(engine)
    if not inspector.has_table("signal_audit"):
        return

    columns = {column["name"] for column in inspector.get_columns("signal_audit")}
    with engine.begin() as connection:
        if "updated_at" not in columns:
            connection.execute(text("ALTER TABLE signal_audit ADD COLUMN updated_at DATETIME"))
            connection.execute(text("UPDATE signal_audit SET updated_at = created_at"))
        if "scan_count" not in columns:
            connection.execute(
                text("ALTER TABLE signal_audit ADD COLUMN scan_count INTEGER NOT NULL DEFAULT 1")
            )
        for column in (
            "trigger_price",
            "entry_zone_low",
            "entry_zone_high",
            "stop_loss",
            "target_price",
            "risk_reward",
            "confidence",
        ):
            if column not in columns:
                connection.execute(text(f"ALTER TABLE signal_audit ADD COLUMN {column} FLOAT"))
        _collapse_duplicate_signal_rows(connection)
    indexes = {index["name"] for index in inspect(engine).get_indexes("signal_audit")}
    if "uq_signal_audit_observation" not in indexes:
        try:
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS uq_signal_audit_observation "
                        "ON signal_audit (symbol, as_of_date, signal_name, source)"
                    )
                )
        except IntegrityError:
            # Existing local MVP databases may contain legacy duplicate rows with
            # SQLite typing quirks. The app-level upsert still prevents new
            # duplicates; a future Alembic migration can rebuild the table if needed.
            return


def _collapse_duplicate_signal_rows(connection) -> None:
    rows = connection.execute(
        text(
            """
            SELECT id, symbol, as_of_date, signal_name, source, COALESCE(scan_count, 1) AS scan_count
            FROM signal_audit
            ORDER BY id
            """
        )
    ).mappings()

    groups: dict[tuple[object, object, object, object], list[dict[str, object]]] = {}
    for row in rows:
        key = (row["symbol"], row["as_of_date"], row["signal_name"], row["source"])
        groups.setdefault(key, []).append(dict(row))

    for duplicate_rows in groups.values():
        if len(duplicate_rows) <= 1:
            continue
        keep = duplicate_rows[-1]
        total_scan_count = sum(int(row["scan_count"] or 1) for row in duplicate_rows)
        connection.execute(
            text("UPDATE signal_audit SET scan_count = :scan_count WHERE id = :id"),
            {"scan_count": total_scan_count, "id": keep["id"]},
        )
        for row in duplicate_rows[:-1]:
            connection.execute(text("DELETE FROM signal_audit WHERE id = :id"), {"id": row["id"]})
