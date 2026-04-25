"""Tests for signal audit persistence."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text

from stock_platform.analytics.signals import scan_technical_signals
from stock_platform.analytics.signals.audit import (
    audits_to_frame,
    fetch_recent_signal_audits,
    fetch_signal_event_export,
    save_signal_audit,
)
from stock_platform.db.models import Base


def _price_frame(n: int = 260) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    close = pd.Series(100.0, index=idx)
    close.iloc[-1] = 120.0
    high = close + 1
    high.iloc[:-1] = 110.0
    volume = pd.Series(100_000, index=idx)
    volume.iloc[-1] = 300_000
    return pd.DataFrame(
        {
            "open": close - 1,
            "high": high,
            "low": close - 2,
            "close": close,
            "adj_close": close,
            "volume": volume,
        },
        index=idx,
    )


def test_save_and_fetch_signal_audits() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    frame = _price_frame()
    signals = scan_technical_signals(
        frame,
        thresholds={
            "breakout_with_volume": {"lookback_days_for_high": 252, "volume_multiple": 2.0},
            "darvas_base_breakout": {"min_consolidation_days": 20, "max_range_pct": 15.0},
        },
    )

    saved = save_signal_audit("TEST.NS", frame, signals, source="unit_test", engine=engine)
    rows = fetch_recent_signal_audits("TEST.NS", engine=engine)
    history = audits_to_frame(rows)

    assert saved == len(signals)
    assert len(rows) == len(signals)
    assert set(history["symbol"]) == {"TEST.NS"}
    assert "Breakout With Volume" in set(history["signal"])
    assert history["source"].eq("unit_test").all()
    assert history["scan_count"].eq(1).all()


def test_signal_audit_upserts_same_symbol_date_signal_source() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    frame = _price_frame()
    signals = scan_technical_signals(
        frame,
        thresholds={
            "breakout_with_volume": {"lookback_days_for_high": 252, "volume_multiple": 2.0},
            "darvas_base_breakout": {"min_consolidation_days": 20, "max_range_pct": 15.0},
        },
    )

    first_saved = save_signal_audit("TEST.NS", frame, signals, source="unit_test", engine=engine)
    second_saved = save_signal_audit("TEST.NS", frame, signals, source="unit_test", engine=engine)
    rows = fetch_recent_signal_audits("TEST.NS", engine=engine)
    history = audits_to_frame(rows)

    assert first_saved == len(signals)
    assert second_saved == len(signals)
    assert len(rows) == len(signals)
    assert history["scan_count"].eq(2).all()


def test_save_signal_audit_noops_without_prices() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    saved = save_signal_audit("TEST.NS", pd.DataFrame(), [], engine=engine)

    assert saved == 0
    assert fetch_recent_signal_audits("TEST.NS", engine=engine) == []


def test_fetch_collapses_legacy_duplicate_signal_rows() -> None:
    engine = create_engine("sqlite:///:memory:")
    frame = _price_frame()
    as_of_date = frame.index[-1].date()

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE signal_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol VARCHAR(32) NOT NULL,
                    as_of_date DATE NOT NULL,
                    signal_name VARCHAR(120) NOT NULL,
                    active BOOLEAN NOT NULL,
                    strength VARCHAR(40) NOT NULL,
                    detail TEXT NOT NULL,
                    close FLOAT,
                    rsi_14 FLOAT,
                    ema_20 FLOAT,
                    ema_50 FLOAT,
                    ema_200 FLOAT,
                    relative_volume FLOAT,
                    source VARCHAR(80) NOT NULL,
                    created_at DATETIME
                )
                """
            )
        )
        for _ in range(2):
            connection.execute(
                text(
                    """
                    INSERT INTO signal_audit (
                        symbol, as_of_date, signal_name, active, strength, detail, source, created_at
                    )
                    VALUES (
                        'TEST.NS', :as_of_date, 'Breakout With Volume', 1, 'breakout',
                        'legacy duplicate', 'unit_test', '2026-01-01 00:00:00'
                    )
                    """
                ),
                {"as_of_date": as_of_date.isoformat()},
            )

    rows = fetch_recent_signal_audits("TEST.NS", engine=engine)

    assert len(rows) == 1
    assert rows[0].scan_count == 2


def test_signal_event_export_returns_active_backtest_rows() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    frame = _price_frame()
    signals = scan_technical_signals(
        frame,
        thresholds={
            "breakout_with_volume": {"lookback_days_for_high": 252, "volume_multiple": 2.0},
            "darvas_base_breakout": {"min_consolidation_days": 20, "max_range_pct": 15.0},
        },
    )
    save_signal_audit("TEST.NS", frame, signals, source="unit_test", engine=engine)

    export = fetch_signal_event_export("TEST.NS", engine=engine)
    full_export = fetch_signal_event_export("TEST.NS", active_only=False, engine=engine)

    assert not export.empty
    assert list(export.columns) == [
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
    assert export["active"].all()
    assert set(export["symbol"]) == {"TEST.NS"}
    assert "Breakout With Volume" in set(export["signal"])
    assert export["risk_reward"].dropna().eq(2.5).all()
    assert len(full_export) == len(signals)
