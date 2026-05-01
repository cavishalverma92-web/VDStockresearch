"""Tests for the Kite instrument-master sync job."""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from stock_platform.data.repositories.instruments import find_instrument_token
from stock_platform.db.models import Base, InstrumentMaster
from stock_platform.jobs.sync_instruments import sync_instruments


class _FakeKite:
    """Duck-typed stand-in for ``KiteProvider``.

    Only exposes the four methods the job touches; never returns or accepts
    portfolio / order data.
    """

    def __init__(
        self,
        *,
        configured: bool = True,
        token: bool = True,
        frame: pd.DataFrame | None = None,
        raises: Exception | None = None,
    ) -> None:
        self._configured = configured
        self._token = token
        self._frame = frame if frame is not None else _sample_frame()
        self._raises = raises
        self.calls: list[str] = []

    def is_configured(self) -> bool:
        return self._configured

    def has_access_token(self) -> bool:
        return self._token

    def get_instruments(self, exchange: str) -> pd.DataFrame:
        self.calls.append(exchange)
        if self._raises is not None:
            raise self._raises
        return self._frame.copy()


def _sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "instrument_token": 738561,
                "exchange_token": 2885,
                "tradingsymbol": "RELIANCE",
                "name": "Reliance Industries",
                "exchange": "NSE",
                "segment": "NSE",
                "instrument_type": "EQ",
                "tick_size": 0.05,
                "lot_size": 1,
                "expiry": pd.NaT,
                "strike": 0.0,
            },
            {
                "instrument_token": 408065,
                "exchange_token": 1594,
                "tradingsymbol": "INFY",
                "name": "Infosys",
                "exchange": "NSE",
                "segment": "NSE",
                "instrument_type": "EQ",
                "tick_size": 0.05,
                "lot_size": 1,
                "expiry": pd.NaT,
                "strike": 0.0,
            },
        ]
    )


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


def test_sync_instruments_persists_rows(engine, tmp_path) -> None:
    kite = _FakeKite()
    summary = sync_instruments(
        kite_provider=kite,
        engine=engine,
        csv_root=tmp_path,
    )

    assert summary.error is None
    assert summary.fetched_rows == 2
    assert summary.inserted == 2
    assert summary.updated == 0
    assert summary.exchange == "NSE"
    assert kite.calls == ["NSE"]

    with Session(engine) as session:
        rows = session.query(InstrumentMaster).order_by(InstrumentMaster.tradingsymbol).all()
        assert [r.tradingsymbol for r in rows] == ["INFY", "RELIANCE"]
        assert find_instrument_token(session, "RELIANCE") == 738561
        assert find_instrument_token(session, "INFY") == 408065


def test_sync_instruments_is_idempotent(engine, tmp_path) -> None:
    kite = _FakeKite()
    sync_instruments(kite_provider=kite, engine=engine, csv_root=tmp_path)

    summary = sync_instruments(kite_provider=kite, engine=engine, csv_root=tmp_path)

    assert summary.inserted == 0
    assert summary.updated == 2

    with Session(engine) as session:
        assert session.query(InstrumentMaster).count() == 2


def test_sync_instruments_writes_csv_snapshots(engine, tmp_path) -> None:
    kite = _FakeKite()
    summary = sync_instruments(
        kite_provider=kite,
        engine=engine,
        csv_root=tmp_path,
    )

    assert summary.raw_csv_path is not None
    assert summary.processed_csv_path is not None
    assert summary.cache_csv_path is not None
    assert summary.raw_csv_path.exists()
    assert summary.processed_csv_path.exists()
    assert summary.cache_csv_path.exists()

    # Stable processed/cache filenames (no timestamp).
    assert summary.processed_csv_path.name == "nse_instruments.csv"
    assert summary.cache_csv_path.name == "nse_instruments_latest.csv"
    # Raw filename is timestamped.
    assert "nse_instruments_" in summary.raw_csv_path.name
    assert summary.raw_csv_path.name != "nse_instruments.csv"

    cached = pd.read_csv(summary.cache_csv_path)
    assert set(cached["tradingsymbol"]) == {"RELIANCE", "INFY"}


def test_sync_instruments_skips_csv_when_disabled(engine, tmp_path) -> None:
    kite = _FakeKite()
    summary = sync_instruments(
        kite_provider=kite,
        engine=engine,
        write_csv_snapshot=False,
        csv_root=tmp_path,
    )

    assert summary.raw_csv_path is None
    assert summary.processed_csv_path is None
    assert summary.cache_csv_path is None
    assert summary.inserted == 2  # DB still populated


def test_sync_instruments_reports_missing_credentials(engine, tmp_path) -> None:
    kite = _FakeKite(configured=False)
    summary = sync_instruments(
        kite_provider=kite,
        engine=engine,
        csv_root=tmp_path,
    )

    assert summary.error is not None
    assert "KITE_API_KEY" in summary.error
    assert summary.fetched_rows == 0
    with Session(engine) as session:
        assert session.query(InstrumentMaster).count() == 0


def test_sync_instruments_reports_missing_access_token(engine, tmp_path) -> None:
    kite = _FakeKite(token=False)
    summary = sync_instruments(
        kite_provider=kite,
        engine=engine,
        csv_root=tmp_path,
    )

    assert summary.error is not None
    assert "ACCESS_TOKEN" in summary.error
    assert kite.calls == []  # never reached the network


def test_sync_instruments_handles_provider_failure(engine, tmp_path) -> None:
    kite = _FakeKite(raises=RuntimeError("kite gateway 502"))
    summary = sync_instruments(
        kite_provider=kite,
        engine=engine,
        csv_root=tmp_path,
    )

    assert summary.error is not None
    assert "502" in summary.error
    assert summary.fetched_rows == 0
    with Session(engine) as session:
        assert session.query(InstrumentMaster).count() == 0


def test_sync_instruments_normalises_exchange_casing(engine, tmp_path) -> None:
    kite = _FakeKite()
    summary = sync_instruments(
        exchange="nse",
        kite_provider=kite,
        engine=engine,
        csv_root=tmp_path,
    )

    assert summary.exchange == "NSE"
    assert kite.calls == ["NSE"]
