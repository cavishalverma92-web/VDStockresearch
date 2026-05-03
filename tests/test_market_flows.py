"""Tests for FII/DII market flows: provider, repo, analytics, job."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from stock_platform.analytics.flows.institutional import (
    compute_institutional_flow_snapshots,
    institutional_flow_score,
)
from stock_platform.data.providers.nse_market_flows import (
    parse_fii_dii_payload,
)
from stock_platform.data.repositories.market_flows import (
    fetch_market_flows,
    latest_market_flow_date,
    upsert_market_flows,
)
from stock_platform.db.models import Base, MarketFlowDaily
from stock_platform.jobs.refresh_market_flows import refresh_market_flows


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


# ---------------------------------------------------------------------------
# Provider parser
# ---------------------------------------------------------------------------


def test_parse_payload_normal_shape():
    payload = [
        {
            "category": "FII/FPI *",
            "date": "11-Apr-2024",
            "buyValue": "1234.56",
            "sellValue": "1100.00",
            "netValue": "134.56",
        },
        {
            "category": "DII **",
            "date": "11-Apr-2024",
            "buyValue": "800.00",
            "sellValue": "700.00",
            "netValue": "100.00",
        },
    ]
    frame = parse_fii_dii_payload(payload)
    assert len(frame) == 2
    assert set(frame["participant"]) == {"FII", "DII"}
    fii = frame[frame["participant"] == "FII"].iloc[0]
    assert fii["buy_value_cr"] == 1234.56
    assert fii["net_value_cr"] == 134.56
    assert fii["trade_date"] == date(2024, 4, 11)


def test_parse_payload_dict_with_data_key():
    payload = {
        "data": [
            {
                "category": "FII",
                "date": "2024-04-11",
                "buyValue": 100,
                "sellValue": 50,
                "netValue": 50,
            }
        ]
    }
    frame = parse_fii_dii_payload(payload)
    assert len(frame) == 1
    assert frame.iloc[0]["participant"] == "FII"


def test_parse_payload_skips_unknown_categories():
    payload = [
        {"category": "Mutual Funds", "date": "11-Apr-2024", "netValue": 50},
        {"category": "FII", "date": "11-Apr-2024", "netValue": 100},
    ]
    frame = parse_fii_dii_payload(payload)
    assert list(frame["participant"]) == ["FII"]


def test_parse_payload_empty_returns_empty_frame():
    assert parse_fii_dii_payload([]).empty
    assert parse_fii_dii_payload({"data": []}).empty
    assert parse_fii_dii_payload("not json-like").empty


def test_parse_payload_handles_comma_numbers():
    payload = [{"category": "FII", "date": "11-Apr-2024", "buyValue": "12,345.67"}]
    frame = parse_fii_dii_payload(payload)
    assert frame.iloc[0]["buy_value_cr"] == 12345.67


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trade_date": [date(2025, 1, 6), date(2025, 1, 6)],
            "participant": ["FII", "DII"],
            "buy_value_cr": [1000.0, 800.0],
            "sell_value_cr": [900.0, 700.0],
            "net_value_cr": [100.0, 100.0],
        }
    )


def test_upsert_inserts_then_updates(engine):
    with Session(engine) as session:
        s1 = upsert_market_flows(session, _frame(), source="nse")
        session.commit()
    assert s1.inserted == 2

    revised = _frame()
    revised.loc[0, "net_value_cr"] = 999.0
    with Session(engine) as session:
        s2 = upsert_market_flows(session, revised, source="nse")
        session.commit()
    assert s2.inserted == 0
    assert s2.updated == 2

    with Session(engine) as session:
        out = fetch_market_flows(session)
    fii = out[out["participant"] == "FII"].iloc[0]
    assert fii["net_value_cr"] == 999.0


def test_upsert_skips_invalid_rows(engine):
    bad = pd.DataFrame(
        {
            "trade_date": [None, date(2025, 1, 6)],
            "participant": ["FII", ""],
            "net_value_cr": [100.0, 200.0],
        }
    )
    with Session(engine) as session:
        s = upsert_market_flows(session, bad, source="nse")
    assert s.inserted == 0
    assert s.skipped == 2


def test_latest_market_flow_date_returns_max(engine):
    with Session(engine) as session:
        upsert_market_flows(
            session,
            pd.DataFrame(
                {
                    "trade_date": [date(2025, 1, 6), date(2025, 1, 7)],
                    "participant": ["FII", "FII"],
                    "net_value_cr": [10.0, 20.0],
                }
            ),
            source="nse",
        )
        session.commit()
    with Session(engine) as session:
        assert latest_market_flow_date(session) == date(2025, 1, 7)


def test_fetch_filters(engine):
    with Session(engine) as session:
        upsert_market_flows(session, _frame(), source="nse")
        session.commit()
    with Session(engine) as session:
        only_fii = fetch_market_flows(session, participant="FII")
    assert list(only_fii["participant"]) == ["FII"]


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------


def _history(net_values: list[float], participant: str = "FII") -> pd.DataFrame:
    dates = pd.date_range(end="2025-01-31", periods=len(net_values), freq="B")
    return pd.DataFrame(
        {
            "trade_date": dates,
            "participant": [participant] * len(net_values),
            "net_value_cr": net_values,
            "buy_value_cr": [v + 50 for v in net_values],
            "sell_value_cr": [50.0] * len(net_values),
        }
    )


def test_snapshots_bullish_when_both_windows_positive():
    frame = _history([100.0] * 25, participant="FII")
    snaps = compute_institutional_flow_snapshots(frame)
    assert "FII" in snaps
    s = snaps["FII"]
    assert s.trend == "bullish"
    assert s.rolling_5d_net_cr == 500.0
    assert s.rolling_20d_net_cr == 2000.0


def test_snapshots_bearish_when_both_windows_negative():
    frame = _history([-50.0] * 25, participant="DII")
    snaps = compute_institutional_flow_snapshots(frame)
    assert snaps["DII"].trend == "bearish"


def test_snapshots_neutral_on_mixed_signs():
    # Recent strong inflows, longer window net negative
    values = [-100.0] * 20 + [200.0] * 5
    frame = _history(values, participant="FII")
    snaps = compute_institutional_flow_snapshots(frame)
    assert snaps["FII"].trend == "neutral"


def test_snapshots_unknown_when_history_too_short():
    frame = _history([100.0, 50.0])
    snaps = compute_institutional_flow_snapshots(frame)
    assert snaps["FII"].trend == "unknown"


def test_score_both_bullish_returns_high():
    fii_frame = _history([100.0] * 25, participant="FII")
    dii_frame = _history([100.0] * 25, participant="DII")
    snaps = compute_institutional_flow_snapshots(pd.concat([fii_frame, dii_frame]))
    score = institutional_flow_score(snaps)
    assert score == 80.0


def test_score_mixed_returns_neutral():
    fii_frame = _history([100.0] * 25, participant="FII")
    dii_frame = _history([-100.0] * 25, participant="DII")
    snaps = compute_institutional_flow_snapshots(pd.concat([fii_frame, dii_frame]))
    score = institutional_flow_score(snaps)
    assert score == 50.0


def test_score_returns_none_when_no_data():
    assert institutional_flow_score({}) is None


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


def test_refresh_persists_fetched_rows(engine):
    fixed = pd.DataFrame(
        {
            "trade_date": [date(2025, 1, 6), date(2025, 1, 6)],
            "participant": ["FII", "DII"],
            "buy_value_cr": [1000.0, 800.0],
            "sell_value_cr": [900.0, 700.0],
            "net_value_cr": [100.0, 100.0],
        }
    )
    summary = refresh_market_flows(fetcher=lambda: fixed, engine=engine)
    assert summary.inserted == 2
    assert summary.error is None

    with Session(engine) as session:
        rows = session.query(MarketFlowDaily).all()
    assert len(rows) == 2


def test_refresh_dry_run_does_not_write(engine):
    fixed = pd.DataFrame(
        {
            "trade_date": [date(2025, 1, 6)],
            "participant": ["FII"],
            "net_value_cr": [100.0],
        }
    )
    summary = refresh_market_flows(fetcher=lambda: fixed, engine=engine, dry_run=True)
    assert summary.dry_run is True
    assert summary.rows_fetched == 1
    with Session(engine) as session:
        assert session.query(MarketFlowDaily).count() == 0


def test_refresh_handles_provider_failure(engine):
    def boom() -> pd.DataFrame:
        raise RuntimeError("nse blocked")

    summary = refresh_market_flows(fetcher=boom, engine=engine)
    assert summary.inserted == 0
    assert "nse blocked" in (summary.error or "")


def test_refresh_handles_empty_fetch(engine):
    summary = refresh_market_flows(fetcher=lambda: pd.DataFrame(), engine=engine)
    assert summary.inserted == 0
    assert summary.error is None
