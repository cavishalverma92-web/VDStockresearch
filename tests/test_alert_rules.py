from stock_platform.alerts import alert_candidates_to_frame, build_alert_candidates
from stock_platform.analytics.signals import SignalResult
from stock_platform.scoring import CompositeScore


def _score(value: float = 82.0) -> CompositeScore:
    return CompositeScore(
        symbol="RELIANCE.NS",
        score=value,
        band="Strong",
        sub_scores={
            "fundamentals": 70.0,
            "technicals": 90.0,
            "flows": 70.0,
            "events_quality": 80.0,
            "macro_sector": 50.0,
        },
        reasons=[],
        risks=[],
        missing_data=[],
    )


def test_build_alert_candidates_marks_research_output_not_advice():
    signals = [
        SignalResult(
            name="Breakout with Volume",
            active=True,
            detail="Close is above resistance with high relative volume.",
            trigger_price=100.0,
            entry_zone_low=98.0,
            entry_zone_high=102.0,
            stop_loss=94.0,
            risk_reward=2.5,
        )
    ]

    candidates = build_alert_candidates(
        symbol="RELIANCE.NS",
        composite=_score(),
        signals=signals,
    )

    assert [candidate.severity for candidate in candidates] == [
        "research_candidate",
        "signal_active",
    ]
    assert all("not investment advice" in candidate.message.lower() for candidate in candidates)


def test_build_alert_candidates_includes_data_quality_warning():
    candidates = build_alert_candidates(
        symbol="RELIANCE.NS",
        composite=_score(55.0),
        signals=[],
        data_warnings=["Missing volume on one row"],
    )

    assert len(candidates) == 1
    assert candidates[0].severity == "data_quality"
    assert "Missing volume" in candidates[0].message


def test_alert_candidates_to_frame_has_stable_columns_for_empty_state():
    frame = alert_candidates_to_frame([])

    assert list(frame.columns) == ["symbol", "severity", "title", "message", "source"]
    assert frame.empty
