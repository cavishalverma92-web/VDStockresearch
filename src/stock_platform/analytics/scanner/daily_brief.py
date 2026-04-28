"""Daily research brief built from saved universe scans and shortlist rows."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import Engine

from stock_platform.analytics.scanner.persistence import compare_latest_universe_scans
from stock_platform.analytics.scanner.watchlist import (
    enrich_watchlist_with_latest_scores,
    fetch_watchlist_items,
    watchlist_to_frame,
)


@dataclass(frozen=True)
class DailyResearchBrief:
    """One dashboard-ready summary of the latest saved scan state."""

    universe_name: str
    latest_run_id: int | None
    previous_run_id: int | None
    latest_run_at: object | None
    requested_symbols: int
    successful_symbols: int
    failed_symbols: int
    average_score: float | None
    top_score: float | None
    improved: pd.DataFrame
    weakened: pd.DataFrame
    new_opportunities: pd.DataFrame
    new_signals: pd.DataFrame
    data_quality_actions: pd.DataFrame
    shortlist_actions: pd.DataFrame

    @property
    def has_latest_scan(self) -> bool:
        return self.latest_run_id is not None


def build_daily_research_brief(
    universe_name: str,
    *,
    min_opportunity_score: float = 60.0,
    meaningful_score_change: float = 5.0,
    engine: Engine | None = None,
) -> DailyResearchBrief:
    """Build the Phase 8.5 daily brief from persisted local state.

    This intentionally avoids fresh network calls. A daily brief should explain
    what the latest saved scan already found; running a new scan remains an
    explicit user action.
    """
    latest, previous, comparison = compare_latest_universe_scans(universe_name, engine=engine)
    successful = _successful_rows(comparison)
    score_changes = _score_change_series(successful)

    improved = _top_rows(
        successful[score_changes >= meaningful_score_change],
        sort_by="score_change",
    )
    weakened = _top_rows(
        successful[score_changes <= -meaningful_score_change],
        sort_by="score_change",
        ascending=True,
    )
    new_opportunities = _top_rows(
        successful[
            successful["comparison_status"].isin(["new symbol", "new scan row"])
            & (successful["composite_score"].fillna(0) >= min_opportunity_score)
        ],
        sort_by="composite_score",
    )
    new_signals = _top_rows(
        successful[successful["new_active_signals"].fillna("").astype(str).str.strip() != ""],
        sort_by="score_change",
    )
    data_quality_actions = _data_quality_actions(comparison)
    shortlist_actions = _shortlist_actions(engine=engine)

    average_score = (
        round(float(successful["composite_score"].dropna().mean()), 1)
        if not successful.empty and not successful["composite_score"].dropna().empty
        else None
    )
    top_score = (
        round(float(successful["composite_score"].dropna().max()), 1)
        if not successful.empty and not successful["composite_score"].dropna().empty
        else None
    )

    return DailyResearchBrief(
        universe_name=universe_name,
        latest_run_id=int(latest.id) if latest else None,
        previous_run_id=int(previous.id) if previous else None,
        latest_run_at=latest.created_at if latest else None,
        requested_symbols=int(latest.requested_symbols) if latest else 0,
        successful_symbols=int(latest.successful_symbols) if latest else 0,
        failed_symbols=int(latest.failed_symbols) if latest else 0,
        average_score=average_score,
        top_score=top_score,
        improved=improved,
        weakened=weakened,
        new_opportunities=new_opportunities,
        new_signals=new_signals,
        data_quality_actions=data_quality_actions,
        shortlist_actions=shortlist_actions,
    )


def daily_brief_table(frame: pd.DataFrame, *, limit: int = 10) -> pd.DataFrame:
    """Return stable high-signal display columns for brief tables."""
    columns = [
        "symbol",
        "composite_score",
        "previous_score",
        "score_change",
        "comparison_status",
        "active_signal_count",
        "new_active_signals",
        "active_signals",
        "data_quality_warnings",
        "error",
    ]
    if frame.empty:
        return pd.DataFrame(columns=columns)
    return frame[[column for column in columns if column in frame.columns]].head(limit)


def _successful_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "error" not in frame.columns:
        return pd.DataFrame()
    return frame[frame["error"].isna()].copy()


def _score_change_series(frame: pd.DataFrame) -> pd.Series:
    if frame.empty or "score_change" not in frame.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame["score_change"], errors="coerce").fillna(0)


def _top_rows(
    frame: pd.DataFrame,
    *,
    sort_by: str,
    ascending: bool = False,
    limit: int = 10,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    if sort_by not in frame.columns:
        return frame.head(limit)
    return frame.sort_values(
        by=[sort_by, "composite_score", "symbol"],
        ascending=[ascending, False, True],
        na_position="last",
    ).head(limit)


def _data_quality_actions(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return daily_brief_table(frame)

    actions = frame[
        frame["error"].notna()
        | (frame["data_quality_warnings"].fillna("").astype(str).str.strip() != "")
    ].copy()
    if actions.empty:
        return daily_brief_table(actions)

    return actions.sort_values(
        by=["error", "data_quality_warnings", "symbol"],
        ascending=[False, False, True],
        na_position="last",
    ).head(15)


def _shortlist_actions(*, engine: Engine | None = None) -> pd.DataFrame:
    shortlist = watchlist_to_frame(fetch_watchlist_items(engine=engine))
    if shortlist.empty:
        return shortlist

    enriched = enrich_watchlist_with_latest_scores(shortlist, engine=engine)
    active = enriched[enriched["active"].fillna(True)].copy()
    if active.empty:
        return active

    needs_review = active[
        active["review_status"].fillna("watch").isin(["watch", "deep_dive"])
        | active["notes"].fillna("").astype(str).str.strip().eq("")
    ]
    return needs_review.sort_values(
        by=["review_status", "latest_score", "updated_at"],
        ascending=[True, False, False],
        na_position="last",
    ).head(15)
