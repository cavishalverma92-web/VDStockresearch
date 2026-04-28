"""Daily research brief built from saved universe scans and shortlist rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

import pandas as pd
from sqlalchemy import Engine

from stock_platform.analytics.scanner.persistence import compare_latest_universe_scans
from stock_platform.analytics.scanner.watchlist import (
    enrich_watchlist_with_latest_scores,
    fetch_watchlist_items,
    watchlist_to_frame,
)

FreshnessStatus = Literal["fresh", "aging", "stale", "unknown"]


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
        successful[score_changes >= meaningful_score_change] if len(successful) else successful,
        sort_by="score_change",
    )
    weakened = _top_rows(
        successful[score_changes <= -meaningful_score_change] if len(successful) else successful,
        sort_by="score_change",
        ascending=True,
    )
    if "comparison_status" in successful.columns and "composite_score" in successful.columns:
        new_opportunities = _top_rows(
            successful[
                successful["comparison_status"].isin(["new symbol", "new scan row"])
                & (successful["composite_score"].fillna(0) >= min_opportunity_score)
            ],
            sort_by="composite_score",
        )
    else:
        new_opportunities = successful.iloc[0:0].copy()
    if "new_active_signals" in successful.columns:
        new_signals = _top_rows(
            successful[successful["new_active_signals"].fillna("").astype(str).str.strip() != ""],
            sort_by="score_change",
        )
    else:
        new_signals = successful.iloc[0:0].copy()
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
    """Return successful rows while preserving the comparison column structure.

    Critically, even when the input is empty, the returned frame retains the
    expected columns (``comparison_status``, ``new_active_signals``, ...) so
    callers can apply column-based filters without ``KeyError``.
    """
    if "error" not in frame.columns:
        # No comparison columns available at all; hand back an empty frame
        # carrying any columns the caller already had.
        return frame.iloc[0:0].copy()
    if frame.empty:
        return frame.copy()
    return frame[frame["error"].isna()].copy()


def _score_change_series(frame: pd.DataFrame) -> pd.Series:
    if frame.empty or "score_change" not in frame.columns:
        return pd.Series([], dtype=float, index=frame.index)
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
    if frame.empty or "error" not in frame.columns or "data_quality_warnings" not in frame.columns:
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


def daily_brief_headline(brief: DailyResearchBrief) -> str:
    """Return a one-line TL;DR of the brief for at-a-glance scanning.

    Examples:
        "No saved scan yet for nifty_50."
        "Nifty 50 scan #5 — 3 new opportunities, 2 score improvers,
         1 newly active signal, 1 data-quality action."
    """
    if not brief.has_latest_scan:
        return f"No saved scan yet for {brief.universe_name}."

    parts: list[str] = []
    new_opp = len(brief.new_opportunities)
    improvers = len(brief.improved)
    weakeners = len(brief.weakened)
    new_sigs = len(brief.new_signals)
    dq_actions = len(brief.data_quality_actions)
    shortlist_actions = len(brief.shortlist_actions)

    if new_opp:
        parts.append(f"{new_opp} new opportunit{'y' if new_opp == 1 else 'ies'}")
    if improvers:
        parts.append(f"{improvers} score improver{'s' if improvers != 1 else ''}")
    if weakeners:
        parts.append(f"{weakeners} score weakener{'s' if weakeners != 1 else ''}")
    if new_sigs:
        parts.append(f"{new_sigs} newly active signal{'s' if new_sigs != 1 else ''}")
    if dq_actions:
        parts.append(f"{dq_actions} data-quality action{'s' if dq_actions != 1 else ''}")
    if shortlist_actions:
        parts.append(
            f"{shortlist_actions} shortlist follow-up{'s' if shortlist_actions != 1 else ''}"
        )

    universe_label = brief.universe_name.replace("_", " ").title()
    if not parts:
        return f"{universe_label} scan #{brief.latest_run_id} — no notable changes since the previous scan."
    return f"{universe_label} scan #{brief.latest_run_id} — " + ", ".join(parts) + "."


def daily_brief_freshness(
    latest_run_at: object | None,
    *,
    fresh_within_hours: int = 24,
    stale_after_hours: int = 72,
    now: datetime | None = None,
) -> tuple[FreshnessStatus, str]:
    """Classify scan freshness and return (status, human-readable age).

    Statuses:
        "fresh"   — scan is within ``fresh_within_hours`` (default 24h)
        "aging"   — between ``fresh_within_hours`` and ``stale_after_hours``
        "stale"   — older than ``stale_after_hours`` (default 72h)
        "unknown" — no timestamp available
    """
    if latest_run_at is None:
        return "unknown", "no saved scan timestamp"

    if isinstance(latest_run_at, str):
        try:
            run_at = datetime.fromisoformat(latest_run_at.replace("Z", "+00:00"))
        except ValueError:
            return "unknown", "unparseable timestamp"
    elif isinstance(latest_run_at, datetime):
        run_at = latest_run_at
    else:
        return "unknown", "unsupported timestamp type"

    if run_at.tzinfo is None:
        run_at = run_at.replace(tzinfo=UTC)
    current = (now or datetime.now(UTC)).astimezone(UTC)
    age = current - run_at.astimezone(UTC)

    age_text = _humanize_age(age)
    if age <= timedelta(hours=fresh_within_hours):
        return "fresh", age_text
    if age <= timedelta(hours=stale_after_hours):
        return "aging", age_text
    return "stale", age_text


def _humanize_age(age: timedelta) -> str:
    total_minutes = int(age.total_seconds() // 60)
    if total_minutes < 60:
        return f"{max(total_minutes, 0)} minute{'s' if total_minutes != 1 else ''} ago"
    total_hours = total_minutes // 60
    if total_hours < 48:
        return f"{total_hours} hour{'s' if total_hours != 1 else ''} ago"
    total_days = total_hours // 24
    return f"{total_days} day{'s' if total_days != 1 else ''} ago"


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
