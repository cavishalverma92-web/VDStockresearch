"""Thin read/write helpers for the persisted data layer.

These functions take an explicit SQLAlchemy ``Session`` so callers (jobs,
scanners, UI) own transaction boundaries. ORM models stay confined to this
package; the rest of the app receives plain dataclasses, dicts, or DataFrames.
"""

from stock_platform.data.repositories.composite_scores import (
    CompositeScoreUpsertSummary,
    fetch_composite_scores,
    latest_composite_score,
    upsert_composite_score,
)
from stock_platform.data.repositories.corporate_actions import (
    CorporateActionUpsertSummary,
    fetch_corporate_actions,
    upsert_corporate_actions,
)
from stock_platform.data.repositories.instruments import (
    InstrumentUpsertSummary,
    count_instruments,
    find_instrument_token,
    upsert_instruments,
)
from stock_platform.data.repositories.index_membership import (
    IndexMembershipSyncSummary,
    list_index_members_on,
    sync_index_membership_snapshot,
    was_index_member_on,
)
from stock_platform.data.repositories.price_daily import (
    PriceUpsertSummary,
    fetch_price_daily,
    latest_trade_date,
    upsert_price_daily,
)
from stock_platform.data.repositories.refresh_runs import (
    complete_refresh_run,
    start_refresh_run,
)
from stock_platform.data.repositories.technical_snapshots import (
    TechnicalUpsertSummary,
    upsert_technical_snapshots,
)

__all__ = [
    "CompositeScoreUpsertSummary",
    "CorporateActionUpsertSummary",
    "InstrumentUpsertSummary",
    "IndexMembershipSyncSummary",
    "PriceUpsertSummary",
    "TechnicalUpsertSummary",
    "complete_refresh_run",
    "count_instruments",
    "fetch_composite_scores",
    "fetch_corporate_actions",
    "fetch_price_daily",
    "find_instrument_token",
    "latest_composite_score",
    "latest_trade_date",
    "list_index_members_on",
    "start_refresh_run",
    "sync_index_membership_snapshot",
    "upsert_composite_score",
    "upsert_corporate_actions",
    "upsert_instruments",
    "upsert_price_daily",
    "upsert_technical_snapshots",
    "was_index_member_on",
]
