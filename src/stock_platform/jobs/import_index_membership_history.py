"""Import archived local NSE index constituent CSVs into membership history.

This job intentionally reads local files only. Put archived constituent CSVs in
``data/universe/history/nifty_50`` with a date in the filename, then run:

    python -m stock_platform.jobs.import_index_membership_history --universe nifty_50
    python -m stock_platform.jobs.import_index_membership_history --universe nifty_50 --apply --replace-existing
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from sqlalchemy import delete, func, select

from stock_platform.config import ROOT_DIR
from stock_platform.data.providers.nse_indices import (
    NSE_INDEX_CSV_SOURCES,
    parse_index_constituents_csv,
)
from stock_platform.data.repositories import sync_index_membership_snapshot
from stock_platform.db import create_all_tables, get_engine, get_session
from stock_platform.db.models import IndexMembershipHistory

DEFAULT_HISTORY_ROOT = ROOT_DIR / "data" / "universe" / "history"
DEFAULT_SOURCE = "nse_index_csv"


@dataclass(frozen=True)
class HistoricalIndexFile:
    """One dated local index constituent CSV."""

    path: Path
    effective_date: date
    row_count: int


@dataclass(frozen=True)
class HistoricalMembershipImportResult:
    """Summary from one historical membership import run."""

    universe_name: str
    index_name: str
    file_count: int
    earliest_file_date: date | None
    latest_file_date: date | None
    inserted: int
    updated: int
    closed: int
    deleted_existing: int
    applied: bool


def import_index_membership_history(
    universe_name: str = "nifty_50",
    *,
    input_dir: Path | None = None,
    apply: bool = False,
    replace_existing: bool = False,
    source: str = DEFAULT_SOURCE,
) -> HistoricalMembershipImportResult:
    """Import dated local CSV snapshots in chronological order."""
    index_source = NSE_INDEX_CSV_SOURCES.get(universe_name)
    if index_source is None:
        supported = ", ".join(sorted(NSE_INDEX_CSV_SOURCES))
        raise ValueError(f"Unsupported universe '{universe_name}'. Supported: {supported}")

    folder = input_dir or DEFAULT_HISTORY_ROOT / universe_name
    files = discover_historical_index_files(folder, universe_name=universe_name)
    if not files:
        raise FileNotFoundError(
            f"No dated CSV files found in {folder}. Add files such as "
            f"{universe_name}_2024-03-31.csv, then rerun."
        )

    earliest = files[0].effective_date
    latest = files[-1].effective_date
    engine = get_engine()
    create_all_tables(engine)

    with get_session(engine) as session:
        existing_count = _existing_period_count(
            session,
            index_name=index_source.display_name,
            source=source,
        )
        existing_latest = _latest_existing_from_date(
            session,
            index_name=index_source.display_name,
            source=source,
        )

        if (
            apply
            and existing_count
            and earliest <= (existing_latest or earliest)
            and not replace_existing
        ):
            raise ValueError(
                "Historical files overlap existing membership rows. Re-run with "
                "--replace-existing after reviewing the CSV folder, or import only newer files."
            )

        deleted_existing = 0
        if apply and replace_existing:
            deleted_existing = _delete_existing_periods(
                session,
                index_name=index_source.display_name,
                source=source,
            )

        inserted = 0
        updated = 0
        closed = 0
        for item in files:
            frame = _load_historical_file(item, universe_name=universe_name)
            if apply:
                summary = sync_index_membership_snapshot(
                    session,
                    index_name=index_source.display_name,
                    constituents=frame,
                    effective_date=item.effective_date,
                    source=source,
                    source_url=str(item.path),
                )
                inserted += summary.inserted
                updated += summary.updated
                closed += summary.closed

        if apply:
            session.commit()
        else:
            session.rollback()

    return HistoricalMembershipImportResult(
        universe_name=universe_name,
        index_name=index_source.display_name,
        file_count=len(files),
        earliest_file_date=earliest,
        latest_file_date=latest,
        inserted=inserted,
        updated=updated,
        closed=closed,
        deleted_existing=deleted_existing,
        applied=apply,
    )


def discover_historical_index_files(
    input_dir: Path,
    *,
    universe_name: str,
) -> list[HistoricalIndexFile]:
    """Return dated CSV files sorted by effective date."""
    if not input_dir.exists():
        return []

    source = NSE_INDEX_CSV_SOURCES.get(universe_name)
    if source is None:
        raise ValueError(f"Unsupported universe '{universe_name}'.")

    files: list[HistoricalIndexFile] = []
    for path in sorted(input_dir.glob("*.csv")):
        effective_date = parse_snapshot_date_from_filename(path)
        if effective_date is None:
            continue
        frame = parse_index_constituents_csv(path.read_text(encoding="utf-8-sig"), source=source)
        files.append(
            HistoricalIndexFile(
                path=path,
                effective_date=effective_date,
                row_count=len(frame),
            )
        )
    return sorted(files, key=lambda item: (item.effective_date, item.path.name))


def parse_snapshot_date_from_filename(path: Path) -> date | None:
    """Extract YYYY-MM-DD or YYYYMMDD from a CSV filename."""
    name = path.stem
    dashed = re.search(r"(20\d{2})[-_](\d{2})[-_](\d{2})", name)
    if dashed:
        return date(int(dashed.group(1)), int(dashed.group(2)), int(dashed.group(3)))
    compact = re.search(r"(20\d{6})", name)
    if compact:
        raw = compact.group(1)
        return date(int(raw[0:4]), int(raw[4:6]), int(raw[6:8]))
    return None


def _load_historical_file(item: HistoricalIndexFile, *, universe_name: str):
    source = NSE_INDEX_CSV_SOURCES[universe_name]
    return parse_index_constituents_csv(item.path.read_text(encoding="utf-8-sig"), source=source)


def _existing_period_count(session, *, index_name: str, source: str) -> int:
    return int(
        session.scalar(
            select(func.count(IndexMembershipHistory.id)).where(
                IndexMembershipHistory.index_name == index_name,
                IndexMembershipHistory.source == source,
            )
        )
        or 0
    )


def _latest_existing_from_date(session, *, index_name: str, source: str) -> date | None:
    return session.scalar(
        select(func.max(IndexMembershipHistory.from_date)).where(
            IndexMembershipHistory.index_name == index_name,
            IndexMembershipHistory.source == source,
        )
    )


def _delete_existing_periods(session, *, index_name: str, source: str) -> int:
    result = session.execute(
        delete(IndexMembershipHistory).where(
            IndexMembershipHistory.index_name == index_name,
            IndexMembershipHistory.source == source,
        )
    )
    return int(result.rowcount or 0)


def _format_date(value: date | None) -> str:
    return value.isoformat() if value else "none"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import local archived NSE index constituent CSVs into membership history."
    )
    parser.add_argument("--universe", default="nifty_50", help="Supported universe, e.g. nifty_50")
    parser.add_argument(
        "--input-dir",
        default=None,
        help="Folder containing dated CSV files. Defaults to data/universe/history/<universe>.",
    )
    parser.add_argument("--apply", action="store_true", help="Write rows. Default is dry-run.")
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Delete existing rows for this index/source before importing.",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir) if args.input_dir else None
    try:
        result = import_index_membership_history(
            args.universe,
            input_dir=input_dir,
            apply=args.apply,
            replace_existing=args.replace_existing,
        )
    except (FileNotFoundError, KeyError, ValueError) as exc:
        print(f"Historical membership import failed: {exc}", file=sys.stderr)
        print("Nothing was changed.", file=sys.stderr)
        raise SystemExit(1) from exc

    print("APPLIED" if result.applied else "DRY RUN")
    print(f"Universe: {result.universe_name}")
    print(f"Index: {result.index_name}")
    print(f"Files: {result.file_count}")
    print(f"Earliest file date: {_format_date(result.earliest_file_date)}")
    print(f"Latest file date: {_format_date(result.latest_file_date)}")
    print(f"Deleted existing periods: {result.deleted_existing}")
    print(f"Inserted periods: {result.inserted}")
    print(f"Updated active periods: {result.updated}")
    print(f"Closed periods: {result.closed}")


if __name__ == "__main__":
    main()
