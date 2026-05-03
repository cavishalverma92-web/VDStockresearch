from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from stock_platform.db.models import Base, IndexMembershipHistory
from stock_platform.jobs import import_index_membership_history as job
from stock_platform.jobs.import_index_membership_history import (
    discover_historical_index_files,
    import_index_membership_history,
    parse_snapshot_date_from_filename,
)


def _csv(symbols: list[str]) -> str:
    lines = ["Company Name,Industry,Symbol,Series,ISIN Code"]
    for idx, symbol in enumerate(symbols):
        lines.append(f"{symbol} Ltd.,Test Industry,{symbol},EQ,INE{idx:09d}")
    return "\n".join(lines)


def test_parse_snapshot_date_from_filename_supports_common_patterns():
    assert parse_snapshot_date_from_filename(Path("nifty_50_2024-03-31.csv")) == date(2024, 3, 31)
    assert parse_snapshot_date_from_filename(Path("ind_nifty50list_20240331.csv")) == date(
        2024, 3, 31
    )
    assert parse_snapshot_date_from_filename(Path("ind_nifty50list.csv")) is None


def test_discover_historical_index_files_skips_undated_files(tmp_path: Path):
    (tmp_path / "nifty_50_2024-03-31.csv").write_text(
        _csv(["RELIANCE", "HDFCBANK"]),
        encoding="utf-8",
    )
    (tmp_path / "ind_nifty50list.csv").write_text(_csv(["OLD"]), encoding="utf-8")

    files = discover_historical_index_files(tmp_path, universe_name="nifty_50")

    assert len(files) == 1
    assert files[0].effective_date == date(2024, 3, 31)
    assert files[0].row_count == 2


def test_import_history_dry_run_does_not_write(monkeypatch, tmp_path: Path):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    monkeypatch.setattr(job, "get_engine", lambda: engine)
    (tmp_path / "nifty_50_2024-03-31.csv").write_text(
        _csv(["RELIANCE", "HDFCBANK"]),
        encoding="utf-8",
    )

    result = import_index_membership_history("nifty_50", input_dir=tmp_path)

    with Session(engine) as session:
        count = session.scalar(select(IndexMembershipHistory.id).limit(1))

    assert result.applied is False
    assert result.file_count == 1
    assert result.inserted == 0
    assert count is None


def test_import_history_apply_replace_rebuilds_periods(monkeypatch, tmp_path: Path):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    monkeypatch.setattr(job, "get_engine", lambda: engine)
    (tmp_path / "nifty_50_2024-03-31.csv").write_text(
        _csv(["RELIANCE", "HDFCBANK"]),
        encoding="utf-8",
    )
    (tmp_path / "nifty_50_2024-06-30.csv").write_text(
        _csv(["RELIANCE", "TMPV"]),
        encoding="utf-8",
    )

    result = import_index_membership_history(
        "nifty_50",
        input_dir=tmp_path,
        apply=True,
        replace_existing=True,
    )

    with Session(engine) as session:
        rows = session.scalars(select(IndexMembershipHistory)).all()
        by_symbol = {row.symbol: row for row in rows}

    assert result.applied is True
    assert result.file_count == 2
    assert result.inserted == 3
    assert result.updated == 1
    assert result.closed == 1
    assert len(rows) == 3
    assert by_symbol["HDFCBANK.NS"].active is False
    assert by_symbol["HDFCBANK.NS"].to_date == date(2024, 6, 29)
    assert by_symbol["TMPV.NS"].active is True


def test_import_history_requires_replace_for_overlapping_existing_rows(
    monkeypatch,
    tmp_path: Path,
):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    monkeypatch.setattr(job, "get_engine", lambda: engine)
    (tmp_path / "nifty_50_2024-03-31.csv").write_text(
        _csv(["RELIANCE", "HDFCBANK"]),
        encoding="utf-8",
    )
    with Session(engine) as session:
        session.add(
            IndexMembershipHistory(
                index_name="Nifty 50",
                symbol="RELIANCE.NS",
                from_date=date(2026, 5, 3),
                source="nse_index_csv",
            )
        )
        session.commit()

    with pytest.raises(ValueError, match="overlap existing membership rows"):
        import_index_membership_history(
            "nifty_50",
            input_dir=tmp_path,
            apply=True,
        )


def test_import_history_dry_run_allows_overlap_for_review(monkeypatch, tmp_path: Path):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    monkeypatch.setattr(job, "get_engine", lambda: engine)
    (tmp_path / "nifty_50_2024-03-31.csv").write_text(
        _csv(["RELIANCE", "HDFCBANK"]),
        encoding="utf-8",
    )
    with Session(engine) as session:
        session.add(
            IndexMembershipHistory(
                index_name="Nifty 50",
                symbol="RELIANCE.NS",
                from_date=date(2026, 5, 3),
                source="nse_index_csv",
            )
        )
        session.commit()

    result = import_index_membership_history("nifty_50", input_dir=tmp_path)

    assert result.applied is False
    assert result.file_count == 1
    assert result.earliest_file_date == date(2024, 3, 31)
