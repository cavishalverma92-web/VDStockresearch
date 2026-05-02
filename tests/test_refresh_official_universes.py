from __future__ import annotations

from pathlib import Path

import pandas as pd

from stock_platform.jobs.refresh_official_universes import (
    refresh_official_universe,
    replace_inline_universe,
)


class FakeNseIndexProvider:
    def fetch_constituents(self, universe_name: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "Symbol": ["RELIANCE", "HDFCBANK"],
                "Series": ["EQ", "EQ"],
                "yfinance_symbol": ["RELIANCE.NS", "HDFCBANK.NS"],
                "source": ["nse_index_csv", "nse_index_csv"],
                "source_url": ["https://example.test", "https://example.test"],
                "universe_name": [universe_name, universe_name],
            }
        )


def test_replace_inline_universe_preserves_other_blocks():
    text = "# comment\nnifty_50:\n  - OLD.NS\n\nnifty_next_50:\n  - KEEP.NS\n"

    updated = replace_inline_universe(text, "nifty_50", ("RELIANCE.NS", "HDFCBANK.NS"))

    assert "  - OLD.NS" not in updated
    assert "  - RELIANCE.NS" in updated
    assert "nifty_next_50:\n  - KEEP.NS" in updated


def test_refresh_official_universe_dry_run_writes_snapshot_but_not_config(tmp_path: Path):
    universes_path = tmp_path / "universes.yaml"
    output_dir = tmp_path / "official"
    universes_path.write_text("nifty_50:\n  - OLD.NS\n", encoding="utf-8")

    result = refresh_official_universe(
        "nifty_50",
        apply=False,
        universes_path=universes_path,
        output_dir=output_dir,
        provider=FakeNseIndexProvider(),  # type: ignore[arg-type]
    )

    assert result.row_count == 2
    assert result.symbols_added == ("HDFCBANK.NS", "RELIANCE.NS")
    assert result.symbols_removed == ("OLD.NS",)
    assert result.config_would_change is True
    assert result.config_updated is False
    assert result.csv_path.exists()
    assert "OLD.NS" in universes_path.read_text(encoding="utf-8")


def test_refresh_official_universe_apply_updates_config(tmp_path: Path):
    universes_path = tmp_path / "universes.yaml"
    output_dir = tmp_path / "official"
    universes_path.write_text("nifty_50:\n  - OLD.NS\n", encoding="utf-8")

    result = refresh_official_universe(
        "nifty_50",
        apply=True,
        universes_path=universes_path,
        output_dir=output_dir,
        provider=FakeNseIndexProvider(),  # type: ignore[arg-type]
    )

    updated = universes_path.read_text(encoding="utf-8")
    assert result.config_would_change is True
    assert result.config_updated is True
    assert "  - RELIANCE.NS" in updated
    assert "  - HDFCBANK.NS" in updated
    assert "OLD.NS" not in updated
