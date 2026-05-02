"""Refresh configured universes from official NSE index CSV files.

Example:
    python -m stock_platform.jobs.refresh_official_universes --universe nifty_50
    python -m stock_platform.jobs.refresh_official_universes --universe nifty_50 --apply
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from stock_platform.config import ROOT_DIR
from stock_platform.data.providers.nse_indices import NseIndexProvider, NseIndexProviderError
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)

DEFAULT_UNIVERSES_PATH = ROOT_DIR / "config" / "universes.yaml"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "data" / "universe" / "official"


@dataclass(frozen=True)
class OfficialUniverseRefreshResult:
    """Summary returned by the official universe refresh job."""

    universe_name: str
    row_count: int
    symbols_added: tuple[str, ...]
    symbols_removed: tuple[str, ...]
    config_would_change: bool
    config_updated: bool
    csv_path: Path


def refresh_official_universe(
    universe_name: str = "nifty_50",
    *,
    apply: bool = False,
    universes_path: Path = DEFAULT_UNIVERSES_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    provider: NseIndexProvider | None = None,
) -> OfficialUniverseRefreshResult:
    """Fetch an official NSE index CSV and optionally update ``universes.yaml``."""
    provider = provider or NseIndexProvider()
    frame = provider.fetch_constituents(universe_name)
    symbols = tuple(frame["yfinance_symbol"].astype(str).tolist())
    if not symbols:
        raise ValueError(f"Official NSE universe '{universe_name}' returned no EQ symbols.")

    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"{universe_name}.csv"
    _write_constituent_snapshot(frame, csv_path)

    current_text = universes_path.read_text(encoding="utf-8")
    current_symbols = tuple(_extract_inline_universe(current_text, universe_name))
    added = tuple(sorted(set(symbols) - set(current_symbols)))
    removed = tuple(sorted(set(current_symbols) - set(symbols)))
    updated_text = replace_inline_universe(current_text, universe_name, symbols)
    config_would_change = updated_text != current_text

    if apply and config_would_change:
        universes_path.write_text(updated_text, encoding="utf-8")
        log.info(
            "Official universe config updated: universe={}, rows={}, added={}, removed={}",
            universe_name,
            len(symbols),
            len(added),
            len(removed),
        )
    else:
        log.info(
            "Official universe refresh dry-run: universe={}, rows={}, changed={}",
            universe_name,
            len(symbols),
            config_would_change,
        )

    return OfficialUniverseRefreshResult(
        universe_name=universe_name,
        row_count=len(symbols),
        symbols_added=added,
        symbols_removed=removed,
        config_would_change=config_would_change,
        config_updated=bool(apply and config_would_change),
        csv_path=csv_path,
    )


def replace_inline_universe(yaml_text: str, universe_name: str, symbols: tuple[str, ...]) -> str:
    """Replace one inline list in ``config/universes.yaml`` while preserving comments."""
    if not symbols:
        raise ValueError("Cannot replace a universe with an empty symbol list.")

    pattern = re.compile(
        rf"^(?P<header>{re.escape(universe_name)}:\r?\n)"
        r"(?P<body>(?:[ \t]+-[^\r\n]*(?:\r?\n|$))+)",
        flags=re.MULTILINE,
    )
    match = pattern.search(yaml_text)
    if not match:
        raise KeyError(f"Could not find inline universe '{universe_name}' in universes.yaml.")

    newline = "\r\n" if "\r\n" in yaml_text else "\n"
    replacement = match.group("header") + newline.join(f"  - {symbol}" for symbol in symbols)
    replacement += newline
    return yaml_text[: match.start()] + replacement + yaml_text[match.end() :]


def _extract_inline_universe(yaml_text: str, universe_name: str) -> list[str]:
    pattern = re.compile(
        rf"^{re.escape(universe_name)}:\r?\n"
        r"(?P<body>(?:[ \t]+-[^\r\n]*(?:\r?\n|$))+)",
        flags=re.MULTILINE,
    )
    match = pattern.search(yaml_text)
    if not match:
        return []
    symbols: list[str] = []
    for line in match.group("body").splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            symbols.append(stripped[2:].strip())
    return symbols


def _write_constituent_snapshot(frame: pd.DataFrame, csv_path: Path) -> None:
    snapshot = frame.copy()
    snapshot["downloaded_at_utc"] = datetime.now(UTC).isoformat(timespec="seconds")
    snapshot.to_csv(csv_path, index=False)


def _format_symbol_list(symbols: tuple[str, ...]) -> str:
    return ", ".join(symbols) if symbols else "none"


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh official NSE index universe CSVs.")
    parser.add_argument("--universe", default="nifty_50", help="Supported universe, e.g. nifty_50")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the official symbols into config/universes.yaml. Without this, dry-run only.",
    )
    args = parser.parse_args()

    try:
        result = refresh_official_universe(args.universe, apply=args.apply)
    except (NseIndexProviderError, KeyError, ValueError) as exc:
        print(f"Official universe refresh failed: {exc}", file=sys.stderr)
        print(
            "Nothing was changed. Please retry later, or verify that the NSE CSV URL is reachable.",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc
    mode = "APPLIED" if args.apply else "DRY RUN"
    print(f"{mode}: {result.universe_name}")
    print(f"Official rows: {result.row_count}")
    print(f"Snapshot CSV: {result.csv_path}")
    print(f"Added: {_format_symbol_list(result.symbols_added)}")
    print(f"Removed: {_format_symbol_list(result.symbols_removed)}")
    print(f"Config would change: {result.config_would_change}")
    print(f"Config updated: {result.config_updated}")


if __name__ == "__main__":
    main()
