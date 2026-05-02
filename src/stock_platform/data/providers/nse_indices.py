"""Official NSE index constituent CSV helpers.

These helpers fetch small public CSV files such as the Nifty 50 constituent
list. They are used to keep local seed universes fresh. They do not provide
survivorship-safe history; they only fetch the current official file.
"""

from __future__ import annotations

from dataclasses import dataclass
from io import StringIO

import httpx
import pandas as pd

from stock_platform.utils.logging import get_logger

log = get_logger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/csv,*/*",
    "Referer": "https://www.nseindia.com/",
}


@dataclass(frozen=True)
class IndexCsvSource:
    """One official NSE index CSV source."""

    universe_name: str
    display_name: str
    url: str


NSE_INDEX_CSV_SOURCES: dict[str, IndexCsvSource] = {
    "nifty_50": IndexCsvSource(
        universe_name="nifty_50",
        display_name="Nifty 50",
        url="https://archives.nseindia.com/content/indices/ind_nifty50list.csv",
    ),
}

REQUIRED_INDEX_COLUMNS = {"Company Name", "Industry", "Symbol", "Series", "ISIN Code"}


class NseIndexProviderError(RuntimeError):
    """Raised when an official NSE index CSV cannot be fetched or parsed."""


class NseIndexProvider:
    """Fetch current official NSE index constituents from public CSV files."""

    def __init__(self, timeout_seconds: float = 20.0) -> None:
        self.timeout_seconds = timeout_seconds

    def fetch_constituents(self, universe_name: str = "nifty_50") -> pd.DataFrame:
        """Return the current official constituents for a supported index.

        The returned DataFrame includes the source URL, source name, and
        normalized yfinance-style symbol with ``.NS`` suffix.
        """
        source = NSE_INDEX_CSV_SOURCES.get(universe_name)
        if source is None:
            supported = ", ".join(sorted(NSE_INDEX_CSV_SOURCES))
            raise NseIndexProviderError(
                f"Unsupported NSE index universe '{universe_name}'. Supported: {supported}"
            )

        log.info(
            "Official NSE index constituent fetch attempted: universe={}, url={}",
            universe_name,
            source.url,
        )
        try:
            with httpx.Client(headers=_HEADERS, timeout=self.timeout_seconds) as client:
                response = client.get(source.url)
                response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            raise NseIndexProviderError(
                f"Could not download official NSE {source.display_name} CSV. "
                "Check your internet connection or try again later."
            ) from exc

        frame = parse_index_constituents_csv(response.text, source=source)
        log.info(
            "Official NSE index constituents fetched: universe={}, rows={}",
            universe_name,
            len(frame),
        )
        return frame


def parse_index_constituents_csv(csv_text: str, *, source: IndexCsvSource) -> pd.DataFrame:
    """Parse and validate one official NSE constituent CSV response."""
    try:
        frame = pd.read_csv(StringIO(csv_text))
    except Exception as exc:  # noqa: BLE001
        raise NseIndexProviderError(
            f"Official NSE {source.display_name} CSV could not be parsed."
        ) from exc

    frame.columns = [str(column).strip().lstrip("\ufeff") for column in frame.columns]
    missing = sorted(REQUIRED_INDEX_COLUMNS - set(frame.columns))
    if missing:
        raise NseIndexProviderError(
            f"Official NSE {source.display_name} CSV is missing columns: {missing}"
        )

    clean = frame.copy()
    clean["Symbol"] = clean["Symbol"].astype(str).str.strip().str.upper()
    clean["Series"] = clean["Series"].astype(str).str.strip().str.upper()
    clean = clean[(clean["Symbol"] != "") & (clean["Series"] == "EQ")]
    clean = clean.drop_duplicates(subset=["Symbol"], keep="first")
    clean["yfinance_symbol"] = clean["Symbol"].map(lambda symbol: f"{symbol}.NS")
    clean["source"] = "nse_index_csv"
    clean["source_url"] = source.url
    clean["universe_name"] = source.universe_name
    return clean.reset_index(drop=True)
