r"""Sync Zerodha Kite instrument master to local cache.

This script fetches instrument metadata only. It does not fetch holdings,
positions, funds, margins, orders, trades, or profile details.

Run from project root:
    .\.venv\Scripts\python.exe scripts\sync_kite_instruments.py
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from stock_platform.config import DATA_DIR, get_settings  # noqa: E402
from stock_platform.data.providers.kite_provider import (  # noqa: E402
    KiteProvider,
    KiteProviderError,
)
from stock_platform.utils.logging import get_logger  # noqa: E402

log = get_logger(__name__)


def main() -> int:
    settings = get_settings()
    provider = KiteProvider(
        api_key=settings.kite_api_key,
        api_secret=settings.kite_api_secret,
        access_token=settings.kite_access_token,
    )

    if not provider.is_configured() or not provider.has_access_token():
        print(
            "Kite credentials/access token missing. Add KITE_API_KEY, KITE_API_SECRET, "
            "and KITE_ACCESS_TOKEN to .env, then retry."
        )
        return 1

    raw_dir = DATA_DIR / "raw" / "kite"
    processed_dir = DATA_DIR / "processed" / "kite"
    cache_dir = DATA_DIR / "cache" / "kite"
    for folder in (raw_dir, processed_dir, cache_dir):
        folder.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    try:
        frame = provider.get_instruments("NSE")
    except KiteProviderError as exc:
        print(f"Kite instrument sync failed: {exc}")
        return 1

    raw_path = raw_dir / f"nse_instruments_{timestamp}.csv"
    processed_path = processed_dir / "nse_instruments.csv"
    cache_path = cache_dir / "nse_instruments_latest.csv"

    frame.to_csv(raw_path, index=False)
    frame.to_csv(processed_path, index=False)
    frame.to_csv(cache_path, index=False)

    log.info("Kite instrument sync completed: rows={}, path={}", len(frame), processed_path)
    print(f"Saved {len(frame):,} NSE instruments")
    print(f"Processed file: {processed_path}")
    print("TODO: Later insert these rows into an instruments database table via Alembic migration.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
