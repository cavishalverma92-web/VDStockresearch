r"""Sync Zerodha Kite instrument master to local SQLite + cache CSVs.

This script fetches instrument metadata only. It does not fetch holdings,
positions, funds, margins, orders, trades, or profile details.

Run from project root:
    .\.venv\Scripts\python.exe scripts\sync_kite_instruments.py
    .\.venv\Scripts\python.exe scripts\sync_kite_instruments.py --exchange BSE
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from stock_platform.jobs.sync_instruments import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
