"""Local-only secret storage for broker tokens.

This package never imports from ``ui/`` or ``jobs/`` — it is a leaf utility
read by providers, jobs, and the UI to resolve the current access token
without exposing it via stdout, logs, or ``.env`` diffs.

Trading and portfolio APIs remain disabled by design (see
``KiteProvider``); this package only handles the read-only access token used
for market-data and instrument metadata calls.
"""

from stock_platform.auth.kite_token_store import (
    clear_kite_access_token,
    has_kite_access_token,
    kite_token_path,
    load_kite_access_token,
    save_kite_access_token,
)

__all__ = [
    "clear_kite_access_token",
    "has_kite_access_token",
    "kite_token_path",
    "load_kite_access_token",
    "save_kite_access_token",
]
