"""Local Kite access-token store.

The store lives at ``data/secure/kite_token.json`` (gitignored). It is the
canonical home for ``KITE_ACCESS_TOKEN``. ``.env`` is treated as a read-only
legacy fallback and is never modified by this module.

On POSIX the file is created with mode ``0o600``. On Windows ``os.chmod``
only toggles the read-only bit; the gitignored containing directory is the
primary defence.

API contract
------------
* Functions never log or print the raw token. Logs only confirm presence and
  approximate length.
* Token migration from ``.env`` is read-only: if a value exists in the env
  but not in the store, ``load_kite_access_token`` returns it. The store is
  populated only when the user explicitly calls :func:`save_kite_access_token`
  (typically via the Streamlit UI's "save generated token" button).
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from stock_platform.config import DATA_DIR, get_settings
from stock_platform.utils.logging import get_logger

log = get_logger(__name__)

_TOKEN_FILENAME = "kite_token.json"
_SECURE_DIRNAME = "secure"


def kite_token_path(*, data_dir: Path | None = None) -> Path:
    """Return the canonical path to the Kite token store file."""
    base = (data_dir or DATA_DIR) / _SECURE_DIRNAME
    return base / _TOKEN_FILENAME


def load_kite_access_token(*, data_dir: Path | None = None) -> str | None:
    """Return the current Kite access token, or ``None`` if none is configured.

    Lookup order:
    1. ``data/secure/kite_token.json`` (canonical store).
    2. ``KITE_ACCESS_TOKEN`` from ``.env`` (legacy read-only fallback).
    """
    path = kite_token_path(data_dir=data_dir)
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            log.warning(
                "Kite token store unreadable at {}: {}",
                path,
                type(exc).__name__,
            )
            payload = None
        if isinstance(payload, dict):
            token = str(payload.get("access_token") or "").strip()
            if token:
                return token

    legacy = (get_settings().kite_access_token or "").strip()
    if legacy:
        return legacy
    return None


def has_kite_access_token(*, data_dir: Path | None = None) -> bool:
    """Return ``True`` when an access token is available from any source."""
    return load_kite_access_token(data_dir=data_dir) is not None


def save_kite_access_token(token: str, *, data_dir: Path | None = None) -> Path:
    """Persist the Kite access token to the secure store.

    Creates the parent directory if needed and applies POSIX 0o600 permissions
    where supported. ``.env`` is left unchanged — the store is the new source
    of truth.
    """
    cleaned = (token or "").strip()
    if not cleaned:
        raise ValueError("access token is required")

    path = kite_token_path(data_dir=data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "access_token": cleaned,
        "saved_at": datetime.now(UTC).isoformat(),
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _restrict_permissions(path)
    log.info(
        "Kite access token saved to secure store: path={}, length={}",
        path,
        len(cleaned),
    )
    return path


def clear_kite_access_token(*, data_dir: Path | None = None) -> bool:
    """Remove the local Kite token file. Returns ``True`` if a file existed."""
    path = kite_token_path(data_dir=data_dir)
    if path.exists():
        path.unlink()
        log.info("Kite access token cleared from secure store: path={}", path)
        return True
    return False


def _restrict_permissions(path: Path) -> None:
    if sys.platform.startswith("win"):
        return
    try:
        os.chmod(path, 0o600)
    except OSError as exc:
        log.warning(
            "Could not restrict permissions on {}: {}. "
            "File is in a gitignored directory; treat the token as still secret.",
            path,
            exc,
        )
