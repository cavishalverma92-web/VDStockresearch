"""Shared pytest fixtures for Windows-friendly local test artifacts."""

from __future__ import annotations

import re
import shutil
from pathlib import Path

import pytest


@pytest.fixture
def tmp_path(request: pytest.FixtureRequest) -> Path:
    """Project-local replacement for pytest's restrictive Windows tmp_path.

    The default pytest tmp_path factory creates directories with mode 0o700.
    In the local Windows sandbox used for this project, those directories can
    become unreadable and fail tests before the application code runs.
    """

    root = Path.cwd().resolve() / "tmp" / "pytest_paths"
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", request.node.nodeid).strip("_")
    path = (root / safe_name[:160]).resolve()

    if root not in path.parents:
        raise RuntimeError(f"Refusing to create tmp_path outside test root: {path}")

    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    return path
