"""Tests for the local Kite access-token store."""

from __future__ import annotations

import json
import sys
from types import SimpleNamespace

import pytest

from stock_platform.auth import kite_token_store as store


@pytest.fixture
def settings_stub(monkeypatch):
    """Replace ``get_settings`` inside the store with a controllable stub.

    The stub starts with no ``kite_access_token`` so tests are isolated from
    the developer's real ``.env``. Tests that exercise the env-fallback path
    set ``stub.kite_access_token`` directly.
    """
    stub = SimpleNamespace(kite_access_token="")
    monkeypatch.setattr(store, "get_settings", lambda: stub)
    return stub


def test_save_and_load_round_trip(tmp_path, settings_stub) -> None:
    saved = store.save_kite_access_token("abc123token", data_dir=tmp_path)

    assert saved == store.kite_token_path(data_dir=tmp_path)
    assert saved.exists()
    assert store.load_kite_access_token(data_dir=tmp_path) == "abc123token"


def test_load_returns_none_when_no_store_and_no_env(tmp_path, settings_stub) -> None:
    assert store.load_kite_access_token(data_dir=tmp_path) is None
    assert store.has_kite_access_token(data_dir=tmp_path) is False


def test_load_falls_back_to_env_when_store_missing(tmp_path, settings_stub) -> None:
    settings_stub.kite_access_token = "from-env-fallback"

    loaded = store.load_kite_access_token(data_dir=tmp_path)

    assert loaded == "from-env-fallback"


def test_store_takes_precedence_over_env(tmp_path, settings_stub) -> None:
    settings_stub.kite_access_token = "from-env"
    store.save_kite_access_token("from-store", data_dir=tmp_path)

    loaded = store.load_kite_access_token(data_dir=tmp_path)

    assert loaded == "from-store"


def test_save_strips_whitespace(tmp_path, settings_stub) -> None:
    store.save_kite_access_token("  spaced-token  ", data_dir=tmp_path)
    assert store.load_kite_access_token(data_dir=tmp_path) == "spaced-token"


def test_save_rejects_empty_token(tmp_path, settings_stub) -> None:
    with pytest.raises(ValueError):
        store.save_kite_access_token("", data_dir=tmp_path)
    with pytest.raises(ValueError):
        store.save_kite_access_token("   ", data_dir=tmp_path)


def test_save_does_not_modify_env_file(tmp_path, monkeypatch, settings_stub) -> None:
    """Audit guarantee: the token store must never write to .env."""
    project_root = tmp_path / "project"
    project_root.mkdir()
    env_path = project_root / ".env"
    env_contents = "KITE_API_KEY=key\nKITE_ACCESS_TOKEN=old-from-env\n"
    env_path.write_text(env_contents, encoding="utf-8")
    monkeypatch.chdir(project_root)

    store.save_kite_access_token("new-token", data_dir=tmp_path / "data")

    assert env_path.read_text(encoding="utf-8") == env_contents


def test_clear_removes_file(tmp_path, settings_stub) -> None:
    store.save_kite_access_token("temp", data_dir=tmp_path)
    assert store.clear_kite_access_token(data_dir=tmp_path) is True
    assert store.clear_kite_access_token(data_dir=tmp_path) is False
    assert store.load_kite_access_token(data_dir=tmp_path) is None


def test_clear_returns_false_when_nothing_to_remove(tmp_path, settings_stub) -> None:
    assert store.clear_kite_access_token(data_dir=tmp_path) is False


def test_save_writes_json_with_token_and_timestamp(tmp_path, settings_stub) -> None:
    store.save_kite_access_token("xyz", data_dir=tmp_path)

    payload = json.loads(store.kite_token_path(data_dir=tmp_path).read_text(encoding="utf-8"))
    assert payload["access_token"] == "xyz"
    assert "saved_at" in payload


def test_load_handles_corrupt_store(tmp_path, settings_stub) -> None:
    path = store.kite_token_path(data_dir=tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not-json{}{", encoding="utf-8")

    # Corrupt store should degrade to no-token (env fallback unset in this fixture).
    assert store.load_kite_access_token(data_dir=tmp_path) is None


def test_load_handles_corrupt_store_with_env_fallback(tmp_path, settings_stub) -> None:
    settings_stub.kite_access_token = "still-have-env-fallback"
    path = store.kite_token_path(data_dir=tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not-json{", encoding="utf-8")

    assert store.load_kite_access_token(data_dir=tmp_path) == "still-have-env-fallback"


@pytest.mark.skipif(sys.platform.startswith("win"), reason="POSIX-only permission check")
def test_save_applies_restrictive_permissions(tmp_path, settings_stub) -> None:
    saved = store.save_kite_access_token("p", data_dir=tmp_path)
    mode = saved.stat().st_mode & 0o777
    assert mode == 0o600
