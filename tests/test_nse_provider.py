"""Tests for NSE provider helpers (pure / offline logic only).

Network-dependent functions (fetch_delivery_data, fetch_deals_for_symbol)
are not tested here — they require live NSE connectivity and are validated
manually or via integration tests.
"""

from __future__ import annotations

import pandas as pd

from stock_platform.data.providers.nse import _fetch_deals, _nse_symbol

# ---------------------------------------------------------------------------
# Symbol stripping
# ---------------------------------------------------------------------------


def test_nse_symbol_strips_ns_suffix():
    assert _nse_symbol("RELIANCE.NS") == "RELIANCE"


def test_nse_symbol_strips_bo_suffix():
    assert _nse_symbol("TCS.BO") == "TCS"


def test_nse_symbol_already_bare():
    assert _nse_symbol("INFY") == "INFY"


def test_nse_symbol_lowercases_to_upper():
    assert _nse_symbol("reliance.ns") == "RELIANCE"


# ---------------------------------------------------------------------------
# _fetch_deals returns empty DataFrame on bad response
# ---------------------------------------------------------------------------


def test_fetch_deals_returns_empty_frame_on_network_error(monkeypatch):
    """When network is unavailable _fetch_deals must return an empty DataFrame."""
    import stock_platform.data.providers.nse as nse_module

    original_client = nse_module.httpx.Client

    class _ErrorClient:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            raise ConnectionError("no network")

        def __exit__(self, *a):
            pass

    monkeypatch.setattr(nse_module.httpx, "Client", _ErrorClient)

    from datetime import date

    result = _fetch_deals("bulk", date(2024, 1, 1), date(2024, 1, 31))
    assert isinstance(result, pd.DataFrame)
    assert result.empty

    monkeypatch.setattr(nse_module.httpx, "Client", original_client)
