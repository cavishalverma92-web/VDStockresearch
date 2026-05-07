"""Settings and safe Zerodha setup page."""

from __future__ import annotations

import streamlit as st

from stock_platform.auth import clear_kite_access_token, load_kite_access_token
from stock_platform.config import ROOT_DIR, get_settings
from stock_platform.data.providers import KiteProvider, KiteProviderError, MarketDataProvider
from stock_platform.ops import run_health_checks
from stock_platform.ui.components.common import is_hosted_demo_mode, save_kite_access_token_locally
from stock_platform.ui.components.layout import render_page_shell

render_page_shell(
    "Settings", "Zerodha market-data setup, local health checks, and operations commands."
)
settings = get_settings()
token = load_kite_access_token() or ""
kite = KiteProvider(
    api_key=settings.kite_api_key,
    api_secret=settings.kite_api_secret,
    access_token=token,
)

st.subheader("Zerodha API Setup")
st.warning("No portfolio, holdings, funds, margins, orders, trades, or trading APIs are enabled.")
st.caption("Kite is used only for market data and instrument metadata. yfinance remains fallback.")

if is_hosted_demo_mode():
    st.info(
        "This Render Free deployment is currently using yfinance only. Kite can be connected "
        "later through Render environment variables, but keep the public app market-data-only."
    )

status_cols = st.columns(3)
status_cols[0].metric("KITE_API_KEY", "Yes" if settings.kite_api_key.strip() else "No")
status_cols[1].metric("KITE_API_SECRET", "Yes" if settings.kite_api_secret.strip() else "No")
status_cols[2].metric("Access token", "Yes" if token.strip() else "No")

flag_cols = st.columns(3)
flag_cols[0].metric(
    "Kite market data", "Enabled" if settings.enable_kite_market_data else "Disabled"
)
flag_cols[1].metric("Kite trading", "Disabled")
flag_cols[2].metric("Kite portfolio", "Disabled")

router_status = MarketDataProvider().status()
with st.expander("Kite market-data connection on Render", expanded=is_hosted_demo_mode()):
    st.caption(
        "Use this only if you want the hosted app to fetch market data through Zerodha Kite. "
        "Never paste API keys, API secrets, access tokens, or request tokens into GitHub files "
        "or chat. Add them only in Render's Environment tab."
    )
    st.dataframe(
        [
            {"setting": "MARKET_DATA_PROVIDER", "safe value": "kite or auto"},
            {"setting": "ENABLE_KITE_MARKET_DATA", "safe value": "true"},
            {"setting": "ENABLE_KITE_TRADING", "safe value": "false"},
            {"setting": "ENABLE_KITE_PORTFOLIO", "safe value": "false"},
            {"setting": "KITE_API_KEY", "safe value": "set secretly in Render"},
            {"setting": "KITE_API_SECRET", "safe value": "set secretly in Render"},
            {"setting": "KITE_ACCESS_TOKEN", "safe value": "set secretly in Render; expires"},
        ],
        width="stretch",
        hide_index=True,
    )
    st.warning(
        "Do not enable Kite trading or portfolio flags on the hosted app. Access tokens expire; "
        "if Kite fails, the provider router should fall back to yfinance when configured as auto."
    )
    status_frame = [
        {"check": "Selected provider", "value": router_status["selected"]},
        {"check": "Kite market-data flag", "value": router_status["kite_enabled"]},
        {"check": "API key + secret present", "value": router_status["kite_configured"]},
        {"check": "Access token present", "value": router_status["kite_access_token"]},
    ]
    st.dataframe(status_frame, width="stretch", hide_index=True)

if token.strip():
    st.info(
        "If Kite tests say the token is incorrect or expired, clear the saved token, "
        "generate a fresh login URL, and create a new access token. Kite access tokens "
        "usually need to be renewed frequently."
    )

if st.button("Clear saved Kite token"):
    removed = clear_kite_access_token()
    if removed:
        st.success("Saved Kite token cleared from the local secure store. Generate a fresh one.")
    else:
        st.info(
            "No secure-store token file was found. If KITE_ACCESS_TOKEN is still in .env, "
            "remove or replace it there manually."
        )

if st.button("Generate Zerodha Login URL"):
    try:
        login_url = kite.get_login_url()
        st.success("Login URL generated. Open it, log in, then copy request_token.")
        st.link_button("Open Zerodha login", login_url)
        st.code(login_url, language="text")
    except KiteProviderError as exc:
        st.error(str(exc))

request_token = st.text_input(
    "Paste temporary request_token",
    type="password",
    help="Copy only the value after request_token= from the redirected URL.",
)
if st.button("Generate Access Token"):
    try:
        result = kite.generate_session(request_token)
        st.session_state["kite_generated_access_token"] = result["access_token"]
        st.success("Access token generated and kept only in this local Streamlit session.")
        st.info(
            "Click Save generated token locally to store it in the gitignored secure token file."
        )
    except KiteProviderError as exc:
        st.error(str(exc))
    except Exception:
        st.error("Could not generate access token. It may be expired, already used, or invalid.")

generated = st.session_state.get("kite_generated_access_token")
if generated and st.button("Save generated token locally"):
    try:
        path = save_kite_access_token_locally(str(generated))
        st.success(
            f"Saved token to `{path.relative_to(ROOT_DIR)}` without displaying it. Restart Streamlit."
        )
    except Exception as exc:
        st.error(f"Could not save token locally: {exc}")

test_cols = st.columns(3)
with test_cols[0]:
    if st.button("Test Kite connection"):
        result = kite.connection_test()
        if result["ok"]:
            st.success(result["message"])
        else:
            st.warning(result["message"])
with test_cols[1]:
    if st.button("Test RELIANCE LTP"):
        try:
            ltp = kite.get_ltp(["RELIANCE"])
            st.dataframe(
                ltp[[c for c in ["symbol", "exchange", "ltp", "source"] if c in ltp]],
                width="stretch",
                hide_index=True,
            )
        except KiteProviderError as exc:
            st.warning(str(exc))
        except Exception as exc:  # noqa: BLE001
            st.warning(f"LTP test failed: {type(exc).__name__}")
with test_cols[2]:
    if st.button("Test RELIANCE candles"):
        from datetime import date, timedelta

        try:
            frame = kite.get_historical_candles(
                "RELIANCE",
                from_date=date.today() - timedelta(days=30),
                to_date=date.today(),
            )
            st.success(f"Kite returned {len(frame):,} candle rows.")
        except KiteProviderError as exc:
            st.warning(str(exc))
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Candle test failed: {type(exc).__name__}")

st.subheader("Local Health Checks")
checks = run_health_checks()
st.dataframe(
    [
        {
            "check": check.name,
            "status": "PASS" if check.ok else "ACTION",
            "detail": check.detail,
            "next_action": check.action,
        }
        for check in checks
    ],
    width="stretch",
    hide_index=True,
)

st.subheader("Operations Commands")
st.code(r".\scripts\health_check.ps1", language="powershell")
st.code(r".\scripts\backup_local.ps1", language="powershell")
st.code(
    r".\.venv\Scripts\python.exe -m stock_platform.jobs.refresh_eod_candles --universe nifty_50",
    language="powershell",
)
st.code(
    r".\.venv\Scripts\python.exe -m stock_platform.jobs.sync_instruments", language="powershell"
)
