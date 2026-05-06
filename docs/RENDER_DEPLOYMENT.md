# Render Deployment Guide

This guide prepares the Streamlit app for a Render web service deployment.

The deployed app must remain a research aid only. Do not add order placement,
portfolio, holdings, positions, funds, margins, or automated trading features.

## What Is Included

- `render.yaml` defines one Python web service.
- The app runs with Streamlit on Render's assigned `$PORT`.
- Streamlit binds to `0.0.0.0`, which Render requires for public web traffic.
- SQLite is configured at `/var/data/stock_platform.db` on a persistent disk.
- Cache, raw, and processed data directories are also under `/var/data`.
- Kite market data is disabled by default for the hosted app.
- yfinance is the default public deployment market-data provider.

## Important Render Notes

- Render services have an ephemeral filesystem unless a persistent disk is
  attached.
- The current Blueprint uses a persistent disk, so it requires a paid web
  service plan.
- Only files under the disk mount path, `/var/data`, survive redeploys.
- A service with a persistent disk cannot scale to multiple instances.
- Persistent disks disable zero-downtime deploys because Render must stop the
  old instance before attaching the disk to the new one.

## Recommended First Deployment

1. Push the repo to GitHub.
2. Open Render and create a new Blueprint from this repository.
3. Select the `main` branch.
4. Confirm the service settings from `render.yaml`.
5. Deploy the service.
6. Open the deployed URL and confirm the app loads.
7. Check the Data Health and Strategy Scanner pages.

The first hosted deployment starts with an empty SQLite database. Run a fresh
data refresh from the app before relying on scanner pages.

## Environment Variables

The Blueprint sets safe defaults:

```text
APP_ENV=production
APP_LOG_LEVEL=INFO
APP_TIMEZONE=Asia/Kolkata
DATABASE_URL=sqlite:////var/data/stock_platform.db
CACHE_DIR=/var/data/cache
RAW_DIR=/var/data/raw
PROCESSED_DIR=/var/data/processed
MARKET_DATA_PROVIDER=yfinance
PROVIDER_PRICE=yfinance
ENABLE_KITE_MARKET_DATA=false
ENABLE_KITE_TRADING=false
ENABLE_KITE_PORTFOLIO=false
```

Do not commit `.env`. Do not paste Zerodha secrets into repo files.

## Optional Kite Market Data

For a private/internal deployment only, Kite market data can be enabled later
from the Render dashboard by setting:

```text
MARKET_DATA_PROVIDER=kite
ENABLE_KITE_MARKET_DATA=true
KITE_API_KEY=<set in Render dashboard>
KITE_API_SECRET=<set in Render dashboard>
KITE_ACCESS_TOKEN=<set in Render dashboard>
```

Keep these as Render secret environment variables only. Kite access tokens
expire and may need daily refresh. The hosted app should still avoid trading,
portfolio, holdings, positions, funds, margins, order placement, order
modification, and order cancellation APIs.

## Build And Start Commands

Build:

```bash
pip install --upgrade pip && pip install -r requirements.txt && pip install -e .
```

Start:

```bash
streamlit run src/stock_platform/ui/streamlit_app.py --server.address 0.0.0.0 --server.port $PORT --server.headless true --browser.gatherUsageStats false
```

## Pre-Launch Checklist

- `render.yaml` is committed.
- `.env` is not committed.
- `DATABASE_URL` points to `/var/data/stock_platform.db`.
- Generated data paths point to `/var/data`.
- Kite trading and portfolio flags remain false.
- The app disclaimer is visible.
- Strategy Scanner wording remains research-only.
- No personal Zerodha account data is shown.

## Known Limitations

- SQLite on a persistent disk is acceptable for a small personal MVP, but
  Postgres is better for multi-user or production-grade use.
- The deployed app has a fresh database unless you separately migrate data.
- yfinance can be delayed, incomplete, or rate-limited.
- Kite does not provide fundamentals, delivery percentage, result calendar, or
  corporate-action reliability by itself.
- Public deployment should be treated as a demo or personal research interface,
  not as a regulated investment advisory product.
