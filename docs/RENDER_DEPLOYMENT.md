# Render Deployment Guide

This guide prepares the Streamlit app for a Render web service deployment.

The deployed app must remain a research aid only. Do not add order placement,
portfolio, holdings, positions, funds, margins, or automated trading features.

## What Is Included

- `render.yaml` defines one Python web service.
- The service uses Render's free instance type for the first demo deployment.
- The app runs with Streamlit on Render's assigned `$PORT`.
- Streamlit binds to `0.0.0.0`, which Render requires for public web traffic.
- SQLite is configured at `data/stock_platform.db` on Render's ephemeral
  filesystem.
- Cache, raw, and processed data directories are also under `data/`.
- Kite market data is disabled by default for the hosted app.
- yfinance is the default public deployment market-data provider.

## Important Render Notes

- This is a free demo deployment, not a durable production deployment.
- Render free web services spin down after idle periods and can restart at any
  time.
- Free web services do not support persistent disks.
- Local SQLite files, refresh history, scanner results, generated cache files,
  and raw/processed data can be lost on redeploy, restart, or spin-down.
- Use this first deployment to confirm the app loads and the UI works online.
- Upgrade later to a paid service with persistent disk or Postgres before using
  the hosted app for daily research history.

## Recommended First Deployment

1. Push the repo to GitHub.
2. Open Render and create a new Blueprint from this repository.
3. Select the `main` branch.
4. Confirm the service settings from `render.yaml`.
5. Deploy the service.
6. Open the deployed URL and confirm the app loads.
7. Check the Data Health and Strategy Scanner pages.

The first hosted deployment starts with an empty SQLite database. On the free
plan, that database is temporary. Run a fresh data refresh from the app after
deploying, and expect those saved results to reset occasionally.

## Environment Variables

The Blueprint sets safe defaults:

```text
APP_ENV=production
APP_LOG_LEVEL=INFO
APP_TIMEZONE=Asia/Kolkata
DATABASE_URL=sqlite:///data/stock_platform.db
CACHE_DIR=data/cache
RAW_DIR=data/raw
PROCESSED_DIR=data/processed
MARKET_DATA_PROVIDER=auto
PROVIDER_PRICE=yfinance
ENABLE_KITE_MARKET_DATA=true
ENABLE_KITE_TRADING=false
ENABLE_KITE_PORTFOLIO=false
KITE_API_KEY=<secret value in Render>
KITE_API_SECRET=<secret value in Render>
KITE_ACCESS_TOKEN=<secret value in Render; expires>
```

Do not commit `.env`. Do not paste Zerodha secrets into repo files.

## Optional Kite Market Data

For a private/internal deployment only, Kite market data is enabled through the
provider router with yfinance fallback. Add the secret values from the Render
dashboard:

```text
MARKET_DATA_PROVIDER=auto
ENABLE_KITE_MARKET_DATA=true
KITE_API_KEY=<set in Render dashboard>
KITE_API_SECRET=<set in Render dashboard>
KITE_ACCESS_TOKEN=<set in Render dashboard>
```

Keep these as Render secret environment variables only. Kite access tokens
expire and may need daily refresh. The hosted app should still avoid trading,
portfolio, holdings, positions, funds, margins, order placement, order
modification, and order cancellation APIs.

The Settings page includes a Kite market-data connection panel that checks
whether the hosted app sees the required environment variables without showing
their values.

## Hosted Demo Refresh

On Render Free, Data Health shows a **Run 5-symbol demo refresh** button. It
seeds the temporary hosted SQLite database with a small Nifty 50 sample using
yfinance. This is useful for checking Market Today and Strategy Scanner online.

The demo refresh does not use Kite, portfolio, holdings, funds, orders, or
trading APIs. Because Render Free storage is temporary, the refreshed rows can
reset after redeploys, restarts, or idle spin-down.

## Build And Start Commands

Build:

```bash
pip install --upgrade pip && pip install -r requirements.txt && pip install -e .
```

Start:

```bash
bash -lc 'python -m streamlit run src/stock_platform/ui/streamlit_app.py --server.address 0.0.0.0 --server.port "$PORT" --server.headless true --server.enableCORS false --server.enableXsrfProtection false --server.fileWatcherType none --browser.gatherUsageStats false'
```

Render health check:

```text
/_stcore/health
```

## Pre-Launch Checklist

- `render.yaml` is committed.
- `.env` is not committed.
- `DATABASE_URL` points to `data/stock_platform.db`.
- Generated data paths point to `data/`.
- Kite trading and portfolio flags remain false.
- The app disclaimer is visible.
- Strategy Scanner wording remains research-only.
- No personal Zerodha account data is shown.

## Known Limitations

- SQLite on Render free is temporary. Refresh runs and scanner results can
  disappear after redeploys, restarts, or idle spin-downs.
- Free web services have cold starts after idle periods.
- A paid persistent disk or Postgres is required for durable hosted history.
- The deployed app has a fresh database unless you separately migrate data, and
  on the free plan that data is not durable.
- yfinance can be delayed, incomplete, or rate-limited.
- Kite does not provide fundamentals, delivery percentage, result calendar, or
  corporate-action reliability by itself.
- Public deployment should be treated as a demo or personal research interface,
  not as a regulated investment advisory product.

## Later Durable Option

When the free demo is working, switch to a durable deployment by changing
`render.yaml` back to a paid plan and adding a persistent disk:

```yaml
plan: starter
disk:
  name: stock-platform-data
  mountPath: /var/data
  sizeGB: 1
```

Then set:

```text
DATABASE_URL=sqlite:////var/data/stock_platform.db
CACHE_DIR=/var/data/cache
RAW_DIR=/var/data/raw
PROCESSED_DIR=/var/data/processed
```
