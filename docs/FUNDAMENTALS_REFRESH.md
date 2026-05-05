# Fundamentals Refresh Guide

This guide explains how to refresh company fundamentals safely for local research.

## What This Does

The platform now supports a stronger fundamentals pipeline:

- `DbFundamentalsProvider` reads saved annual and quarterly rows from SQLite.
- `YFinanceFundamentalsProvider` can fill the DB when rows are missing.
- `ScreenerFundamentalsProvider` is available as an experimental source foundation.
- `refresh_fundamentals` can write source-tagged rows into:
  - `fundamentals_annual`
  - `fundamentals_quarterly`

The app can compare sources and show warnings when values disagree.

## Important Limits

Use this as a research aid only. Do not assume a metric is correct just because it appears in the UI.

- yfinance coverage for Indian companies can be incomplete.
- Screener parsing is implemented, but source ToS and redistribution rules still need review before scaled or public use.
- Do not run large aggressive refreshes against public sites.
- Do not redistribute cached fundamentals commercially without a proper data-source and legal review.
- Bank and NBFC metrics still need better source handling for NIM, GNPA, NNPA, CASA, capital adequacy, and related fields.

## Safe First Command

Run a dry-run first. A dry-run fetches and validates rows but does not write to the database.

```powershell
.\.venv\Scripts\python.exe -m stock_platform.jobs.refresh_fundamentals --universe nifty_50 --source yfinance --max-symbols 3 --dry-run
```

Expected result:

- It prints one row per symbol.
- It shows how many annual and quarterly rows would be found.
- It does not change the local database.

## Small Local Write

After the dry-run looks sensible, write a tiny batch:

```powershell
.\.venv\Scripts\python.exe -m stock_platform.jobs.refresh_fundamentals --universe nifty_50 --source yfinance --max-symbols 3
```

Then open the app:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_app.ps1
```

Check:

- `Stock Research`
- `Fundamentals`
- `Data trust details`

## Experimental Screener Dry-Run

Only use this for small tests while source policy is unsettled:

```powershell
.\.venv\Scripts\python.exe -m stock_platform.jobs.refresh_fundamentals --universe nifty_50 --source screener --max-symbols 3 --dry-run
```

If it works, compare output with yfinance before trusting it:

```powershell
.\.venv\Scripts\python.exe -m stock_platform.jobs.refresh_fundamentals --universe nifty_50 --source yfinance --source screener --max-symbols 3 --dry-run
```

## What Not To Do Yet

- Do not run all-listed Screener refreshes.
- Do not deploy cached Screener data publicly.
- Do not treat cross-source disagreement as a buy/sell signal.
- Do not use these outputs as investment advice.

## Next Improvement

Pick a long-term fundamentals source policy:

- Screener: useful Indian coverage, but ToS must be reviewed.
- Tijori: good Indian-market context if subscription/export/API access is available.
- Trendlyne: useful structured data if access terms fit the project.

Until then, keep fundamentals source labels and missing-data warnings visible.
