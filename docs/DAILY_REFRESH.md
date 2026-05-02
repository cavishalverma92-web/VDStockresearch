# Daily EOD Refresh

The daily refresh stores local market data used by **Market Today**:

- daily OHLCV prices
- technical indicators
- signal audit rows
- persisted Research Conviction scores
- refresh-run audit history

It does **not** use portfolio, holdings, funds, orders, trades, or trading APIs.

## Start Safely From The Website

Open:

```text
http://localhost:8501
```

Go to:

```text
Data Health -> Run Daily EOD Refresh
```

Recommended first run:

- Universe: `nifty_50`
- Max symbols: `5`
- Dry run only: checked

If the dry run looks sensible, uncheck **Dry run only** and run again.

## Command Line

Dry run:

```powershell
.\scripts\run_daily_refresh.ps1 -Universe nifty_50 -MaxSymbols 5 -DryRun
```

Small real refresh:

```powershell
.\scripts\run_daily_refresh.ps1 -Universe nifty_50 -MaxSymbols 5
```

Full Nifty 50 refresh:

```powershell
.\scripts\run_daily_refresh.ps1 -Universe nifty_50
```

## Windows Task Scheduler

Use this only after the manual website run works.

1. Open **Task Scheduler** from the Windows Start menu.
2. Choose **Create Basic Task**.
3. Name it `Stock Platform Daily Refresh`.
4. Trigger: Daily, after Indian market close, for example `18:30`.
5. Action: Start a program.
6. Program/script:

```text
powershell.exe
```

7. Arguments:

```text
-ExecutionPolicy Bypass -File "C:\Users\Vishal Verma\Desktop\Stock Tracker\indian-stock-research-platform\scripts\run_daily_refresh.ps1" -Universe nifty_50
```

8. Start in:

```text
C:\Users\Vishal Verma\Desktop\Stock Tracker\indian-stock-research-platform
```

## What To Check After It Runs

In the app, open **Data Health** and confirm:

- Last refresh has a run number.
- Failed symbols are low or zero.
- Price rows increased.
- Composite rows increased.
- Market Today shows fresh breadth and attention rows.

## Common Issues

If Kite token is expired, the market-data router should fall back to yfinance
where possible. For Kite-first refreshes, open **Settings** and regenerate the
Kite token.

If many symbols fail, use a smaller `Max symbols` value first and inspect the
per-symbol error table in Data Health.
