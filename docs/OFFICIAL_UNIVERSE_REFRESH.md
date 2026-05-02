# Official NSE Universe Refresh

This project uses `config/universes.yaml` as a small local seed list for scans.
Index membership changes over time, so the Nifty 50 list should be refreshed
from the official NSE CSV before serious daily use.

## What This Does

- Downloads the current official NSE Nifty 50 constituent CSV.
- Saves a local snapshot under `data/universe/official/`.
- Validates required columns such as `Symbol`, `Series`, and `ISIN Code`.
- Converts NSE symbols such as `RELIANCE` to app symbols such as `RELIANCE.NS`.
- Optionally updates the `nifty_50` block in `config/universes.yaml`.

## What This Does Not Do Yet

- It does not create survivorship-safe historical membership.
- It does not prove which stocks were in Nifty 50 on an old backtest date.
- It does not update fundamentals, events, holdings, or portfolio data.
- It does not use any trading, holdings, positions, funds, margins, or order APIs.

## Dry Run

Use this first. It downloads and compares but does not edit the config file.

```powershell
.\scripts\update_nse_index_universe.ps1 -Universe nifty_50
```

## Apply The Update

Use this when the dry run looks sensible.

```powershell
.\scripts\update_nse_index_universe.ps1 -Universe nifty_50 -Apply
```

## Verify

```powershell
.\.venv\Scripts\python.exe -c "from stock_platform.analytics.scanner.universe_scanner import load_universe; print(len(load_universe('nifty_50')))"
```

Expected result:

```text
50
```

Then run the daily refresh:

```powershell
.\.venv\Scripts\python.exe -m stock_platform.jobs.refresh_eod_candles --universe nifty_50
```

## Source

Current Nifty 50 constituent CSV:

```text
https://archives.nseindia.com/content/indices/ind_nifty50list.csv
```

Treat this as current membership only. Historical backtests still need a
separate index membership history table to avoid survivorship bias.
