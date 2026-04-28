# Local Universe Files

This folder is for local exchange master files.

To create the all-NSE-listed universe, run from the project root:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\update_nse_universe.ps1
```

That downloads NSE's `EQUITY_L.csv` to:

```text
data/universe/nse_equity_list.csv
```

The downloaded CSV is intentionally ignored by Git because listed securities
change over time and source terms should be reviewed before redistribution.
