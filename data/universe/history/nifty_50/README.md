# Nifty 50 Historical Constituents

Place archived official NSE Nifty 50 constituent CSV files in this folder.

Use filenames with a snapshot date:

```text
nifty_50_2024-03-31.csv
nifty_50_2024-06-30.csv
ind_nifty50list_20240930.csv
```

The CSV files themselves are intentionally not committed to Git. They are local
research data and should be reviewed for source/licensing limits before sharing.

Dry run:

```powershell
.\scripts\import_index_membership_history.ps1 -Universe nifty_50
```

Apply after review:

```powershell
.\scripts\import_index_membership_history.ps1 -Universe nifty_50 -Apply -ReplaceExisting
```
