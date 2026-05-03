# Index Membership History

Backtests can become misleading if they use today's Nifty 50 list for old
dates. That mistake is called survivorship bias: weak or removed companies
disappear from the test universe, making past performance look cleaner than it
really was.

This project now has a lightweight index-membership history foundation.

## What Was Added

- Database table: `index_membership_history`
- Job: `stock_platform.jobs.refresh_index_membership`
- PowerShell helper: `scripts/refresh_index_membership.ps1`
- Repository helpers:
  - `sync_index_membership_snapshot`
  - `list_index_members_on`
  - `was_index_member_on`

## What It Does Today

The job downloads the current official NSE Nifty 50 CSV and records those
symbols as active members from the effective date you choose.

```powershell
.\scripts\refresh_index_membership.ps1 -Universe nifty_50
```

To set a specific start date:

```powershell
.\scripts\refresh_index_membership.ps1 -Universe nifty_50 -EffectiveDate 2026-05-03
```

If a symbol was active in the table before but is missing from the new official
CSV, the old membership period is closed one day before the new effective date.

## Important Limitation

The first snapshot date is not the true historical entry date for every stock.
It means:

```text
We confirmed this stock was in the index as of this snapshot date.
```

Historical backtests still need a backfill from archived NSE constituent files
before they can be called fully survivorship-safe.

## How Backtests Use It

The signal backtest runner can now optionally filter events by index membership
on the event date. This is the first code-level guardrail against using the
wrong universe for old signals.

## Next Improvement

Backfill monthly historical NSE index files into `index_membership_history`.
That will let us test strategies against the actual Nifty 50 membership on each
old signal date instead of only the current snapshot.

## Historical Backfill From Local CSV Files

The project now has a safe local-file importer. It does not scrape aggressively
or guess from websites. You place archived constituent CSVs in a folder, and the
job imports them in chronological order.

Recommended folder:

```text
data/universe/history/nifty_50/
```

Recommended filenames:

```text
nifty_50_2024-03-31.csv
nifty_50_2024-06-30.csv
nifty_50_2024-09-30.csv
```

The date in the filename becomes the snapshot effective date.

Dry run first:

```powershell
.\scripts\import_index_membership_history.ps1 -Universe nifty_50
```

Apply and rebuild this index/source from the files:

```powershell
.\scripts\import_index_membership_history.ps1 -Universe nifty_50 -Apply -ReplaceExisting
```

Why `-ReplaceExisting` exists:

If you already have today's Nifty 50 snapshot and then import old files, the
job must rebuild the periods chronologically. Without replacement, old files can
overlap newer active rows and create misleading periods. The explicit flag makes
that choice visible.

After import, run:

```powershell
.\scripts\refresh_index_membership.ps1 -Universe nifty_50
```

That re-adds the latest official snapshot as the current open-ended period.
