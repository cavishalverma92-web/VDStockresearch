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
