# Phase 0-8 Refinement Review

Last updated: 2026-04-26

This document records the current state after reviewing the project against the
master prompt from Phase 0 through Phase 8.

## Overall Status

The project is now a verified local research MVP through Phase 8. It can:

- Load Indian stock OHLCV data through yfinance
- Validate price data before analytics
- Display chart, technical indicators, signals, fundamentals, flows/events, and holdings
- Build an explainable composite research score
- Run signal backtests and walk-forward validation where local signal history exists
- Show data provenance, health checks, alert previews, and backup guidance
- Scan configured universes through the Phase 8 Top Opportunities scanner
- Save Phase 8 scanner runs to SQLite for later review/export
- Compare the latest saved scanner run against the previous run
- Maintain a local research shortlist from scanner rows
- Apply financial-sector fundamentals rules for banks and financial services
- Load an all-NSE-listed local universe from NSE's `EQUITY_L.csv`

## Phase Review

| Phase | Current status | Refinement made |
|---|---|---|
| Phase 0 - Foundations | Complete | App runs locally; logs, `.env`, tests, local DB, and health checks exist |
| Phase 1 - Fundamentals | MVP complete / source risk remains | yfinance fundamentals added with CSV fallback and missing-data warnings |
| Phase 2 - Technicals | Complete for MVP | Indicators, signals, trade-plan fields, and price structure are present |
| Phase 3 - Flows & Events | Partial / MVP complete | Delivery %, corporate actions, result volatility, holdings, and deal fallback exist |
| Phase 4 - Composite Scoring | Complete for MVP | Config-driven scoring with reasons, risks, missing-data notes, and position sizing |
| Phase 5 - Backtesting | Complete for local signal history | Strategy summaries, portfolio diagnostics, MFE/MAE, and walk-forward validation |
| Phase 6 - Polish / Alerts / Compliance | Complete for local MVP | Clean UI styling, provenance table, health checks, backup scripts, and alert previews |
| Phase 7 - Real Data Gap Fill | Partial / useful | yfinance fundamentals/holdings, extended ratios, CAGR, bank-safe validation/UI/scoring |
| Phase 8 - Universe Scanner | MVP verified | Nifty scans, CSV-backed all-NSE-listed universe, scan limit controls, saved scan history, score/signal comparison filters, research shortlist, CSV export |

## All-Listed Universe

The all-listed universe is intentionally CSV-backed instead of hardcoded.

Current local file:

```text
data/universe/nse_equity_list.csv
```

Downloaded source:

```text
https://archives.nseindia.com/content/equities/EQUITY_L.csv
```

Latest local download result:

```text
Rows: 2364
EQ equity rows: 2167
```

The scanner universe key is:

```text
all_nse_listed
```

To refresh the file:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\update_nse_universe.ps1
```

## Important Limitations

- The all-NSE-listed CSV is local data and intentionally ignored by Git.
- Index constituents and listed securities change. Refresh before serious research.
- yfinance fundamentals coverage for Indian stocks is uneven.
- Bank-specific fields such as NIM, GNPA, NNPA, CASA, and capital adequacy are not inferred from yfinance.
- Universe-wide scanner scoring currently emphasizes price/technical inputs; full fundamentals/flows across thousands of stocks will need broader caching.
- Full-market scans can take a long time. The UI caps scan size per run so you can increase gradually.
- Outputs remain research aids only, not investment advice.

## Phase 8.1 and 8.2 Refinements

Phase 8.1 is now implemented. Scanner runs are persisted to SQLite with:

- scan date/time
- universe name
- symbol
- score
- active signals
- data-quality warnings
- errors

Phase 8.2 is now implemented. The app can:

- latest score vs previous score
- new active signals
- dropped active signals
- signal-count changes
- local research shortlist from saved scan rows

## Recommended Next Refinement

Add shortlist review actions:

- mark shortlist rows inactive
- add notes/tags
- show latest price/score beside each shortlist row

For Phase 7 fundamentals, the next useful refinement is a structured banking
fundamentals import template for NIM, NPAs, CASA, credit growth, deposit growth,
and capital adequacy.
