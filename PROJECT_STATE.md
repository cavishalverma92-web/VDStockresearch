# PROJECT_STATE.md

> This file tracks the current state of the project. Update it at the end of every working session so any AI assistant, or future-you, can pick up exactly where you left off.

---

## Current phase

**Phase 9 - Multi-page research workspace and data-health foundation** *(in progress)*

---

## Last successful verification

2026-04-25:
- Python 3.12.10 installed and `.venv` created.
- Dependencies installed from `requirements.txt`.
- `.env` created from `.env.example`.
- `pytest` passed: 5 tests passed.
- Streamlit returned HTTP 200 at `http://localhost:8501`.
- `RELIANCE.NS` data loaded through yfinance and the Streamlit render path.
- Log files created at `logs/app.log` and `logs/data_quality.log`.
- Phase 1 foundation added: stock universe/fundamentals ORM models, fundamentals ratio helpers, Piotroski F-Score, Altman Z-Score, and tests.
- `pytest` passed: 12 tests passed.
- `ruff check src tests` passed.
- Phase 1 local CSV fundamentals provider added with sample/template data.
- Fundamentals data quality validator added.
- Streamlit now displays a fundamentals panel with source visibility and sample-data warnings.
- `pytest` passed: 19 tests passed.
- Fundamentals dashboard improved with a watchlist overview, selected-stock drill-down, and annual revenue/net-income trend.
- `pytest` passed: 20 tests passed.
- Phase 2 foundation started: technical indicators added (SMA/EMA, RSI, MACD, Bollinger Bands, ATR, relative volume, 52W high).
- Educational technical signal scanner added for the seven MVP pattern families.
- Streamlit chart now overlays 20/50/200 EMA and includes Technicals tabs for indicators/signals.
- `pytest` passed: 24 tests passed.
- Signal audit storage added with SQLite-backed `signal_audit` table.
- Streamlit now saves each scan and shows recent signal history in the Signals tab.
- `pytest` passed: 26 tests passed.
- Signal audit persistence now upserts repeated same-day rows by symbol/date/signal/source and collapses legacy duplicates when reading older local databases.
- `ruff check src tests` passed.
- `ruff format --check src tests` passed.
- Basic signal history CSV export added for later backtesting.
- `pytest` passed: 29 tests passed.
- Streamlit smoke test passed with 0 exceptions and 0 rendered errors.
- **Phase 1 completed**: sector-relative percentile ranking added (`analytics/fundamentals/sector_ranking.py`). Sample CSV extended to 5 stocks (RELIANCE, TCS, HDFCBANK, INFY, ICICIBANK) with sector/industry/market_cap_bucket columns. Summary builder now includes sector metadata and calls ranking. New "Sector rankings" tab in Streamlit shows 0-100 percentile ranks vs sector/industry/market cap bucket peers.
- **Phase 2 completed**: signal backtest runner added (`analytics/backtest/signal_backtest.py`). Evaluates forward returns (MFE, MAE, win rate, profit factor) for saved signal observations. New "Signal backtest" tab in Streamlit with holding period selector (5/10/20/60 days), per-signal summary stats, individual trade list, and CSV download.
- Fundamentals validator updated to treat sector/industry/market_cap_bucket as optional metadata columns (not required financial columns).
- `pytest` passed: 52 tests passed.
- `ruff check src tests` passed.
- `ruff format --check src tests` passed.
- **Phase 3 completed**: Institutional Flows & Corporate Events panel added.
  - `data/providers/nse.py`: NSE provider for delivery % (historical equity API), bulk deals, block deals; browser-like session warm-up, graceful fallback to empty DataFrame on network failure.
  - `data/providers/corporate_actions.py`: yfinance provider for dividends, splits, upcoming earnings calendar, earnings history from income statement.
  - `analytics/flows/delivery.py`: rolling 20-day MA, z-score, unusual delivery spike flag (>1.5Ã— MA AND â‰¥40%), trend detection.
  - `analytics/flows/result_volatility.py`: ATR expansion around earnings windows vs baseline; returns event/baseline ATR and volatility multiple.
  - 3 new ORM tables: `DeliveryData`, `BulkBlockDeal`, `CorporateAction`.
  - Streamlit: new "Flows & Events" section with Delivery %, Bulk/Block Deals, and Corporate Actions tabs; all NSE fetches use `@st.cache_data` with 1-hour TTL.
  - `pytest` passed: 76 tests passed.
  - `ruff check src tests` passed.
  - `ruff format --check src tests` passed.
- **Phase 4 completed (MVP)**: config-driven composite scoring engine added.
  - `scoring/composite.py`: combines fundamentals, technicals, delivery/flow context, event-risk context, and sector context into an explainable 0-100 research score.
  - Streamlit: new "Composite Score" section with headline score, sub-scores, positive drivers, risks, and missing-data notes.
  - `tests/test_composite_scoring.py`: verifies score output, missing-data behavior, and UI-ready table conversion.
  - Streamlit deprecation warnings removed by replacing `use_container_width=True` with `width="stretch"`.
  - `pytest` passed: 79 tests passed.
  - `ruff check src tests` passed.
  - `ruff format --check src tests` passed.
- Phase 0-4 gap fill completed:
  - Git is installed locally; local repository initialization is the remaining safe local Phase 0 step before GitHub push.
  - Stock universe model now includes market cap, listing/delisting dates, index membership, and index entry/exit dates.
  - Annual and quarterly fundamentals schemas now include EBITDA, EPS, book value, free cash flow, debt, net debt, cash, enterprise value, and related ratios.
  - Technical indicators now include SMA/EMA 100, ATR %, 20-day historical volatility, 52-week low, all-time high, distance-to-high/low metrics, and MA stack status.
  - Signal outputs now include educational trigger price, entry zone, stop-loss, target, risk/reward, confidence, data used, risk per share, and position sizing.
  - `pytest` passed: 79 tests passed.
  - `ruff check src tests` passed.
  - `ruff format --check src tests` passed.
- **Phase 5 completed (MVP)**: strengthened signal backtesting and walk-forward validation.
  - `analytics/backtest/signal_backtest.py`: added portfolio-level diagnostics: absolute return, CAGR, win rate, average return, max drawdown, Sharpe, Sortino, exposure time, turnover, average holding period, unique symbols, and max symbol concentration.
  - Added walk-forward validation using a 3-year train / 1-year validation window where enough completed trade history exists.
  - Streamlit Signal backtest tab now shows portfolio diagnostics and walk-forward validation in addition to per-signal summary and trade rows.
  - `logs/backtests.log` added for backtest-tagged records.
  - `docs/phase6_readiness.md` added as a lightweight daily-use, backup, alert, compliance, and deployment-readiness checklist.
  - `pytest` passed: 81 tests passed.
  - `ruff check src tests` passed.
  - `ruff format --check src tests` passed.
- **Phase 6 completed (MVP)**: added local daily-use readiness and compliance-safe alert previews.
  - `alerts/rules.py`: builds alert preview rows for strong research candidates, active technical signals, and data-quality warnings. No Telegram/email messages are sent yet.
  - `ops/health.py`: checks local project state, `.env`, sample data, SQLite database, logs folder, Git repository, and Git identity.
  - Streamlit: new "Operations & Alerts" section with alert previews, setup health checks, and backup/GitHub commands.
  - `scripts/health_check.ps1`: prints a beginner-readable local health report.
  - `scripts/backup_local.ps1`: creates a local backup of project state, docs, config, sample data, and SQLite signal history.
  - `README.md` and `docs/phase6_readiness.md` updated for Phase 6.
  - `pytest` passed: 87 tests passed.
  - `ruff check src tests` passed.
  - `ruff format --check src tests` passed.
  - Streamlit AppTest passed with 0 exceptions and 0 rendered errors.
  - Localhost returned HTTP 200 at `http://localhost:8501`.
  - Backup verified at `backups/20260425-222512`.
- **Master prompt audit pass completed**:
  - `docs/master_prompt_audit.md` added with phase-by-phase complete / partial / deferred status.
  - Streamlit Operations & Alerts now includes a "Data provenance" tab showing source, freshness, status, caveat, and generated timestamp for major outputs.
  - `ops/provenance.py` added for UI-ready provenance summaries.
  - `docs/compliance.md` corrected: full raw-response provenance is still open; the current UI source/provenance summary is complete for major local outputs.
  - `config/data_sources.yaml` corrected so yfinance corporate actions are active and NSE bulk/block deals are marked partial.
  - `pytest` passed: 89 tests passed.
  - `ruff check src tests` passed.
  - `ruff format --check src tests` passed.
  - Streamlit AppTest passed with 0 exceptions and 0 rendered errors.
- **Phase 7 completed**: Real fundamentals data via yfinance + institutional holdings.
  - `data/providers/yfinance_fundamentals.py`: `YFinanceFundamentalsProvider` reads `income_stmt`, `balance_sheet`, `cashflow` from yfinance; maps to `FundamentalSnapshot` schema; handles Indian fiscal year (Apr–Mar); derives net_debt, enterprise_value, market_cap_bucket; falls back to empty DataFrame on any error.
  - `data/providers/institutional_holdings.py`: `get_major_holders`, `get_institutional_holders`, `get_mutualfund_holders`, `holdings_summary` via yfinance; normalises column names across yfinance versions; graceful empty-DataFrame fallback.
  - `data/providers/__init__.py`: exports all new providers.
  - `analytics/fundamentals/summary.py`: uses structural `_FundamentalsProviderLike` Protocol so any provider works (not just `CsvFundamentalsProvider`).
  - Streamlit: fundamentals section now tries yfinance first (1-hour cache), falls back to CSV; shows "yfinance (live)" source badge. New "Holdings" tab in Flows & Events shows insider %, institution %, float %, top institutional holders, and top MF holders. Provenance table reflects the live source label.
  - `tests/test_yfinance_fundamentals.py`: 15 tests (fiscal year mapping, market_cap_bucket, row build, net_debt derivation, snapshot typing, empty-on-error).
  - `tests/test_institutional_holdings.py`: 9 tests (major/institutional/MF holders, holdings_summary, graceful fallback).
  - `pytest` passed: 113 tests passed.
  - `ruff check src tests` passed.
  - `ruff format --check src tests` passed.
- **Master prompt gap-fill pass (Phase 0–7) completed**:
  - `analytics/fundamentals/cagr.py`: multi-year CAGR (3Y / 5Y / 10Y) for Revenue, EBITDA, Net income, EPS, OCF, FCF, Book value. Returns None when fewer than the required years exist or when start ≤ 0.
  - `analytics/fundamentals/extended_ratios.py`: interest coverage, DSO/DIO/DPO, cash conversion cycle (days), working capital trend (latest, prior-year, YoY change, 3Y slope), `compute_extended_health()` composite.
  - `analytics/fundamentals/schema.py`: `FundamentalSnapshot` extended with `accounts_receivable`, `inventory`, `accounts_payable`, `interest_expense`, `cost_of_revenue`.
  - `data/providers/yfinance_fundamentals.py`: maps `Interest Expense`, `Cost Of Revenue`, `Accounts Receivable`, `Inventory`, `Accounts Payable` from yfinance.
  - `analytics/technicals/structure.py`: `detect_swing_pivots()`, `find_support_resistance_zones()`, `latest_swing_levels()` for fractal-based price structure analysis.
  - Streamlit: fundamentals "Selected stock" tab now shows multi-year CAGR table + extended balance-sheet health (interest coverage, CCC, working capital, DSO/DIO/DPO). Technicals section gains a new "Price structure" tab with last swing high/low and clustered S/R zones.
  - `tests/test_cagr.py`: 9 tests covering CAGR edge cases.
  - `tests/test_extended_ratios.py`: 10 tests covering interest coverage, CCC, working capital trend.
  - `tests/test_price_structure.py`: 8 tests covering swing pivots and zone clustering.
  - `pytest` passed: 140 tests passed.
  - `ruff check src tests` passed.
  - `ruff format --check src tests` passed.
- **Phase 8 takeover and hardening completed**:
  - Reviewed Claude Code's Phase 8 scanner work.
  - `config/universes.yaml`: seed universe lists added for `nifty_50` and `nifty_next_50`; wording corrected so they are not presented as legally verified current index files.
  - `config.py`: added `get_universes_config()`.
  - `analytics/scanner/universe_scanner.py`: scans configured universes or explicit symbol lists through yfinance OHLCV, OHLCV validation, technical indicators, signal scanner, and composite score.
  - Scanner now defaults to sequential yfinance scans for reliability, clamps unsafe worker counts, validates OHLCV before scoring, stores data-quality warnings, and degrades per-symbol instead of killing the whole scan.
  - Streamlit: added "Top Opportunities (universe scan)" expander with universe selector, min score filter, min active-signal filter, progress bar, results table, data-quality warnings, and CSV export.
  - Provider logging corrected from `%s` / `%d` placeholders to Loguru `{}` formatting for readable logs.
  - `tests/test_universe_scanner.py`: scanner tests added and expanded for data-quality failure and worker-count clamping.
  - `pytest` passed: 153 tests passed.
  - `ruff check src tests` passed.
  - `ruff format --check src tests` passed.
  - Streamlit AppTest passed with 0 exceptions and 0 rendered errors.
- **Phase 0-8 refinement pass completed**:
  - UI header/disclaimer styling tightened for a cleaner Streamlit experience.
  - `all_nse_listed` universe added as a CSV-backed local universe.
  - `scripts/update_nse_universe.ps1` added to download NSE's official `EQUITY_L.csv`.
  - Local NSE equity list downloaded successfully to `data/universe/nse_equity_list.csv`.
  - Download result: 2,364 rows, including 2,167 `EQ` equity rows.
  - Scanner UI now shows universe size, limits symbols per run, disables scans when CSV-backed universes are missing, and displays the refresh command.
  - `docs/phase_0_8_refinement.md` added.
- **Phase 8.1 completed**: scanner result persistence.
  - `UniverseScanRun` and `UniverseScanResult` ORM tables added.
  - `analytics/scanner/persistence.py` added to save scan runs, fetch the latest run, and convert saved rows back to a UI-ready DataFrame.
  - Streamlit Top Opportunities scanner now saves each completed scan to SQLite and shows the latest saved scan for the selected universe.
  - `tests/test_universe_scan_persistence.py` added.
- **Phase 8.2 completed**: saved-scan comparison and local shortlist.
  - Latest saved scan is compared against the previous saved scan for the same universe.
  - UI now shows previous score, score change, signal-count change, newly active signals, and comparison status.
  - `ResearchWatchlistItem` ORM table added for a local research shortlist.
  - `analytics/scanner/watchlist.py` added to add/fetch/export shortlist rows.
  - Streamlit Top Opportunities scanner now lets selected latest-scan symbols be added to the local research shortlist.
  - `tests/test_research_watchlist.py` added.
- **Phase 7/8 gap-fill completed**: sector-aware financial fundamentals and scanner filters.
  - `analytics/fundamentals/sector_policy.py` added to identify banks/financial services and define sector-aware required fields.
  - Fundamentals validation now applies bank-safe rules and no longer treats missing industrial fields as critical errors for financial stocks.
  - Composite scoring now skips Altman Z-Score and industrial debt/equity penalties for financial-sector stocks.
  - Streamlit selected-stock fundamentals UI now labels financial-sector rules and hides industrial working-capital metrics for banks/financials.
  - Phase 8 latest saved scan now has filters for comparison status, minimum score change, and newly active signals.
- **User-flow smoke pass completed**:
  - Streamlit AppTest passed for default `RELIANCE.NS`, dropdown `HDFCBANK.NS`, and custom `HDFCBANK` entry with 0 exceptions and 0 rendered errors.
  - Custom ticker entry now appends `.NS` automatically when the user enters a bare NSE symbol.
  - Missing/stale ticker data now shows a beginner-readable explanation instead of a terse data error.
  - `TATAMOTORS.NS` currently returns no yfinance price data in the smoke test and should be treated as a stale/source-specific symbol until verified against an official exchange source.
  - `pytest` passed: 165 tests passed, 4 warnings.
  - `.venv\Scripts\ruff.exe check src tests` passed.
  - `.venv\Scripts\ruff.exe format --check src tests` passed.
  - `scripts/health_check.ps1` passed all local checks, including Git identity.
  - Localhost returned HTTP 200 at `http://localhost:8501`.
- **RELIANCE.NS yfinance incomplete-row fix completed**:
  - yfinance returned one latest RELIANCE.NS row with missing close/adjusted close on 2026-04-28.
  - `YahooFinanceProvider` now drops incomplete OHLC rows while keeping valid historical rows.
  - RELIANCE.NS provider check returned 1,234 usable rows, last valid close on 2026-04-24.
  - Streamlit AppTest passed for RELIANCE.NS with 0 exceptions and 0 rendered errors.
  - Localhost returned HTTP 200 after restarting Streamlit.
  - `.venv\Scripts\ruff.exe check src tests` passed.
  - `.venv\Scripts\ruff.exe format --check src tests` passed.
  - `pytest tests\test_ohlcv_validator.py tests\test_universe_scanner.py` passed: 20 tests passed, 4 warnings.
- **Phase 8.3 completed**: research shortlist review workflow.
  - `ResearchWatchlistItem` now supports `review_status`, `tags`, and `notes`.
  - Existing local SQLite databases are upgraded in-place with the new shortlist review columns.
  - `analytics/scanner/watchlist.py` now supports review edits, inactive shortlist rows, and latest saved scanner score enrichment.
  - Streamlit Top Opportunities section now shows an editable shortlist review table with status, tags, notes, active/inactive toggle, latest score, latest band, latest close, latest active signals, and source scan run.
  - `tests/test_research_watchlist.py` expanded for review edits, inactive row hiding, latest score enrichment, and legacy SQLite column upgrade.
  - `pytest` passed: 168 tests passed, 4 warnings.
  - `.venv\Scripts\ruff.exe check src tests` passed.
  - `.venv\Scripts\ruff.exe format --check src tests` passed.
  - Streamlit AppTest passed with 0 exceptions and 0 rendered errors.
  - Localhost returned HTTP 200 at `http://localhost:8501`.
- **Phase 7.1 completed**: manual banking fundamentals template.
  - `BankingFundamentalSnapshot` added for bank/financial-services metrics.
  - `CsvBankingFundamentalsProvider` added for manual CSV-backed NIM, GNPA, NNPA, CASA, credit growth, deposit growth, and capital adequacy.
  - Empty audited-data template added at `data/sample/banking_fundamentals_template.csv`.
  - `validate_banking_fundamentals()` added with schema, duplicate year, numeric, percentage range, source URL, and last-updated checks.
  - Streamlit financial-stock fundamentals section now shows banking metrics when rows exist, or a clear template path when no manual rows exist.
  - HDFCBANK Streamlit AppTest passed with 0 exceptions and 0 rendered errors.
  - `pytest` passed: 173 tests passed, 4 warnings.
  - `.venv\Scripts\ruff.exe check src tests` passed.
  - `.venv\Scripts\ruff.exe format --check src tests` passed.
- **Phase 7.2 completed**: banking metrics composite score integration.
  - `score_stock()` now accepts optional `banking_fundamentals` rows.
  - Financial-sector fundamentals scoring uses manual banking metrics when validated CSV rows exist.
  - Banking score logic rewards healthier NIM, lower GNPA/NNPA, stronger CASA, comfortable capital adequacy, and balanced credit/deposit growth.
  - If no manual banking metrics exist, the composite score explicitly records `manual banking metrics` as missing and uses the general financial-sector fallback.
  - Score explainability now includes manual banking metric source and last-updated metadata when available.
  - Weak banking metrics add risk notes for elevated GNPA/NNPA, thin capital adequacy, and credit growth materially ahead of deposit growth.
  - HDFCBANK Streamlit AppTest passed with 0 exceptions and 0 rendered errors.
  - `pytest` passed: 175 tests passed, 4 warnings.
  - `.venv\Scripts\ruff.exe check src tests` passed.
  - `.venv\Scripts\ruff.exe format --check src tests` passed.
- **Phase 7.3 entry support added**: banking metrics data-entry guide.
  - `docs/banking_fundamentals_entry_guide.md` added with field definitions, source priority, and data-entry rules.
  - Streamlit bank/financial fundamentals section now includes a data-entry helper when no manual banking rows exist.
  - The helper shows the exact CSV header and a blank row skeleton for the selected symbol without inventing metric values.
  - `.venv\Scripts\ruff.exe check src tests` passed.
  - `.venv\Scripts\ruff.exe format --check src tests` passed.
  - `pytest` passed: 175 tests passed, 4 warnings.
  - Localhost returned HTTP 200 at `http://localhost:8501`.
  - Streamlit AppTest passed for default `RELIANCE.NS` and dropdown `HDFCBANK.NS` with 0 exceptions and 0 rendered errors.
- **Original prompt gap-fill completed**: selected-stock Data Trust panel.
  - `ops/data_trust.py` added to summarize source freshness, missing inputs, validation warnings, and score reliability.
  - Streamlit Composite Score section now shows a visible `Data trust` level, action-item count, and expandable source/reliability checklist.
  - Banking metrics are marked not applicable for non-financial stocks, and marked as an action item for banks when audited manual rows are missing.
  - `tests/test_data_trust.py` added.
  - `pytest` passed: 178 tests passed, 4 warnings.
  - `.venv\Scripts\ruff.exe check src tests` passed.
  - `.venv\Scripts\ruff.exe format --check src tests` passed.
  - Localhost returned HTTP 200 at `http://localhost:8501`.
  - Streamlit AppTest passed for default `RELIANCE.NS` and dropdown `HDFCBANK.NS` with 0 exceptions and 0 rendered errors.
- **Phase 8.4 reliability improvement completed**: one-click local app start.
  - `scripts/run_app.ps1` upgraded into a robust local starter.
  - `start_app.cmd` added for double-click startup on Windows.
  - Startup now checks `.venv`, frees port `8501` when needed, starts Streamlit in the background, writes live logs, verifies HTTP 200, and opens `http://localhost:8501`.
  - Live startup logs are written to `logs/streamlit_live_stdout.log` and `logs/streamlit_live_stderr.log`.
  - Startup script verification passed with existing app detection, correct port PID capture, and HTTP 200.
  - `pytest` passed: 178 tests passed, 4 warnings.
  - `.venv\Scripts\ruff.exe check src tests` passed.
  - `.venv\Scripts\ruff.exe format --check src tests` passed.

---

## Completed tasks

- [x] Project scaffold created (folders, configs, VS Code settings, placeholders)
- [x] Python installed and `python --version` works in terminal
- [x] Git installed and `git --version` works
- [ ] VS Code installed with recommended extensions
- [x] Virtual environment created (`.venv`)
- [x] Dependencies installed from `requirements.txt`
- [x] `.env` created from `.env.example`
- [x] First run of `streamlit run src/stock_platform/ui/streamlit_app.py` succeeded
- [x] `RELIANCE.NS` chart visible on `localhost:8501`
- [x] Log file created at `logs/app.log`
- [x] Phase 1 stock universe and fundamentals schema foundation added
- [x] Phase 1 basic ratios, growth, Piotroski, and Altman calculations added
- [x] Phase 1 local CSV fundamentals provider added
- [x] Fundamentals source and missing-data warnings visible in UI
- [x] Fundamentals watchlist overview and selected-stock trend view added
- [x] Phase 2 technical indicators added
- [x] Phase 2 educational signal scanner added
- [x] Technical overlays and signal table visible in UI
- [x] Signal audit storage added
- [x] Recent signal history visible in UI
- [x] Repeated same-day signal audit rows are deduplicated/upserted
- [x] Basic signal history CSV export available from the Signals tab
- [x] Phase 3 delivery % analytics, bulk/block deals, corporate actions added
- [x] Phase 4 composite research score added
- [x] Composite score explainability panel visible in UI
- [x] Phase 0-4 prompt-gap pass completed for locally implementable MVP items
- [x] Local Git repository initialized
- [x] Phase 5 portfolio-level backtest diagnostics added
- [x] Phase 5 walk-forward validation added
- [x] Phase 6 readiness checklist added
- [x] Phase 6 alert preview rules added
- [x] Phase 6 local health-check helper added
- [x] Phase 6 local backup helper added
- [x] Operations & Alerts section visible in Streamlit
- [x] Data provenance tab visible in Streamlit
- [x] Master prompt audit document added
- [x] Phase 7 yfinance fundamentals provider added
- [x] Phase 7 institutional holdings provider added
- [x] Phase 7 multi-year CAGR, extended balance-sheet health, and price-structure gap fill added
- [x] Phase 8 universe scanner added
- [x] Phase 8 top-opportunities UI added
- [x] Phase 8 scanner data-quality validation added
- [x] `all_nse_listed` CSV-backed universe added
- [x] Official NSE equity list downloaded locally for all-listed universe
- [x] Phase 0-8 refinement review documented
- [x] Phase 8.1 scanner results persisted to SQLite
- [x] Phase 8.2 saved-scan comparison added
- [x] Phase 8.2 local research shortlist added
- [x] Phase 7 sector-aware bank/financial fundamentals rules added
- [x] Phase 8 saved-scan comparison filters added
- [x] Custom ticker UX improved: bare NSE symbols auto-normalize to `.NS`, and stale/no-data symbols show practical next checks
- [x] Phase 8.3 research shortlist review workflow added
- [x] Phase 7.1 manual banking fundamentals CSV template/provider/validator/UI added
- [x] Phase 7.2 banking metrics integrated into composite score when manual rows exist
- [x] Phase 7.3 banking metrics data-entry guide/helper added
- [x] Selected-stock Data Trust panel added for source freshness, missing inputs, and score reliability
- [x] One-click Windows startup script added for reliable localhost app launch
- [x] Product UX/design pass added: cleaner sidebar navigation, research guardrails, interactive chart controls, and hover definitions for technical terms
- [x] Zerodha Kite Connect market-data foundation added: Kite-preferred router, yfinance fallback, instrument metadata, LTP/OHLC/quote methods, historical candles, and safe auth setup
- [x] Zerodha API Setup moved to a directly reachable standalone section in the Streamlit UI
- [x] Priority 1 multi-page Streamlit split started: Market Today, Stock Research, Top Opportunities, Watchlist, Backtests, Data Health, and Settings pages
- [ ] Initial commit pushed to private GitHub repo

---

## Open issues / blockers

- PowerShell `CurrentUser` execution policy could not be changed in this environment; use process-scoped policy or run the app through `.venv\Scripts\python.exe -m streamlit ...`.
- Running `.ps1` scripts directly may be blocked by Windows execution policy. Use a temporary process-only bypass when needed: `powershell -ExecutionPolicy Bypass -File .\scripts\health_check.ps1`.
- Local Git repository is initialized, but no private GitHub remote is connected yet.

---

## Data sources connected

| Source | Status | Used for | Notes |
|---|---|---|---|
| Zerodha Kite Connect | preferred when configured | Market data + instrument metadata | Preferred provider for instrument master, token mapping, historical candles, LTP, OHLC, and quotes. No holdings, positions, funds, margins, orders, trades, or profile display are enabled. |
| yfinance | connected fallback | OHLCV prices | Used automatically when Kite is not configured, token is missing/expired, Kite fails, instrument token is missing, validation fails, or `MARKET_DATA_PROVIDER=yfinance`. |
| local CSV | connected | Fundamentals sample/template | Safe Phase 1 bridge; current sample rows are placeholders, not verified source data |
| NSE equity API | connected | Delivery %, bulk/block deals | Phase 3; requires live network + browser-like session |
| Screener.in | not yet | Fundamentals | Phase 1; verify ToS before connecting |
| AMFI | not yet | MF holdings | Phase 4 |
| SEBI PIT | not yet | Insider trades | Phase 4 |

---

## Database

- **Target**: PostgreSQL 16+ (managed: Supabase or Neon for production)
- **Current**: SQLite fallback path configured through `DATABASE_URL`
- **Phase 1 status**: stock universe and fundamentals schema foundation added; migrations and real provider still pending
- **Phase 2 status**: signal audit rows are saved to local SQLite at `data/stock_platform.db`; repeated same-day scans are upserted instead of duplicated
- **Phase 8.1 status**: universe scanner runs and per-symbol results are saved to local SQLite in `universe_scan_runs` and `universe_scan_results`
- **Phase 8.2 status**: local research shortlist rows are saved in `research_watchlist_items`

---

## Known limitations

- Fundamentals provider currently uses local CSV sample/template data only; real source selection and ToS review pending
- Phase 2 signals are educational pattern observations only; no backtest or recommendation engine yet
- NSE delivery % and bulk/block deal APIs require live network and may fail if NSE blocks automated requests; UI degrades gracefully to "data unavailable" messages
- Composite scoring, entry-zone, stop-loss, target-zone, risk/reward, and position-sizing MVPs exist; they remain educational approximations until validated with real data and backtests
- Full raw-response storage with URL/timestamp provenance is not complete yet; the UI now shows a provenance summary for major local outputs.
- Phase 6 alerts are preview-only. No Telegram/email delivery exists yet because alert compliance wording, rate limits, and user preferences must be reviewed first.
- Phase 8 universe lists are seed watchlists. Refresh them from official NSE index files before treating them as current index membership.
- Phase 8 scanner currently uses price/technical inputs first; universe-wide fundamentals and flow scoring are still limited.
- Phase 8 scans run sequentially by default because live yfinance calls showed thread-safety issues during two-symbol verification.
- Full all-listed scans can take a long time; use the UI's `Max symbols` control and increase gradually.
- Saved scanner rows are append-only. There is no pruning/cleanup UI yet, so old local scan history may grow over time.
- Research shortlist rows can now be edited with status/tags/notes and marked inactive. There is no permanent delete UI by design; keeping inactive rows is better for audit history.
- Financial-sector rules prevent irrelevant industrial warnings. Manual banking metrics can now be entered through `data/sample/banking_fundamentals_template.csv`, but real values still need audited source entry and later official/provider automation.
- Live yfinance symbols can be stale or unavailable even when a seed universe contains them. The UI now explains this, but the universe files still need periodic official refresh and symbol-change review.
- yfinance can occasionally return an incomplete latest OHLC row. The provider now drops incomplete rows, so the chart may show the most recent fully usable trading day rather than the newest partially broken row.
- Zerodha Kite Connect is now the preferred market-data provider only when configured and usable. It does not show or fetch portfolio holdings, positions, funds, margins, orders, trades, or profile details. yfinance remains the automatic fallback.

---

## Model / scoring weight history

| Date | Change | Reason |
|---|---|---|
| - | Initial weights in `config/scoring_weights.yaml` | Phase 0 placeholder |

---

## Backtest assumptions

- Signal backtests currently use saved signal events from local signal history.
- Entry price uses the stored signal close.
- Exit price uses the close after the selected fixed holding period.
- Forward bars are strictly after the signal date to reduce look-ahead bias.
- MFE/MAE use high/low within the holding window.
- Walk-forward validation uses 3 calendar years for train and the next 1 calendar year for validation when enough completed trade history exists.
- Current results are small-sample and educational until the platform has historical signal events across many stocks and regimes.
- Phase 6 alert previews must not be treated as backtested live alerts. They are readiness checks for future notification delivery.

---

## Outstanding compliance concerns

- [x] Disclaimer text is present in UI (Phase 0)
- [ ] Disclaimer text reviewed against SEBI RA/RIA guidelines before any monetization
- [ ] Data source ToS reviewed before any redistribution
- [ ] Legal review required before opening platform to any third party
- [x] Zerodha trading and portfolio APIs intentionally disabled in UI/provider foundation

---

## Current folder structure

See `README.md` -> "Folder structure".

---

## Current commands to run the app

```powershell
# from the project root
powershell -ExecutionPolicy Bypass -File .\scripts\run_app.ps1
```

Windows one-click option: double-click `start_app.cmd`.

## Current commands for daily health / backup

```powershell
# health check
powershell -ExecutionPolicy Bypass -File .\scripts\health_check.ps1

# local backup
powershell -ExecutionPolicy Bypass -File .\scripts\backup_local.ps1
```

---

## Next planned task

Verify the new multi-page Streamlit navigation manually in the browser, then continue shrinking duplicated/legacy page logic into shared components.

---

## Recent updates

- **Phase 8.5 polish completed (2026-04-29)**:
  - `analytics/scanner/daily_brief.py`: `daily_brief_headline()` returns a one-line TL;DR (universe + counts of new opportunities, score movers, new signals, DQ actions, shortlist follow-ups), and `daily_brief_freshness()` classifies the latest saved scan as fresh / aging / stale / unknown with a human-readable age string.
  - Streamlit Daily Research Brief now shows the TL;DR sentence above the metric row, plus a green/amber/red freshness banner so stale scans are not relied on by mistake.
  - `tests/test_daily_brief.py`: 11 new tests (headline pluralisation + freshness for fresh/aging/stale/unknown/iso-string/naive-datetime/under-an-hour).
  - `pytest` passed: 192 tests passed.
  - `ruff check src tests` passed.
  - `ruff format --check src tests` passed.
  - Localhost returned HTTP 200 at `http://localhost:8501`.
- **Phase 8.6 product UX/design pass completed (2026-04-30)**:
  - Streamlit sidebar now has clearer navigation links for Daily Brief, Universe Scanner, Fundamentals, Research Guardrails, Composite Score, Interactive Chart, Technicals, Flows & Events, and Operations.
  - New Research Guardrails section added with compliance-safe research stance, supportive evidence, and risk checks. The UI avoids direct buy/sell advice and frames outputs as research queues only.
  - Main chart is now more interactive: range slider, 1M/3M/6M/1Y/All range buttons, unified hover, optional Bollinger Bands, optional 52-week high/low levels, and optional volume overlay.
  - Technical metrics now include hover definitions for RSI, MACD, ATR, relative volume, EMA, ATR %, historical volatility, 52-week high gap, MA stack, and Bollinger Bands.
  - Daily brief empty-scan handling fixed so Streamlit does not crash when saved scanner rows are unavailable or all rows are errors.
  - `pytest` passed: 193 tests passed, 4 warnings.
  - `ruff check src tests` passed.
  - `ruff format --check src tests` passed.
  - Streamlit AppTest passed with 0 exceptions and 0 rendered errors.
  - Localhost returned HTTP 200 at `http://localhost:8501`.
- **Phase 8.7 Zerodha Kite auth foundation completed (2026-05-01)**:
  - `kiteconnect` added to `requirements.txt`.
  - `.env.example` and local `.env` now include `KITE_ACCESS_TOKEN=` placeholder.
  - `KiteProvider` added with login URL generation, request-token exchange, local access-token handling, safe connection test, and future `get_historical_candles()` method.
  - Streamlit now includes a `Zerodha API Setup` section with credential presence checks, login URL generation, request-token input, access-token generation, local `.env` save, and safe connection test.
  - The UI and provider intentionally do not expose API secret, access token, profile details, holdings, positions, funds, margins, orders, trades, or trading actions.
  - `docs/ZERODHA_KITE_SETUP.md` added with beginner setup instructions and disabled API list.
  - yfinance remains the default data source and fallback.
  - `kiteconnect==5.2.0` installed in the local virtual environment through `pip install -r requirements.txt`.
  - `pytest` passed: 197 tests passed, 4 warnings.
  - `ruff check src tests` passed.
  - `ruff format --check src tests` passed.
  - Streamlit AppTest passed with 0 exceptions and 0 rendered errors.
  - Localhost returned HTTP 200 at `http://localhost:8501`.
- **Phase 8.8 Zerodha Kite market-data default completed (2026-05-01)**:
  - `MARKET_DATA_PROVIDER=kite`, `ENABLE_KITE_MARKET_DATA=true`, `ENABLE_KITE_TRADING=false`, and `ENABLE_KITE_PORTFOLIO=false` added to `.env.example` and local `.env` without overwriting existing values.
  - `KiteProvider` expanded for instrument master, instrument token lookup, historical candles, LTP, OHLC, and quote data. Connection testing now uses market data, not profile.
  - Portfolio/trading method names are blocked with `KiteSecurityError`; holdings, positions, funds, margins, orders, trades, and order actions remain disabled.
  - `MarketDataProvider` router added: Kite preferred, yfinance fallback; `MARKET_DATA_PROVIDER=yfinance` bypasses Kite; `auto` uses Kite only when fully configured.
  - Main Streamlit chart now uses the market-data router and visibly reports `Zerodha Kite` or `yfinance fallback`.
  - Zerodha API Setup section now includes safe tests for Kite market-data connection, RELIANCE LTP, and RELIANCE historical candles.
  - `scripts/sync_kite_instruments.py` added to cache NSE instruments under `data/raw/kite`, `data/processed/kite`, and `data/cache/kite`.
  - `docs/ZERODHA_KITE_SETUP.md` updated for market-data default, fallback behavior, instrument sync, and security limits.
  - `tests/test_market_data_provider.py` added and `tests/test_kite_provider.py` expanded.
  - `pytest` passed: 204 tests passed, 4 warnings.
  - `ruff check src tests scripts` passed.
  - `ruff format --check src tests scripts` passed.
  - Streamlit AppTest passed with 0 exceptions and 0 rendered errors.
  - Instrument sync script was smoke-tested with missing credentials and failed gracefully with a beginner-readable message.
- **Phase 8.9 Zerodha setup navigation fix completed (2026-05-01)**:
  - Zerodha API Setup was moved out of the hidden Operations tab into a standalone page section.
  - The sidebar link now points directly to `#zerodha-api-setup`, so clicking it scrolls to the setup controls.
  - The Operations tabs now only contain Alert preview, Data provenance, Health checks, and Backup / GitHub.
  - `ruff format --check src\stock_platform\ui\streamlit_app.py` passed.
  - `ruff check src\stock_platform\ui\streamlit_app.py` passed.
  - Streamlit AppTest passed with 0 exceptions and 0 rendered errors.
  - Localhost returned HTTP 200 at `http://localhost:8501`.
- **Priority 1 multi-page UI split completed (2026-05-02)**:
  - `src/stock_platform/ui/streamlit_app.py` is now a small Streamlit navigation shell instead of a single long-scroll app.
  - Added page files under `src/stock_platform/ui/pages/`: Market Today, Stock Research, Top Opportunities, Watchlist, Backtests, Data Health, and Settings.
  - Added shared UI components under `src/stock_platform/ui/components/` for layout, formatting/helpers, stock loading context, and the cleaner price chart.
  - Market Today is now the default home page and focuses on provider/data freshness, daily brief, saved scans, and persisted conviction scores.
  - Zerodha API Setup moved to Settings, away from the research scroll.
  - Stock Research now defaults back to the configured starter symbol instead of the first alphabetic universe symbol.
  - `ruff check src\stock_platform\ui\streamlit_app.py src\stock_platform\ui\components src\stock_platform\ui\pages` passed.
  - Streamlit router AppTest passed with 0 exceptions and 0 rendered errors.
  - Each new page file passed Streamlit AppTest with 0 exceptions and 0 rendered errors.
  - Focused tests passed: 44 tests passed across Kite provider, market-data router, data-health report, and repositories.
  - Localhost returned HTTP 200 at `http://localhost:8501`.
- **Alembic migration foundation completed (2026-05-02)**:
  - Added Alembic config, environment, and a baseline schema migration under `alembic/`.
  - Added migration helpers in `src/stock_platform/db/migrations.py`.
  - Real file databases now run `alembic upgrade head` through `create_all_tables()`.
  - Existing unversioned local SQLite databases are safely stamped on first rollout after ensuring current tables exist.
  - In-memory test databases still use fast SQLAlchemy table creation.
  - Added beginner documentation at `docs/ALEMBIC_MIGRATIONS.md`.
- **Market Today dashboard upgrade completed (2026-05-02)**:
  - Added `src/stock_platform/ops/market_today.py` to assemble homepage data from persisted local tables.
  - Market Today now shows provider health, persisted breadth, Research Conviction score movers, top attention rows, upcoming event risk, stale symbols, and Kite token expiry status.
  - Live index LTP remains an optional button and does not block the saved-data dashboard.
  - Added tests in `tests/test_market_today.py`.
  - Validation: targeted Ruff passed, 36 focused tests passed, Market Today AppTest passed with 0 exceptions and 0 errors, router AppTest passed with 0 exceptions and 0 errors.
- **Manual daily refresh controls completed (2026-05-02)**:
  - Added a safe Run Daily EOD Refresh form to the Data Health page.
  - The form supports dry runs, small max-symbol test runs, backfill-window controls, overlap days, and a run note.
  - Added `scripts/run_daily_refresh.ps1` for command-line and Windows Task Scheduler usage.
  - Added beginner documentation at `docs/DAILY_REFRESH.md`.
  - Refresh remains market-data only; no portfolio, holdings, funds, orders, or trading APIs are used.
- **First real 5-symbol EOD refresh completed (2026-05-03)**:
  - Ran `refresh_eod_candles --universe nifty_50 --max-symbols 5`.
  - Processed RELIANCE.NS, TCS.NS, HDFCBANK.NS, ICICIBANK.NS, and INFY.NS.
  - Result: run #1 completed, 5 successful, 0 failed.
  - Persisted 6,175 price rows, 6,110 technical rows, and 5 Research Conviction score rows.
  - Kite historical candles returned TokenException, so the provider router correctly used yfinance fallback.
  - Market Today now has persisted breadth and 5 top-attention rows from local data.
- **Kite token diagnostic and UX hardening completed (2026-05-03)**:
  - Safe market-data diagnostic confirmed API key, API secret, and access token are present.
  - Kite instrument lookup works, but LTP and historical candles are rejected with an incorrect API key/access token error.
  - Improved Kite error handling to show a beginner-readable regenerate-token message without exposing secrets.
  - Added a Settings button to clear the saved local Kite token before regenerating.
  - No portfolio, holdings, funds, order, trade, or profile APIs were added or called.
- **First Kite-backed 5-symbol refresh completed (2026-05-03)**:
  - After token regeneration, Kite LTP and RELIANCE historical candles tested successfully.
  - Initial Kite refresh exposed duplicate-date technical snapshot handling when yfinance and Kite rows coexist.
  - Fixed technical snapshot upsert to deduplicate incoming dates before insert/update.
  - Reran 5-symbol refresh successfully as run #3: 5 successful, 0 failed, 20 Kite price rows, 20 Kite technical rows, and 5 Kite score rows.
  - Current local source mix: yfinance historical backfill plus Kite recent bars.
- **Official Nifty 50 universe refresh completed (2026-05-03)**:
  - Pulled the official NSE Nifty 50 CSV from `https://archives.nseindia.com/content/indices/ind_nifty50list.csv`.
  - Updated `config/universes.yaml` so the `nifty_50` seed list matches the official NSE symbols.
  - Replaced stale/removed entries such as `TATAMOTORS.NS` and `LTIM.NS` with current official symbols including `TMPV.NS`, `ETERNAL.NS`, `JIOFIN.NS`, `BEL.NS`, `INDIGO.NS`, `MAXHEALTH.NS`, and `TRENT.NS`.
  - Verification: `load_universe("nifty_50")` returns exactly 50 symbols.
  - Focused tests passed: 53 tests passed across universe scanner, daily refresh, and repositories.
- **First full Kite-backed Nifty 50 refresh completed (2026-05-03)**:
  - Ran `refresh_eod_candles --universe nifty_50` after updating the official NSE universe.
  - Result: run #6 completed, 50 requested, 50 successful, 0 skipped, 0 failed.
  - Persisted/updated 9,453 price rows, 9,349 technical snapshot rows, and 50 Research Conviction score rows from Kite.
  - Local data health summary after refresh: 68,757 total price rows; source mix `kite: 62,582`, `yfinance: 6,175`.
  - Market Today summary reads the latest run as completed, with breadth from the 2026-04-30 latest trade date and 5 top-attention rows.
  - Streamlit router AppTest passed with 0 exceptions and 0 rendered errors.
- **Official NSE universe updater completed (2026-05-03)**:
  - Added `NseIndexProvider` for official NSE index constituent CSV files.
  - Added `refresh_official_universes` job and `scripts/update_nse_index_universe.ps1`.
  - Added documentation at `docs/OFFICIAL_UNIVERSE_REFRESH.md`.
  - The job supports dry-run by default and only edits `config/universes.yaml` when `--apply` / `-Apply` is used.
  - Ran the updater against official NSE Nifty 50: 50 official rows, no symbols added, no symbols removed.
  - Applied the official NSE row ordering, then verified a second dry-run reports `Config would change: False`.
  - Focused tests passed: 20 tests passed across NSE index parsing, official universe refresh, and universe scanner.
  - Targeted Ruff checks passed for the new provider, job, and tests.
- **Index membership history foundation completed (2026-05-03)**:
  - Added `index_membership_history` ORM model and Alembic migration.
  - Added repository helpers to sync current membership snapshots, list members on a date, and test whether a symbol was an index member on a date.
  - Added `refresh_index_membership` job and `scripts/refresh_index_membership.ps1`.
  - Signal backtests can now optionally filter events by point-in-time index membership.
  - Added documentation at `docs/INDEX_MEMBERSHIP_HISTORY.md`.
  - Ran the current official NSE Nifty 50 snapshot with effective date `2026-05-03`.
  - Local DB verification: 50 membership rows, 50 active rows, `RELIANCE.NS=True`, `LTIM.NS=False` for `2026-05-03`.
  - Focused tests passed: 22 tests passed across index membership repository, signal backtest, database models, and migrations.
  - Targeted Ruff checks and format checks passed.
- **Data Health index membership visibility completed (2026-05-03)**:
  - Data Health report now includes Nifty 50 membership coverage: active members, total periods, earliest/latest period dates, latest observation timestamp, source URL, and historical-backfill status.
  - Data Health page now shows an Index Membership History section with a clear warning while historical backfill is pending.
  - Local DB verification: `Nifty 50`, 50 active members, 50 total periods, latest from date `2026-05-03`, historical backfill pending.
  - Focused tests passed: 16 tests passed across data health, migrations, and index membership repository.
  - Data Health Streamlit AppTest passed with 0 exceptions and 0 rendered errors.
  - Targeted Ruff checks and format checks passed.
- **Historical index membership importer skeleton completed (2026-05-03)**:
  - Added local-file importer job `stock_platform.jobs.import_index_membership_history`.
  - Added PowerShell helper `scripts/import_index_membership_history.ps1`.
  - Added `data/universe/history/nifty_50/README.md` and updated `.gitignore` so local archived CSVs stay uncommitted.
  - The importer discovers dated CSV files such as `nifty_50_2024-03-31.csv`, dry-runs by default, and requires explicit `-Apply -ReplaceExisting` to rebuild overlapping history.
  - Updated `docs/INDEX_MEMBERSHIP_HISTORY.md` with the historical backfill workflow and safety notes.
  - Smoke-tested default dry-run with no archived files: failed safely with a beginner-readable message and changed nothing.
  - Focused tests passed: 19 tests passed across importer, index membership repository, and data health.
  - Targeted Ruff checks and format checks passed.
- **Backtests survivorship-bias filter completed (2026-05-03)**:
  - Backtests page now has a `Filter by Nifty 50 membership on signal date` checkbox.
  - When enabled, active signal events are filtered through `index_membership_history` before return calculations.
  - The page reports how many active signal events were excluded by the membership filter.
  - The page warns when historical membership backfill is still pending.
  - `filter_events_by_index_membership` is now a public backtest helper and is reused by `run_signal_backtest`.
  - Focused tests passed: 29 tests passed across signal backtest, index membership repository, and data health.
  - Backtests Streamlit AppTest passed with 0 exceptions and 0 rendered errors.
  - Targeted Ruff checks and format checks passed.
