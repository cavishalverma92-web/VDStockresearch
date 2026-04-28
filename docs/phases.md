# Phased Build Plan

> This mirrors §8 of `MASTER_PROMPT.md`. Use it as a checklist.
> Do not jump phases — each phase depends on the previous one being verified.

---

## Phase 0 - Foundations (verified)

**Goal:** Run one command, see a Streamlit page showing `RELIANCE.NS` with a disclaimer, basic logging, and basic DQ checks.

**Scaffolded:**
- [x] Folder structure
- [x] `config/*.yaml` (weights, universe, sources, thresholds)
- [x] `.env.example`, `.gitignore`, `requirements.txt`, `pyproject.toml`
- [x] VS Code workspace settings
- [x] `logging.py` utility
- [x] Provider abstraction (`base.py`, `yahoo.py`)
- [x] OHLCV validator
- [x] Streamlit app stub
- [x] Smoke test

**Still to do (you):**
- [ ] Install Python, Git, VS Code
- [ ] Create and push a **private** GitHub repo
- [ ] Run `scripts/setup.sh` (or `.ps1`) successfully
- [ ] Launch the app and confirm the chart renders
- [ ] Run `pytest` — all tests green
- [ ] Commit + push

**Exit criteria:** app runs, chart appears, logs written, tests pass, code pushed.

---

## Phase 1 - Fundamentals Engine (MVP complete / real source pending)

**Goal:** Ingest and display fundamental data for an initial equity universe.

- Stock master table (`stock_universe`) with universe from `config/universe.yaml`
- Fundamentals schema (annual + quarterly)
- Ratios, growth metrics
- Piotroski F-Score, Altman Z-Score (Beneish deferred)
- Fundamental dashboard + stock drill-down
- Sector-relative ranking
- Missing-data warnings everywhere

**Exit criteria:** sortable table of stocks with fundamental metrics; click-through drill-down with source + freshness visible.

---

## Phase 2 — Technicals Engine

**Goal:** Calculate indicators + the seven prebuilt signals.

- OHLCV storage (clean)
- SMA/EMA, RSI, MACD, Bollinger Bands, ATR
- Signals: 200 EMA Pullback, RSI 60 Momentum, Breakout+Volume, Darvas Base Breakout, Mean-Reversion Oversold, MA Stack, Golden/Death Cross
- Each signal stores entry zone, stop-loss, target, R/R, explanation
- Chart overlays in the UI

**Exit criteria:** per-stock chart with indicators + list of signals fired with full explanation.

---

## Phase 3 — Flows & Events

**Goal:** Add market flow, ownership, and event context.

- FII/DII daily
- Delivery %
- Bulk / block deals
- Insider trades (SEBI PIT)
- MF holding changes
- Earnings calendar
- Result volatility score
- Corporate actions

**Deferred:** concall summaries, LLM news sentiment.

**Exit criteria:** per-stock panel showing flow + event context with source / freshness.

---

## Phase 4 — Composite Scoring & Entry Signals

**Goal:** First explainable opportunity engine.

- Config-driven scoring (reads `scoring_weights.yaml`)
- Sub-scores: fundamentals, technicals, flows, events/quality
- Entry zone calculation
- Stop-loss calculation
- Position sizing helper (educational)
- Top Opportunities page
- Full explainability panel per opportunity

**Exit criteria:** ranked list of stocks; every ranking is explainable in the UI.

---

## Phase 5 — Backtesting & Walk-Forward

**Goal:** Validate signals historically, reduce overfitting.

- Backtest engine (strategy-level + composite)
- Walk-forward (3 yr train / 1 yr validate)
- Metrics: CAGR, win rate, PF, max DD, Sharpe, Sortino, exposure, turnover, holding period, sector concentration
- Signal audit log
- Live-vs-backtest comparison

**Exit criteria:** can test any strategy over history with realistic assumptions; best-performing signals identified by sector and cap bucket.

---

## Phase 6 — Polish, Alerts, Compliance Hardening

**Goal:** Daily usability + optional monetization readiness.

- [x] UI polish inside Streamlit
- [ ] Optional Next.js migration
- [x] Alert previews
- [ ] Email / Telegram sending
- [x] Data provenance table
- [x] Strengthened disclaimers
- [x] Compliance review checklist
- [x] Local health checks
- [x] Local backup helper
- [ ] Deployment
- [ ] Production monitoring
- [ ] User-specific watchlists (first SaaS-aware touch)

**Exit criteria:** stable daily use, reliable jobs, documented compliance boundaries.

---

## Phase 7 - Real Fundamentals and Ownership Data

**Goal:** Move beyond sample CSV fundamentals while keeping safe fallbacks.

- [x] yfinance fundamentals provider
- [x] CSV fallback remains available
- [x] Institutional / mutual fund holder views where yfinance provides data
- [x] Multi-year CAGR
- [x] Extended balance-sheet health metrics
- [x] Price-structure gap fill
- [x] Sector-aware fundamentals validation for banks/financial services
- [x] Bank-safe UI handling for industrial ratios that are not applicable
- [x] Financial-sector branch in composite fundamentals scoring

**Remaining gaps:** yfinance coverage is uneven for Indian stocks; verified official / paid
fundamentals source still needs ToS and reliability review. Bank-specific fields
such as NIM, GNPA, NNPA, CASA, and capital adequacy still need a better source.

---

## Phase 8 - Universe Opportunity Scanner

**Goal:** Scan configured universes and rank stocks by the existing research pipeline.

- [x] `config/universes.yaml` seed universe lists
- [x] Scanner module for configured universes and custom symbol lists
- [x] OHLCV validation before per-symbol scoring
- [x] Per-symbol graceful failure handling
- [x] Top Opportunities Streamlit expander
- [x] CSV-backed `all_nse_listed` universe from NSE `EQUITY_L.csv`
- [x] Saved scanner runs/results in SQLite
- [x] Latest saved scan view in Streamlit
- [x] Latest-vs-previous saved scan comparison
- [x] Saved-scan filters for comparison status, score change, and newly active signals
- [x] Local research shortlist from scanner rows
- [x] CSV export of scan results

**Remaining gaps:** configured universe lists must be refreshed from official NSE files
before being treated as current index membership; universe-wide fundamentals/flows are
limited; shortlist note editing/removal is not added yet.
