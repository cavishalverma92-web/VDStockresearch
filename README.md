# Indian Stock Research & Aggregator Platform

A personal research and aggregator platform for Indian equities (NSE / BSE), built by a Chartered Accountant learning to code with AI pair-programming assistants.

> **Disclaimer**: This platform is a **research aid**, not investment advice. It is not a SEBI-registered Research Analyst or Investment Adviser service. See [`DISCLAIMER.md`](./DISCLAIMER.md).

---

## What this project is

A modular stock research platform that will eventually:

- Ingest fundamentals, price, volume, flow, and event data for Indian listed equities
- Compute technical indicators and prebuilt strategy signals
- Produce an **explainable composite conviction score** (0–100)
- Suggest entry zones, stop-loss, and risk/reward — as educational output only
- Support historical backtesting with walk-forward validation
- Log every signal for audit

See [`MASTER_PROMPT.md`](./MASTER_PROMPT.md) for the full vision and phased build plan.

---

## Current phase: Phase 6 - Polish, Alerts, and Compliance Hardening

Phases 0, 1, 2, 3, 4, and 5 are complete for the local MVP foundation. Phase 6 now adds daily-use readiness: local health checks, backup helpers, alert previews, and clearer compliance/deployment gates.

The current fundamentals provider is `local_csv`. It reads sample/template data from `data/sample/fundamentals_annual_sample.csv` so the pipeline can be tested without scraping a website.

See [`PROJECT_STATE.md`](./PROJECT_STATE.md) for current status and
[`docs/master_prompt_audit.md`](./docs/master_prompt_audit.md) for the phase-by-phase audit
against the original master prompt.

---

## Folder structure

```
indian-stock-research-platform/
├── .vscode/                    # VS Code workspace settings
├── config/                     # YAML config (weights, thresholds, sources)
├── src/stock_platform/         # Application code
│   ├── alerts/                 # Alert preview rules; no messages sent yet
│   ├── data/                   # Providers, validators, cache
│   ├── analytics/              # Fundamentals, technicals, signals
│   ├── scoring/                # Composite scoring engine
│   ├── db/                     # DB models and migrations
│   ├── ops/                    # Local health checks and operations helpers
│   ├── ui/                     # Streamlit app
│   └── utils/                  # Logging, helpers
├── scripts/                    # Shell helpers (setup, run)
├── tests/                      # Unit / integration tests
├── data/                       # Raw, processed, cache (git-ignored)
├── logs/                       # Log files (git-ignored)
└── docs/                       # Architecture, phases, compliance
```

---

## Quick start (Phase 0)

### 1. Install prerequisites

- **Python 3.11+** ([python.org](https://www.python.org/downloads/))
- **Git** ([git-scm.com](https://git-scm.com/downloads))
- **VS Code** ([code.visualstudio.com](https://code.visualstudio.com/))
- **PostgreSQL 16+** (optional for Phase 0; SQLite fallback is fine initially)

### 2. Clone / open the folder

```bash
cd indian-stock-research-platform
```

### 3. Create a virtual environment

**macOS / Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

**Windows (PowerShell):**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 4. Install dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

### 5. Copy the environment file

```bash
cp .env.example .env          # macOS / Linux
copy .env.example .env        # Windows
```

Edit `.env` later as needed. Phase 0 does not require any API keys.

### 6. Run the Streamlit app

```bash
streamlit run src/stock_platform/ui/streamlit_app.py
```

You should see the disclaimer, a symbol input (default `RELIANCE.NS`), and a candlestick chart. A log file will appear at `logs/app.log`.

Keep the terminal running while using the browser. If the terminal stops, `http://localhost:8501` will show "connection refused" until you start Streamlit again.

The fundamentals section currently shows local sample rows when available. Treat any row with `source=sample_placeholder` as test data only.

The technicals section overlays 20/50/200 EMA on the price chart and shows educational pattern observations. These are not recommendations and have not been backtested yet.

Each technical scan is saved to local SQLite at `data/stock_platform.db` in the `signal_audit` table. Repeated scans for the same symbol, date, signal, and source update the existing row instead of creating duplicate same-day rows. This file is intentionally ignored by Git.

The Signals tab includes a CSV download for saved signal history. This is a Phase 2 export shape for later backtesting, not a finished backtest engine.

The Composite Score section shows a 0-100 research score with sub-scores, positive drivers, risks, and missing-data notes. Treat it as an explainable research aid, not investment advice.

The Signals table also shows educational trigger price, entry zone, stop-loss, target, risk/reward, confidence, risk per share, and position size based on the configurable risk setting in `config/thresholds.yaml`.

The Signal backtest tab now shows per-signal returns, portfolio diagnostics, walk-forward validation where enough history exists, individual trades, and CSV downloads. Results are educational and only as good as the available saved signal history.

The Operations & Alerts section shows local setup health checks, data provenance, and alert previews. No Telegram or email alerts are sent in this MVP; the preview confirms wording and compliance boundaries first.

### 7. Run a local health check

```powershell
.\scripts\health_check.ps1
```

### 8. Create a local backup

```powershell
.\scripts\backup_local.ps1
```

Backups are written under `backups/`, which is ignored by Git because it may contain your local SQLite signal history.

---

## Working with AI pair-programming

This repo is built to work well with Claude, Cursor, ChatGPT, Codex, and GitHub Copilot. When you ask for help:

1. Point the assistant at [`MASTER_PROMPT.md`](./MASTER_PROMPT.md) and [`PROJECT_STATE.md`](./PROJECT_STATE.md).
2. Tell it which phase you're on.
3. Ask it to **verify** after each change (don't just trust it).
4. Commit after every verified step.

---

## License

Personal / private project. Not licensed for redistribution. Data sources are subject to their own terms of service.
