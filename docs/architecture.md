# Architecture

> A living document. Update when layers, dependencies, or flows change.

---

## High-level layers

```
┌──────────────────────────────────────────────────────────────┐
│                      Streamlit UI (MVP)                       │
│                 [Phase 6: optional Next.js]                   │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                       Scoring Engine                          │
│   Reads config/scoring_weights.yaml. Produces explainable     │
│   sub-scores + composite score per stock.                     │
└──────────────────────────────────────────────────────────────┘
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
       Fundamentals       Technicals          Flows
         Engine             Engine             Engine
   (Piotroski, ratios, (indicators +      (FII/DII, MF,
    growth, Altman)     7 signals)         insider, bulk)
            │                 │                 │
            └─────────────────┼─────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                  Analytics Data Layer (clean)                 │
│       Validated, normalized OHLCV / fundamentals / flows      │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                       Data Quality                            │
│    Schema / missing / duplicate / outlier / staleness checks  │
│          Fail-fast on critical, warn on non-critical          │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                   Provider Abstraction                        │
│    PriceDataProvider • FundamentalsDataProvider • Holdings    │
│    CorporateActions • InsiderTrades • Events • Macro • News   │
└──────────────────────────────────────────────────────────────┘
                              │
       ┌──────────┬──────────┼──────────┬──────────┐
       ▼          ▼          ▼          ▼          ▼
   yfinance    NSE/BSE   Screener     AMFI       SEBI
   (MVP)      (bhavcopy) (later)    (later)    (later)
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│              Raw cache (data/raw/) — untouched                │
│    Every API response stored verbatim with provenance         │
└──────────────────────────────────────────────────────────────┘
```

---

## Three-tier data separation

| Tier | Location | Content | Who writes | Who reads |
|---|---|---|---|---|
| Raw | `data/raw/` | Unmodified source responses | Providers | Reprocessing / debug only |
| Clean | `data/processed/` and DB | Validated, normalized | Validators | Analytics engines |
| Analytics | DB tables / Parquet | Derived indicators, scores, signals | Analytics / scoring | UI + backtests |

**Never** overwrite raw with clean. **Never** compute signals directly from unvalidated raw.

---

## Config-as-code

All tunables live in `config/*.yaml`:

| File | Purpose |
|---|---|
| `scoring_weights.yaml` | Composite score weights per bucket and sub-factor |
| `universe.yaml` | Which indices / cap buckets / watchlists to analyse |
| `data_sources.yaml` | Provider registry + per-source rate limits |
| `thresholds.yaml` | All decision thresholds (pledge %, RSI bounds, DQ limits) |

Rules:
1. No magic numbers in code.
2. Any weight change must be logged in `PROJECT_STATE.md` → *Model weight history*.
3. `lru_cache` is used on config loaders — restart the process after editing a YAML.

---

## Provider abstraction

Every feature module depends on `PriceDataProvider`, `FundamentalsDataProvider`, etc. — **never** on `yfinance`, `nsepython`, or similar libs directly.

This means:
- Swapping yfinance → Kite Connect is a single-class change.
- Tests can inject a fake provider.
- The SaaS path doesn't require rewriting business logic.

See `src/stock_platform/data/providers/base.py`.

---

## Logging

Two structured log streams:

- `logs/app.log` — everything, rotating at 10 MB, 30-day retention
- `logs/data_quality.log` — only records tagged with `dq=True`, 90-day retention

Use:
```python
from stock_platform.utils.logging import get_logger, get_dq_logger
log = get_logger(__name__)
dq = get_dq_logger(__name__)
```

---

## Compliance boundaries

- Every UI page shows a disclaimer banner.
- `DISCLAIMER.md` is the canonical compliance document.
- No output is framed as advice or a recommendation.
- Data provenance (source, URL, timestamp) is preserved for every raw response.
- Redistribution / monetization requires a formal SEBI review — see `docs/compliance.md`.
