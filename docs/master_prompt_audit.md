# Master Prompt Audit

Last updated: 2026-05-06

This audit compares the local repository against the master prompt. It is deliberately
plain-spoken: **complete** means usable in the local MVP, **partial** means started but
not production-grade, and **deferred** means intentionally not built yet.

## Summary

| Phase | MVP status | Evidence | Remaining gap |
|---|---|---|---|
| Phase 0 - Foundations | Complete locally | Streamlit app, Kite/yfinance market data fallback, logging, data quality checks, local SQLite, `.env`, tests, GitHub push | Keep local secrets and database files out of Git |
| Phase 1 - Fundamentals | Partial / stronger MVP complete | DB-backed fundamentals cache, yfinance fallback, Screener parser foundation, annual/quarterly rows, ratios, growth, Piotroski, Altman, sector ranks, cross-source checks | Commercial/source reliability review and ToS review still pending |
| Phase 2 - Technicals | Complete locally | Indicators, seven signal families, chart overlays, signal audit table | More robust support/resistance and strategy calibration later |
| Phase 3 - Flows & Events | Partial / MVP complete | Delivery %, corporate actions, result volatility, bulk/block graceful fallback | FII/DII, MF holdings, SEBI PIT insider trades, robust event calendar pending |
| Phase 4 - Composite Scoring | Complete locally | Config-driven score, sub-scores, risks, missing-data notes, entry/stop/risk sizing, persisted scan scores | Needs more live history before treating rankings as validated |
| Phase 5 - Backtesting | Partial / MVP complete | Signal backtests, MFE/MAE, PF, drawdown, Sharpe/Sortino, walk-forward where history exists | Needs years of historical signal events and regime/sector analysis |
| Phase 6 - Polish / Alerts / Compliance | Partial / MVP complete | Health checks, backup helper, alert previews, compliance docs, provenance table | Real alert sending, deployment, monitoring, auth, and legal review remain deferred |

## Confirmed Strengths

- The app can run locally and show `RELIANCE.NS` with a chart.
- Disclaimers are visible in the UI and docs.
- Data quality checks run before analytics.
- Provider-style abstraction exists for price, fundamentals, corporate actions, and future providers.
- Signal audit storage exists and supports later backtesting.
- Composite scoring is explainable and config-driven.
- Phase 6 now has a visible data provenance table and operational health checks.
- Phase 8+ adds multi-page navigation, Market Today, persisted universe scans, Kite market data, and data-health visibility.

## Important Gaps

| Area | Status | Why it matters | Smallest safe next step |
|---|---|---|---|
| Fundamentals source | Partial | yfinance and Screener foundations exist, but source quality/ToS are not fully settled | Run small dry-runs, compare sources, then choose Screener/Tijori/Trendlyne policy |
| PostgreSQL | Deferred | Needed before serious daily scale / deployment | Keep SQLite until ingestion design stabilizes |
| FII/DII, MF holdings, SEBI PIT | Deferred | Needed for ownership/flow depth | Add one official source at a time after ToS check |
| Real alerts | Deferred | Alerts can look like advice if wording/rules are weak | Keep preview-only until backtests and compliance review improve |
| Public deployment | Deferred | Data licensing and SEBI boundaries matter | Do not deploy publicly before legal/data-source review |
| User-specific watchlists | Deferred | Needed for SaaS path, not local MVP | Add config-driven local watchlists before auth |

## Compliance Position

Current outputs are local research aids. The project must not be marketed or shared as
investment advice. Before public access or monetization, complete:

- SEBI RA/RIA applicability review
- Data-source ToS and redistribution review
- Commercial data licensing review
- Formal terms, privacy policy, risk disclosure, and user consent
- Stronger audit logging for user actions and generated outputs

## Audit Conclusion

The project is in a healthy local MVP state beyond the original Phase 8. The right next
work is to harden source quality: refresh fundamentals carefully, backfill survivorship
history, and add official ownership/flow sources one at a time.
