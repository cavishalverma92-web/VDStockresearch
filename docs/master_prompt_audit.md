# Master Prompt Audit

Last updated: 2026-04-25

This audit compares the local repository against the master prompt. It is deliberately
plain-spoken: **complete** means usable in the local MVP, **partial** means started but
not production-grade, and **deferred** means intentionally not built yet.

## Summary

| Phase | MVP status | Evidence | Remaining gap |
|---|---|---|---|
| Phase 0 - Foundations | Complete locally | Streamlit app, yfinance price load, logging, data quality checks, local SQLite, `.env`, tests | Private GitHub push remains manual |
| Phase 1 - Fundamentals | Partial / MVP complete | CSV fundamentals provider, ratios, growth, Piotroski, Altman, dashboard, sector ranks | Real verified fundamentals source and ToS review still pending |
| Phase 2 - Technicals | Complete locally | Indicators, seven signal families, chart overlays, signal audit table | More robust support/resistance and strategy calibration later |
| Phase 3 - Flows & Events | Partial / MVP complete | Delivery %, corporate actions, result volatility, bulk/block graceful fallback | FII/DII, MF holdings, SEBI PIT insider trades, robust event calendar pending |
| Phase 4 - Composite Scoring | Complete locally | Config-driven score, sub-scores, risks, missing-data notes, entry/stop/risk sizing | Top-opportunities universe scan pending until real universe data is connected |
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

## Important Gaps

| Area | Status | Why it matters | Smallest safe next step |
|---|---|---|---|
| GitHub backup | Manual | Protects the project before larger changes | Configure Git name/email, commit, then optionally push private |
| Fundamentals source | Deferred | Current CSV rows are placeholders | Decide legal MVP source; test 3-5 stocks only |
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

The project is in a healthy local MVP state through Phase 6. The right next work is not
more features yet; it is to preserve the verified baseline with Git, then choose the next
data-source upgrade carefully.
