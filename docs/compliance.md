# Compliance Checklist

> Personal research tool. Not advice. This document exists to keep the
> boundary visible as the project grows.

---

## Always-on guardrails

- [x] `DISCLAIMER.md` exists at repo root
- [x] Disclaimer banner displayed on every UI page
- [x] No output uses the words "recommend", "buy", "sell" in an advisory sense
- [ ] Every raw response stored with provenance (source, URL, timestamp)
- [x] Current UI shows source/provenance summary for major local outputs
- [x] Conservative rate limits documented per source (see `config/data_sources.yaml`)
- [x] `.env` ignored by Git; no secrets committed

---

## Before sharing any output with anyone else

- [ ] Review ToS of every data source in use
- [ ] Verify no NSE/BSE data is being redistributed outside permitted use
- [ ] Confirm disclaimer is visible wherever output is shared
- [ ] Remove any personal watchlist / portfolio data

---

## Before opening the platform to any third party

- [ ] Consult a SEBI-compliance professional
- [ ] Review Research Analyst (RA) registration requirement
- [ ] Review Investment Adviser (RIA) registration requirement
- [ ] Review data licensing from NSE/BSE for commercial use
- [ ] Implement proper user authentication + audit logging
- [ ] Add explicit T&C + risk disclosure acceptance flow
- [ ] Separate "facts" from any "opinion" in output clearly
- [ ] Document signal logic, data sources, and model assumptions in public-facing docs
- [ ] Implement grievance-redressal process as required

---

## Before any monetization

Everything above, plus:

- [ ] Formal SEBI registration where applicable
- [ ] Commercial data licenses from NSE/BSE and any paid vendor
- [ ] Conflict-of-interest policy
- [ ] Cybersecurity + data-privacy review (including personal data handling)
- [ ] Professional indemnity cover
- [ ] Accounting / tax advice on revenue structure
- [ ] Legal review of T&C, disclaimer, privacy policy
- [ ] Review of advertising / marketing language against SEBI rules
- [ ] Review under the Consumer Protection Act
- [ ] Review under IT Act + applicable data protection laws

---

## Data source ToS quick reference

| Source | Redistribution | Commercial use | Notes |
|---|---|---|---|
| yfinance | Restricted | Review Yahoo ToS | Data sourced from Yahoo, not Yahoo Finance direct |
| NSE | Very restricted | Paid license required | `www.nseindia.com/terms` |
| BSE | Very restricted | Paid license required | `www.bseindia.com/terms` |
| Screener | Check site ToS | Not permitted without agreement | Respect rate limits |
| AMFI | Public disclosures | Cite source | Use official downloads |
| SEBI | Public disclosures | Cite source | Use official data |

**When in doubt, don't redistribute. Cite the source and link back.**
