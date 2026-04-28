# Banking Fundamentals Entry Guide

Use this guide to fill `data/sample/banking_fundamentals_template.csv` with audited bank metrics.

Do not estimate or invent values. Use annual reports, investor presentations, exchange filings, or another source you can cite in `source_url`.

## CSV Columns

| Column | What to enter | Notes |
|---|---|---|
| `symbol` | NSE ticker with `.NS` suffix | Example: `HDFCBANK.NS` |
| `fiscal_year` | Financial year end | Example: `2025` |
| `nim_pct` | Net Interest Margin percentage | Enter `3.50` for 3.50% |
| `gnpa_pct` | Gross NPA percentage | Lower is generally better |
| `nnpa_pct` | Net NPA percentage | Lower is generally better |
| `casa_pct` | CASA ratio percentage | Current Account + Savings Account deposit ratio |
| `credit_growth_pct` | Loan/advances growth percentage | Prefer YoY or full-year annual growth |
| `deposit_growth_pct` | Deposit growth percentage | Prefer YoY or full-year annual growth |
| `capital_adequacy_pct` | Capital adequacy / CRAR percentage | Use consolidated or standalone consistently |
| `source` | Short source label | Example: `annual_report_2025` |
| `source_url` | URL or local source reference | Must point to where you found the numbers |
| `last_updated` | Date you entered or verified the row | Use `YYYY-MM-DD` |

## Recommended Source Priority

1. Bank annual report
2. Bank investor presentation
3. Exchange filing / results presentation
4. Audited financial statement notes
5. Reputable paid data provider, after terms are reviewed

## Data Entry Rules

- Use percentages as plain numbers, not fractions. Enter `4.25`, not `0.0425`.
- Keep the metric basis consistent across years.
- Do not mix standalone and consolidated values unless you clearly label the source.
- If a metric is unavailable, leave it blank. The validator will warn instead of guessing.
- Keep one row per `symbol` + `fiscal_year`.

## Example Row Format

```csv
symbol,fiscal_year,nim_pct,gnpa_pct,nnpa_pct,casa_pct,credit_growth_pct,deposit_growth_pct,capital_adequacy_pct,source,source_url,last_updated
HDFCBANK.NS,2025,,,,,,,,annual_report_2025,PASTE_SOURCE_URL_HERE,2026-04-28
```

Fill the blank numeric fields only after checking the audited source.
