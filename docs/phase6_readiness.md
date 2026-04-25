# Phase 6 Readiness Checklist

This is the lightweight daily-use and compliance-hardening checklist before any
cloud deployment, alerts, monetization, or public access.

## Daily Local Use

- Start the app from the project root with:
  `.\.venv\Scripts\python.exe -m streamlit run src\stock_platform\ui\streamlit_app.py`
- Run the health-check helper:
  `.\scripts\health_check.ps1`
- Confirm `RELIANCE.NS` loads before trusting any other symbol screen.
- Review the Streamlit "Operations & Alerts" -> "Data provenance" tab before relying on a screen.
- Check `logs/app.log` for app errors.
- Check `logs/data_quality.log` for data-quality warnings.
- Check `logs/backtests.log` after running backtests.

## Backup Routine

- Commit verified code changes to Git after each working session.
- Push to a private GitHub repository after the remote is connected.
- Do not commit `.env`, local databases, raw downloads, or log files.
- Run `.\scripts\backup_local.ps1` if you want a local snapshot of config, project state,
  documentation, sample data, and the local SQLite signal-history database.

## Alert Readiness

Do not add Telegram/email alerts until these are true:

- Signal logic has been backtested over a useful sample.
- Every alert message says it is a research aid, not advice.
- Every alert includes source, signal date, trigger price, and invalidation logic.
- Alerts are rate-limited to avoid spam.
- Phase 6 currently creates alert previews only. Telegram/email sending remains deferred.

## Compliance Gate Before Sharing

- Review SEBI RA/RIA implications with a qualified professional.
- Review data-source terms for yfinance, NSE, BSE, Screener, AMFI, and SEBI.
- Replace sample fundamentals with verified sourced fundamentals.
- Keep raw source links and timestamps for every displayed data point.
- Treat the current provenance table as a summary; full raw-response provenance is still a future hardening task.
- Add formal terms, privacy policy, and risk disclosure before third-party access.

## Deployment Gate

- Move from SQLite to managed PostgreSQL.
- Add migrations with Alembic.
- Add proper scheduled jobs instead of manual page refreshes.
- Add monitoring and backups.
- Add authentication only after the research engine is stable.
