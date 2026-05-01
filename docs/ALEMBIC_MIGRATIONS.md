# Alembic Migrations

Alembic is the database change log for this project. It turns schema changes
into reviewable files instead of silently mutating the local database.

## Why This Matters

Earlier versions used `Base.metadata.create_all()` at app startup. That is fine
for a first prototype, but it does not show what changed, when it changed, or how
to reproduce the same schema on another machine.

With Alembic:

- every schema change gets a migration file in `alembic/versions/`
- startup can upgrade the local database safely
- future PostgreSQL migration becomes easier
- database changes become visible in Git

## Daily Use

You normally do not need to run anything manually. The app calls the migration
helper when it starts against the local file database.

Manual command:

```powershell
.\.venv\Scripts\alembic.exe upgrade head
```

## Creating a New Migration

Use this only after changing SQLAlchemy models in:

```text
src/stock_platform/db/models.py
```

Command:

```powershell
.\.venv\Scripts\alembic.exe revision --autogenerate -m "short description"
```

Then open the generated file in:

```text
alembic/versions/
```

Review it before running:

```powershell
.\.venv\Scripts\alembic.exe upgrade head
```

## Current Baseline

The first migration captures the current local MVP schema, including:

- stock universe
- fundamentals
- OHLCV prices
- technical snapshots
- composite scores
- signal audit
- universe scans
- research watchlist
- instrument master
- daily refresh audit rows

## Beginner Safety Notes

- Do not delete `alembic/versions/`.
- Do not edit old migration files after they are pushed to GitHub.
- Create a new migration for each future model change.
- Keep `.env` out of Git; migrations do not need API keys or broker tokens.
