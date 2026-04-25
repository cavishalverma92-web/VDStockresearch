#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Convenience runner. Activates venv (if present) and launches Streamlit.
#     bash scripts/run_app.sh
# -----------------------------------------------------------------------------
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

if [ -d ".venv" ]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
fi

exec streamlit run src/stock_platform/ui/streamlit_app.py
