#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# One-shot Phase 0 setup for macOS / Linux.
# Run from the project root:
#     bash scripts/setup.sh
# -----------------------------------------------------------------------------

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "📦  Project root: $PROJECT_ROOT"

# --- Python check ---
if ! command -v python3 >/dev/null 2>&1; then
    echo "❌  python3 not found. Install Python 3.11+ from https://www.python.org/downloads/"
    exit 1
fi

PYVER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "🐍  Python version: $PYVER"

# --- venv ---
if [ ! -d ".venv" ]; then
    echo "📦  Creating virtual environment at .venv"
    python3 -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

# --- pip install ---
echo "⬆️   Upgrading pip"
python -m pip install --upgrade pip

echo "📚  Installing dependencies"
pip install -r requirements.txt

# --- .env ---
if [ ! -f ".env" ]; then
    echo "📝  Creating .env from template"
    cp .env.example .env
fi

# --- directories ---
mkdir -p data/raw data/processed data/cache logs
touch data/raw/.gitkeep data/processed/.gitkeep data/cache/.gitkeep logs/.gitkeep

echo ""
echo "✅  Setup complete."
echo ""
echo "Next steps:"
echo "  1. Activate the venv:       source .venv/bin/activate"
echo "  2. Run the app:             streamlit run src/stock_platform/ui/streamlit_app.py"
echo "  3. Open in browser:         http://localhost:8501"
