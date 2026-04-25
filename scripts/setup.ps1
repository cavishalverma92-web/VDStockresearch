# -----------------------------------------------------------------------------
# One-shot Phase 0 setup for Windows.
# Run from the project root in PowerShell:
#     .\scripts\setup.ps1
#
# If you see an execution-policy error, run this once (as your user):
#     Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
# -----------------------------------------------------------------------------

$ErrorActionPreference = "Stop"
$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $ProjectRoot

Write-Host "📦  Project root: $ProjectRoot"

# --- Python check ---
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "❌  python not found. Install Python 3.11+ from https://www.python.org/downloads/" -ForegroundColor Red
    exit 1
}

$pyver = python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
Write-Host "🐍  Python version: $pyver"

# --- venv ---
if (-not (Test-Path ".venv")) {
    Write-Host "📦  Creating virtual environment at .venv"
    python -m venv .venv
}

& ".\.venv\Scripts\Activate.ps1"

# --- pip install ---
Write-Host "⬆️   Upgrading pip"
python -m pip install --upgrade pip

Write-Host "📚  Installing dependencies"
pip install -r requirements.txt

# --- .env ---
if (-not (Test-Path ".env")) {
    Write-Host "📝  Creating .env from template"
    Copy-Item ".env.example" ".env"
}

# --- directories ---
$dirs = @("data\raw", "data\processed", "data\cache", "logs")
foreach ($d in $dirs) {
    if (-not (Test-Path $d)) { New-Item -ItemType Directory -Path $d | Out-Null }
    $keep = Join-Path $d ".gitkeep"
    if (-not (Test-Path $keep)) { New-Item -ItemType File -Path $keep | Out-Null }
}

Write-Host ""
Write-Host "✅  Setup complete." -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Activate the venv:       .\.venv\Scripts\Activate.ps1"
Write-Host "  2. Run the app:             streamlit run src\stock_platform\ui\streamlit_app.py"
Write-Host "  3. Open in browser:         http://localhost:8501"
