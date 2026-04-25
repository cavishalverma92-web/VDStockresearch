# -----------------------------------------------------------------------------
# Convenience runner. Activates venv (if present) and launches Streamlit.
#     .\scripts\run_app.ps1
# -----------------------------------------------------------------------------
$ErrorActionPreference = "Stop"
$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $ProjectRoot

if (Test-Path ".venv\Scripts\Activate.ps1") {
    & ".\.venv\Scripts\Activate.ps1"
}

streamlit run src\stock_platform\ui\streamlit_app.py
