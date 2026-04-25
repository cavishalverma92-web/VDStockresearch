param()

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
    Write-Host "Virtual environment not found. Run scripts\setup.ps1 first."
    exit 1
}

.\.venv\Scripts\python.exe -c "from stock_platform.ops.health import health_checks_to_markdown, run_health_checks; print(health_checks_to_markdown(run_health_checks()))"
