param(
    [string]$Universe = "nifty_50",
    [int]$MaxSymbols = 0,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    throw "Virtual environment Python not found at $Python. Run Phase 0 setup first."
}

$ArgsList = @(
    "-m",
    "stock_platform.jobs.refresh_eod_candles",
    "--universe",
    $Universe,
    "--note",
    "local scheduled refresh"
)

if ($MaxSymbols -gt 0) {
    $ArgsList += @("--max-symbols", "$MaxSymbols")
}

if ($DryRun) {
    $ArgsList += "--dry-run"
}

& $Python @ArgsList
