param(
    [string]$Universe = "nifty_50",
    [switch]$Apply
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    $Python = "python"
}

$Arguments = @(
    "-m",
    "stock_platform.jobs.refresh_official_universes",
    "--universe",
    $Universe
)

if ($Apply) {
    $Arguments += "--apply"
}

& $Python @Arguments
