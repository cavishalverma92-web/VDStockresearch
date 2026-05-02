param(
    [string]$Universe = "nifty_50",
    [string]$EffectiveDate = ""
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
    "stock_platform.jobs.refresh_index_membership",
    "--universe",
    $Universe
)

if ($EffectiveDate -ne "") {
    $Arguments += "--effective-date"
    $Arguments += $EffectiveDate
}

& $Python @Arguments
