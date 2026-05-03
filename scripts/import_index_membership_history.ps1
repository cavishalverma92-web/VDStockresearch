param(
    [string]$Universe = "nifty_50",
    [string]$InputDir = "",
    [switch]$Apply,
    [switch]$ReplaceExisting
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
    "stock_platform.jobs.import_index_membership_history",
    "--universe",
    $Universe
)

if ($InputDir -ne "") {
    $Arguments += "--input-dir"
    $Arguments += $InputDir
}

if ($Apply) {
    $Arguments += "--apply"
}

if ($ReplaceExisting) {
    $Arguments += "--replace-existing"
}

& $Python @Arguments
