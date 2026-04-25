param(
    [string]$BackupRoot = "backups"
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$Destination = Join-Path $ProjectRoot (Join-Path $BackupRoot $Stamp)
New-Item -ItemType Directory -Force -Path $Destination | Out-Null

$Items = @(
    "PROJECT_STATE.md",
    "README.md",
    "DISCLAIMER.md",
    "config",
    "data\sample"
)

foreach ($Item in $Items) {
    if (Test-Path $Item) {
        Copy-Item -Path $Item -Destination $Destination -Recurse -Force
    }
}

if (Test-Path "data\stock_platform.db") {
    New-Item -ItemType Directory -Force -Path (Join-Path $Destination "data") | Out-Null
    Copy-Item -Path "data\stock_platform.db" -Destination (Join-Path $Destination "data\stock_platform.db") -Force
}

Write-Host "Local backup created at:"
Write-Host $Destination
