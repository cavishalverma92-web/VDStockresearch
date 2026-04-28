param(
    [string]$OutputPath = "data\universe\nse_equity_list.csv"
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$Url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
$Destination = Join-Path $ProjectRoot $OutputPath
$DestinationDir = Split-Path -Parent $Destination

New-Item -ItemType Directory -Force -Path $DestinationDir | Out-Null

Write-Host "Downloading official NSE equity list..."
Write-Host $Url

Invoke-WebRequest -Uri $Url -OutFile $Destination -UseBasicParsing

$rows = Import-Csv $Destination
$eqRows = @($rows | Where-Object { $_.SERIES -eq "EQ" })

Write-Host "Saved:"
Write-Host $Destination
Write-Host "Rows: $($rows.Count)"
Write-Host "EQ equity rows: $($eqRows.Count)"
Write-Host ""
Write-Host "The app universe key is: all_nse_listed"
