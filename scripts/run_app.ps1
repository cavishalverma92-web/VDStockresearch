param(
    [int]$Port = 8501,
    [switch]$NoBrowser,
    [switch]$NoStopExisting
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $ProjectRoot

$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$AppPath = "src\stock_platform\ui\streamlit_app.py"
$LogDir = Join-Path $ProjectRoot "logs"
$StdoutLog = Join-Path $LogDir "streamlit_live_stdout.log"
$StderrLog = Join-Path $LogDir "streamlit_live_stderr.log"
$PidFile = Join-Path $LogDir "streamlit.pid"
$LocalUrl = "http://localhost:$Port"

function Get-PortProcessIds {
    param([int]$PortToCheck)

    $Lines = netstat -ano | Select-String ":$PortToCheck" | Select-String "LISTENING"
    $Ids = @()
    foreach ($Line in $Lines) {
        $Parts = ($Line.ToString() -split "\s+") | Where-Object { $_ -ne "" }
        if ($Parts.Count -gt 0) {
            $Ids += [int]$Parts[-1]
        }
    }
    return $Ids | Sort-Object -Unique
}

Write-Host ""
Write-Host "Indian Stock Research Platform" -ForegroundColor Cyan
Write-Host "Project: $ProjectRoot"
Write-Host ""

if (-not (Test-Path $PythonExe)) {
    Write-Host "Virtual environment was not found at .venv\Scripts\python.exe" -ForegroundColor Red
    Write-Host "Run this first from the project root:"
    Write-Host "  python -m venv .venv"
    Write-Host "  .\.venv\Scripts\python.exe -m pip install -r requirements.txt"
    exit 1
}

if (-not (Test-Path $AppPath)) {
    Write-Host "Streamlit app file was not found: $AppPath" -ForegroundColor Red
    Write-Host "Make sure you are running this from the project repository."
    exit 1
}

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$ExistingIds = @(Get-PortProcessIds -PortToCheck $Port)
if ($ExistingIds.Count -gt 0) {
    if ($NoStopExisting) {
        Write-Host "Port ${Port} is already in use by process id(s): $($ExistingIds -join ', ')" -ForegroundColor Yellow
        Write-Host "Open the existing app here: $LocalUrl"
        Set-Content -Path $PidFile -Value ($ExistingIds -join ",")
        if (-not $NoBrowser) {
            Start-Process $LocalUrl
        }
        exit 0
    }

    Write-Host "Stopping existing process(es) on port ${Port}: $($ExistingIds -join ', ')" -ForegroundColor Yellow
    foreach ($Id in $ExistingIds) {
        Stop-Process -Id $Id -Force
    }
    Start-Sleep -Seconds 2
}

Write-Host "Starting Streamlit on port $Port..."
Write-Host "Logs:"
Write-Host "  $StdoutLog"
Write-Host "  $StderrLog"

$ArgumentList = @(
    "-m",
    "streamlit",
    "run",
    $AppPath,
    "--server.port",
    "$Port",
    "--server.headless",
    "true",
    "--server.runOnSave",
    "false",
    "--browser.gatherUsageStats",
    "false"
)

$Process = Start-Process `
    -FilePath $PythonExe `
    -ArgumentList $ArgumentList `
    -WorkingDirectory $ProjectRoot `
    -RedirectStandardOutput $StdoutLog `
    -RedirectStandardError $StderrLog `
    -WindowStyle Hidden `
    -PassThru

$Started = $false
for ($Attempt = 1; $Attempt -le 30; $Attempt++) {
    Start-Sleep -Seconds 1
    if ($Process.HasExited) {
        break
    }

    try {
        $Response = Invoke-WebRequest -Uri $LocalUrl -UseBasicParsing -TimeoutSec 2
        if ($Response.StatusCode -eq 200) {
            $Started = $true
            break
        }
    } catch {
        # Streamlit may still be starting. Try again until the timeout.
    }
}

if (-not $Started) {
    Write-Host ""
    Write-Host "Streamlit did not become reachable on $LocalUrl." -ForegroundColor Red
    Write-Host "Check the log files above. Most common causes are a missing package or a Python import error."
    Write-Host ""
    Write-Host "Last stderr lines:"
    if (Test-Path $StderrLog) {
        Get-Content $StderrLog -Tail 30
    }
    exit 1
}

$RunningIds = @(Get-PortProcessIds -PortToCheck $Port)
if ($RunningIds.Count -gt 0) {
    Set-Content -Path $PidFile -Value ($RunningIds -join ",")
} else {
    Set-Content -Path $PidFile -Value $Process.Id
    $RunningIds = @($Process.Id)
}

Write-Host ""
Write-Host "App is running." -ForegroundColor Green
Write-Host "Open this link:"
Write-Host "  $LocalUrl"
Write-Host ""
Write-Host "Process id(s): $($RunningIds -join ', ')"
Write-Host "To stop later:"
Write-Host "  Stop-Process -Id $($RunningIds -join ',') -Force"
Write-Host ""

if (-not $NoBrowser) {
    Start-Process $LocalUrl
}
