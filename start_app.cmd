@echo off
setlocal

cd /d "%~dp0"

echo Starting Indian Stock Research Platform...
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\run_app.ps1"

echo.
echo If the app opened successfully, you can close this window.
echo If there was an error, keep this window open and share the message.
pause
