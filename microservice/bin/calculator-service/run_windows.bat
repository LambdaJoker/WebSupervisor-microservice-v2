@echo off
cd /d "%~dp0"
echo [calculator-service] starting...
calculator-service_windows_amd64.exe -config_path=./config.json
if errorlevel 1 (
    echo.
    echo [calculator-service] exited with error %errorlevel%
    pause
)
