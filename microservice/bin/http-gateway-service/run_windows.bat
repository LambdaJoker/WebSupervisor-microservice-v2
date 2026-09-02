@echo off
cd /d "%~dp0"
echo [http-gateway-service] starting...
http-gateway-service_windows_amd64.exe -config_path=./config.json
if errorlevel 1 (
    echo.
    echo [http-gateway-service] exited with error %errorlevel%
    pause
)
