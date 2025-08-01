@echo off
echo Starting Docker Compose containers...
cd /d %~dp0
docker compose -p barchartcollect up -d --build
if %ERRORLEVEL% EQU 0 (
    echo Containers started successfully.
) else (
    echo Failed to start containers.
)

