@echo off
echo Stopping Docker Compose containers...
cd /d %~dp0
docker compose -p barchartcollect down
if %ERRORLEVEL% EQU 0 (
    echo Containers stopped and removed successfully.
) else (
    echo Failed to stop containers.
)

