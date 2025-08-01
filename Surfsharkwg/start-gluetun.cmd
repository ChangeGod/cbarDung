@echo off
echo ===============================
echo  Starting Gluetun Containers...
echo ===============================

REM Go to the folder where this .cmd file is located
cd /d "%~dp0"

REM Run docker compose
docker compose -p surfshark up -d

if %ERRORLEVEL%==0 (
    echo Containers started successfully!
    docker ps --filter "ancestor=qmcgaw/gluetun"
) else (
    echo Failed to start containers. Is Docker Desktop running?
)


