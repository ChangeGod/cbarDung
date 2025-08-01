@echo off
echo ===============================
echo  Stopping Gluetun Containers...
echo ===============================

cd /d "%~dp0"

REM Check if any containers exist for surfsharkwg
FOR /F "tokens=*" %%i IN ('docker ps -a --filter "label=com.docker.compose.project=surfsharkwg" -q') DO (
    docker compose -p surfsharkwg down
    goto done
)

echo No SurfsharkWG containers found.
:done
