@echo off
echo ===============================
echo  Stopping Gluetun Containers...
echo ===============================

cd /d "%~dp0"
docker compose -p surfshark down

