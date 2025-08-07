@echo off
echo Stopping Docker Desktop processes...

REM Kill Docker Desktop main process
taskkill /F /IM "Docker Desktop.exe" >nul 2>&1

REM Kill Docker backend process
taskkill /F /IM "com.docker.backend.exe" >nul 2>&1

echo Waiting for processes to fully stop...
timeout /t 5 >nul

echo Starting Docker Desktop...
start "" "C:\Program Files\Docker\Docker\Docker Desktop.exe"

echo Docker Desktop restart initiated.
