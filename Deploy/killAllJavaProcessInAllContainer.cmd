@echo off
FOR /F "tokens=*" %%i IN ('docker ps --filter "label=com.docker.compose.project=barchartcollect" -q') DO (
    docker exec %%i pkill -9 java
)
echo Java processes killed in all containers of project "barchartcollect".
pause
