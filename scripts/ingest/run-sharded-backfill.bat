@echo off
REM Sharded Backfill Launcher for Windows
REM Usage: run-sharded-backfill.bat [SHARD_COUNT] [TARGET_MIGRATION]

setlocal enabledelayedexpansion

set SHARD_COUNT=%1
set TARGET_MIGRATION=%2

if "%SHARD_COUNT%"=="" set SHARD_COUNT=4
if "%TARGET_MIGRATION%"=="" set TARGET_MIGRATION=3

echo ========================================
echo Sharded Backfill Launcher
echo ========================================
echo Shard Count: %SHARD_COUNT%
echo Target Migration: %TARGET_MIGRATION%
echo ========================================
echo.

REM Create logs directory
if not exist "..\..\data\logs" mkdir "..\..\data\logs"

REM Launch each shard in a new window
for /L %%i in (0,1,%SHARD_COUNT%) do (
    if %%i lss %SHARD_COUNT% (
        echo Starting Shard %%i of %SHARD_COUNT%...
        start "Shard %%i" cmd /k "set SHARD_INDEX=%%i && set SHARD_TOTAL=%SHARD_COUNT% && set TARGET_MIGRATION=%TARGET_MIGRATION% && node fetch-backfill-parquet.js"
        timeout /t 2 /nobreak >nul
    )
)

echo.
echo All %SHARD_COUNT% shards launched!
echo.
echo To monitor progress, run:
echo   node shard-progress.js --watch
echo.

endlocal