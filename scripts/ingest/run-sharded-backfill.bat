@echo off
REM Sharded Backfill Launcher for Windows
REM Usage: run-sharded-backfill.bat [SHARD_COUNT] [TARGET_MIGRATION]

setlocal enabledelayedexpansion

set SHARD_COUNT=%1
set TARGET_MIGRATION=%2

REM Defaults - 6 shards balances parallelism with data distribution
if "%SHARD_COUNT%"=="" set SHARD_COUNT=6
if "%TARGET_MIGRATION%"=="" set TARGET_MIGRATION=3

REM Data directory - unified location for all data
if "%DATA_DIR%"=="" set DATA_DIR=C:\ledger_raw

REM Derived directories (all under DATA_DIR)
set CURSOR_DIR=%DATA_DIR%\cursors
set LOG_DIR=%DATA_DIR%\logs

REM Performance tuning
if "%PARALLEL_FETCHES%"=="" set PARALLEL_FETCHES=8
if "%MAX_WORKERS%"=="" set MAX_WORKERS=12
if "%ZSTD_LEVEL%"=="" set ZSTD_LEVEL=1
if "%MAX_ROWS_PER_FILE%"=="" set MAX_ROWS_PER_FILE=20000
if "%IO_BUFFER_SIZE%"=="" set IO_BUFFER_SIZE=1048576
if "%CHUNK_SIZE%"=="" set CHUNK_SIZE=4096

echo ========================================
echo Sharded Backfill Launcher
echo ========================================
echo Shard Count: %SHARD_COUNT%
echo Target Migration: %TARGET_MIGRATION%
echo.
echo Performance Settings:
echo   DATA_DIR: %DATA_DIR%
echo   PARALLEL_FETCHES: %PARALLEL_FETCHES%
echo   MAX_WORKERS: %MAX_WORKERS%
echo   ZSTD_LEVEL: %ZSTD_LEVEL%
echo   MAX_ROWS_PER_FILE: %MAX_ROWS_PER_FILE%
echo ========================================
echo.

REM Create directories
if not exist "%DATA_DIR%\raw" mkdir "%DATA_DIR%\raw"
if not exist "%CURSOR_DIR%" mkdir "%CURSOR_DIR%"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Build env string to pass to child windows
set ENV_VARS=DATA_DIR=%DATA_DIR%^&^&set CURSOR_DIR=%CURSOR_DIR%^&^&set PARALLEL_FETCHES=%PARALLEL_FETCHES%^&^&set MAX_WORKERS=%MAX_WORKERS%^&^&set ZSTD_LEVEL=%ZSTD_LEVEL%^&^&set MAX_ROWS_PER_FILE=%MAX_ROWS_PER_FILE%^&^&set IO_BUFFER_SIZE=%IO_BUFFER_SIZE%^&^&set CHUNK_SIZE=%CHUNK_SIZE%

REM Launch each shard in a new window with all env vars
for /L %%i in (0,1,%SHARD_COUNT%) do (
    if %%i lss %SHARD_COUNT% (
        echo Starting Shard %%i of %SHARD_COUNT%...
        start "Shard %%i" cmd /k "set SHARD_INDEX=%%i&&set SHARD_TOTAL=%SHARD_COUNT%&&set TARGET_MIGRATION=%TARGET_MIGRATION%&&set %ENV_VARS%&&node fetch-backfill-parquet.js"
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