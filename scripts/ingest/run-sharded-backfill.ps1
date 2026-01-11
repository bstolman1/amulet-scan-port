# Sharded Backfill Launcher for Windows PowerShell
# Usage: .\run-sharded-backfill.ps1 [-ShardCount 4] [-TargetMigration 3]

param(
    [int]$ShardCount = 4,
    [int]$TargetMigration = 3
)

Write-Host "========================================"
Write-Host "Sharded Backfill Launcher"
Write-Host "========================================"
Write-Host "Shard Count: $ShardCount"
Write-Host "Target Migration: $TargetMigration"
Write-Host "========================================"
Write-Host ""

# Create logs directory
$logsDir = Join-Path $PSScriptRoot "..\..\data\logs"
if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
}

# Launch each shard in a new window
for ($i = 0; $i -lt $ShardCount; $i++) {
    Write-Host "Starting Shard $i of $ShardCount..."
    
    $env = @{
        SHARD_INDEX = $i
        SHARD_TOTAL = $ShardCount
        TARGET_MIGRATION = $TargetMigration
    }
    
    $envString = "set SHARD_INDEX=$i && set SHARD_TOTAL=$ShardCount && set TARGET_MIGRATION=$TargetMigration"
    
    Start-Process cmd -ArgumentList "/k", "cd /d `"$PSScriptRoot`" && $envString && node fetch-backfill.js" -WindowStyle Normal
    
    Start-Sleep -Seconds 2
}

Write-Host ""
Write-Host "All $ShardCount shards launched!" -ForegroundColor Green
Write-Host ""
Write-Host "To monitor progress, run:"
Write-Host "  node shard-progress.js --watch" -ForegroundColor Cyan
Write-Host ""