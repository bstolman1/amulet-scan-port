#!/bin/bash
# Sharded Backfill Launcher
# Launches multiple parallel backfill processes, each handling a time slice
#
# Usage: ./run-sharded-backfill.sh [SHARD_COUNT] [MIGRATION_ID]
# Example: ./run-sharded-backfill.sh 4 3

set -e

SHARD_COUNT=${1:-4}
TARGET_MIGRATION=${2:-""}
PARALLEL_FETCHES=${PARALLEL_FETCHES:-5}  # Lower per-shard to avoid rate limits

echo "════════════════════════════════════════════════════════════════════════════════"
echo "🚀 Sharded Backfill Launcher"
echo "   Shard count: $SHARD_COUNT"
echo "   Target migration: ${TARGET_MIGRATION:-"all"}"
echo "   Parallel fetches per shard: $PARALLEL_FETCHES"
echo "════════════════════════════════════════════════════════════════════════════════"

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/../../data/logs"
mkdir -p "$LOG_DIR"

# Array to hold PIDs
declare -a PIDS

# Cleanup function
cleanup() {
    echo ""
    echo "🛑 Stopping all shards..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -SIGINT "$pid" 2>/dev/null || true
        fi
    done
    wait
    echo "✅ All shards stopped"
    exit 0
}

trap cleanup SIGINT SIGTERM

# Launch shards
for ((i=0; i<SHARD_COUNT; i++)); do
    LOG_FILE="$LOG_DIR/shard-${i}.log"
    
    echo "   Starting shard $i/$SHARD_COUNT → $LOG_FILE"
    
    # Build environment
    ENV_VARS="SHARD_INDEX=$i SHARD_TOTAL=$SHARD_COUNT PARALLEL_FETCHES=$PARALLEL_FETCHES"
    if [ -n "$TARGET_MIGRATION" ]; then
        ENV_VARS="$ENV_VARS TARGET_MIGRATION=$TARGET_MIGRATION"
    fi
    
    # Launch in background, redirect to log file
    (
        cd "$SCRIPT_DIR"
        env $ENV_VARS node fetch-backfill-parquet.js 2>&1 | tee "$LOG_FILE"
    ) &
    
    PIDS+=($!)
    
    # Small delay between launches to stagger initial API calls
    sleep 2
done

echo ""
echo "════════════════════════════════════════════════════════════════════════════════"
echo "✅ All $SHARD_COUNT shards launched"
echo "   Logs: $LOG_DIR/shard-*.log"
echo "   Monitor: node scripts/ingest/shard-progress.js"
echo "   Stop: Ctrl+C"
echo "════════════════════════════════════════════════════════════════════════════════"
echo ""

# Wait for all shards to complete
wait

echo ""
echo "🎉 All shards completed!"
