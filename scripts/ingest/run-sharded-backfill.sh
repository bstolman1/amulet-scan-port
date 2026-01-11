#!/usr/bin/env bash
set -euo pipefail

# ==========================================================
#  WSL2 / Linux High-Performance Sharded Backfill Launcher
#  Auto-tunes CPU usage, decode workers, and shard count
# ==========================================================

# --- Auto Detect CPU Cores ---
TOTAL_CORES=$(nproc)
P_CORES=$((TOTAL_CORES - 4 > 0 ? TOTAL_CORES - 4 : TOTAL_CORES))
DEFAULT_SHARDS=$P_CORES

# --- CLI Args ---
SHARD_COUNT="${1:-$DEFAULT_SHARDS}"
TARGET_MIGRATION="${2:-3}"

# --- Environment Defaults (can be overridden) ---
# Use /mnt/c/ledger_raw for Windows interop (maps to C:\ledger_raw)
export DATA_DIR="${DATA_DIR:-/mnt/c/ledger_raw}"
export PARALLEL_FETCHES="${PARALLEL_FETCHES:-6}"
export MAX_WORKERS="${MAX_WORKERS:-30}"
export MAX_ROWS_PER_FILE="${MAX_ROWS_PER_FILE:-15000}"

# Derived directories (all under DATA_DIR)
export CURSOR_DIR="${DATA_DIR}/cursors"
export LOG_DIR="${DATA_DIR}/logs"

# Decode workers = CPU cores / 2 (minimum 2)
DEFAULT_DECODE_WORKERS=$(( TOTAL_CORES / 2 ))
if [ "$DEFAULT_DECODE_WORKERS" -lt 2 ]; then
  DEFAULT_DECODE_WORKERS=2
fi
export DECODE_WORKERS="${DECODE_WORKERS:-$DEFAULT_DECODE_WORKERS}"

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Display config ---
echo "=========================================================="
echo "🚀 WSL2 Canton Ledger Backfill Launcher"
echo "=========================================================="
echo "CPU cores detected:       $TOTAL_CORES"
echo "Shards to launch:         $SHARD_COUNT"
echo "Decode workers per shard: $DECODE_WORKERS"
echo "Parallel fetches:         $PARALLEL_FETCHES"
echo "Max binary workers:       $MAX_WORKERS"
echo "Max rows per file:        $MAX_ROWS_PER_FILE"
echo "Target migration:         $TARGET_MIGRATION"
echo "DATA_DIR:                 $DATA_DIR"
echo "CURSOR_DIR:               $CURSOR_DIR"
echo "LOG_DIR:                  $LOG_DIR"
echo "Working dir:              $SCRIPT_DIR"
echo "=========================================================="
echo

# --- Ensure directories exist ---
mkdir -p "$DATA_DIR/raw"
mkdir -p "$CURSOR_DIR"
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

# --- Calculate and display time ranges ---
echo "📊 Shard Time Distribution:"
echo "   (Each shard handles an equal slice of the migration time range)"
echo ""

# --- Launch shards ---
for ((i=0; i<SHARD_COUNT; i++)); do
    LOG_FILE="$LOG_DIR/shard-${i}.log"
    
    echo "   ➡️  Starting shard $i of $SHARD_COUNT → $LOG_FILE"
    
    # Launch in background
    (
        cd "$SCRIPT_DIR"
        SHARD_INDEX=$i SHARD_TOTAL=$SHARD_COUNT TARGET_MIGRATION=$TARGET_MIGRATION \
            node fetch-backfill.js 2>&1 | tee "$LOG_FILE"
    ) &
    
    PIDS+=($!)
    
    # Small delay between launches to stagger initial API calls
    sleep 0.5
done

echo ""
echo "=========================================================="
echo "🎉 All $SHARD_COUNT shards launched!"
echo "📄 Logs: $LOG_DIR/shard-*.log"
echo "=========================================================="
echo ""
echo "Monitor commands:"
echo "   Progress:    node $SCRIPT_DIR/shard-progress.js"
echo "   Live logs:   tail -f $LOG_DIR/shard-0.log"
echo "   All shards:  tail -f $LOG_DIR/shard-*.log"
echo "   Stop:        Ctrl+C"
echo ""
echo "=========================================================="
echo ""

# Wait for all shards to complete
wait

echo ""
echo "🎉 All shards completed!"
