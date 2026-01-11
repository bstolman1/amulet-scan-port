#!/bin/bash
set -e

echo ""
echo "🚀 Optimized Canton Backfill (WSL2) - Auto-Tuning Mode"
echo "======================================================="

CPU=$(nproc)
echo "Detected CPU cores: $CPU"

# Sharding config
export SHARD_COUNT=${SHARD_COUNT:-12}

# Auto-tuning: Parallel Fetches (API calls)
export PARALLEL_FETCHES=${PARALLEL_FETCHES:-2}        # Starting point
export MIN_PARALLEL_FETCHES=${MIN_PARALLEL_FETCHES:-1} # Lower bound
export MAX_PARALLEL_FETCHES=${MAX_PARALLEL_FETCHES:-6} # Upper bound

# Auto-tuning: Decode Workers
export DECODE_WORKERS=${DECODE_WORKERS:-8}             # Starting point
export MIN_DECODE_WORKERS=${MIN_DECODE_WORKERS:-4}     # Lower bound
export MAX_DECODE_WORKERS=${MAX_DECODE_WORKERS:-16}    # Upper bound

# Writer pool (static)
export MAX_BINARY_WORKERS=${MAX_BINARY_WORKERS:-24}
export MAX_ROWS_PER_FILE=${MAX_ROWS_PER_FILE:-15000}
export ZSTD_LEVEL=${ZSTD_LEVEL:-1}
export CHUNK_SIZE=${CHUNK_SIZE:-4096}

export TARGET_MIGRATION=${TARGET_MIGRATION:-3}

# Directories (Windows-mounted)
export DATA_DIR=${DATA_DIR:-/mnt/c/ledger_raw}
export CURSOR_DIR=$DATA_DIR/cursors
export LOG_DIR=$DATA_DIR/logs

mkdir -p "$CURSOR_DIR" "$LOG_DIR"

echo ""
echo "Configuration:"
echo "  Shards:              $SHARD_COUNT"
echo "  Parallel fetches:    $PARALLEL_FETCHES (auto: $MIN_PARALLEL_FETCHES-$MAX_PARALLEL_FETCHES)"
echo "  Decode workers:      $DECODE_WORKERS (auto: $MIN_DECODE_WORKERS-$MAX_DECODE_WORKERS)"
echo "  Writer workers:      $MAX_BINARY_WORKERS (static)"
echo "  Max rows per file:   $MAX_ROWS_PER_FILE"
echo "  Target migration:    $TARGET_MIGRATION"
echo "  Data dir:            $DATA_DIR"
echo ""

cd "$(dirname "$0")"

echo "Launching shards..."
echo "======================================================="

for i in $(seq 0 $((SHARD_COUNT-1))); do
    LOG_FILE="$LOG_DIR/shard-$i.log"
    echo "➡️  Starting shard $i → $LOG_FILE"

    SHARD_INDEX=$i \
    SHARD_TOTAL=$SHARD_COUNT \
    TARGET_MIGRATION=$TARGET_MIGRATION \
    DATA_DIR=$DATA_DIR \
    PARALLEL_FETCHES=$PARALLEL_FETCHES \
    MIN_PARALLEL_FETCHES=$MIN_PARALLEL_FETCHES \
    MAX_PARALLEL_FETCHES=$MAX_PARALLEL_FETCHES \
    DECODE_WORKERS=$DECODE_WORKERS \
    MIN_DECODE_WORKERS=$MIN_DECODE_WORKERS \
    MAX_DECODE_WORKERS=$MAX_DECODE_WORKERS \
    MAX_BINARY_WORKERS=$MAX_BINARY_WORKERS \
    MAX_ROWS_PER_FILE=$MAX_ROWS_PER_FILE \
    ZSTD_LEVEL=$ZSTD_LEVEL \
    CHUNK_SIZE=$CHUNK_SIZE \
    node fetch-backfill.js >"$LOG_FILE" 2>&1 &
done

echo ""
echo "🎉 All $SHARD_COUNT shards launched with auto-tuning!"
echo ""
echo "Monitor with:"
echo "   tail -f $LOG_DIR/shard-0.log"
echo "   tail -f $LOG_DIR/shard-*.log"
echo "   node shard-progress.js --watch"
echo ""
echo "Look for auto-tune messages like:"
echo "   🔧 Auto-tune: high 503 rate → reducing PARALLEL_FETCHES"
echo "   🔧 Auto-tune: stable → increasing PARALLEL_FETCHES"
echo "   🔧 Auto-tune: decode queue growing → increasing workers"
echo ""
echo "======================================================="
echo "To run the server with the warehouse engine:"
echo ""
echo "   ENGINE_ENABLED=true DATA_DIR=$DATA_DIR CURSOR_DIR=$CURSOR_DIR node ../../server/server.js"
echo ""
echo "Engine endpoints:"
echo "   GET  /api/engine/status  - Engine status"
echo "   GET  /api/engine/stats   - Ingestion stats"
echo "   POST /api/engine/cycle   - Trigger ingestion cycle"
echo "   POST /api/engine/scan    - Scan for new files"
echo "======================================================="
echo ""