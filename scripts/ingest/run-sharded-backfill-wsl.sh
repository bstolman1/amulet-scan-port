#!/bin/bash
set -e

echo ""
echo "🚀 Optimized Canton Backfill (WSL2) Launcher"
echo "============================================"

CPU=$(nproc)
echo "Detected CPU cores: $CPU"

# Optimized defaults
export SHARD_COUNT=${SHARD_COUNT:-12}
export PARALLEL_FETCHES=${PARALLEL_FETCHES:-3}
export DECODE_WORKERS=${DECODE_WORKERS:-8}
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

echo "Shard count:         $SHARD_COUNT"
echo "Parallel fetches:    $PARALLEL_FETCHES"
echo "Decode workers:      $DECODE_WORKERS"
echo "Writer workers:      $MAX_BINARY_WORKERS"
echo "Max rows per file:   $MAX_ROWS_PER_FILE"
echo "Target migration:    $TARGET_MIGRATION"
echo "Data dir:            $DATA_DIR"
echo ""

cd "$(dirname "$0")"

echo "Launching shards..."
echo "============================================"

for i in $(seq 0 $((SHARD_COUNT-1))); do
    LOG_FILE="$LOG_DIR/shard-$i.log"
    echo "➡️  Starting shard $i → $LOG_FILE"

    SHARD_INDEX=$i \
    SHARD_TOTAL=$SHARD_COUNT \
    TARGET_MIGRATION=$TARGET_MIGRATION \
    DATA_DIR=$DATA_DIR \
    PARALLEL_FETCHES=$PARALLEL_FETCHES \
    DECODE_WORKERS=$DECODE_WORKERS \
    MAX_BINARY_WORKERS=$MAX_BINARY_WORKERS \
    MAX_ROWS_PER_FILE=$MAX_ROWS_PER_FILE \
    ZSTD_LEVEL=$ZSTD_LEVEL \
    CHUNK_SIZE=$CHUNK_SIZE \
    node fetch-backfill-parquet.js >"$LOG_FILE" 2>&1 &
done

echo ""
echo "🎉 All $SHARD_COUNT shards launched!"
echo ""
echo "Monitor with:"
echo "   tail -f $LOG_DIR/shard-0.log"
echo "   tail -f $LOG_DIR/shard-*.log"
echo "   node shard-progress.js --watch"
echo ""
