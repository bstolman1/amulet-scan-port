# Canton Ledger Ingestion Pipeline

Scripts for ingesting ledger data from Canton/Daml networks into efficient storage formats.

## Quick Start

```bash
# Backfill historical data (writes directly to Parquet)
node fetch-backfill.js

# Fetch ACS snapshot (writes directly to Parquet)
node fetch-acs.js

# Fetch live updates (writes directly to Parquet)
node fetch-updates.js --live
```

## Default Output Format

All scripts now write **directly to Parquet** by default - no separate materialization step needed!

### Optional: Keep Intermediate Formats

Use `--keep-raw` to also preserve intermediate formats (for debugging or streaming access):

| Script | Default Output | With `--keep-raw` |
|--------|----------------|-------------------|
| `fetch-backfill.js` | `.parquet` | `.parquet` + `.pb.zst` |
| `fetch-updates.js` | `.parquet` | `.parquet` + `.pb.zst` |
| `fetch-acs.js` | `.parquet` | `.parquet` + `.jsonl` |

```bash
# Example: Backfill with both formats
node fetch-backfill.js --keep-raw
```

## Data Formats

| Format | Extension | Use Case | Query Method |
|--------|-----------|----------|--------------|
| Parquet | `.parquet` | **Default** - Analytics & SQL queries | DuckDB `read_parquet()` |
| Protobuf + ZSTD | `.pb.zst` | Streaming, debugging | `binary-reader.js` |
| JSON Lines | `.jsonl` | ACS snapshots (legacy) | DuckDB `read_json_auto()` |

### Format Details

**`.parquet` (Apache Parquet) - DEFAULT**
- Columnar format optimized for analytics
- Best SQL query performance
- ZSTD compression for good file sizes
- Immediate SQL access after ingestion

**`.pb.zst` (Protobuf + ZSTD)**
- Chunked format: `[4-byte length][compressed chunk]...`
- Highest compression ratio (~5-10% of raw JSON)
- Streaming-friendly via `read-binary.js`
- Only written with `--keep-raw` flag

**`.jsonl` (JSON Lines)**
- Human-readable, easy to debug
- DuckDB can read directly with `read_json_auto()`
- Only written for ACS with `--keep-raw` flag

## Scripts

### Ingestion

| Script | Purpose | Default Output |
|--------|---------|----------------|
| `fetch-backfill.js` | Historical backfill | `.parquet` |
| `fetch-updates.js` | Live ledger updates | `.parquet` |
| `fetch-acs.js` | Active Contract Set snapshots | `.parquet` |

### Materialization (Legacy)

| Script | Purpose |
|--------|---------|
| `materialize-parquet.js` | Convert `.pb.zst`/`.jsonl` to `.parquet` (only needed for legacy data) |

**Usage for legacy data:**
```bash
# Convert JSONL files only (default)
node materialize-parquet.js

# Convert pb.zst files to Parquet
node materialize-parquet.js --include-binary

# Convert only pb.zst files
node materialize-parquet.js --binary-only

# Keep original files after conversion
node materialize-parquet.js --include-binary --keep-originals
```

### Reading/Debugging

| Script | Purpose |
|--------|---------|
| `read-binary.js` | Decode and inspect `.pb.zst` files |

**Usage:**
```bash
# Print as JSON
node read-binary.js file.pb.zst

# Print as JSON lines
node read-binary.js file.pb.zst --jsonl

# Show file stats only
node read-binary.js file.pb.zst --stats

# Convert all in directory to JSONL
node read-binary.js ./data --convert
```

## Directory Structure

```
data/raw/
├── migration=1/
│   └── year=2024/
│       └── month=12/
│           └── day=25/
│               ├── updates-*.parquet     # Default output
│               ├── events-*.parquet      # Default output
│               ├── updates-*.pb.zst      # Only with --keep-raw
│               └── events-*.pb.zst       # Only with --keep-raw
└── acs/
    └── migration=1/
        └── year=2024/
            └── month=12/
                └── day=25/
                    └── snapshot=1430/
                        ├── contracts-*.parquet  # Default output
                        ├── contracts-*.jsonl    # Only with --keep-raw
                        └── _COMPLETE            # Completion marker
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATA_DIR` | Base directory for data files | `C:\ledger_raw` (Windows) |
| `SCAN_URL` | Canton Scan API URL | Production scan URL |
| `BATCH_SIZE` | Records per API request | 1000 (backfill), 100 (updates) |
| `PARALLEL_FETCHES` | Concurrent API requests | 8 |
| `MAX_ROWS_PER_FILE` | Rows before flushing to file | 5000 |

## npm Scripts

```bash
npm run backfill     # Run historical backfill
npm run ingest       # Fetch live updates
npm run materialize  # Convert legacy data to Parquet
npm run acs          # Fetch ACS snapshot
npm run acs:schedule # Run ACS on schedule
```

## Requirements

- **DuckDB CLI** must be installed and available in PATH for Parquet writes
- Node.js 18+

### Installing DuckDB

```bash
# Windows (via Chocolatey)
choco install duckdb

# macOS (via Homebrew)
brew install duckdb

# Linux (download from releases)
wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
sudo mv duckdb /usr/local/bin/
```
