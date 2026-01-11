# Canton Ledger Ingestion Pipeline

Scripts for ingesting ledger data from Canton/Daml networks into efficient storage formats.

## Data Formats

| Format | Extension | Use Case | Query Method |
|--------|-----------|----------|--------------|
| Protobuf + ZSTD | `.pb.zst` | Backfill (high volume) | `binary-reader.js` streaming |
| JSON Lines | `.jsonl` | ACS snapshots | DuckDB `read_json_auto()` |
| Parquet | `.parquet` | Materialized analytics | DuckDB `read_parquet()` |

### Format Details

**`.pb.zst` (Protobuf + ZSTD)**
- Primary format for ledger updates and events
- Chunked format: `[4-byte length][compressed chunk]...`
- Highest compression ratio (~5-10% of raw JSON)
- Streaming-friendly via `read-binary.js`

**`.jsonl` (JSON Lines)**
- Used for ACS (Active Contract Set) snapshots
- Human-readable, easy to debug
- DuckDB can read directly with `read_json_auto()`

**`.parquet` (Apache Parquet)**
- Columnar format optimized for analytics
- Best SQL query performance
- ZSTD compression for good file sizes

## Scripts

### Ingestion

| Script | Purpose | Output |
|--------|---------|--------|
| `fetch-updates.js` | Fetch live ledger updates | `.pb.zst` |
| `fetch-backfill.js` | Historical backfill | `.pb.zst` |
| `fetch-acs.js` | Active Contract Set snapshots | `.jsonl` |

### Materialization

| Script | Purpose |
|--------|---------|
| `materialize-parquet.js` | Convert `.pb.zst`/`.jsonl` to `.parquet` |

**Usage:**
```bash
# Convert JSONL files only (default)
node materialize-parquet.js

# Convert pb.zst files to Parquet (direct, no intermediate JSONL)
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

## When to Materialize to Parquet?

### Keep `.pb.zst` (Default)
- Streaming queries via `binary-reader.js` work well
- Storage space is limited (best compression)
- Data is append-only, rarely queried

### Materialize to `.parquet`
- Need fastest SQL query performance
- Running complex analytical queries (joins, aggregations)
- Integration with external tools (Tableau, DBeaver, etc.)
- Ad-hoc exploration with DuckDB CLI

## Directory Structure

```
data/raw/
├── updates/
│   └── YYYY-MM-DD/
│       ├── updates-{timestamp}.pb.zst    # Primary format
│       └── updates-{timestamp}.parquet   # After materialization
├── events/
│   └── YYYY-MM-DD/
│       ├── events-{timestamp}.pb.zst
│       └── events-{timestamp}.parquet
└── acs/
    └── {snapshot-id}.jsonl
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATA_DIR` | Base directory for data files | `C:\ledger_raw` (Windows) |

## npm Scripts

```bash
npm run backfill     # Run historical backfill
npm run ingest       # Fetch live updates
npm run materialize  # Convert to Parquet
npm run acs          # Fetch ACS snapshot
npm run acs:schedule # Run ACS on schedule
```
