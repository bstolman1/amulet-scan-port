# Backend Architecture

## Overview

This project uses a **hybrid backend architecture**:
- **DuckDB + Parquet** for heavy ledger data (updates, events)
- **Supabase** for lightweight metadata (cursors, snapshots, user data)

## Two Ingestion Pipelines

You have two options for ingesting data:

### Pipeline A: JSONL → Parquet (Original)
```bash
cd scripts/ingest
node fetch-backfill-parquet.js   # Historical backfill
node fetch-updates-parquet.js    # Live updates
```
- Writes intermediate JSONL files
- Converts to Parquet on flush
- Good for debugging (human-readable intermediate files)

### Pipeline B: Direct DuckDB → Parquet (Faster)
```bash
cd scripts/ingest
node fetch-backfill-duckdb.js    # Historical backfill
node fetch-updates-duckdb.js     # Live updates
```
- No intermediate files
- Uses DuckDB in-memory tables
- Writes directly to compressed Parquet with ZSTD
- **~2-3x faster** for large backfills

### Environment Variables
```bash
# Both pipelines
SCAN_URL=https://scan.sv-1.global.canton.network.sync.global/api/scan
BATCH_SIZE=500
PARALLEL_FETCHES=4

# DuckDB pipeline only
FLUSH_ROWS=250000    # Rows before flush (default 250k)
FLUSH_MS=30000       # Time before flush (default 30s)
```

## Backend Toggle

The frontend can switch between backends via environment variables:

```env
# Use DuckDB for ledger data (default)
VITE_LEDGER_BACKEND=duckdb

# Or use Supabase for ledger data
VITE_LEDGER_BACKEND=supabase

# DuckDB API URL
VITE_DUCKDB_API_URL=http://localhost:3001
```

## Data Flow

```
Canton Network API
        │
        ▼
┌───────────────────┐
│  Ingestion Scripts │  (fetch-*-parquet.js OR fetch-*-duckdb.js)
└───────────────────┘
        │
        ▼
┌───────────────────┐
│  Parquet Files    │  (data/raw/**/*.parquet)
│  (~1.8TB storage) │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│  DuckDB Server    │  (server/server.js)
│  Port 3001        │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│  React Frontend   │  (Lovable)
└───────────────────┘
```

## Running Locally

1. **Start the DuckDB API server:**
   ```bash
   cd server
   npm install
   npm start  # Runs on port 3001
   ```

2. **Run ingestion (in another terminal):**
   ```bash
   cd scripts/ingest
   npm install
   
   # Choose one:
   node fetch-backfill-duckdb.js   # Faster (recommended)
   # OR
   node fetch-backfill-parquet.js  # Original pipeline
   ```

3. **Access the frontend:**
   - Lovable preview: Uses the DuckDB API at localhost:3001
   - Or expose via Cloudflare Tunnel for team access

## Archived Scripts

The following scripts were used for the old Supabase-only architecture:
- `scripts/archive/fetch-backfill-history.js.archived`
- `scripts/archive/ingest-updates.js.archived`

These have been replaced by Parquet-based scripts in `scripts/ingest/`.

## API Endpoints

The DuckDB server exposes these endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /api/events/latest` | Latest ledger events |
| `GET /api/events/by-type/:type` | Events by type |
| `GET /api/events/by-template/:id` | Events by template |
| `GET /api/contracts/:id` | Contract lifecycle |
| `GET /api/contracts/templates/list` | All templates |
| `GET /api/stats/overview` | Overview statistics |
| `GET /api/search` | Search events |

## Supabase Tables (Metadata Only)

These tables remain in Supabase for lightweight metadata:
- `backfill_cursors` - Track ingestion progress
- `acs_snapshots` - ACS snapshot metadata
- `acs_template_stats` - Template statistics
- `cips`, `cip_types` - Governance data
- `user_roles` - Authentication

Heavy data tables (`ledger_updates`, `ledger_events`) are now stored in Parquet files.
