# Backend Architecture

## Overview

This project uses a **hybrid backend architecture**:
- **DuckDB + Parquet** for heavy ledger data (updates, events)
- **Supabase** for lightweight metadata (cursors, snapshots, user data)

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
│  Ingestion Scripts │  (fetch-updates-parquet.js)
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
   node fetch-updates-parquet.js
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
| `GET /api/acs/snapshots` | List ACS snapshots |
| `GET /api/acs/latest` | Latest ACS snapshot |
| `GET /api/acs/templates` | Template statistics |
| `GET /api/acs/contracts` | Contracts by template |
| `GET /api/acs/stats` | ACS overview stats |
| `GET /api/acs/supply` | Supply statistics |

## Supabase Tables (Metadata Only)

These tables remain in Supabase for lightweight metadata:
- `backfill_cursors` - Track ingestion progress
- `acs_snapshots` - ACS snapshot metadata
- `acs_template_stats` - Template statistics
- `cips`, `cip_types` - Governance data
- `user_roles` - Authentication

Heavy data tables (`ledger_updates`, `ledger_events`) are now stored in Parquet files.

## Local ACS Snapshots

The ACS snapshot data is stored in `data/acs/` directory with the following structure:
```
data/acs/
  year=2024/
    month=12/
      day=11/
        contracts-1.jsonl
        contracts-2.jsonl
```

Run the ACS snapshot script to populate local data:
```bash
cd scripts/ingest
node fetch-acs-parquet.js
```

The UI will automatically use local ACS data when `VITE_LEDGER_BACKEND=duckdb` is set.
