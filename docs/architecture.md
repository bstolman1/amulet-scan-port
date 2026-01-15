# Amulet Scan Architecture

## Overview

This project provides a Canton ledger explorer using a local-first DuckDB + Parquet architecture.

---

## ⚠️ DATA AUTHORITY CONTRACT

> **Parquet files produced by ledger ingestion are the sole authoritative data source.**
>
> All governance, rewards, and party state **must** be derived via DuckDB SQL queries
> over Parquet files. Binary formats (JSONL, PBZST) are **export-only** and must not
> be read by API routes or business logic.
>
> This is not documentation fluff — it's a contract with future you (and future collaborators).

### Enforcement

- `server/api/` — DuckDB queries over Parquet only
- `server/analytics/` — DuckDB analytical queries only  
- `scripts/ingest/` — Write-only (produces Parquet)
- `scripts/export/` — JSONL/PBZST writers (export-only, never imported by API)

**CI enforces this** via `data-authority-check` job (see `.github/workflows/test.yml`).

---

## DuckDB Architecture

### Why This Approach?

- **TB-scale data**: Parquet with ZSTD compression handles 1.8TB+ efficiently
- **Zero cost**: No database hosting fees
- **BigQuery ready**: Parquet files upload directly to GCS/BigQuery
- **Simple**: No connection pooling, no ORM, just files + SQL
- **Immutable**: Parquet files are append-only, derived views are disposable

### Directory Structure

```
amulet-scan-port/
├── scripts/ingest/           # Data ingestion from Canton API
│   ├── fetch-updates.js      # Live incremental updates
│   ├── fetch-backfill.js     # Historical backfill
│   ├── write-binary.js       # Binary file writing
│   └── write-parquet.js      # Parquet conversion
│
├── data/
│   ├── ledger_raw/           # Compressed binary files
│   ├── parquet/              # Materialized Parquet files
│   ├── cursors/              # Backfill progress tracking
│   └── cache/                # Aggregation cache
│
├── server/                   # Express + DuckDB API
│   ├── server.js
│   ├── duckdb/connection.js
│   └── api/
│       ├── events.js
│       ├── party.js
│       ├── contracts.js
│       ├── stats.js
│       ├── acs.js
│       └── search.js
│
└── src/                      # React UI (Lovable project)
```

### Data Flow

```
Canton Scan API
      │
      ▼
[fetch-updates.js]           ◄── Live polling (5s intervals)
      │
      ▼
[write-binary.js]            ◄── ZSTD-compressed Protobuf
      │
      ▼
/data/ledger_raw/            ◄── Binary files
      │
      ▼
[DuckDB Server]              ◄── Query engine
      │
      ▼
[React Frontend]             ◄── API calls
```

### Running Locally

1. **Install dependencies**
   ```bash
   cd server && npm install
   cd ../scripts/ingest && npm install
   ```

2. **Start ingestion**
   ```bash
   # Live updates
   node scripts/ingest/fetch-updates.js
   
   # Or backfill historical data
   node scripts/ingest/fetch-backfill.js
   ```

3. **Convert to Parquet** (optional)
   ```bash
   node scripts/ingest/materialize-parquet.js
   ```

4. **Start API server**
   ```bash
   cd server && npm start
   ```

5. **Connect frontend**
   - Set `VITE_DUCKDB_API_URL=http://localhost:3001` in `.env`

### BigQuery Migration

When ready to move to BigQuery:

1. Upload parquet files to Google Cloud Storage
2. Create external BigQuery table pointing to GCS
3. Update API queries to use BigQuery client

No re-ingestion needed - parquet files work directly.

## API Endpoints

### Events
- `GET /api/events/latest?limit=100` - Latest events
- `GET /api/events/by-type/:type` - Filter by event type
- `GET /api/events/by-template/:id` - Filter by template
- `GET /api/events/count` - Total count

### Party
- `GET /api/party/:partyId` - Party's events
- `GET /api/party/:partyId/summary` - Activity summary
- `GET /api/party/list/all` - All unique parties

### Contracts
- `GET /api/contracts/:contractId` - Contract lifecycle
- `GET /api/contracts/active/by-template/:suffix` - Active contracts
- `GET /api/contracts/templates/list` - All templates

### Stats
- `GET /api/stats/overview` - Dashboard stats
- `GET /api/stats/daily?days=30` - Daily counts
- `GET /api/stats/by-type` - By event type
- `GET /api/stats/hourly` - Last 24h by hour

### Search
- `GET /api/search?q=...&type=...&template=...` - Full search
