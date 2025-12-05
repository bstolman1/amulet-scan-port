# Amulet Scan Architecture

## Overview

This project provides a Canton ledger explorer with two backend options:

1. **Supabase/Postgres** - Cloud-hosted, managed database (current)
2. **Parquet/DuckDB** - Local-first, file-based analytics (new)

## Parquet/DuckDB Architecture

### Why This Approach?

- **TB-scale data**: Parquet handles 1.8TB+ efficiently
- **Zero cost**: No database hosting fees
- **BigQuery ready**: Parquet files upload directly to GCS/BigQuery
- **Simple**: No connection pooling, no ORM, just files + SQL

### Directory Structure

```
amulet-scan-port/
├── scripts/ingest/           # Data ingestion from Canton API
│   ├── fetch-updates-parquet.js    # Live incremental updates
│   ├── fetch-backfill-parquet.js   # Historical backfill
│   ├── parquet-schema.js           # Schema definitions
│   ├── write-parquet.js            # File writing logic
│   └── rotate-parquet.js           # File compaction
│
├── data/
│   ├── raw/                  # Partitioned data files
│   │   └── year=YYYY/month=MM/day=DD/
│   │       ├── updates-00001.parquet
│   │       └── events-00001.parquet
│   ├── cursors/              # Backfill progress tracking
│   └── metadata/             # Reference data (validators, etc.)
│
├── server/                   # Express + DuckDB API
│   ├── server.js
│   ├── duckdb/connection.js
│   └── api/
│       ├── events.js
│       ├── party.js
│       ├── contracts.js
│       ├── stats.js
│       └── search.js
│
└── frontend/lovable/         # React UI (this Lovable project)
```

### Data Flow

```
Canton Scan API
      │
      ▼
[fetch-updates-parquet.js]  ◄── Live polling (5s intervals)
      │
      ▼
[write-parquet.js]          ◄── Batch to 25k rows
      │
      ▼
/data/raw/year=.../          ◄── Partitioned files
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
   node scripts/ingest/fetch-updates-parquet.js
   
   # Or backfill historical data
   node scripts/ingest/fetch-backfill-parquet.js
   ```

3. **Convert to Parquet** (requires DuckDB CLI)
   ```bash
   node scripts/ingest/rotate-parquet.js
   ```

4. **Start API server**
   ```bash
   cd server && npm start
   ```

5. **Connect frontend**
   - Set `VITE_DUCKDB_API_URL=http://localhost:3001` in `.env`
   - Or use Cloudflare Tunnel for remote access

### BigQuery Migration

When ready to move to BigQuery:

1. Upload `/data/raw/` to Google Cloud Storage
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
