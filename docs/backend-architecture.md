# Backend Architecture

## Overview

The backend uses a **read-only DuckDB API server** that queries pre-ingested data:

- **DuckDB** for querying ledger events and ACS snapshots
- **Parquet files** for ledger data (written by ingestion scripts)
- **Local filesystem** for ACS snapshots and metadata

**Key principle**: The API server is read-only and request-driven. All ingestion runs separately via scripts.

## Data Flow

```
Canton Network API
        │
        ▼
┌───────────────────┐
│  Ingestion Scripts │  (scripts/ingest/*.js)
│  Run via cron/CI   │  Manual, scheduled, or CI-triggered
└───────────────────┘
        │
        ▼
┌───────────────────┐
│  Parquet Files    │  (ledger_raw/...)
│  ACS Snapshots    │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│  DuckDB API       │  (server/server.js)
│  Port 3001        │  READ-ONLY
└───────────────────┘
        │
        ▼
┌───────────────────┐
│  React Frontend   │
└───────────────────┘
```

## Running Locally

1. **Run ingestion (to populate data):**
   ```bash
   cd scripts/ingest
   npm install
   node fetch-updates.js      # Live updates
   node fetch-backfill.js     # Historical backfill
   node fetch-acs.js          # ACS snapshots
   ```

2. **Start the API server:**
   ```bash
   cd server
   npm install
   npm start  # Runs on port 3001
   ```

3. **Access the frontend:**
   - Lovable preview: Uses the DuckDB API at localhost:3001
   - Or expose via Cloudflare Tunnel for team access

## Server Characteristics

- **Read-only**: No background loops, no ingestion, no file scanning
- **Request-driven**: Only opens DuckDB and queries in response to HTTP requests
- **Stateless**: Can run indefinitely with minimal memory usage
- **Safe to restart**: No state to lose, no cursors to corrupt

## Security Features

- **SQL Injection Prevention**: All queries use centralized sanitization utilities
- **Dangerous Pattern Detection**: UNION injection, DROP/DELETE statements, and comment injection are rejected at input
- **Input Validation**: Numeric parameters have enforced bounds; string inputs are validated against patterns
- **Rate Limiting**: 100 req/min general, 20 req/min for expensive operations

## API Endpoints

The DuckDB server exposes these endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /health/detailed` | Detailed health with memory stats |
| `GET /health/config` | Configuration debug info |
| `GET /api/events/latest` | Latest ledger events |
| `GET /api/events/by-type/:type` | Events by type |
| `GET /api/events/by-template/:id` | Events by template |
| `GET /api/events/governance` | Governance events |
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
| `GET /api/acs/rich-list` | Top token holders |
| `POST /api/refresh-views` | Refresh DuckDB views (after ingestion) |

## Ingestion Scripts

Ingestion runs **separately** from the API server:

```bash
# Manual execution
node scripts/ingest/fetch-updates.js   # Live V2 updates
node scripts/ingest/fetch-backfill.js  # Historical backfill
node scripts/ingest/fetch-acs.js       # ACS snapshots

# Or via PM2 for persistence
pm2 start scripts/ingest/ingest-all.js --name ingest-all
```

Schedule via cron or CI for production. The API server does not need to be restarted after ingestion completes.

## Environment Variables

Configure the server via `server/.env`:

```env
# Required
DATA_DIR=/path/to/ledger_raw

# Optional
PORT=3001
LOG_LEVEL=info
```

## Local ACS Snapshots

ACS snapshot data is stored in the `DATA_DIR/acs/` directory:
```
acs/
  migration=1/
    year=2024/
      month=12/
        day=11/
          snapshot=120000/
            contracts-Amulet.parquet
            contracts-LockedAmulet.parquet
            _COMPLETE
```
