# Backend Architecture

## Overview

The backend uses a **DuckDB-based architecture** with local file storage:

- **DuckDB** for querying ledger events and ACS snapshots
- **Binary files** (Protobuf + ZSTD compression) for raw ledger data
- **Parquet files** for materialized analytics
- **Local filesystem** for ACS snapshots and metadata

## Data Flow

```
Canton Network API
        │
        ▼
┌───────────────────┐
│  Ingestion Scripts │  (scripts/ingest/*.js)
└───────────────────┘
        │
        ▼
┌───────────────────┐
│  Binary Storage   │  (*.pb.zst compressed Protobuf)
│  Parquet Files    │  (optional for analytics)
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
│  React Frontend   │
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
   node fetch-updates.js      # Live updates
   node fetch-backfill.js     # Historical backfill
   node fetch-acs.js          # ACS snapshots
   ```

3. **Access the frontend:**
   - Lovable preview: Uses the DuckDB API at localhost:3001
   - Or expose via Cloudflare Tunnel for team access

## Security Features

- **SQL Injection Prevention**: All queries use centralized sanitization utilities
- **Dangerous Pattern Detection**: UNION injection, DROP/DELETE statements, and comment injection are rejected at input
- **Input Validation**: Numeric parameters have enforced bounds; string inputs are validated against patterns
- **Parameterized Queries**: Where possible, values are escaped rather than interpolated

## API Endpoints

The DuckDB server exposes these endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
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

Run the ACS snapshot script to populate local data:
```bash
cd scripts/ingest
node fetch-acs.js
```

## Environment Variables

Configure the server via `server/.env`:

```env
# Required
DATA_DIR=/path/to/ledger_raw

# Optional
PORT=3001
ENGINE_ENABLED=true
ENGINE_INTERVAL_MS=30000
LOG_LEVEL=info
```
