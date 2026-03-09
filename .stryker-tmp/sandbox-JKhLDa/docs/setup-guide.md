# Setup Guide

Complete guide for setting up Amulet Scan for local development.

## Prerequisites

### Required Software

| Software | Version | Purpose |
|----------|---------|---------|
| Node.js | 20.x or later | Runtime for API server and ingestion |
| npm | 10.x or later | Package management |
| Git | Any recent | Version control |

### System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| RAM | 8 GB | 16+ GB |
| Storage | 50 GB SSD | 500+ GB SSD |
| CPU | 4 cores | 8+ cores |

> **Note**: Storage requirements increase with data retention. Full historical backfill requires ~2TB.

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/amulet-scan.git
cd amulet-scan
```

### 2. Install Dependencies

```bash
# Frontend dependencies
npm install

# Server dependencies
cd server
npm install

# Ingestion script dependencies
cd ../scripts/ingest
npm install
```

### 3. Create Data Directories

```bash
# Create directories for data storage
mkdir -p data/ledger_raw
mkdir -p data/acs
mkdir -p data/cursors
```

### 4. Configure Environment

Create `server/.env`:

```bash
# Copy the example file
cp server/.env.example server/.env

# Edit with your settings
nano server/.env
```

**Required settings:**

```env
# Server port
PORT=3001

# Data directories (use absolute paths)
DATA_DIR=/path/to/amulet-scan/data/ledger_raw
CURSOR_DIR=/path/to/amulet-scan/data/cursors

# Enable warehouse engine for better performance
ENGINE_ENABLED=true
```

**Optional settings:**

```env
# Logging level
LOG_LEVEL=info

# Groups.io API key (for SV announcements)
GROUPS_IO_API_KEY=your_key_here

# Kaiko API key (for price data)
KAIKO_API_KEY=your_key_here

# OpenAI API key (for LLM classification)
OPENAI_API_KEY=your_key_here
```

Create `scripts/ingest/.env`:

```bash
cp scripts/ingest/.env.example scripts/ingest/.env
nano scripts/ingest/.env
```

**Required settings:**

```env
# Canton Scan API endpoint
SCAN_URL=https://scan.sv-1.global.canton.network.sync.global/api/scan

# Data directories (match server/.env)
DATA_DIR=/path/to/amulet-scan/data/ledger_raw
CURSOR_DIR=/path/to/amulet-scan/data/cursors

# Performance tuning
PARALLEL_FETCHES=8
MAX_WORKERS=12
BATCH_SIZE=1000
```

## Running the Application

### Start the API Server

```bash
cd server
npm start
```

The server starts on `http://localhost:3001`. Verify it's running:

```bash
curl http://localhost:3001/health
# Should return: {"status":"ok","timestamp":"..."}
```

### Start the Frontend

In a new terminal:

```bash
# From project root
npm run dev
```

The frontend opens at `http://localhost:5173`.

### Run Data Ingestion

In another terminal, choose one or more ingestion modes:

```bash
cd scripts/ingest

# Live updates (runs continuously)
node fetch-updates.js

# Historical backfill (one-time or scheduled)
node fetch-backfill.js

# ACS snapshot (periodic)
node fetch-acs.js
```

## Initial Data Setup

### Option A: Start Fresh (Live Updates Only)

1. Start the API server
2. Run `fetch-updates.js` to begin collecting live data
3. Data will accumulate over time

### Option B: Historical Backfill

For complete historical data:

```bash
cd scripts/ingest

# Start backfill (can take hours to days depending on network speed)
node fetch-backfill.js

# Monitor progress
tail -f ../../data/logs/backfill.log
```

**Backfill characteristics:**
- Resumes automatically if interrupted
- Uses cursor files to track progress
- Parallel fetching for performance

### Option C: ACS Snapshot First

For quick supply/holder data:

```bash
cd scripts/ingest
node fetch-acs.js
```

This provides immediate access to:
- Current token supply
- Rich list (top holders)
- Active contracts by template

## Building Indexes

After ingesting data, build indexes for fast queries:

### Template File Index

```bash
# Via API
curl -X POST http://localhost:3001/api/engine/templates/build

# Check progress
curl http://localhost:3001/api/engine/templates/status
```

### Party Index

```bash
# Start build
curl -X POST http://localhost:3001/api/party/index/build

# Check status
curl http://localhost:3001/api/party/index/status
```

### Vote Request Index

```bash
# Triggers automatically or via
curl -X POST http://localhost:3001/api/events/vote-request-index/build
```

## Verification

### Check Server Health

```bash
curl http://localhost:3001/health/detailed
```

### Check Data Stats

```bash
curl http://localhost:3001/api/stats/overview
```

### Check Index Status

```bash
curl http://localhost:3001/api/engine/status
```

## Common Issues

### "No data found"

1. Verify `DATA_DIR` environment variable is set correctly
2. Check that ingestion scripts have run
3. Verify file permissions on data directories

### "Index not built"

Some endpoints require indexes. Build them:

```bash
curl -X POST http://localhost:3001/api/engine/templates/build
```

### "Connection refused"

1. Ensure server is running: `npm start`
2. Check port 3001 is not in use: `lsof -i :3001`

### High Memory Usage

For large datasets:
- Increase Node.js heap: `NODE_OPTIONS="--max-old-space-size=8192" npm start`
- Reduce `MAX_WORKERS` in ingestion scripts
- Enable streaming mode (default for binary reader)

## Next Steps

1. **Deploy to Production**: See [Deployment Guide](deployment.md)
2. **Configure HTTPS**: Add nginx reverse proxy with SSL
3. **Set Up Monitoring**: Use systemd for process management
4. **Schedule Ingestion**: Add cron jobs for automated data updates

## Directory Structure After Setup

```
amulet-scan/
├── data/
│   ├── ledger_raw/
│   │   └── migration=1/
│   │       └── year=2024/
│   │           └── month=01/
│   │               └── day=15/
│   │                   └── events-*.pb.zst
│   ├── acs/
│   │   └── migration=1/
│   │       └── year=2024/
│   │           └── month=01/
│   │               └── day=15/
│   │                   └── snapshot=120000/
│   │                       ├── *.parquet
│   │                       └── _COMPLETE
│   └── cursors/
│       ├── updates-cursor.json
│       └── backfill-cursor.json
├── server/
│   └── .env
└── scripts/ingest/
    └── .env
```
