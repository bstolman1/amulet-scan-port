# BigQuery SQL Queries for Canton Ledger Data

This directory contains BigQuery SQL queries to parse JSON payloads from Parquet files and create structured tables for analysis.

## Directory Structure

- `01-create-base-views.sql` - Creates external tables pointing to GCS Parquet files
- `02-parse-events-by-template.sql` - Parses event payloads by template type
- `03-parse-updates.sql` - Parses update data
- `04-governance-tables.sql` - Creates governance-specific parsed tables
- `05-rewards-tables.sql` - Creates reward-specific parsed tables
- `06-amulet-tables.sql` - Creates amulet/currency parsed tables

## Setup

1. Replace `YOUR_PROJECT_ID` with your GCP project ID
2. Replace `YOUR_BUCKET` with your GCS bucket name
3. Run scripts in order (01, 02, 03, etc.)

## Data Sources

The queries expect Parquet files organized by data source:

### Backfill (Historical Data)
```
gs://YOUR_BUCKET/raw/backfill/events/migration=X/year=YYYY/month=M/day=D/*.parquet
gs://YOUR_BUCKET/raw/backfill/updates/migration=X/year=YYYY/month=M/day=D/*.parquet
```

### Live Updates (Streaming Data)
```
gs://YOUR_BUCKET/raw/updates/events/migration=X/year=YYYY/month=M/day=D/*.parquet
gs://YOUR_BUCKET/raw/updates/updates/migration=X/year=YYYY/month=M/day=D/*.parquet
```

### ACS Snapshots
```
gs://YOUR_BUCKET/raw/acs/migration=X/year=YYYY/month=M/day=D/snapshot_id=HHMMSS/*.parquet
```

## Unified Views

The `01-create-base-views.sql` script creates unified views that combine backfill and live data:
- `events_raw` - Union of backfill_events_raw + live_events_raw with `data_source` column
- `updates_raw` - Union of backfill_updates_raw + live_updates_raw with `data_source` column

This allows queries to transparently access all data without caring about the source.
