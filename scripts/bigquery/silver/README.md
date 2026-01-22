# BigQuery Silver Layer - Parsed & Typed Tables

This directory contains SQL scripts to create the **Silver Layer** of the data lakehouse:

## Medallion Architecture

| Layer | Description | Storage |
|-------|-------------|---------|
| **Bronze** | Raw Parquet files from ingestion | External tables on GCS |
| **Silver** | Parsed, typed, cleaned tables | Native BigQuery tables |
| **Gold** | Aggregated business metrics | Materialized views/tables |

## Silver Layer Characteristics

- **Fully parsed JSON** - No JSON extraction at query time
- **Strongly typed columns** - NUMERIC, INT64, TIMESTAMP, BOOL, ARRAY
- **Partitioned** - By date for efficient querying
- **Clustered** - By common filter columns for performance

## Scripts

1. `01-amulet-silver.sql` - Core currency contracts
2. `02-governance-silver.sql` - Governance and voting
3. `03-rewards-silver.sql` - Reward coupons
4. `04-validators-silver.sql` - Validator operations
5. `05-network-silver.sql` - Traffic, rounds, ANS
6. `06-exercised-silver.sql` - Choice executions

## Usage

```bash
# Run in order to create silver tables
bq query --use_legacy_sql=false < 01-amulet-silver.sql
```

## Refresh Strategy

Silver tables should be refreshed on a schedule:
- **Incremental**: Append new data based on `effective_at > last_load`
- **Full refresh**: For initial load or schema changes
