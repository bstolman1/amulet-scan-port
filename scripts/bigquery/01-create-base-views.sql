-- ============================================================================
-- BigQuery: Create External Tables for Canton Ledger Parquet Data
-- ============================================================================
-- Run this first to set up external tables pointing to GCS Parquet files
-- Replace YOUR_PROJECT_ID and YOUR_BUCKET with your actual values
-- 
-- Data Sources:
--   - backfill/: Historical data from backfill process (finite, complete)
--   - updates/: Live streaming data from v2/updates API (ongoing)
--   - acs/: Active Contract Set snapshots (periodic)
-- ============================================================================

-- Create dataset if not exists
CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT_ID.canton_ledger`
OPTIONS (
  location = 'US',
  description = 'Canton Network ledger data warehouse'
);

-- ============================================================================
-- BACKFILL EVENTS External Table (Historical)
-- ============================================================================
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT_ID.canton_ledger.backfill_events_raw`
WITH PARTITION COLUMNS (
  migration INT64,
  year INT64,
  month INT64,
  day INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://YOUR_BUCKET/raw/backfill/events/*.parquet'],
  hive_partition_uri_prefix = 'gs://YOUR_BUCKET/raw/backfill/events/',
  require_hive_partition_filter = false
);

-- ============================================================================
-- BACKFILL UPDATES External Table (Historical)
-- ============================================================================
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT_ID.canton_ledger.backfill_updates_raw`
WITH PARTITION COLUMNS (
  migration INT64,
  year INT64,
  month INT64,
  day INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://YOUR_BUCKET/raw/backfill/updates/*.parquet'],
  hive_partition_uri_prefix = 'gs://YOUR_BUCKET/raw/backfill/updates/',
  require_hive_partition_filter = false
);

-- ============================================================================
-- LIVE EVENTS External Table (Streaming)
-- ============================================================================
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT_ID.canton_ledger.live_events_raw`
WITH PARTITION COLUMNS (
  migration INT64,
  year INT64,
  month INT64,
  day INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://YOUR_BUCKET/raw/updates/events/*.parquet'],
  hive_partition_uri_prefix = 'gs://YOUR_BUCKET/raw/updates/events/',
  require_hive_partition_filter = false
);

-- ============================================================================
-- LIVE UPDATES External Table (Streaming)
-- ============================================================================
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT_ID.canton_ledger.live_updates_raw`
WITH PARTITION COLUMNS (
  migration INT64,
  year INT64,
  month INT64,
  day INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://YOUR_BUCKET/raw/updates/updates/*.parquet'],
  hive_partition_uri_prefix = 'gs://YOUR_BUCKET/raw/updates/updates/',
  require_hive_partition_filter = false
);

-- ============================================================================
-- UNIFIED EVENTS View (Backfill + Live)
-- ============================================================================
-- Use this view for queries that need all events across both sources
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.events_raw` AS
SELECT *, 'backfill' AS data_source FROM `YOUR_PROJECT_ID.canton_ledger.backfill_events_raw`
UNION ALL
SELECT *, 'live' AS data_source FROM `YOUR_PROJECT_ID.canton_ledger.live_events_raw`;

-- ============================================================================
-- UNIFIED UPDATES View (Backfill + Live)
-- ============================================================================
-- Use this view for queries that need all updates across both sources
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.updates_raw` AS
SELECT *, 'backfill' AS data_source FROM `YOUR_PROJECT_ID.canton_ledger.backfill_updates_raw`
UNION ALL
SELECT *, 'live' AS data_source FROM `YOUR_PROJECT_ID.canton_ledger.live_updates_raw`;

-- ============================================================================
-- ACS (Active Contract Set) External Table
-- ============================================================================
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT_ID.canton_ledger.acs_raw`
WITH PARTITION COLUMNS (
  migration INT64,
  year INT64,
  month INT64,
  day INT64,
  snapshot_id STRING
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://YOUR_BUCKET/raw/acs/*.parquet'],
  hive_partition_uri_prefix = 'gs://YOUR_BUCKET/raw/acs/',
  require_hive_partition_filter = false
);

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Count events by source (backfill vs live)
-- SELECT 
--   data_source,
--   COUNT(*) as event_count
-- FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
-- GROUP BY data_source;

-- Count events by template (top 20)
-- SELECT 
--   template_id,
--   COUNT(*) as event_count
-- FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
-- GROUP BY template_id
-- ORDER BY event_count DESC
-- LIMIT 20;

-- Count updates by type and source
-- SELECT
--   data_source,
--   update_type,
--   COUNT(*) as update_count
-- FROM `YOUR_PROJECT_ID.canton_ledger.updates_raw`
-- GROUP BY data_source, update_type;
