-- ============================================================================
-- BigQuery: Create External Tables for Canton Ledger Parquet Data
-- ============================================================================
-- Run this first to set up external tables pointing to GCS Parquet files
-- Replace YOUR_PROJECT_ID and YOUR_BUCKET with your actual values
-- ============================================================================

-- Create dataset if not exists
CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT_ID.canton_ledger`
OPTIONS (
  location = 'US',
  description = 'Canton Network ledger data warehouse'
);

-- ============================================================================
-- EVENTS External Table
-- ============================================================================
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT_ID.canton_ledger.events_raw`
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
-- UPDATES External Table
-- ============================================================================
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT_ID.canton_ledger.updates_raw`
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

-- Count events by template (top 20)
-- SELECT 
--   template_id,
--   COUNT(*) as event_count
-- FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
-- GROUP BY template_id
-- ORDER BY event_count DESC
-- LIMIT 20;

-- Count updates by type
-- SELECT
--   update_type,
--   COUNT(*) as update_count
-- FROM `YOUR_PROJECT_ID.canton_ledger.updates_raw`
-- GROUP BY update_type;
