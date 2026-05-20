-- Create External Tables for Canton Ledger Parquet Data
-- Points to GCS Hive-partitioned Parquet under raw/updates/
-- These are raw Parquet tables — timestamps are strings, arrays are
-- Parquet LIST structs, JSON fields are strings. Use 02-transform-raw-data.sql
-- to materialize properly typed tables before querying.

CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.raw`
OPTIONS (
  location = 'US',
  description = 'Canton Network raw external tables on GCS Parquet'
);

-- Events External Table
CREATE OR REPLACE EXTERNAL TABLE `${PROJECT_ID}.raw.events`
WITH PARTITION COLUMNS (
  migration INT64,
  year INT64,
  month INT64,
  day INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://${BUCKET_NAME}/raw/updates/events/*'],
  hive_partition_uri_prefix = 'gs://${BUCKET_NAME}/raw/updates/events/',
  require_hive_partition_filter = false
);

-- Updates External Table
CREATE OR REPLACE EXTERNAL TABLE `${PROJECT_ID}.raw.updates`
WITH PARTITION COLUMNS (
  migration INT64,
  year INT64,
  month INT64,
  day INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://${BUCKET_NAME}/raw/updates/updates/*'],
  hive_partition_uri_prefix = 'gs://${BUCKET_NAME}/raw/updates/updates/',
  require_hive_partition_filter = false
);
