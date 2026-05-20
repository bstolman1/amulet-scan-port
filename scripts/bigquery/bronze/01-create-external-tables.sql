-- Create External Tables for Canton Ledger Parquet Data
-- Points to GCS Hive-partitioned Parquet under raw/updates/

CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.canton_ledger`
OPTIONS (
  location = 'US',
  description = 'Canton Network ledger data warehouse'
);

-- Events External Table
CREATE OR REPLACE EXTERNAL TABLE `${PROJECT_ID}.canton_ledger.events_raw`
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
CREATE OR REPLACE EXTERNAL TABLE `${PROJECT_ID}.canton_ledger.updates_raw`
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
