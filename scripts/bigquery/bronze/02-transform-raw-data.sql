-- Transform raw Parquet data into properly typed BigQuery tables
--
-- Raw Parquet files have type mismatches that BigQuery cannot auto-resolve:
--   - Timestamps stored as ISO 8601 strings (not TIMESTAMP)
--   - Arrays stored as Parquet LIST structs with a .list field
--   - JSON fields stored as strings (not JSON type)
--   - Some numeric fields need explicit casting
--
-- This script materializes two parsed tables that all downstream
-- views and silver-layer tables query against.

CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.transformed`
OPTIONS (
  location = 'US',
  description = 'Canton Network transformed/parsed tables'
);

-- ============================================================================
-- EVENTS: raw.events → transformed.events_parsed
-- ============================================================================
DROP TABLE IF EXISTS `${PROJECT_ID}.transformed.events_parsed`;

CREATE TABLE `${PROJECT_ID}.transformed.events_parsed` (
  event_id STRING,
  update_id STRING,
  contract_id STRING,
  template_id STRING,
  package_name STRING,
  event_type STRING,
  event_type_original STRING,
  synchronizer_id STRING,
  migration_id INT64,
  choice STRING,
  interface_id STRING,
  consuming BOOL,
  effective_at TIMESTAMP,
  recorded_at TIMESTAMP,
  timestamp TIMESTAMP,
  created_at_ts TIMESTAMP,
  signatories ARRAY<STRING>,
  observers ARRAY<STRING>,
  acting_parties ARRAY<STRING>,
  witness_parties ARRAY<STRING>,
  child_event_ids ARRAY<STRING>,
  reassignment_counter INT64,
  source_synchronizer STRING,
  target_synchronizer STRING,
  unassign_id STRING,
  submitter STRING,
  payload JSON,
  contract_key JSON,
  exercise_result JSON,
  raw_event JSON,
  trace_context JSON,
  year INT64,
  month INT64,
  day INT64
)
PARTITION BY DATE(effective_at)
CLUSTER BY template_id, event_type, migration_id;

INSERT INTO `${PROJECT_ID}.transformed.events_parsed`
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  package_name,
  event_type,
  event_type_original,
  synchronizer_id,
  CAST(migration_id AS INT64) AS migration_id,
  choice,
  interface_id,
  CAST(consuming AS BOOL) AS consuming,

  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', effective_at) AS effective_at,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', recorded_at) AS recorded_at,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', timestamp) AS timestamp,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at_ts) AS created_at_ts,

  CASE
    WHEN signatories IS NOT NULL THEN
      ARRAY(SELECT element FROM UNNEST(signatories.list))
    ELSE NULL
  END AS signatories,
  CASE
    WHEN observers IS NOT NULL THEN
      ARRAY(SELECT element FROM UNNEST(observers.list))
    ELSE NULL
  END AS observers,
  CASE
    WHEN acting_parties IS NOT NULL THEN
      ARRAY(SELECT element FROM UNNEST(acting_parties.list))
    ELSE NULL
  END AS acting_parties,
  CASE
    WHEN witness_parties IS NOT NULL THEN
      ARRAY(SELECT element FROM UNNEST(witness_parties.list))
    ELSE NULL
  END AS witness_parties,
  CASE
    WHEN child_event_ids IS NOT NULL THEN
      ARRAY(SELECT element FROM UNNEST(child_event_ids.list))
    ELSE NULL
  END AS child_event_ids,

  CAST(reassignment_counter AS INT64) AS reassignment_counter,
  source_synchronizer,
  target_synchronizer,
  unassign_id,
  submitter,

  CASE WHEN payload IS NOT NULL THEN SAFE.PARSE_JSON(payload) ELSE NULL END AS payload,
  CASE WHEN contract_key IS NOT NULL THEN SAFE.PARSE_JSON(contract_key) ELSE NULL END AS contract_key,
  CASE WHEN exercise_result IS NOT NULL THEN SAFE.PARSE_JSON(exercise_result) ELSE NULL END AS exercise_result,
  CASE WHEN raw_event IS NOT NULL THEN SAFE.PARSE_JSON(raw_event) ELSE NULL END AS raw_event,
  CASE WHEN trace_context IS NOT NULL THEN SAFE.PARSE_JSON(trace_context) ELSE NULL END AS trace_context,

  CAST(year AS INT64) AS year,
  CAST(month AS INT64) AS month,
  CAST(day AS INT64) AS day

FROM `${PROJECT_ID}.raw.events`;


-- ============================================================================
-- UPDATES: raw.updates → transformed.updates_parsed
-- ============================================================================
DROP TABLE IF EXISTS `${PROJECT_ID}.transformed.updates_parsed`;

CREATE TABLE `${PROJECT_ID}.transformed.updates_parsed` (
  update_id STRING,
  update_type STRING,
  migration_id INT64,
  synchronizer_id STRING,
  workflow_id STRING,
  command_id STRING,
  offset INT64,
  record_time TIMESTAMP,
  effective_at TIMESTAMP,
  recorded_at TIMESTAMP,
  timestamp TIMESTAMP,
  kind STRING,
  root_event_ids ARRAY<STRING>,
  event_count INT64,
  source_synchronizer STRING,
  target_synchronizer STRING,
  unassign_id STRING,
  submitter STRING,
  reassignment_counter INT64,
  trace_context JSON,
  update_data JSON,
  year INT64,
  month INT64,
  day INT64
)
PARTITION BY DATE(effective_at)
CLUSTER BY update_type, migration_id;

INSERT INTO `${PROJECT_ID}.transformed.updates_parsed`
SELECT
  update_id,
  update_type,
  CAST(migration_id AS INT64) AS migration_id,
  synchronizer_id,
  workflow_id,
  command_id,
  CAST(offset AS INT64) AS offset,

  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', record_time) AS record_time,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', effective_at) AS effective_at,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', recorded_at) AS recorded_at,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', timestamp) AS timestamp,

  kind,

  CASE
    WHEN root_event_ids IS NOT NULL THEN
      ARRAY(SELECT element FROM UNNEST(root_event_ids.list))
    ELSE NULL
  END AS root_event_ids,

  CAST(event_count AS INT64) AS event_count,
  source_synchronizer,
  target_synchronizer,
  unassign_id,
  submitter,
  CAST(reassignment_counter AS INT64) AS reassignment_counter,

  CASE WHEN trace_context IS NOT NULL THEN SAFE.PARSE_JSON(trace_context) ELSE NULL END AS trace_context,
  CASE WHEN update_data IS NOT NULL THEN SAFE.PARSE_JSON(update_data) ELSE NULL END AS update_data,

  CAST(year AS INT64) AS year,
  CAST(month AS INT64) AS month,
  CAST(day AS INT64) AS day

FROM `${PROJECT_ID}.raw.updates`;
