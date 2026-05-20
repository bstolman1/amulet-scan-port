-- Verify data transformation
-- Run after 02-transform-raw-data.sql to confirm parsing succeeded

-- 1. Row counts match
SELECT
  'raw' AS table_name, COUNT(*) AS row_count
FROM `${PROJECT_ID}.raw.events`
UNION ALL
SELECT
  'parsed' AS table_name, COUNT(*) AS row_count
FROM `${PROJECT_ID}.transformed.events_parsed`;

-- 2. Timestamps parsed correctly
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(effective_at IS NULL) AS null_effective_at,
  COUNTIF(recorded_at IS NULL) AS null_recorded_at,
  COUNTIF(timestamp IS NULL) AS null_timestamp,
  COUNTIF(created_at_ts IS NULL) AS null_created_at_ts,
  MIN(timestamp) AS earliest_timestamp,
  MAX(timestamp) AS latest_timestamp
FROM `${PROJECT_ID}.transformed.events_parsed`;

-- 3. Data types are correct
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(migration_id IS NOT NULL AND CAST(migration_id AS STRING) != 'NULL') AS valid_migration_ids,
  COUNTIF(consuming IS NOT NULL) AS valid_consuming_flags,
  COUNTIF(year IS NOT NULL) AS valid_years,
  COUNTIF(month BETWEEN 1 AND 12) AS valid_months,
  COUNTIF(day BETWEEN 1 AND 31) AS valid_days
FROM `${PROJECT_ID}.transformed.events_parsed`;

-- 4. Array extraction worked
SELECT
  'Arrays extracted' AS verification,
  COUNT(*) AS total_events,
  COUNTIF(signatories IS NOT NULL) AS events_with_signatories,
  COUNTIF(observers IS NOT NULL) AS events_with_observers,
  COUNTIF(acting_parties IS NOT NULL) AS events_with_acting_parties,
  COUNTIF(child_event_ids IS NOT NULL) AS events_with_children
FROM `${PROJECT_ID}.transformed.events_parsed`;

-- 5. JSON fields parsed correctly
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(payload IS NOT NULL) AS valid_payload_json,
  COUNTIF(raw_event IS NOT NULL) AS valid_raw_event_json,
  COUNTIF(exercise_result IS NOT NULL) AS valid_exercise_result_json
FROM `${PROJECT_ID}.transformed.events_parsed`;

-- 6. Compare specific records (raw vs parsed)
WITH sample_comparison AS (
  SELECT
    r.event_id,
    r.effective_at AS raw_effective_at,
    p.effective_at AS parsed_effective_at,
    r.migration_id AS raw_migration_id,
    p.migration_id AS parsed_migration_id,
    r.consuming AS raw_consuming,
    p.consuming AS parsed_consuming,
    ARRAY_LENGTH(p.signatories) AS signatories_count
  FROM `${PROJECT_ID}.raw.events` r
  JOIN `${PROJECT_ID}.transformed.events_parsed` p
  ON r.event_id = p.event_id
  LIMIT 10
)
SELECT * FROM sample_comparison;

-- 7. Check for parsing errors or data loss
SELECT
  'Potential Issues' AS check_type,
  COUNTIF(p.timestamp IS NULL AND r.timestamp IS NOT NULL) AS failed_timestamp_parsing,
  COUNTIF(p.payload IS NULL AND r.payload IS NOT NULL AND r.payload != '') AS failed_payload_parsing,
  COUNTIF(p.signatories IS NULL AND r.signatories IS NOT NULL) AS failed_signatory_extraction
FROM `${PROJECT_ID}.raw.events` r
LEFT JOIN `${PROJECT_ID}.transformed.events_parsed` p
ON r.event_id = p.event_id;

-- 8. Updates transform verification
SELECT
  'raw_updates' AS table_name, COUNT(*) AS row_count
FROM `${PROJECT_ID}.raw.updates`
UNION ALL
SELECT
  'parsed_updates' AS table_name, COUNT(*) AS row_count
FROM `${PROJECT_ID}.transformed.updates_parsed`;

-- 9. Updates timestamps and types
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(record_time IS NULL) AS null_record_time,
  COUNTIF(effective_at IS NULL) AS null_effective_at,
  COUNTIF(update_type IS NOT NULL) AS valid_update_types,
  COUNTIF(event_count IS NOT NULL) AS valid_event_counts,
  MIN(record_time) AS earliest_update,
  MAX(record_time) AS latest_update
FROM `${PROJECT_ID}.transformed.updates_parsed`;
