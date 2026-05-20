-- Daily incremental refresh for transformed.updates_parsed
-- Schedule: daily at 03:00 UTC
-- Approach: INSERT NOT EXISTS with 1-day lookback for late arrivals
-- Cost: ~$0.04/day (scans only new raw partitions + dedup check on target partitions)

BEGIN
  DECLARE latest_loaded DATE;
  DECLARE load_date DATE;
  DECLARE today DATE DEFAULT CURRENT_DATE();

  -- Free metadata query: find latest partition in updates_parsed
  SET latest_loaded = (
    SELECT PARSE_DATE('%Y%m%d', MAX(partition_id))
    FROM `governence-483517.transformed.INFORMATION_SCHEMA.PARTITIONS`
    WHERE table_name = 'updates_parsed'
      AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
  );

  -- Start from 1 day before latest (lookback for late-arriving data)
  SET load_date = DATE_SUB(latest_loaded, INTERVAL 1 DAY);

  -- Process one day at a time, up to yesterday (today may be incomplete)
  WHILE load_date < today DO

    INSERT INTO `governence-483517.transformed.updates_parsed`
    SELECT S.*
    FROM (
      SELECT
        update_id, update_type,
        CAST(migration_id AS INT64) AS migration_id,
        synchronizer_id, workflow_id, command_id,
        CAST(offset AS INT64) AS offset,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', record_time) AS record_time,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', effective_at) AS effective_at,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', recorded_at) AS recorded_at,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', timestamp) AS timestamp,
        kind,
        CASE WHEN root_event_ids IS NOT NULL THEN ARRAY(SELECT element FROM UNNEST(root_event_ids.list)) ELSE NULL END AS root_event_ids,
        CAST(event_count AS INT64) AS event_count,
        source_synchronizer, target_synchronizer, unassign_id, submitter,
        CAST(reassignment_counter AS INT64) AS reassignment_counter,
        CASE WHEN trace_context IS NOT NULL THEN SAFE.PARSE_JSON(trace_context) ELSE NULL END AS trace_context,
        CASE WHEN update_data IS NOT NULL THEN SAFE.PARSE_JSON(update_data) ELSE NULL END AS update_data,
        CAST(year AS INT64) AS year, CAST(month AS INT64) AS month, CAST(day AS INT64) AS day
      FROM `governence-483517.raw.updates`
      WHERE year = EXTRACT(YEAR FROM load_date)
        AND month = EXTRACT(MONTH FROM load_date)
        AND day = EXTRACT(DAY FROM load_date)
    ) S
    WHERE NOT EXISTS (
      SELECT 1
      FROM `governence-483517.transformed.updates_parsed` T
      WHERE T.update_id = S.update_id
        AND DATE(T.effective_at) = load_date
    );

    SET load_date = DATE_ADD(load_date, INTERVAL 1 DAY);
  END WHILE;
END;
