-- Daily incremental refresh for transformed.events_parsed
-- Schedule: daily at 03:00 UTC
-- Approach: INSERT NOT EXISTS with 1-day lookback for late arrivals
-- Cost: ~$0.07/day (scans only new raw partitions + dedup check on target partitions)

BEGIN
  DECLARE latest_loaded DATE;
  DECLARE load_date DATE;
  DECLARE today DATE DEFAULT CURRENT_DATE();

  -- Free metadata query: find latest partition in events_parsed
  SET latest_loaded = (
    SELECT PARSE_DATE('%Y%m%d', MAX(partition_id))
    FROM `governence-483517.transformed.INFORMATION_SCHEMA.PARTITIONS`
    WHERE table_name = 'events_parsed'
      AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
  );

  -- Start from 1 day before latest (lookback for late-arriving data)
  SET load_date = DATE_SUB(latest_loaded, INTERVAL 1 DAY);

  -- Process one day at a time, up to yesterday (today may be incomplete)
  WHILE load_date < today DO

    INSERT INTO `governence-483517.transformed.events_parsed`
    SELECT S.*
    FROM (
      SELECT
        event_id, update_id, contract_id, template_id, package_name,
        event_type, event_type_original, synchronizer_id,
        CAST(migration_id AS INT64) AS migration_id,
        choice, interface_id, CAST(consuming AS BOOL) AS consuming,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', effective_at) AS effective_at,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', recorded_at) AS recorded_at,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', timestamp) AS timestamp,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at_ts) AS created_at_ts,
        CASE WHEN signatories IS NOT NULL THEN ARRAY(SELECT element FROM UNNEST(signatories.list)) ELSE NULL END AS signatories,
        CASE WHEN observers IS NOT NULL THEN ARRAY(SELECT element FROM UNNEST(observers.list)) ELSE NULL END AS observers,
        CASE WHEN acting_parties IS NOT NULL THEN ARRAY(SELECT element FROM UNNEST(acting_parties.list)) ELSE NULL END AS acting_parties,
        CASE WHEN witness_parties IS NOT NULL THEN ARRAY(SELECT element FROM UNNEST(witness_parties.list)) ELSE NULL END AS witness_parties,
        CASE WHEN child_event_ids IS NOT NULL THEN ARRAY(SELECT element FROM UNNEST(child_event_ids.list)) ELSE NULL END AS child_event_ids,
        CAST(reassignment_counter AS INT64) AS reassignment_counter,
        source_synchronizer, target_synchronizer, unassign_id, submitter,
        CASE WHEN payload IS NOT NULL THEN SAFE.PARSE_JSON(payload) ELSE NULL END AS payload,
        CASE WHEN contract_key IS NOT NULL THEN SAFE.PARSE_JSON(contract_key) ELSE NULL END AS contract_key,
        CASE WHEN exercise_result IS NOT NULL THEN SAFE.PARSE_JSON(exercise_result) ELSE NULL END AS exercise_result,
        CASE WHEN raw_event IS NOT NULL THEN SAFE.PARSE_JSON(raw_event) ELSE NULL END AS raw_event,
        CASE WHEN trace_context IS NOT NULL THEN SAFE.PARSE_JSON(trace_context) ELSE NULL END AS trace_context,
        CAST(year AS INT64) AS year, CAST(month AS INT64) AS month, CAST(day AS INT64) AS day
      FROM `governence-483517.raw.events`
      WHERE year = EXTRACT(YEAR FROM load_date)
        AND month = EXTRACT(MONTH FROM load_date)
        AND day = EXTRACT(DAY FROM load_date)
    ) S
    WHERE NOT EXISTS (
      SELECT 1
      FROM `governence-483517.transformed.events_parsed` T
      WHERE T.event_id = S.event_id
        AND DATE(T.effective_at) = load_date
    );

    SET load_date = DATE_ADD(load_date, INTERVAL 1 DAY);
  END WHILE;
END;
