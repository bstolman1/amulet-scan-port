-- ============================================================================
-- BigQuery: Parse Update Data
-- ============================================================================
-- Creates parsed views for transaction and reassignment updates
-- ============================================================================

-- ============================================================================
-- TRANSACTIONS - All transaction updates with parsed metadata
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_transactions` AS
SELECT
  update_id,
  update_type,
  migration_id,
  synchronizer_id,
  workflow_id,
  command_id,
  offset,
  record_time,
  effective_at,
  timestamp,
  event_count,
  -- Parse root event IDs (array)
  root_event_ids,
  -- Parse trace context if needed
  JSON_VALUE(trace_context, '$.traceId') AS trace_id,
  JSON_VALUE(trace_context, '$.spanId') AS span_id,
  -- Raw update data for complex queries
  update_data
FROM `YOUR_PROJECT_ID.canton_ledger.updates_raw`
WHERE update_type = 'transaction';


-- ============================================================================
-- REASSIGNMENTS - All reassignment updates with parsed metadata
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_reassignments` AS
SELECT
  update_id,
  update_type,
  migration_id,
  synchronizer_id,
  kind,  -- 'assign' or 'unassign'
  record_time,
  effective_at,
  timestamp,
  -- Reassignment-specific fields
  source_synchronizer,
  target_synchronizer,
  unassign_id,
  submitter,
  reassignment_counter,
  -- Event tracking
  event_count,
  root_event_ids,
  update_data
FROM `YOUR_PROJECT_ID.canton_ledger.updates_raw`
WHERE update_type = 'reassignment';


-- ============================================================================
-- UPDATES SUMMARY - Aggregated view of all updates
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.updates_summary` AS
SELECT
  DATE(record_time) AS record_date,
  update_type,
  migration_id,
  COUNT(*) AS update_count,
  SUM(event_count) AS total_events,
  MIN(record_time) AS first_update,
  MAX(record_time) AS last_update
FROM `YOUR_PROJECT_ID.canton_ledger.updates_raw`
GROUP BY 1, 2, 3
ORDER BY record_date DESC, update_type;


-- ============================================================================
-- DAILY ACTIVITY - Daily transaction activity metrics
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.daily_activity` AS
SELECT
  DATE(record_time) AS activity_date,
  COUNT(DISTINCT update_id) AS transaction_count,
  SUM(event_count) AS total_events,
  COUNT(DISTINCT synchronizer_id) AS active_synchronizers,
  MIN(record_time) AS first_tx_time,
  MAX(record_time) AS last_tx_time
FROM `YOUR_PROJECT_ID.canton_ledger.updates_raw`
WHERE update_type = 'transaction'
GROUP BY 1
ORDER BY activity_date DESC;
