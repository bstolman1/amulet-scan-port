-- Parse Update Data
-- Creates parsed views for transaction and reassignment updates

-- Transactions
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.parsed_transactions` AS
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
  root_event_ids,
  JSON_VALUE(trace_context, '$.traceId') AS trace_id,
  JSON_VALUE(trace_context, '$.spanId') AS span_id,
  update_data
FROM `${PROJECT_ID}.canton_ledger.updates_raw`
WHERE update_type = 'transaction';


-- Reassignments
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.parsed_reassignments` AS
SELECT
  update_id,
  update_type,
  migration_id,
  synchronizer_id,
  kind,
  record_time,
  effective_at,
  timestamp,
  source_synchronizer,
  target_synchronizer,
  unassign_id,
  submitter,
  reassignment_counter,
  event_count,
  root_event_ids,
  update_data
FROM `${PROJECT_ID}.canton_ledger.updates_raw`
WHERE update_type = 'reassignment';


-- Updates Summary
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.updates_summary` AS
SELECT
  DATE(record_time) AS record_date,
  update_type,
  migration_id,
  COUNT(*) AS update_count,
  SUM(event_count) AS total_events,
  MIN(record_time) AS first_update,
  MAX(record_time) AS last_update
FROM `${PROJECT_ID}.canton_ledger.updates_raw`
GROUP BY 1, 2, 3
ORDER BY record_date DESC, update_type;


-- Daily Activity
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.daily_activity` AS
SELECT
  DATE(record_time) AS activity_date,
  COUNT(DISTINCT update_id) AS transaction_count,
  SUM(event_count) AS total_events,
  COUNT(DISTINCT synchronizer_id) AS active_synchronizers,
  MIN(record_time) AS first_tx_time,
  MAX(record_time) AS last_tx_time
FROM `${PROJECT_ID}.canton_ledger.updates_raw`
WHERE update_type = 'transaction'
GROUP BY 1
ORDER BY activity_date DESC;
