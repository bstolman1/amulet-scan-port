-- ============================================================================
-- BigQuery: Exercised Choices Parsing
-- ============================================================================
-- Creates parsed views for exercised events (choices executed on contracts)
-- ============================================================================

-- ============================================================================
-- ALL EXERCISED EVENTS - Base view for all choices
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_exercised_events` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  -- Choice metadata
  choice,
  consuming,
  interface_id,
  -- Parties
  acting_parties,
  witness_parties,
  -- Child events (for tree traversal)
  child_event_ids,
  -- Choice argument and result
  payload AS choice_argument,
  exercise_result,
  -- Raw event for full details
  raw_event
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised';


-- ============================================================================
-- CHOICE FREQUENCY ANALYSIS
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.choice_frequency` AS
SELECT
  choice,
  template_id,
  consuming,
  COUNT(*) AS exercise_count,
  COUNT(DISTINCT contract_id) AS unique_contracts,
  MIN(effective_at) AS first_exercise,
  MAX(effective_at) AS last_exercise
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
GROUP BY 1, 2, 3
ORDER BY exercise_count DESC;


-- ============================================================================
-- DSO RULES CHOICES - Governance actions exercised
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.dso_rules_choices` AS
SELECT
  event_id,
  update_id,
  contract_id,
  choice,
  consuming,
  effective_at,
  migration_id,
  acting_parties,
  -- Parse common choice arguments
  JSON_VALUE(payload, '$.action.tag') AS action_category,
  JSON_VALUE(payload, '$.action.value.tag') AS action_type,
  payload AS choice_argument,
  exercise_result
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND (template_id LIKE '%DsoRules:DsoRules' OR template_id LIKE '%:DsoRules');


-- ============================================================================
-- AMULET RULES CHOICES - Transfer and fee operations
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.amulet_rules_choices` AS
SELECT
  event_id,
  update_id,
  contract_id,
  choice,
  consuming,
  effective_at,
  migration_id,
  acting_parties,
  payload AS choice_argument,
  exercise_result
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND (template_id LIKE '%AmuletRules:AmuletRules' OR template_id LIKE '%:AmuletRules');


-- ============================================================================
-- TRANSFER OPERATIONS - Extract transfer details from exercised choices
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.transfer_operations` AS
SELECT
  event_id,
  update_id,
  contract_id,
  choice,
  effective_at,
  migration_id,
  -- Parse transfer-specific arguments
  JSON_VALUE(payload, '$.sender') AS sender_party,
  JSON_VALUE(payload, '$.receiver') AS receiver_party,
  CAST(JSON_VALUE(payload, '$.amount') AS NUMERIC) AS transfer_amount,
  -- Parse result if available
  JSON_VALUE(exercise_result, '$.summary.inputAmuletAmount') AS input_amount,
  JSON_VALUE(exercise_result, '$.summary.outputAmuletAmount') AS output_amount,
  JSON_VALUE(exercise_result, '$.summary.senderChangeFee') AS sender_fee,
  payload,
  exercise_result
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND choice IN ('AmuletRules_Transfer', 'Transfer', 'AmuletRules_BuyMemberTraffic');


-- ============================================================================
-- MINING ROUND CHOICES
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.mining_round_choices` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  choice,
  effective_at,
  migration_id,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  payload,
  exercise_result
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND template_id LIKE '%Round:%';


-- ============================================================================
-- VOTE REQUEST CHOICES - Voting actions
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.vote_request_choices` AS
SELECT
  event_id,
  update_id,
  contract_id,
  choice,  -- e.g., 'DsoRules_CastVote', 'DsoRules_CloseVoteRequest'
  effective_at,
  migration_id,
  acting_parties,
  -- Parse vote details from argument
  JSON_VALUE(payload, '$.vote.accept') AS vote_accept,
  JSON_VALUE(payload, '$.vote.reason.body') AS vote_reason,
  payload,
  exercise_result
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND (choice LIKE '%Vote%' OR choice LIKE '%DsoRules_%');


-- ============================================================================
-- DAILY CHOICE ACTIVITY
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.daily_choice_activity` AS
SELECT
  DATE(effective_at) AS activity_date,
  choice,
  COUNT(*) AS exercise_count,
  COUNT(DISTINCT contract_id) AS unique_contracts,
  SUM(CASE WHEN consuming THEN 1 ELSE 0 END) AS consuming_count
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
GROUP BY 1, 2
ORDER BY activity_date DESC, exercise_count DESC;


-- ============================================================================
-- INTERFACE EXERCISES - Choices exercised via interfaces
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.interface_exercises` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  interface_id,
  choice,
  consuming,
  effective_at,
  acting_parties,
  payload,
  exercise_result
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND interface_id IS NOT NULL;
