-- ============================================================================
-- SILVER LAYER: Exercised Choices (Actions/Transactions)
-- ============================================================================
-- Fully parsed choice executions with typed columns
-- ============================================================================

-- ============================================================================
-- ALL EXERCISED CHOICES - Base table for all choice executions
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.exercised_choices`
PARTITION BY DATE(effective_at)
CLUSTER BY choice, template_id
AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  migration_id,
  synchronizer_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  
  -- Choice metadata
  CAST(choice AS STRING) AS choice,
  CAST(consuming AS BOOL) AS is_consuming,
  CAST(interface_id AS STRING) AS interface_id,
  
  -- Parties
  ARRAY(
    SELECT CAST(p AS STRING)
    FROM UNNEST(acting_parties) AS p
  ) AS acting_parties,
  ARRAY(
    SELECT CAST(p AS STRING)
    FROM UNNEST(witness_parties) AS p
  ) AS witness_parties,
  
  -- Child events for tree traversal
  ARRAY(
    SELECT CAST(c AS STRING)
    FROM UNNEST(child_event_ids) AS c
  ) AS child_event_ids,
  ARRAY_LENGTH(child_event_ids) AS child_event_count,
  
  -- Choice argument (parsed for common fields)
  payload AS choice_argument_json,
  
  -- Exercise result (parsed for common fields)
  exercise_result AS exercise_result_json

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised';


-- ============================================================================
-- TRANSFER OPERATIONS - AmuletRules transfer choices
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.transfer_operations`
PARTITION BY DATE(effective_at)
CLUSTER BY sender_party, receiver_party
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  CAST(choice AS STRING) AS choice,
  
  -- Transfer parties
  CAST(JSON_VALUE(payload, '$.transfer.sender') AS STRING) AS sender_party,
  CAST(JSON_VALUE(payload, '$.transfer.receivers[0].party') AS STRING) AS receiver_party,
  
  -- Transfer amounts from result
  CAST(JSON_VALUE(exercise_result, '$.summary.inputAmuletAmount') AS NUMERIC) AS input_amount,
  CAST(JSON_VALUE(exercise_result, '$.summary.balanceChanges[0].changeToInitialAmountAsOfRoundZero') AS NUMERIC) AS amount_transferred,
  CAST(JSON_VALUE(exercise_result, '$.summary.senderChangeFee') AS NUMERIC) AS sender_fee,
  CAST(JSON_VALUE(exercise_result, '$.summary.amuletPrice') AS NUMERIC) AS amulet_price_at_transfer,
  CAST(JSON_VALUE(exercise_result, '$.round.number') AS INT64) AS round_number,
  
  -- Provider info
  CAST(JSON_VALUE(payload, '$.provider') AS STRING) AS provider_party,
  
  exercise_result AS full_result_json

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND choice IN ('AmuletRules_Transfer', 'Transfer');


-- ============================================================================
-- TRAFFIC PURCHASES - BuyMemberTraffic operations
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.traffic_purchases`
PARTITION BY DATE(effective_at)
CLUSTER BY buyer_party
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  
  CAST(JSON_VALUE(payload, '$.buyer') AS STRING) AS buyer_party,
  CAST(JSON_VALUE(payload, '$.memberId') AS STRING) AS member_id,
  CAST(JSON_VALUE(payload, '$.synchronizerId') AS STRING) AS synchronizer_id,
  CAST(JSON_VALUE(payload, '$.trafficAmount') AS INT64) AS traffic_amount,
  
  -- Result
  CAST(JSON_VALUE(exercise_result, '$.summary.inputAmuletAmount') AS NUMERIC) AS amulet_paid,
  CAST(JSON_VALUE(exercise_result, '$.round.number') AS INT64) AS round_number

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND choice LIKE '%BuyMemberTraffic%';


-- ============================================================================
-- GOVERNANCE ACTIONS - DSO Rules choices
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.governance_actions`
PARTITION BY DATE(effective_at)
CLUSTER BY choice, action_type
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  CAST(choice AS STRING) AS choice,
  
  -- Action details from payload
  CAST(JSON_VALUE(payload, '$.action.tag') AS STRING) AS action_category,
  CAST(JSON_VALUE(payload, '$.action.value.tag') AS STRING) AS action_type,
  
  -- Vote details if present
  CAST(JSON_VALUE(payload, '$.vote.accept') AS BOOL) AS vote_accepted,
  CAST(JSON_VALUE(payload, '$.vote.reason.body') AS STRING) AS vote_reason,
  
  -- Acting party
  ARRAY(
    SELECT CAST(p AS STRING)
    FROM UNNEST(acting_parties) AS p
  ) AS acting_parties,
  
  -- Result
  exercise_result AS result_json

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND (template_id LIKE '%:DsoRules' OR choice LIKE 'DsoRules_%');


-- ============================================================================
-- REWARD CLAIMS - Reward collection operations
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.reward_claims`
PARTITION BY DATE(effective_at)
CLUSTER BY choice
AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  CAST(choice AS STRING) AS choice,
  
  -- Determine reward type from template
  CASE
    WHEN template_id LIKE '%:AppRewardCoupon' THEN 'app'
    WHEN template_id LIKE '%:SvRewardCoupon' THEN 'sv'
    WHEN template_id LIKE '%:ValidatorRewardCoupon' THEN 'validator'
    ELSE 'unknown'
  END AS reward_type,
  
  -- Acting parties (claimants)
  ARRAY(
    SELECT CAST(p AS STRING)
    FROM UNNEST(acting_parties) AS p
  ) AS claimant_parties,
  
  exercise_result AS result_json

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND (
    choice LIKE '%Claim%'
    OR choice LIKE '%Collect%'
    OR template_id LIKE '%RewardCoupon'
  );


-- ============================================================================
-- ROUND OPERATIONS - Mining round lifecycle choices
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.round_operations`
PARTITION BY DATE(effective_at)
CLUSTER BY choice, round_number
AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  CAST(choice AS STRING) AS choice,
  CAST(consuming AS BOOL) AS is_consuming,
  
  -- Round number from various sources
  COALESCE(
    CAST(JSON_VALUE(payload, '$.round.number') AS INT64),
    CAST(JSON_VALUE(exercise_result, '$.round.number') AS INT64)
  ) AS round_number,
  
  exercise_result AS result_json

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND template_id LIKE '%Round:%';


-- ============================================================================
-- CHOICE STATISTICS - Daily choice execution metrics
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.daily_choice_stats`
PARTITION BY choice_date
AS
SELECT
  DATE(effective_at) AS choice_date,
  choice,
  
  COUNT(*) AS execution_count,
  COUNTIF(consuming = TRUE) AS consuming_count,
  COUNT(DISTINCT contract_id) AS unique_contracts,
  COUNT(DISTINCT update_id) AS unique_transactions,
  
  -- Child event metrics
  SUM(ARRAY_LENGTH(child_event_ids)) AS total_child_events,
  AVG(ARRAY_LENGTH(child_event_ids)) AS avg_child_events_per_choice

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
GROUP BY 1, 2;


-- ============================================================================
-- CHOICE BY TEMPLATE - Template-choice relationship analysis
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.template_choice_map`
AS
SELECT
  template_id,
  choice,
  
  COUNT(*) AS total_executions,
  COUNTIF(consuming = TRUE) AS consuming_executions,
  MIN(effective_at) AS first_execution,
  MAX(effective_at) AS last_execution,
  COUNT(DISTINCT DATE(effective_at)) AS active_days

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'exercised'
GROUP BY 1, 2
ORDER BY total_executions DESC;
