-- SILVER LAYER: Exercised Choices (Actions/Transactions)
-- Fully parsed choice executions with typed columns

-- All Exercised Choices
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.exercised_choices`
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
  CAST(choice AS STRING) AS choice,
  CAST(consuming AS BOOL) AS is_consuming,
  CAST(interface_id AS STRING) AS interface_id,
  ARRAY(
    SELECT CAST(p AS STRING)
    FROM UNNEST(acting_parties) AS p
  ) AS acting_parties,
  ARRAY(
    SELECT CAST(p AS STRING)
    FROM UNNEST(witness_parties) AS p
  ) AS witness_parties,
  ARRAY(
    SELECT CAST(c AS STRING)
    FROM UNNEST(child_event_ids) AS c
  ) AS child_event_ids,
  ARRAY_LENGTH(child_event_ids) AS child_event_count,
  payload AS choice_argument_json,
  exercise_result AS exercise_result_json
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE event_type = 'exercised';


-- Transfer Operations
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.transfer_operations`
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
  CAST(JSON_VALUE(payload, '$.transfer.sender') AS STRING) AS sender_party,
  CAST(JSON_VALUE(payload, '$.transfer.receivers[0].party') AS STRING) AS receiver_party,
  CAST(JSON_VALUE(exercise_result, '$.summary.inputAmuletAmount') AS NUMERIC) AS input_amount,
  CAST(JSON_VALUE(exercise_result, '$.summary.balanceChanges[0].changeToInitialAmountAsOfRoundZero') AS NUMERIC) AS amount_transferred,
  CAST(JSON_VALUE(exercise_result, '$.summary.senderChangeFee') AS NUMERIC) AS sender_fee,
  CAST(JSON_VALUE(exercise_result, '$.summary.amuletPrice') AS NUMERIC) AS amulet_price_at_transfer,
  CAST(JSON_VALUE(exercise_result, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.provider') AS STRING) AS provider_party,
  exercise_result AS full_result_json
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE event_type = 'exercised'
  AND choice IN ('AmuletRules_Transfer', 'Transfer');


-- Traffic Purchases
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.traffic_purchases`
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
  CAST(JSON_VALUE(exercise_result, '$.summary.inputAmuletAmount') AS NUMERIC) AS amulet_paid,
  CAST(JSON_VALUE(exercise_result, '$.round.number') AS INT64) AS round_number
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE event_type = 'exercised'
  AND choice LIKE '%BuyMemberTraffic%';


-- Governance Actions
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.governance_actions`
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
  CAST(JSON_VALUE(payload, '$.action.tag') AS STRING) AS action_category,
  CAST(JSON_VALUE(payload, '$.action.value.tag') AS STRING) AS action_type,
  CAST(JSON_VALUE(payload, '$.vote.accept') AS BOOL) AS vote_accepted,
  CAST(JSON_VALUE(payload, '$.vote.reason.body') AS STRING) AS vote_reason,
  ARRAY(
    SELECT CAST(p AS STRING)
    FROM UNNEST(acting_parties) AS p
  ) AS acting_parties,
  exercise_result AS result_json
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE event_type = 'exercised'
  AND (template_id LIKE '%:DsoRules' OR choice LIKE 'DsoRules_%');


-- Reward Claims
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.reward_claims`
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
  CASE
    WHEN template_id LIKE '%:AppRewardCoupon' THEN 'app'
    WHEN template_id LIKE '%:SvRewardCoupon' THEN 'sv'
    WHEN template_id LIKE '%:ValidatorRewardCoupon' THEN 'validator'
    ELSE 'unknown'
  END AS reward_type,
  ARRAY(
    SELECT CAST(p AS STRING)
    FROM UNNEST(acting_parties) AS p
  ) AS claimant_parties,
  exercise_result AS result_json
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE event_type = 'exercised'
  AND (
    choice LIKE '%Claim%'
    OR choice LIKE '%Collect%'
    OR template_id LIKE '%RewardCoupon'
  );


-- Round Operations
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.round_operations`
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
  COALESCE(
    CAST(JSON_VALUE(payload, '$.round.number') AS INT64),
    CAST(JSON_VALUE(exercise_result, '$.round.number') AS INT64)
  ) AS round_number,
  exercise_result AS result_json
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE event_type = 'exercised'
  AND template_id LIKE '%Round:%';


-- Daily Choice Stats
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.daily_choice_stats`
PARTITION BY choice_date
AS
SELECT
  DATE(effective_at) AS choice_date,
  choice,
  COUNT(*) AS execution_count,
  COUNTIF(consuming = TRUE) AS consuming_count,
  COUNT(DISTINCT contract_id) AS unique_contracts,
  COUNT(DISTINCT update_id) AS unique_transactions,
  SUM(ARRAY_LENGTH(child_event_ids)) AS total_child_events,
  AVG(ARRAY_LENGTH(child_event_ids)) AS avg_child_events_per_choice
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE event_type = 'exercised'
GROUP BY 1, 2;


-- Template-Choice Map
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.template_choice_map`
AS
SELECT
  template_id,
  choice,
  COUNT(*) AS total_executions,
  COUNTIF(consuming = TRUE) AS consuming_executions,
  MIN(effective_at) AS first_execution,
  MAX(effective_at) AS last_execution,
  COUNT(DISTINCT DATE(effective_at)) AS active_days
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE event_type = 'exercised'
GROUP BY 1, 2
ORDER BY total_executions DESC;
