-- Exercised Choices Parsing
-- Views for exercised events (choices executed on contracts)

-- All Exercised Events
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.parsed_exercised_events` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  choice,
  consuming,
  interface_id,
  acting_parties,
  witness_parties,
  child_event_ids,
  payload AS choice_argument,
  exercise_result,
  raw_event
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE event_type = 'exercised';


-- Choice Frequency Analysis
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.choice_frequency` AS
SELECT
  choice,
  template_id,
  consuming,
  COUNT(*) AS exercise_count,
  COUNT(DISTINCT contract_id) AS unique_contracts,
  MIN(effective_at) AS first_exercise,
  MAX(effective_at) AS last_exercise
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE event_type = 'exercised'
GROUP BY 1, 2, 3
ORDER BY exercise_count DESC;


-- DSO Rules Choices
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.dso_rules_choices` AS
SELECT
  event_id,
  update_id,
  contract_id,
  choice,
  consuming,
  effective_at,
  migration_id,
  acting_parties,
  JSON_VALUE(payload, '$.action.tag') AS action_category,
  JSON_VALUE(payload, '$.action.value.tag') AS action_type,
  payload AS choice_argument,
  exercise_result
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND (template_id LIKE '%DsoRules:DsoRules' OR template_id LIKE '%:DsoRules');


-- Amulet Rules Choices
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.amulet_rules_choices` AS
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
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND (template_id LIKE '%AmuletRules:AmuletRules' OR template_id LIKE '%:AmuletRules');


-- Transfer Operations
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.transfer_operations` AS
SELECT
  event_id,
  update_id,
  contract_id,
  choice,
  effective_at,
  migration_id,
  JSON_VALUE(payload, '$.sender') AS sender_party,
  JSON_VALUE(payload, '$.receiver') AS receiver_party,
  CAST(JSON_VALUE(payload, '$.amount') AS NUMERIC) AS transfer_amount,
  JSON_VALUE(exercise_result, '$.summary.inputAmuletAmount') AS input_amount,
  JSON_VALUE(exercise_result, '$.summary.outputAmuletAmount') AS output_amount,
  JSON_VALUE(exercise_result, '$.summary.senderChangeFee') AS sender_fee,
  payload,
  exercise_result
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND choice IN ('AmuletRules_Transfer', 'Transfer', 'AmuletRules_BuyMemberTraffic');


-- Mining Round Choices
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.mining_round_choices` AS
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
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND template_id LIKE '%Round:%';


-- Vote Request Choices
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.vote_request_choices` AS
SELECT
  event_id,
  update_id,
  contract_id,
  choice,
  effective_at,
  migration_id,
  acting_parties,
  JSON_VALUE(payload, '$.vote.accept') AS vote_accept,
  JSON_VALUE(payload, '$.vote.reason.body') AS vote_reason,
  payload,
  exercise_result
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND (choice LIKE '%Vote%' OR choice LIKE '%DsoRules_%');


-- Daily Choice Activity
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.daily_choice_activity` AS
SELECT
  DATE(effective_at) AS activity_date,
  choice,
  COUNT(*) AS exercise_count,
  COUNT(DISTINCT contract_id) AS unique_contracts,
  SUM(CASE WHEN consuming THEN 1 ELSE 0 END) AS consuming_count
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE event_type = 'exercised'
GROUP BY 1, 2
ORDER BY activity_date DESC, exercise_count DESC;


-- Interface Exercises
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.interface_exercises` AS
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
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE event_type = 'exercised'
  AND interface_id IS NOT NULL;
