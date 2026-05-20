-- SILVER LAYER: Rewards Tables
-- Fully parsed reward coupons with typed columns

-- App Reward Coupons
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.app_reward_coupons`
PARTITION BY DATE(effective_at)
CLUSTER BY provider_party, round_number
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  event_type,
  CAST(JSON_VALUE(payload, '$.dso') AS STRING) AS dso_party,
  CAST(JSON_VALUE(payload, '$.provider') AS STRING) AS provider_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.amount') AS NUMERIC) AS reward_amount,
  CAST(JSON_VALUE(payload, '$.featured') AS BOOL) AS is_featured
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%:AppRewardCoupon';


-- SV Reward Coupons
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.sv_reward_coupons`
PARTITION BY DATE(effective_at)
CLUSTER BY sv_party, round_number
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  event_type,
  CAST(JSON_VALUE(payload, '$.dso') AS STRING) AS dso_party,
  CAST(JSON_VALUE(payload, '$.sv') AS STRING) AS sv_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.weight') AS INT64) AS sv_weight,
  CAST(JSON_VALUE(payload, '$.beneficiary') AS STRING) AS beneficiary_party
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%:SvRewardCoupon';


-- Validator Reward Coupons
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.validator_reward_coupons`
PARTITION BY DATE(effective_at)
CLUSTER BY validator_party, round_number
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  event_type,
  CAST(JSON_VALUE(payload, '$.dso') AS STRING) AS dso_party,
  CAST(JSON_VALUE(payload, '$.user') AS STRING) AS user_party,
  CAST(JSON_VALUE(payload, '$.validator') AS STRING) AS validator_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%:ValidatorRewardCoupon';


-- Unclaimed Rewards
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.unclaimed_rewards`
PARTITION BY DATE(effective_at)
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  event_type,
  CAST(JSON_VALUE(payload, '$.dso') AS STRING) AS dso_party,
  CAST(JSON_VALUE(payload, '$.amount') AS NUMERIC) AS reward_amount
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%:UnclaimedReward';


-- Unified Rewards (all types)
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.all_rewards`
PARTITION BY DATE(effective_at)
CLUSTER BY reward_type, round_number, recipient_party
AS
SELECT
  event_id, update_id, contract_id, migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at, event_type,
  'app' AS reward_type,
  CAST(JSON_VALUE(payload, '$.provider') AS STRING) AS recipient_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.amount') AS NUMERIC) AS reward_amount,
  CAST(NULL AS INT64) AS sv_weight,
  CAST(JSON_VALUE(payload, '$.featured') AS BOOL) AS is_featured,
  CAST(NULL AS STRING) AS user_party
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%:AppRewardCoupon'

UNION ALL

SELECT
  event_id, update_id, contract_id, migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at, event_type,
  'sv' AS reward_type,
  CAST(JSON_VALUE(payload, '$.sv') AS STRING) AS recipient_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(NULL AS NUMERIC) AS reward_amount,
  CAST(JSON_VALUE(payload, '$.weight') AS INT64) AS sv_weight,
  CAST(NULL AS BOOL) AS is_featured,
  CAST(NULL AS STRING) AS user_party
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%:SvRewardCoupon'

UNION ALL

SELECT
  event_id, update_id, contract_id, migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at, event_type,
  'validator' AS reward_type,
  CAST(JSON_VALUE(payload, '$.validator') AS STRING) AS recipient_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(NULL AS NUMERIC) AS reward_amount,
  CAST(NULL AS INT64) AS sv_weight,
  CAST(NULL AS BOOL) AS is_featured,
  CAST(JSON_VALUE(payload, '$.user') AS STRING) AS user_party
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%:ValidatorRewardCoupon';


-- Rewards by Round
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.rewards_by_round`
PARTITION BY round_date
CLUSTER BY round_number
AS
SELECT
  round_number,
  DATE(MIN(effective_at)) AS round_date,
  COUNTIF(reward_type = 'app' AND event_type = 'created') AS app_coupons_created,
  COUNTIF(reward_type = 'app' AND event_type = 'archived') AS app_coupons_claimed,
  SUM(CASE WHEN reward_type = 'app' AND event_type = 'created' THEN reward_amount ELSE 0 END) AS total_app_rewards,
  COUNTIF(reward_type = 'app' AND is_featured = TRUE) AS featured_app_count,
  COUNTIF(reward_type = 'sv' AND event_type = 'created') AS sv_coupons_created,
  COUNTIF(reward_type = 'sv' AND event_type = 'archived') AS sv_coupons_claimed,
  SUM(CASE WHEN reward_type = 'sv' THEN sv_weight ELSE 0 END) AS total_sv_weight,
  COUNTIF(reward_type = 'validator' AND event_type = 'created') AS validator_coupons_created,
  COUNTIF(reward_type = 'validator' AND event_type = 'archived') AS validator_coupons_claimed,
  COUNT(DISTINCT CASE WHEN event_type = 'created' THEN recipient_party END) AS unique_recipients
FROM `${PROJECT_ID}.canton_silver.all_rewards`
GROUP BY round_number;


-- Reward Leaderboard
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.reward_leaderboard`
CLUSTER BY reward_type, total_rewards_earned
AS
SELECT
  recipient_party,
  reward_type,
  COUNT(DISTINCT contract_id) AS total_coupons,
  COUNT(DISTINCT round_number) AS active_rounds,
  SUM(COALESCE(reward_amount, 0)) AS total_rewards_earned,
  AVG(COALESCE(reward_amount, 0)) AS avg_reward_per_coupon,
  MIN(round_number) AS first_active_round,
  MAX(round_number) AS last_active_round,
  MIN(effective_at) AS first_reward_time,
  MAX(effective_at) AS last_reward_time,
  COUNTIF(is_featured = TRUE) AS featured_count
FROM `${PROJECT_ID}.canton_silver.all_rewards`
WHERE event_type = 'created'
GROUP BY recipient_party, reward_type;


-- SV Weight History
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.sv_weight_history`
PARTITION BY DATE(effective_at)
CLUSTER BY sv_party, round_number
AS
SELECT
  sv_party,
  round_number,
  sv_weight,
  effective_at,
  contract_id,
  LAG(sv_weight) OVER (
    PARTITION BY sv_party
    ORDER BY round_number
  ) AS previous_weight,
  sv_weight - COALESCE(LAG(sv_weight) OVER (
    PARTITION BY sv_party
    ORDER BY round_number
  ), sv_weight) AS weight_change
FROM `${PROJECT_ID}.canton_silver.sv_reward_coupons`
WHERE event_type = 'created'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY sv_party, round_number
  ORDER BY effective_at DESC
) = 1;
