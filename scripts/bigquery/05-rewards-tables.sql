-- ============================================================================
-- BigQuery: Rewards Data Parsing
-- ============================================================================
-- Creates parsed tables for reward-related templates
-- ============================================================================

-- ============================================================================
-- REWARDS SUMMARY TABLE - Materialized for performance
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_ledger.rewards_summary` AS
WITH app_rewards AS (
  SELECT
    'app' AS reward_type,
    JSON_VALUE(payload, '$.provider') AS recipient_party,
    CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
    CAST(JSON_VALUE(payload, '$.amount') AS NUMERIC) AS amount,
    CAST(JSON_VALUE(payload, '$.featured') AS BOOL) AS is_featured,
    event_type,
    effective_at,
    contract_id
  FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
  WHERE template_id LIKE '%:AppRewardCoupon'
),
sv_rewards AS (
  SELECT
    'sv' AS reward_type,
    JSON_VALUE(payload, '$.sv') AS recipient_party,
    CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
    NULL AS amount,  -- SV rewards use weight, not direct amount
    FALSE AS is_featured,
    event_type,
    effective_at,
    contract_id
  FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
  WHERE template_id LIKE '%:SvRewardCoupon'
),
validator_rewards AS (
  SELECT
    'validator' AS reward_type,
    JSON_VALUE(payload, '$.validator') AS recipient_party,
    CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
    NULL AS amount,
    FALSE AS is_featured,
    event_type,
    effective_at,
    contract_id
  FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
  WHERE template_id LIKE '%:ValidatorRewardCoupon'
)
SELECT * FROM app_rewards
UNION ALL
SELECT * FROM sv_rewards
UNION ALL
SELECT * FROM validator_rewards;


-- ============================================================================
-- REWARDS BY ROUND - Aggregated rewards per round
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.rewards_by_round` AS
SELECT
  round_number,
  reward_type,
  event_type,
  COUNT(*) AS coupon_count,
  COUNT(DISTINCT recipient_party) AS unique_recipients,
  SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) AS total_amount,
  SUM(CASE WHEN is_featured THEN 1 ELSE 0 END) AS featured_count
FROM `YOUR_PROJECT_ID.canton_ledger.rewards_summary`
GROUP BY 1, 2, 3
ORDER BY round_number DESC, reward_type;


-- ============================================================================
-- REWARDS BY RECIPIENT - Top reward recipients
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.rewards_by_recipient` AS
SELECT
  recipient_party,
  reward_type,
  COUNT(*) AS total_coupons,
  COUNT(DISTINCT round_number) AS active_rounds,
  SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) AS total_amount,
  MIN(round_number) AS first_round,
  MAX(round_number) AS last_round,
  MIN(effective_at) AS first_reward_time,
  MAX(effective_at) AS last_reward_time
FROM `YOUR_PROJECT_ID.canton_ledger.rewards_summary`
WHERE event_type = 'created'
GROUP BY 1, 2
ORDER BY total_coupons DESC;


-- ============================================================================
-- SV WEIGHT HISTORY - Track SV weights over time from SvRewardCoupon
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.sv_weight_history` AS
SELECT
  JSON_VALUE(payload, '$.sv') AS sv_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.weight') AS INT64) AS sv_weight,
  effective_at,
  event_type,
  contract_id
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:SvRewardCoupon'
  AND event_type = 'created'
ORDER BY round_number DESC, sv_party;


-- ============================================================================
-- APP PROVIDER LEADERBOARD - Top app providers by rewards
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.app_provider_leaderboard` AS
SELECT
  JSON_VALUE(payload, '$.provider') AS provider_party,
  COUNT(*) AS reward_count,
  SUM(CAST(JSON_VALUE(payload, '$.amount') AS NUMERIC)) AS total_rewards,
  SUM(CASE WHEN CAST(JSON_VALUE(payload, '$.featured') AS BOOL) THEN 1 ELSE 0 END) AS featured_count,
  COUNT(DISTINCT CAST(JSON_VALUE(payload, '$.round.number') AS INT64)) AS active_rounds,
  MIN(effective_at) AS first_reward,
  MAX(effective_at) AS last_reward
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:AppRewardCoupon'
  AND event_type = 'created'
GROUP BY 1
ORDER BY total_rewards DESC;


-- ============================================================================
-- VALIDATOR PERFORMANCE - Validator reward tracking
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.validator_performance` AS
SELECT
  JSON_VALUE(payload, '$.validator') AS validator_party,
  JSON_VALUE(payload, '$.user') AS user_party,
  COUNT(*) AS reward_count,
  COUNT(DISTINCT CAST(JSON_VALUE(payload, '$.round.number') AS INT64)) AS active_rounds,
  MIN(CAST(JSON_VALUE(payload, '$.round.number') AS INT64)) AS first_round,
  MAX(CAST(JSON_VALUE(payload, '$.round.number') AS INT64)) AS last_round,
  MIN(effective_at) AS first_reward,
  MAX(effective_at) AS last_reward
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:ValidatorRewardCoupon'
  AND event_type = 'created'
GROUP BY 1, 2
ORDER BY reward_count DESC;
