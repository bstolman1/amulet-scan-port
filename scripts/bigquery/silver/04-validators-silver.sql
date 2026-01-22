-- ============================================================================
-- SILVER LAYER: Validator Tables
-- ============================================================================
-- Fully parsed validator licenses, rights, and activity records
-- ============================================================================

-- ============================================================================
-- VALIDATOR LICENSES - Authorization to operate as validator
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.validator_licenses`
PARTITION BY DATE(effective_at)
CLUSTER BY validator_party, sponsor_party
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  event_type,
  
  -- Core parties
  CAST(JSON_VALUE(payload, '$.dso') AS STRING) AS dso_party,
  CAST(JSON_VALUE(payload, '$.validator') AS STRING) AS validator_party,
  CAST(JSON_VALUE(payload, '$.sponsor') AS STRING) AS sponsor_party,
  
  -- Faucet state (fully parsed)
  CAST(JSON_VALUE(payload, '$.faucetState.tag') AS STRING) AS faucet_state_tag,
  CAST(JSON_VALUE(payload, '$.faucetState.value.numCouponsMissed') AS INT64) AS coupons_missed,
  CAST(JSON_VALUE(payload, '$.faucetState.value.firstReceivedFor.number') AS INT64) AS first_received_round,
  CAST(JSON_VALUE(payload, '$.faucetState.value.lastReceivedFor.number') AS INT64) AS last_received_round,
  
  -- Metadata
  CAST(JSON_VALUE(payload, '$.metadata.version') AS STRING) AS version,
  CAST(JSON_VALUE(payload, '$.metadata.contactPoint') AS STRING) AS contact_point,
  CAST(JSON_VALUE(payload, '$.metadata.lastUpdatedAt') AS TIMESTAMP) AS metadata_last_updated,
  
  -- Derived metrics
  CAST(JSON_VALUE(payload, '$.faucetState.value.lastReceivedFor.number') AS INT64) -
  CAST(JSON_VALUE(payload, '$.faucetState.value.firstReceivedFor.number') AS INT64) AS active_round_span

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:ValidatorLicense';


-- ============================================================================
-- VALIDATOR RIGHTS - User-to-validator relationships
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.validator_rights`
PARTITION BY DATE(effective_at)
CLUSTER BY validator_party, user_party
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
  CAST(JSON_VALUE(payload, '$.validator') AS STRING) AS validator_party

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:ValidatorRight';


-- ============================================================================
-- VALIDATOR FAUCET COUPONS - Onboarding rewards
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.validator_faucet_coupons`
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
  CAST(JSON_VALUE(payload, '$.validator') AS STRING) AS validator_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:ValidatorFaucetCoupon';


-- ============================================================================
-- VALIDATOR LIVENESS RECORDS - Uptime tracking
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.validator_liveness`
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
  
  CAST(JSON_VALUE(payload, '$.validator') AS STRING) AS validator_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.domain') AS STRING) AS synchronizer_id,
  
  -- Activity record details
  CAST(JSON_VALUE(payload, '$.activityRecord.trafficReceived') AS INT64) AS traffic_received,
  CAST(JSON_VALUE(payload, '$.activityRecord.lastActiveAt') AS TIMESTAMP) AS last_active_at

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:ValidatorLivenessActivityRecord';


-- ============================================================================
-- VALIDATOR LIFECYCLE - Complete validator state tracking
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.validator_lifecycle`
PARTITION BY DATE(license_created_at)
CLUSTER BY validator_party, status
AS
WITH license_creates AS (
  SELECT
    contract_id,
    effective_at AS license_created_at,
    JSON_VALUE(payload, '$.validator') AS validator_party,
    JSON_VALUE(payload, '$.sponsor') AS sponsor_party,
    JSON_VALUE(payload, '$.metadata.contactPoint') AS contact_point,
    migration_id
  FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
  WHERE template_id LIKE '%:ValidatorLicense'
    AND event_type = 'created'
),
license_archives AS (
  SELECT contract_id, effective_at AS license_archived_at
  FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
  WHERE template_id LIKE '%:ValidatorLicense'
    AND event_type = 'archived'
),
user_counts AS (
  SELECT
    JSON_VALUE(payload, '$.validator') AS validator_party,
    COUNT(DISTINCT JSON_VALUE(payload, '$.user')) AS user_count
  FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
  WHERE template_id LIKE '%:ValidatorRight'
    AND event_type = 'created'
  GROUP BY 1
),
reward_counts AS (
  SELECT
    JSON_VALUE(payload, '$.validator') AS validator_party,
    COUNT(*) AS reward_count,
    MIN(CAST(JSON_VALUE(payload, '$.round.number') AS INT64)) AS first_reward_round,
    MAX(CAST(JSON_VALUE(payload, '$.round.number') AS INT64)) AS last_reward_round
  FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
  WHERE template_id LIKE '%:ValidatorRewardCoupon'
    AND event_type = 'created'
  GROUP BY 1
)
SELECT
  lc.contract_id AS license_contract_id,
  CAST(lc.license_created_at AS TIMESTAMP) AS license_created_at,
  CAST(lc.validator_party AS STRING) AS validator_party,
  CAST(lc.sponsor_party AS STRING) AS sponsor_party,
  CAST(lc.contact_point AS STRING) AS contact_point,
  lc.migration_id,
  CAST(la.license_archived_at AS TIMESTAMP) AS license_archived_at,
  
  CASE 
    WHEN la.license_archived_at IS NOT NULL THEN 'inactive'
    ELSE 'active'
  END AS status,
  
  COALESCE(uc.user_count, 0) AS user_count,
  COALESCE(rc.reward_count, 0) AS reward_count,
  rc.first_reward_round,
  rc.last_reward_round,
  
  TIMESTAMP_DIFF(
    COALESCE(la.license_archived_at, CURRENT_TIMESTAMP()),
    lc.license_created_at,
    DAY
  ) AS license_age_days

FROM license_creates lc
LEFT JOIN license_archives la ON lc.contract_id = la.contract_id
LEFT JOIN user_counts uc ON lc.validator_party = uc.validator_party
LEFT JOIN reward_counts rc ON lc.validator_party = rc.validator_party;


-- ============================================================================
-- VALIDATOR PERFORMANCE METRICS - Aggregated performance stats
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.validator_performance`
CLUSTER BY validator_party, total_rewards
AS
SELECT
  vl.validator_party,
  vl.sponsor_party,
  vl.status,
  vl.license_created_at,
  vl.license_age_days,
  vl.user_count,
  vl.reward_count AS total_rewards,
  vl.first_reward_round,
  vl.last_reward_round,
  
  -- Calculate activity metrics
  CASE 
    WHEN vl.last_reward_round IS NOT NULL AND vl.first_reward_round IS NOT NULL
    THEN vl.last_reward_round - vl.first_reward_round
    ELSE 0 
  END AS active_round_span,
  
  -- Reward rate
  CASE 
    WHEN vl.last_reward_round IS NOT NULL 
      AND vl.first_reward_round IS NOT NULL 
      AND vl.last_reward_round > vl.first_reward_round
    THEN vl.reward_count / (vl.last_reward_round - vl.first_reward_round)
    ELSE 0
  END AS rewards_per_round

FROM `YOUR_PROJECT_ID.canton_silver.validator_lifecycle` vl;
