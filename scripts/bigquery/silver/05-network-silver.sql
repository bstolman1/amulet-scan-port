-- ============================================================================
-- SILVER LAYER: Network Operations Tables
-- ============================================================================
-- Member traffic, mining rounds, ANS entries, and network state
-- ============================================================================

-- ============================================================================
-- MEMBER TRAFFIC - Network traffic purchases
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.member_traffic`
PARTITION BY DATE(effective_at)
CLUSTER BY member_id, round_number
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  event_type,
  
  CAST(JSON_VALUE(payload, '$.dso') AS STRING) AS dso_party,
  CAST(JSON_VALUE(payload, '$.memberId') AS STRING) AS member_id,
  CAST(JSON_VALUE(payload, '$.synchronizerId') AS STRING) AS synchronizer_id,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.totalTrafficPurchased') AS INT64) AS total_traffic_purchased,
  CAST(JSON_VALUE(payload, '$.migrationId') AS INT64) AS traffic_migration_id

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:MemberTraffic';


-- ============================================================================
-- OPEN MINING ROUNDS - Currently open mining rounds
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.open_mining_rounds`
PARTITION BY DATE(effective_at)
CLUSTER BY round_number
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  event_type,
  
  CAST(JSON_VALUE(payload, '$.dso') AS STRING) AS dso_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.amuletPrice') AS NUMERIC) AS amulet_price,
  CAST(JSON_VALUE(payload, '$.opensAt') AS TIMESTAMP) AS opens_at,
  CAST(JSON_VALUE(payload, '$.targetClosesAt') AS TIMESTAMP) AS target_closes_at,
  CAST(JSON_VALUE(payload, '$.issuingFor.number') AS INT64) AS issuing_for_round,
  
  -- Fee schedule parsing
  CAST(JSON_VALUE(payload, '$.transferConfigUsd.createFee.fee') AS NUMERIC) AS create_fee_usd,
  CAST(JSON_VALUE(payload, '$.transferConfigUsd.holdingFee.rate') AS NUMERIC) AS holding_fee_rate,
  CAST(JSON_VALUE(payload, '$.transferConfigUsd.transferFee.initialRate') AS NUMERIC) AS transfer_fee_rate

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:OpenMiningRound';


-- ============================================================================
-- CLOSED MINING ROUNDS - Completed mining rounds
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.closed_mining_rounds`
PARTITION BY DATE(effective_at)
CLUSTER BY round_number
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  event_type,
  
  CAST(JSON_VALUE(payload, '$.dso') AS STRING) AS dso_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:ClosedMiningRound';


-- ============================================================================
-- ISSUING MINING ROUNDS - Rounds issuing rewards
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.issuing_mining_rounds`
PARTITION BY DATE(effective_at)
CLUSTER BY round_number
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  event_type,
  
  CAST(JSON_VALUE(payload, '$.dso') AS STRING) AS dso_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  
  -- Issuance details
  CAST(JSON_VALUE(payload, '$.issuanceConfig.amuletToIssuePerYear') AS NUMERIC) AS amulet_to_issue_per_year,
  CAST(JSON_VALUE(payload, '$.issuanceConfig.validatorRewardPercentage') AS NUMERIC) AS validator_reward_pct,
  CAST(JSON_VALUE(payload, '$.issuanceConfig.appRewardPercentage') AS NUMERIC) AS app_reward_pct,
  CAST(JSON_VALUE(payload, '$.issuanceConfig.svRewardPercentage') AS NUMERIC) AS sv_reward_pct

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:IssuingMiningRound';


-- ============================================================================
-- ROUND LIFECYCLE - Complete round state transitions
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.round_lifecycle`
PARTITION BY DATE(round_opened_at)
CLUSTER BY round_number
AS
WITH opens AS (
  SELECT
    CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
    effective_at AS round_opened_at,
    CAST(JSON_VALUE(payload, '$.amuletPrice') AS NUMERIC) AS amulet_price,
    CAST(JSON_VALUE(payload, '$.targetClosesAt') AS TIMESTAMP) AS target_closes_at,
    contract_id
  FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
  WHERE template_id LIKE '%:OpenMiningRound'
    AND event_type = 'created'
),
closes AS (
  SELECT
    CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
    effective_at AS round_closed_at
  FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
  WHERE template_id LIKE '%:ClosedMiningRound'
    AND event_type = 'created'
)
SELECT
  o.round_number,
  CAST(o.round_opened_at AS TIMESTAMP) AS round_opened_at,
  CAST(c.round_closed_at AS TIMESTAMP) AS round_closed_at,
  o.amulet_price,
  o.target_closes_at,
  
  CASE 
    WHEN c.round_closed_at IS NOT NULL THEN 'closed'
    ELSE 'open'
  END AS status,
  
  TIMESTAMP_DIFF(
    COALESCE(c.round_closed_at, CURRENT_TIMESTAMP()),
    o.round_opened_at,
    MINUTE
  ) AS duration_minutes

FROM opens o
LEFT JOIN closes c ON o.round_number = c.round_number;


-- ============================================================================
-- ANS ENTRIES - Name service registrations
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.ans_entries`
PARTITION BY DATE(effective_at)
CLUSTER BY user_party, ans_name
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
  CAST(JSON_VALUE(payload, '$.name') AS STRING) AS ans_name,
  CAST(JSON_VALUE(payload, '$.url') AS STRING) AS ans_url,
  CAST(JSON_VALUE(payload, '$.description') AS STRING) AS description,
  CAST(JSON_VALUE(payload, '$.expiresAt') AS TIMESTAMP) AS expires_at

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:AnsEntry';


-- ============================================================================
-- SV NODE STATE - Super Validator node status
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.sv_node_state`
PARTITION BY DATE(effective_at)
CLUSTER BY sv_party
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
  CAST(JSON_VALUE(payload, '$.name') AS STRING) AS sv_name,
  
  -- Parse synchronizer nodes (complex nested structure)
  TO_JSON_STRING(JSON_QUERY(payload, '$.state.synchronizerNodes')) AS synchronizer_nodes_json

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:SvNodeState';


-- ============================================================================
-- AMULET PRICE VOTES - SV price votes
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.amulet_price_votes`
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
  CAST(JSON_VALUE(payload, '$.amuletPrice') AS NUMERIC) AS voted_price

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:AmuletPriceVote';


-- ============================================================================
-- DAILY NETWORK METRICS - Aggregated network activity
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.daily_network_metrics`
PARTITION BY metric_date
AS
SELECT
  DATE(effective_at) AS metric_date,
  
  -- Round metrics
  COUNTIF(template_id LIKE '%:OpenMiningRound' AND event_type = 'created') AS rounds_opened,
  COUNTIF(template_id LIKE '%:ClosedMiningRound' AND event_type = 'created') AS rounds_closed,
  
  -- Traffic metrics
  COUNTIF(template_id LIKE '%:MemberTraffic' AND event_type = 'created') AS traffic_contracts_created,
  COUNT(DISTINCT CASE 
    WHEN template_id LIKE '%:MemberTraffic' 
    THEN JSON_VALUE(payload, '$.memberId') 
  END) AS unique_traffic_members,
  
  -- ANS metrics
  COUNTIF(template_id LIKE '%:AnsEntry' AND event_type = 'created') AS ans_entries_created,
  
  -- Price votes
  COUNTIF(template_id LIKE '%:AmuletPriceVote' AND event_type = 'created') AS price_votes_cast

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:OpenMiningRound'
   OR template_id LIKE '%:ClosedMiningRound'
   OR template_id LIKE '%:MemberTraffic'
   OR template_id LIKE '%:AnsEntry'
   OR template_id LIKE '%:AmuletPriceVote'
GROUP BY 1;
