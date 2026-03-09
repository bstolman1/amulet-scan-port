-- ============================================================================
-- BigQuery: Parse Events by Template Type
-- ============================================================================
-- Creates parsed views/tables for each major template type
-- JSON payloads are parsed into typed columns for efficient querying
-- ============================================================================

-- ============================================================================
-- AMULET (Core Currency) - Splice.Amulet:Amulet
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_amulet` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  timestamp,
  -- Parse payload JSON
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.owner') AS owner_party,
  CAST(JSON_VALUE(payload, '$.amount.initialAmount') AS NUMERIC) AS initial_amount,
  CAST(JSON_VALUE(payload, '$.amount.createdAt.number') AS INT64) AS created_at_round,
  CAST(JSON_VALUE(payload, '$.amount.ratePerRound.rate') AS NUMERIC) AS rate_per_round,
  -- Raw for debugging
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%Splice.Amulet:Amulet'
  OR template_id LIKE '%:Amulet'
  AND template_id NOT LIKE '%LockedAmulet%';


-- ============================================================================
-- LOCKED AMULET - Splice.Amulet:LockedAmulet
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_locked_amulet` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  -- Parse nested amulet structure
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.amulet.owner') AS owner_party,
  CAST(JSON_VALUE(payload, '$.amulet.amount.initialAmount') AS NUMERIC) AS locked_amount,
  CAST(JSON_VALUE(payload, '$.amulet.amount.createdAt.number') AS INT64) AS created_at_round,
  -- Lock details
  JSON_VALUE_ARRAY(payload, '$.lock.holders') AS lock_holders,
  CAST(JSON_VALUE(payload, '$.lock.expiresAt.number') AS INT64) AS expires_at_round,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%Splice.Amulet:LockedAmulet'
  OR template_id LIKE '%:LockedAmulet';


-- ============================================================================
-- VALIDATOR LICENSE - Splice.ValidatorLicense:ValidatorLicense
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_validator_license` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  -- Core parties
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.validator') AS validator_party,
  JSON_VALUE(payload, '$.sponsor') AS sponsor_party,
  -- Faucet state
  CAST(JSON_VALUE(payload, '$.faucetState.value.numCouponsMissed') AS INT64) AS coupons_missed,
  CAST(JSON_VALUE(payload, '$.faucetState.value.firstReceivedFor.number') AS INT64) AS first_received_round,
  CAST(JSON_VALUE(payload, '$.faucetState.value.lastReceivedFor.number') AS INT64) AS last_received_round,
  -- Metadata
  JSON_VALUE(payload, '$.metadata.version') AS version,
  JSON_VALUE(payload, '$.metadata.contactPoint') AS contact_point,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%ValidatorLicense:ValidatorLicense'
  OR template_id LIKE '%:ValidatorLicense';


-- ============================================================================
-- VALIDATOR RIGHT - Splice.Amulet:ValidatorRight
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_validator_right` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.user') AS user_party,
  JSON_VALUE(payload, '$.validator') AS validator_party,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%Splice.Amulet:ValidatorRight'
  OR template_id LIKE '%:ValidatorRight';


-- ============================================================================
-- APP REWARD COUPON - Splice.Amulet:AppRewardCoupon
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_app_reward_coupon` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.provider') AS provider_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.amount') AS NUMERIC) AS reward_amount,
  CAST(JSON_VALUE(payload, '$.featured') AS BOOL) AS is_featured,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%Splice.Amulet:AppRewardCoupon'
  OR template_id LIKE '%:AppRewardCoupon';


-- ============================================================================
-- SV REWARD COUPON - Splice.Amulet:SvRewardCoupon
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_sv_reward_coupon` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.sv') AS sv_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.weight') AS INT64) AS sv_weight,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%Splice.Amulet:SvRewardCoupon'
  OR template_id LIKE '%:SvRewardCoupon';


-- ============================================================================
-- VALIDATOR REWARD COUPON - Splice.Amulet:ValidatorRewardCoupon
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_validator_reward_coupon` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.user') AS user_party,
  JSON_VALUE(payload, '$.validator') AS validator_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%Splice.Amulet:ValidatorRewardCoupon'
  OR template_id LIKE '%:ValidatorRewardCoupon';


-- ============================================================================
-- MEMBER TRAFFIC - Splice.DecentralizedSynchronizer:MemberTraffic
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_member_traffic` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.memberId') AS member_id,
  JSON_VALUE(payload, '$.synchronizerId') AS synchronizer_id,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.totalTrafficPurchased') AS INT64) AS total_traffic_purchased,
  CAST(JSON_VALUE(payload, '$.migrationId') AS INT64) AS traffic_migration_id,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%DecentralizedSynchronizer:MemberTraffic'
  OR template_id LIKE '%:MemberTraffic';


-- ============================================================================
-- FEATURED APP RIGHT - Splice.Amulet:FeaturedAppRight
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_featured_app_right` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.provider') AS provider_party,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%Splice.Amulet:FeaturedAppRight'
  OR template_id LIKE '%:FeaturedAppRight';


-- ============================================================================
-- ANS ENTRY - Splice.Ans:AnsEntry
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_ans_entry` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.user') AS user_party,
  JSON_VALUE(payload, '$.name') AS ans_name,
  JSON_VALUE(payload, '$.url') AS ans_url,
  JSON_VALUE(payload, '$.description') AS description,
  CAST(JSON_VALUE(payload, '$.expiresAt') AS TIMESTAMP) AS expires_at,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%Splice.Ans:AnsEntry'
  OR template_id LIKE '%:AnsEntry';


-- ============================================================================
-- OPEN MINING ROUND - Splice.Round:OpenMiningRound
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_open_mining_round` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.amuletPrice') AS NUMERIC) AS amulet_price,
  CAST(JSON_VALUE(payload, '$.opensAt') AS TIMESTAMP) AS opens_at,
  CAST(JSON_VALUE(payload, '$.targetClosesAt') AS TIMESTAMP) AS target_closes_at,
  CAST(JSON_VALUE(payload, '$.issuingFor.number') AS INT64) AS issuing_for_round,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%Splice.Round:OpenMiningRound'
  OR template_id LIKE '%:OpenMiningRound';


-- ============================================================================
-- CLOSED MINING ROUND - Splice.Round:ClosedMiningRound
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_closed_mining_round` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%Splice.Round:ClosedMiningRound'
  OR template_id LIKE '%:ClosedMiningRound';
