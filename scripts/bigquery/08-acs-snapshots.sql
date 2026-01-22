-- ============================================================================
-- BigQuery: ACS (Active Contract Set) Snapshot Parsing
-- ============================================================================
-- Creates parsed views for ACS snapshot data - current state of all contracts
-- ============================================================================

-- ============================================================================
-- ACS OVERVIEW - Summary of all active contracts by template
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.acs_template_summary` AS
SELECT
  template_id,
  module_name,
  entity_name,
  COUNT(*) AS contract_count,
  COUNT(DISTINCT ARRAY_TO_STRING(signatories, ',')) AS unique_signatory_groups,
  MIN(record_time) AS earliest_contract,
  MAX(record_time) AS latest_contract,
  MAX(snapshot_time) AS snapshot_time
FROM `YOUR_PROJECT_ID.canton_ledger.acs_raw`
GROUP BY 1, 2, 3
ORDER BY contract_count DESC;


-- ============================================================================
-- ACS AMULETS - All active Amulet contracts
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.acs_amulets` AS
SELECT
  contract_id,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.owner') AS owner_party,
  CAST(JSON_VALUE(payload, '$.amount.initialAmount') AS NUMERIC) AS initial_amount,
  CAST(JSON_VALUE(payload, '$.amount.createdAt.number') AS INT64) AS created_at_round,
  CAST(JSON_VALUE(payload, '$.amount.ratePerRound.rate') AS NUMERIC) AS rate_per_round,
  signatories,
  record_time,
  snapshot_time,
  migration_id
FROM `YOUR_PROJECT_ID.canton_ledger.acs_raw`
WHERE template_id LIKE '%:Amulet'
  AND template_id NOT LIKE '%LockedAmulet%';


-- ============================================================================
-- ACS VALIDATOR LICENSES - All active validator licenses
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.acs_validator_licenses` AS
SELECT
  contract_id,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.validator') AS validator_party,
  JSON_VALUE(payload, '$.sponsor') AS sponsor_party,
  CAST(JSON_VALUE(payload, '$.faucetState.value.numCouponsMissed') AS INT64) AS coupons_missed,
  CAST(JSON_VALUE(payload, '$.faucetState.value.lastReceivedFor.number') AS INT64) AS last_received_round,
  JSON_VALUE(payload, '$.metadata.version') AS version,
  JSON_VALUE(payload, '$.metadata.contactPoint') AS contact_point,
  record_time,
  snapshot_time
FROM `YOUR_PROJECT_ID.canton_ledger.acs_raw`
WHERE template_id LIKE '%:ValidatorLicense';


-- ============================================================================
-- ACS VOTE REQUESTS - Active governance proposals
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.acs_vote_requests` AS
SELECT
  contract_id,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.requester') AS requester_party,
  JSON_VALUE(payload, '$.action.tag') AS action_category,
  JSON_VALUE(payload, '$.action.value.tag') AS action_type,
  JSON_VALUE(payload, '$.reason.url') AS reason_url,
  JSON_VALUE(payload, '$.reason.body') AS reason_body,
  CAST(JSON_VALUE(payload, '$.expiresAt') AS TIMESTAMP) AS expires_at,
  -- Count votes
  ARRAY_LENGTH(JSON_EXTRACT_ARRAY(payload, '$.votes')) AS vote_count,
  record_time,
  snapshot_time,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.acs_raw`
WHERE template_id LIKE '%:VoteRequest';


-- ============================================================================
-- ACS DSO RULES - Current DSO configuration
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.acs_dso_rules` AS
SELECT
  contract_id,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  CAST(JSON_VALUE(payload, '$.epoch') AS INT64) AS epoch,
  JSON_VALUE(payload, '$.dsoDelegate') AS dso_delegate,
  CAST(JSON_VALUE(payload, '$.config.numMemberTrafficContractsThreshold') AS INT64) AS traffic_threshold,
  -- Extract SV information
  payload AS full_payload,
  record_time,
  snapshot_time
FROM `YOUR_PROJECT_ID.canton_ledger.acs_raw`
WHERE template_id LIKE '%:DsoRules';


-- ============================================================================
-- ACS ANS ENTRIES - Active name service entries
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.acs_ans_entries` AS
SELECT
  contract_id,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.user') AS user_party,
  JSON_VALUE(payload, '$.name') AS ans_name,
  JSON_VALUE(payload, '$.url') AS ans_url,
  JSON_VALUE(payload, '$.description') AS description,
  CAST(JSON_VALUE(payload, '$.expiresAt') AS TIMESTAMP) AS expires_at,
  record_time,
  snapshot_time
FROM `YOUR_PROJECT_ID.canton_ledger.acs_raw`
WHERE template_id LIKE '%:AnsEntry';


-- ============================================================================
-- ACS OPEN MINING ROUNDS - Currently open rounds
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.acs_open_mining_rounds` AS
SELECT
  contract_id,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.amuletPrice') AS NUMERIC) AS amulet_price,
  CAST(JSON_VALUE(payload, '$.opensAt') AS TIMESTAMP) AS opens_at,
  CAST(JSON_VALUE(payload, '$.targetClosesAt') AS TIMESTAMP) AS target_closes_at,
  record_time,
  snapshot_time
FROM `YOUR_PROJECT_ID.canton_ledger.acs_raw`
WHERE template_id LIKE '%:OpenMiningRound';


-- ============================================================================
-- ACS REWARD COUPONS - Active unclaimed rewards
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.acs_reward_coupons` AS
SELECT
  contract_id,
  template_id,
  entity_name AS reward_type,
  CASE entity_name
    WHEN 'AppRewardCoupon' THEN JSON_VALUE(payload, '$.provider')
    WHEN 'SvRewardCoupon' THEN JSON_VALUE(payload, '$.sv')
    WHEN 'ValidatorRewardCoupon' THEN JSON_VALUE(payload, '$.validator')
    ELSE NULL
  END AS recipient_party,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CASE entity_name
    WHEN 'AppRewardCoupon' THEN CAST(JSON_VALUE(payload, '$.amount') AS NUMERIC)
    ELSE NULL
  END AS reward_amount,
  CASE entity_name
    WHEN 'SvRewardCoupon' THEN CAST(JSON_VALUE(payload, '$.weight') AS INT64)
    ELSE NULL
  END AS sv_weight,
  record_time,
  snapshot_time
FROM `YOUR_PROJECT_ID.canton_ledger.acs_raw`
WHERE template_id LIKE '%RewardCoupon';


-- ============================================================================
-- ACS MEMBER TRAFFIC - Active traffic contracts
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.acs_member_traffic` AS
SELECT
  contract_id,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.memberId') AS member_id,
  JSON_VALUE(payload, '$.synchronizerId') AS synchronizer_id,
  CAST(JSON_VALUE(payload, '$.round.number') AS INT64) AS round_number,
  CAST(JSON_VALUE(payload, '$.totalTrafficPurchased') AS INT64) AS total_traffic_purchased,
  record_time,
  snapshot_time
FROM `YOUR_PROJECT_ID.canton_ledger.acs_raw`
WHERE template_id LIKE '%:MemberTraffic';


-- ============================================================================
-- ACS SNAPSHOT COMPARISON - Compare snapshots over time
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.acs_snapshot_stats` AS
SELECT
  DATE(snapshot_time) AS snapshot_date,
  snapshot_id,
  migration_id,
  COUNT(*) AS total_contracts,
  COUNT(DISTINCT template_id) AS unique_templates,
  COUNT(CASE WHEN template_id LIKE '%:Amulet' AND template_id NOT LIKE '%Locked%' THEN 1 END) AS amulet_count,
  COUNT(CASE WHEN template_id LIKE '%:VoteRequest' THEN 1 END) AS vote_request_count,
  COUNT(CASE WHEN template_id LIKE '%:ValidatorLicense' THEN 1 END) AS validator_license_count,
  MIN(record_time) AS earliest_contract_time,
  MAX(record_time) AS latest_contract_time
FROM `YOUR_PROJECT_ID.canton_ledger.acs_raw`
GROUP BY 1, 2, 3
ORDER BY snapshot_date DESC, snapshot_id DESC;
