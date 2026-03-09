-- ============================================================================
-- BigQuery: Governance Data Parsing
-- ============================================================================
-- Creates parsed tables for governance templates: VoteRequest, DsoRules, etc.
-- ============================================================================

-- ============================================================================
-- DSO RULES - Splice.DsoRules:DsoRules
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_dso_rules` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  -- Core DSO fields
  JSON_VALUE(payload, '$.dso') AS dso_party,
  CAST(JSON_VALUE(payload, '$.epoch') AS INT64) AS epoch,
  JSON_VALUE(payload, '$.dsoDelegate') AS dso_delegate,
  -- Config extraction
  CAST(JSON_VALUE(payload, '$.config.numMemberTrafficContractsThreshold') AS INT64) AS member_traffic_threshold,
  JSON_VALUE(payload, '$.config.actionConfirmationTimeout') AS action_confirmation_timeout,
  JSON_VALUE(payload, '$.config.svOnboardingRequestTimeout') AS sv_onboarding_timeout,
  -- SV count (parse the svs map)
  ARRAY_LENGTH(JSON_EXTRACT_ARRAY(payload, '$.svs')) AS sv_count,
  -- Raw for complex queries
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%DsoRules:DsoRules'
  OR template_id LIKE '%:DsoRules';


-- ============================================================================
-- VOTE REQUESTS - Splice.DsoRules:VoteRequest
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_vote_requests` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  -- Core fields
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.requester') AS requester_party,
  -- Action details (nested tagged union)
  JSON_VALUE(payload, '$.action.tag') AS action_category,
  JSON_VALUE(payload, '$.action.value.tag') AS action_type,
  -- Reason
  JSON_VALUE(payload, '$.reason.url') AS reason_url,
  JSON_VALUE(payload, '$.reason.body') AS reason_body,
  -- Expiration
  CAST(JSON_VALUE(payload, '$.expiresAt') AS TIMESTAMP) AS expires_at,
  -- Vote count (number of entries in votes map)
  ARRAY_LENGTH(JSON_EXTRACT_ARRAY(payload, '$.votes')) AS vote_count,
  -- Tracking
  JSON_VALUE(payload, '$.trackingCid') AS tracking_cid,
  -- Raw payload for detailed vote analysis
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%DsoRules:VoteRequest'
  OR template_id LIKE '%:VoteRequest';


-- ============================================================================
-- PARSED VOTES (from VoteRequest payload) - Flattened vote details
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_votes` AS
SELECT
  e.event_id,
  e.contract_id AS vote_request_contract_id,
  e.effective_at,
  JSON_VALUE(e.payload, '$.requester') AS requester_party,
  JSON_VALUE(e.payload, '$.action.tag') AS action_category,
  JSON_VALUE(e.payload, '$.action.value.tag') AS action_type,
  -- Extract individual vote details
  vote_key AS voter_party,
  CAST(JSON_VALUE(vote_value, '$.accept') AS BOOL) AS vote_accepted,
  JSON_VALUE(vote_value, '$.reason.body') AS vote_reason,
  JSON_VALUE(vote_value, '$.reason.url') AS vote_reason_url
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw` e,
  UNNEST(JSON_KEYS(JSON_QUERY(e.payload, '$.votes'))) AS vote_key
  LEFT JOIN UNNEST([JSON_QUERY(e.payload, CONCAT('$.votes.', vote_key))]) AS vote_value
WHERE e.template_id LIKE '%DsoRules:VoteRequest'
  OR e.template_id LIKE '%:VoteRequest';


-- ============================================================================
-- CONFIRMATIONS - Splice.DsoRules:Confirmation
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_confirmations` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.action.tag') AS action_category,
  JSON_VALUE(payload, '$.action.value.tag') AS action_type,
  JSON_VALUE_ARRAY(payload, '$.confirmedBy') AS confirmed_by_parties,
  ARRAY_LENGTH(JSON_VALUE_ARRAY(payload, '$.confirmedBy')) AS confirmation_count,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%DsoRules:Confirmation'
  OR template_id LIKE '%:Confirmation';


-- ============================================================================
-- ELECTION REQUESTS - Splice.DsoRules:ElectionRequest
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_election_requests` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  CAST(JSON_VALUE(payload, '$.epoch') AS INT64) AS epoch,
  -- Ranking count
  ARRAY_LENGTH(JSON_EXTRACT_ARRAY(payload, '$.ranking')) AS ranking_count,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%DsoRules:ElectionRequest'
  OR template_id LIKE '%:ElectionRequest';


-- ============================================================================
-- SV NODE STATE - Splice.DSO.SvState:SvNodeState
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_sv_node_state` AS
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
  JSON_VALUE(payload, '$.name') AS sv_name,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%SvState:SvNodeState'
  OR template_id LIKE '%:SvNodeState';


-- ============================================================================
-- SV REWARD STATE - Splice.DSO.SvState:SvRewardState
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_sv_reward_state` AS
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
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%SvState:SvRewardState'
  OR template_id LIKE '%:SvRewardState';


-- ============================================================================
-- GOVERNANCE ACTION ANALYSIS - Summary of governance activity
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.governance_action_summary` AS
SELECT
  DATE(effective_at) AS action_date,
  JSON_VALUE(payload, '$.action.tag') AS action_category,
  JSON_VALUE(payload, '$.action.value.tag') AS action_type,
  event_type,
  COUNT(*) AS action_count,
  COUNT(DISTINCT JSON_VALUE(payload, '$.requester')) AS unique_requesters
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%DsoRules:VoteRequest'
  OR template_id LIKE '%:VoteRequest'
GROUP BY 1, 2, 3, 4
ORDER BY action_date DESC, action_count DESC;
