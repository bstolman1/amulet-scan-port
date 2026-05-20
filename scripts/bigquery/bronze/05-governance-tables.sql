-- Governance Data Parsing
-- VoteRequest, DsoRules, Confirmations, ElectionRequests, SV state

-- DSO Rules
CREATE OR REPLACE VIEW `${PROJECT_ID}.transformed.parsed_dso_rules` AS
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
  JSON_VALUE(payload, '$.dsoDelegate') AS dso_delegate,
  CAST(JSON_VALUE(payload, '$.config.numMemberTrafficContractsThreshold') AS INT64) AS member_traffic_threshold,
  JSON_VALUE(payload, '$.config.actionConfirmationTimeout') AS action_confirmation_timeout,
  JSON_VALUE(payload, '$.config.svOnboardingRequestTimeout') AS sv_onboarding_timeout,
  ARRAY_LENGTH(JSON_EXTRACT_ARRAY(payload, '$.svs')) AS sv_count,
  payload
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%DsoRules:DsoRules'
   OR template_id LIKE '%:DsoRules';


-- Vote Requests
CREATE OR REPLACE VIEW `${PROJECT_ID}.transformed.parsed_vote_requests` AS
SELECT
  event_id,
  update_id,
  contract_id,
  template_id,
  event_type,
  migration_id,
  effective_at,
  JSON_VALUE(payload, '$.dso') AS dso_party,
  JSON_VALUE(payload, '$.requester') AS requester_party,
  JSON_VALUE(payload, '$.action.tag') AS action_category,
  JSON_VALUE(payload, '$.action.value.tag') AS action_type,
  JSON_VALUE(payload, '$.reason.url') AS reason_url,
  JSON_VALUE(payload, '$.reason.body') AS reason_body,
  CAST(JSON_VALUE(payload, '$.expiresAt') AS TIMESTAMP) AS expires_at,
  ARRAY_LENGTH(JSON_EXTRACT_ARRAY(payload, '$.votes')) AS vote_count,
  JSON_VALUE(payload, '$.trackingCid') AS tracking_cid,
  payload
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%DsoRules:VoteRequest'
   OR template_id LIKE '%:VoteRequest';


-- Parsed Votes (flattened from VoteRequest payload)
CREATE OR REPLACE VIEW `${PROJECT_ID}.transformed.parsed_votes` AS
SELECT
  e.event_id,
  e.contract_id AS vote_request_contract_id,
  e.effective_at,
  JSON_VALUE(e.payload, '$.requester') AS requester_party,
  JSON_VALUE(e.payload, '$.action.tag') AS action_category,
  JSON_VALUE(e.payload, '$.action.value.tag') AS action_type,
  vote_key AS voter_party,
  CAST(JSON_VALUE(vote_value, '$.accept') AS BOOL) AS vote_accepted,
  JSON_VALUE(vote_value, '$.reason.body') AS vote_reason,
  JSON_VALUE(vote_value, '$.reason.url') AS vote_reason_url
FROM `${PROJECT_ID}.transformed.events_parsed` e,
  UNNEST(JSON_KEYS(JSON_QUERY(e.payload, '$.votes'))) AS vote_key
  LEFT JOIN UNNEST([JSON_QUERY(e.payload, CONCAT('$.votes.', vote_key))]) AS vote_value
WHERE e.template_id LIKE '%DsoRules:VoteRequest'
   OR e.template_id LIKE '%:VoteRequest';


-- Confirmations
CREATE OR REPLACE VIEW `${PROJECT_ID}.transformed.parsed_confirmations` AS
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
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%DsoRules:Confirmation'
   OR template_id LIKE '%:Confirmation';


-- Election Requests
CREATE OR REPLACE VIEW `${PROJECT_ID}.transformed.parsed_election_requests` AS
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
  ARRAY_LENGTH(JSON_EXTRACT_ARRAY(payload, '$.ranking')) AS ranking_count,
  payload
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%DsoRules:ElectionRequest'
   OR template_id LIKE '%:ElectionRequest';


-- SV Node State
CREATE OR REPLACE VIEW `${PROJECT_ID}.transformed.parsed_sv_node_state` AS
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
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%SvState:SvNodeState'
   OR template_id LIKE '%:SvNodeState';


-- SV Reward State
CREATE OR REPLACE VIEW `${PROJECT_ID}.transformed.parsed_sv_reward_state` AS
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
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%SvState:SvRewardState'
   OR template_id LIKE '%:SvRewardState';


-- Governance Action Summary
CREATE OR REPLACE VIEW `${PROJECT_ID}.transformed.governance_action_summary` AS
SELECT
  DATE(effective_at) AS action_date,
  JSON_VALUE(payload, '$.action.tag') AS action_category,
  JSON_VALUE(payload, '$.action.value.tag') AS action_type,
  event_type,
  COUNT(*) AS action_count,
  COUNT(DISTINCT JSON_VALUE(payload, '$.requester')) AS unique_requesters
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%DsoRules:VoteRequest'
   OR template_id LIKE '%:VoteRequest'
GROUP BY 1, 2, 3, 4
ORDER BY action_date DESC, action_count DESC;
