-- SILVER LAYER: Governance Tables
-- Fully parsed governance data - VoteRequests, DsoRules, Confirmations

-- Vote Requests (fully parsed)
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.vote_requests`
PARTITION BY DATE(effective_at)
CLUSTER BY action_type, requester_party
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  event_type,
  CAST(JSON_VALUE(payload, '$.dso') AS STRING) AS dso_party,
  CAST(JSON_VALUE(payload, '$.requester') AS STRING) AS requester_party,
  CAST(JSON_VALUE(payload, '$.action.tag') AS STRING) AS action_category,
  CAST(JSON_VALUE(payload, '$.action.value.tag') AS STRING) AS action_type,
  COALESCE(
    JSON_VALUE(payload, '$.action.value.value.sv'),
    JSON_VALUE(payload, '$.action.value.value.validator'),
    JSON_VALUE(payload, '$.action.value.value.provider'),
    JSON_VALUE(payload, '$.action.value.value.user')
  ) AS action_target_party,
  CAST(JSON_VALUE(payload, '$.reason.url') AS STRING) AS reason_url,
  CAST(JSON_VALUE(payload, '$.reason.body') AS STRING) AS reason_body,
  CAST(JSON_VALUE(payload, '$.expiresAt') AS TIMESTAMP) AS expires_at,
  CAST(JSON_VALUE(payload, '$.trackingCid') AS STRING) AS tracking_cid,
  ARRAY_LENGTH(
    ARRAY(SELECT key FROM UNNEST(JSON_KEYS(JSON_QUERY(payload, '$.votes'))) AS key)
  ) AS total_votes,
  TO_JSON_STRING(JSON_QUERY(payload, '$.action')) AS action_json,
  TO_JSON_STRING(JSON_QUERY(payload, '$.votes')) AS votes_json
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%:VoteRequest'
  AND event_type = 'created';


-- Individual Votes (flattened from VoteRequest payloads)
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.votes`
PARTITION BY DATE(vote_recorded_at)
CLUSTER BY voter_party, vote_accepted
AS
WITH vote_requests AS (
  SELECT
    event_id AS vote_request_event_id,
    contract_id AS vote_request_contract_id,
    effective_at AS vote_recorded_at,
    JSON_VALUE(payload, '$.requester') AS requester_party,
    JSON_VALUE(payload, '$.action.tag') AS action_category,
    JSON_VALUE(payload, '$.action.value.tag') AS action_type,
    JSON_VALUE(payload, '$.reason.url') AS proposal_url,
    JSON_QUERY(payload, '$.votes') AS votes_map
  FROM `${PROJECT_ID}.transformed.events_parsed`
  WHERE template_id LIKE '%:VoteRequest'
)
SELECT
  vr.vote_request_event_id,
  vr.vote_request_contract_id,
  CAST(vr.vote_recorded_at AS TIMESTAMP) AS vote_recorded_at,
  vr.requester_party,
  vr.action_category,
  vr.action_type,
  vr.proposal_url,
  CAST(voter_key AS STRING) AS voter_party,
  CAST(JSON_VALUE(vote_data, '$.accept') AS BOOL) AS vote_accepted,
  CAST(JSON_VALUE(vote_data, '$.reason.body') AS STRING) AS vote_reason,
  CAST(JSON_VALUE(vote_data, '$.reason.url') AS STRING) AS vote_reason_url,
  CAST(JSON_VALUE(vote_data, '$.expiresAt') AS TIMESTAMP) AS vote_expires_at
FROM vote_requests vr,
  UNNEST(JSON_KEYS(vr.votes_map)) AS voter_key,
  UNNEST([JSON_QUERY(vr.votes_map, CONCAT('$."', voter_key, '"'))]) AS vote_data
WHERE vr.votes_map IS NOT NULL;


-- DSO Rules State
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.dso_rules_state`
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
  CAST(JSON_VALUE(payload, '$.epoch') AS INT64) AS epoch,
  CAST(JSON_VALUE(payload, '$.dsoDelegate') AS STRING) AS dso_delegate,
  CAST(JSON_VALUE(payload, '$.config.numMemberTrafficContractsThreshold') AS INT64) AS traffic_threshold,
  CAST(JSON_VALUE(payload, '$.config.numUnclaimedRewardsThreshold') AS INT64) AS unclaimed_rewards_threshold,
  CAST(JSON_VALUE(payload, '$.config.actionConfirmationTimeout') AS STRING) AS action_confirmation_timeout,
  CAST(JSON_VALUE(payload, '$.config.svOnboardingRequestTimeout') AS STRING) AS sv_onboarding_timeout,
  CAST(JSON_VALUE(payload, '$.config.voteRequestTimeout') AS STRING) AS vote_request_timeout,
  CAST(JSON_VALUE(payload, '$.config.decentralizedSynchronizer.requiredSynchronizers') AS INT64) AS required_synchronizers,
  ARRAY_LENGTH(
    ARRAY(SELECT key FROM UNNEST(JSON_KEYS(JSON_QUERY(payload, '$.svs'))) AS key)
  ) AS sv_count,
  TO_JSON_STRING(JSON_QUERY(payload, '$.svs')) AS svs_json,
  TO_JSON_STRING(JSON_QUERY(payload, '$.config')) AS config_json
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%:DsoRules';


-- SV Membership (parsed from DsoRules svs map)
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.sv_membership`
PARTITION BY DATE(snapshot_at)
CLUSTER BY sv_party
AS
WITH dso_snapshots AS (
  SELECT
    effective_at AS snapshot_at,
    contract_id AS dso_rules_contract_id,
    CAST(JSON_VALUE(payload, '$.epoch') AS INT64) AS epoch,
    JSON_QUERY(payload, '$.svs') AS svs_map
  FROM `${PROJECT_ID}.transformed.events_parsed`
  WHERE template_id LIKE '%:DsoRules'
    AND event_type = 'created'
)
SELECT
  CAST(ds.snapshot_at AS TIMESTAMP) AS snapshot_at,
  ds.dso_rules_contract_id,
  ds.epoch,
  CAST(sv_key AS STRING) AS sv_party,
  CAST(JSON_VALUE(sv_data, '$.name') AS STRING) AS sv_name,
  CAST(JSON_VALUE(sv_data, '$.weight') AS INT64) AS sv_weight,
  CAST(JSON_VALUE(sv_data, '$.joinedAt') AS TIMESTAMP) AS joined_at,
  CAST(JSON_VALUE(sv_data, '$.participantId') AS STRING) AS participant_id,
  TO_JSON_STRING(sv_data) AS sv_config_json
FROM dso_snapshots ds,
  UNNEST(JSON_KEYS(ds.svs_map)) AS sv_key,
  UNNEST([JSON_QUERY(ds.svs_map, CONCAT('$."', sv_key, '"'))]) AS sv_data
WHERE ds.svs_map IS NOT NULL;


-- Confirmations
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.confirmations`
PARTITION BY DATE(effective_at)
CLUSTER BY action_type
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  event_type,
  CAST(JSON_VALUE(payload, '$.dso') AS STRING) AS dso_party,
  CAST(JSON_VALUE(payload, '$.action.tag') AS STRING) AS action_category,
  CAST(JSON_VALUE(payload, '$.action.value.tag') AS STRING) AS action_type,
  ARRAY(
    SELECT CAST(p AS STRING)
    FROM UNNEST(JSON_VALUE_ARRAY(payload, '$.confirmedBy')) AS p
  ) AS confirmed_by_parties,
  ARRAY_LENGTH(JSON_VALUE_ARRAY(payload, '$.confirmedBy')) AS confirmation_count,
  TO_JSON_STRING(JSON_QUERY(payload, '$.action')) AS action_json
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%:Confirmation';


-- Election Requests
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.election_requests`
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
  CAST(JSON_VALUE(payload, '$.epoch') AS INT64) AS epoch,
  ARRAY(
    SELECT AS STRUCT
      CAST(JSON_VALUE(r, '$.sv') AS STRING) AS sv_party,
      CAST(JSON_VALUE(r, '$.rank') AS INT64) AS rank
    FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.ranking')) AS r
  ) AS ranking,
  ARRAY_LENGTH(JSON_QUERY_ARRAY(payload, '$.ranking')) AS candidates_count
FROM `${PROJECT_ID}.transformed.events_parsed`
WHERE template_id LIKE '%:ElectionRequest';


-- Governance Proposal Lifecycle
CREATE OR REPLACE TABLE `${PROJECT_ID}.canton_silver.proposal_lifecycle`
PARTITION BY DATE(created_at)
CLUSTER BY action_type, status
AS
WITH proposals AS (
  SELECT
    contract_id,
    event_id,
    effective_at AS created_at,
    JSON_VALUE(payload, '$.requester') AS requester_party,
    JSON_VALUE(payload, '$.action.tag') AS action_category,
    JSON_VALUE(payload, '$.action.value.tag') AS action_type,
    JSON_VALUE(payload, '$.reason.url') AS reason_url,
    JSON_VALUE(payload, '$.reason.body') AS reason_body,
    CAST(JSON_VALUE(payload, '$.expiresAt') AS TIMESTAMP) AS expires_at,
    event_type
  FROM `${PROJECT_ID}.transformed.events_parsed`
  WHERE template_id LIKE '%:VoteRequest'
),
archives AS (
  SELECT contract_id, effective_at AS archived_at
  FROM `${PROJECT_ID}.transformed.events_parsed`
  WHERE template_id LIKE '%:VoteRequest'
    AND event_type = 'archived'
)
SELECT
  p.contract_id,
  p.event_id,
  CAST(p.created_at AS TIMESTAMP) AS created_at,
  CAST(p.requester_party AS STRING) AS requester_party,
  CAST(p.action_category AS STRING) AS action_category,
  CAST(p.action_type AS STRING) AS action_type,
  CAST(p.reason_url AS STRING) AS reason_url,
  CAST(p.reason_body AS STRING) AS reason_body,
  p.expires_at,
  CAST(a.archived_at AS TIMESTAMP) AS archived_at,
  CASE
    WHEN a.archived_at IS NOT NULL THEN 'completed'
    WHEN p.expires_at < CURRENT_TIMESTAMP() THEN 'expired'
    ELSE 'active'
  END AS status,
  TIMESTAMP_DIFF(
    COALESCE(a.archived_at, CURRENT_TIMESTAMP()),
    p.created_at,
    HOUR
  ) AS duration_hours
FROM proposals p
LEFT JOIN archives a ON p.contract_id = a.contract_id
WHERE p.event_type = 'created';
