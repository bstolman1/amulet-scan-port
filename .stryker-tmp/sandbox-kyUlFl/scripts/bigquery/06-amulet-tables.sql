-- ============================================================================
-- BigQuery: Amulet/Currency Data Parsing
-- ============================================================================
-- Creates parsed tables for Amulet, transfers, and supply analysis
-- ============================================================================

-- ============================================================================
-- AMULET SUPPLY SNAPSHOT - Current supply from ACS
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.acs_amulet_supply` AS
SELECT
  contract_id,
  JSON_VALUE(payload, '$.owner') AS owner_party,
  CAST(JSON_VALUE(payload, '$.amount.initialAmount') AS NUMERIC) AS initial_amount,
  CAST(JSON_VALUE(payload, '$.amount.createdAt.number') AS INT64) AS created_at_round,
  CAST(JSON_VALUE(payload, '$.amount.ratePerRound.rate') AS NUMERIC) AS rate_per_round,
  migration_id,
  snapshot_time,
  -- Calculate approximate current amount (requires knowing current round)
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.acs_raw`
WHERE template_id LIKE '%:Amulet'
  AND template_id NOT LIKE '%LockedAmulet%';


-- ============================================================================
-- AMULET CREATION EVENTS - All Amulet creation history
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.amulet_creations` AS
SELECT
  event_id,
  update_id,
  contract_id,
  effective_at,
  migration_id,
  JSON_VALUE(payload, '$.owner') AS owner_party,
  CAST(JSON_VALUE(payload, '$.amount.initialAmount') AS NUMERIC) AS initial_amount,
  CAST(JSON_VALUE(payload, '$.amount.createdAt.number') AS INT64) AS created_at_round,
  CAST(JSON_VALUE(payload, '$.amount.ratePerRound.rate') AS NUMERIC) AS rate_per_round
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:Amulet'
  AND template_id NOT LIKE '%LockedAmulet%'
  AND event_type = 'created';


-- ============================================================================
-- AMULET ARCHIVES - All Amulet consumption/archive history
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.amulet_archives` AS
SELECT
  event_id,
  update_id,
  contract_id,
  effective_at,
  migration_id
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:Amulet'
  AND template_id NOT LIKE '%LockedAmulet%'
  AND event_type = 'archived';


-- ============================================================================
-- DAILY MINT/BURN - Daily supply changes
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.daily_mint_burn` AS
WITH mints AS (
  SELECT
    DATE(effective_at) AS event_date,
    SUM(CAST(JSON_VALUE(payload, '$.amount.initialAmount') AS NUMERIC)) AS minted_amount,
    COUNT(*) AS mint_count
  FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
  WHERE template_id LIKE '%:Amulet'
    AND template_id NOT LIKE '%LockedAmulet%'
    AND event_type = 'created'
  GROUP BY 1
),
burns AS (
  SELECT
    DATE(effective_at) AS event_date,
    COUNT(*) AS burn_count
  FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
  WHERE template_id LIKE '%:Amulet'
    AND template_id NOT LIKE '%LockedAmulet%'
    AND event_type = 'archived'
  GROUP BY 1
)
SELECT
  COALESCE(m.event_date, b.event_date) AS event_date,
  COALESCE(m.minted_amount, 0) AS minted_amount,
  COALESCE(m.mint_count, 0) AS mint_count,
  COALESCE(b.burn_count, 0) AS burn_count
FROM mints m
FULL OUTER JOIN burns b ON m.event_date = b.event_date
ORDER BY event_date DESC;


-- ============================================================================
-- AMULET HOLDERS DISTRIBUTION - Holdings by owner
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.amulet_holdings` AS
SELECT
  JSON_VALUE(payload, '$.owner') AS owner_party,
  COUNT(*) AS amulet_count,
  SUM(CAST(JSON_VALUE(payload, '$.amount.initialAmount') AS NUMERIC)) AS total_initial_amount,
  MIN(CAST(JSON_VALUE(payload, '$.amount.createdAt.number') AS INT64)) AS earliest_round,
  MAX(CAST(JSON_VALUE(payload, '$.amount.createdAt.number') AS INT64)) AS latest_round,
  MIN(snapshot_time) AS snapshot_time
FROM `YOUR_PROJECT_ID.canton_ledger.acs_raw`
WHERE template_id LIKE '%:Amulet'
  AND template_id NOT LIKE '%LockedAmulet%'
GROUP BY 1
ORDER BY total_initial_amount DESC;


-- ============================================================================
-- TRANSFER COMMANDS - Splice.ExternalPartyAmuletRules:TransferCommand
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_transfer_commands` AS
SELECT
  event_id,
  update_id,
  contract_id,
  event_type,
  effective_at,
  migration_id,
  JSON_VALUE(payload, '$.sender') AS sender_party,
  JSON_VALUE(payload, '$.provider') AS provider_party,
  JSON_VALUE(payload, '$.receiverParty') AS receiver_party,
  CAST(JSON_VALUE(payload, '$.nonce') AS INT64) AS nonce,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:TransferCommand';


-- ============================================================================
-- TRANSFER COUNTERS - Track transfer activity
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.parsed_transfer_counters` AS
SELECT
  event_id,
  update_id,
  contract_id,
  event_type,
  effective_at,
  JSON_VALUE(payload, '$.sender') AS sender_party,
  JSON_VALUE(payload, '$.provider') AS provider_party,
  CAST(JSON_VALUE(payload, '$.nextNonce') AS INT64) AS next_nonce,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:TransferCommandCounter';


-- ============================================================================
-- LOCKED AMULET ANALYSIS
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.locked_amulet_analysis` AS
SELECT
  contract_id,
  event_id,
  event_type,
  effective_at,
  JSON_VALUE(payload, '$.amulet.owner') AS owner_party,
  CAST(JSON_VALUE(payload, '$.amulet.amount.initialAmount') AS NUMERIC) AS locked_amount,
  CAST(JSON_VALUE(payload, '$.lock.expiresAt.number') AS INT64) AS expires_at_round,
  JSON_VALUE_ARRAY(payload, '$.lock.holders') AS lock_holders,
  payload
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:LockedAmulet';


-- ============================================================================
-- SUPPLY OVER TIME - Cumulative supply tracking
-- ============================================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.canton_ledger.supply_over_time` AS
SELECT
  DATE(effective_at) AS event_date,
  SUM(CASE 
    WHEN event_type = 'created' THEN CAST(JSON_VALUE(payload, '$.amount.initialAmount') AS NUMERIC)
    ELSE 0 
  END) AS daily_minted,
  COUNT(CASE WHEN event_type = 'created' THEN 1 END) AS contracts_created,
  COUNT(CASE WHEN event_type = 'archived' THEN 1 END) AS contracts_archived,
  -- Running totals would need window functions over ordered data
  COUNT(*) AS total_events
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE template_id LIKE '%:Amulet'
  AND template_id NOT LIKE '%LockedAmulet%'
GROUP BY 1
ORDER BY event_date;
