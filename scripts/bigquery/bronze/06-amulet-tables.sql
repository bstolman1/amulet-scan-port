-- Amulet/Currency Data Parsing
-- Amulet creation, archive, supply, transfer, and locked analysis
-- (ACS-dependent views removed — no ACS data in current GCS layout)

-- Amulet Creation Events
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.amulet_creations` AS
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
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE template_id LIKE '%:Amulet'
  AND template_id NOT LIKE '%LockedAmulet%'
  AND event_type = 'created';


-- Amulet Archives
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.amulet_archives` AS
SELECT
  event_id,
  update_id,
  contract_id,
  effective_at,
  migration_id
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE template_id LIKE '%:Amulet'
  AND template_id NOT LIKE '%LockedAmulet%'
  AND event_type = 'archived';


-- Daily Mint/Burn
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.daily_mint_burn` AS
WITH mints AS (
  SELECT
    DATE(effective_at) AS event_date,
    SUM(CAST(JSON_VALUE(payload, '$.amount.initialAmount') AS NUMERIC)) AS minted_amount,
    COUNT(*) AS mint_count
  FROM `${PROJECT_ID}.canton_ledger.events_raw`
  WHERE template_id LIKE '%:Amulet'
    AND template_id NOT LIKE '%LockedAmulet%'
    AND event_type = 'created'
  GROUP BY 1
),
burns AS (
  SELECT
    DATE(effective_at) AS event_date,
    COUNT(*) AS burn_count
  FROM `${PROJECT_ID}.canton_ledger.events_raw`
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


-- Transfer Commands
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.parsed_transfer_commands` AS
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
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE template_id LIKE '%:TransferCommand';


-- Transfer Counters
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.parsed_transfer_counters` AS
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
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE template_id LIKE '%:TransferCommandCounter';


-- Locked Amulet Analysis
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.locked_amulet_analysis` AS
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
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE template_id LIKE '%:LockedAmulet';


-- Supply Over Time
CREATE OR REPLACE VIEW `${PROJECT_ID}.canton_ledger.supply_over_time` AS
SELECT
  DATE(effective_at) AS event_date,
  SUM(CASE
    WHEN event_type = 'created' THEN CAST(JSON_VALUE(payload, '$.amount.initialAmount') AS NUMERIC)
    ELSE 0
  END) AS daily_minted,
  COUNT(CASE WHEN event_type = 'created' THEN 1 END) AS contracts_created,
  COUNT(CASE WHEN event_type = 'archived' THEN 1 END) AS contracts_archived,
  COUNT(*) AS total_events
FROM `${PROJECT_ID}.canton_ledger.events_raw`
WHERE template_id LIKE '%:Amulet'
  AND template_id NOT LIKE '%LockedAmulet%'
GROUP BY 1
ORDER BY event_date;
