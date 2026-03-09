-- ============================================================================
-- SILVER LAYER: Amulet (Core Currency) Tables
-- ============================================================================
-- Fully parsed, typed, partitioned tables - no JSON at query time
-- ============================================================================

-- ============================================================================
-- AMULET CONTRACTS - Parsed from created events
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.amulet_contracts`
PARTITION BY DATE(effective_at)
CLUSTER BY owner_party, created_at_round
AS
SELECT
  -- Event metadata
  event_id,
  update_id,
  contract_id,
  migration_id,
  synchronizer_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  CAST(timestamp AS TIMESTAMP) AS ingested_at,
  event_type,
  
  -- Parsed payload fields (fully typed)
  CAST(JSON_VALUE(payload, '$.dso') AS STRING) AS dso_party,
  CAST(JSON_VALUE(payload, '$.owner') AS STRING) AS owner_party,
  
  -- Amount structure - fully parsed
  CAST(JSON_VALUE(payload, '$.amount.initialAmount') AS NUMERIC) AS initial_amount,
  CAST(JSON_VALUE(payload, '$.amount.createdAt.number') AS INT64) AS created_at_round,
  CAST(JSON_VALUE(payload, '$.amount.ratePerRound.rate') AS NUMERIC) AS holding_fee_rate,
  
  -- Signatories as array
  ARRAY(
    SELECT CAST(s AS STRING)
    FROM UNNEST(JSON_VALUE_ARRAY(raw_event, '$.created_event.signatories')) AS s
  ) AS signatories,
  
  -- Observers as array  
  ARRAY(
    SELECT CAST(o AS STRING)
    FROM UNNEST(JSON_VALUE_ARRAY(raw_event, '$.created_event.observers')) AS o
  ) AS observers

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'created'
  AND (template_id LIKE '%Splice.Amulet:Amulet' OR template_id LIKE '%:Amulet')
  AND template_id NOT LIKE '%LockedAmulet%';


-- ============================================================================
-- AMULET ARCHIVES - When amulets are consumed/burned
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.amulet_archives`
PARTITION BY DATE(effective_at)
CLUSTER BY contract_id
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  synchronizer_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  CAST(timestamp AS TIMESTAMP) AS ingested_at
  
FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'archived'
  AND (template_id LIKE '%Splice.Amulet:Amulet' OR template_id LIKE '%:Amulet')
  AND template_id NOT LIKE '%LockedAmulet%';


-- ============================================================================
-- LOCKED AMULET CONTRACTS - Time-locked tokens
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.locked_amulet_contracts`
PARTITION BY DATE(effective_at)
CLUSTER BY owner_party
AS
SELECT
  event_id,
  update_id,
  contract_id,
  migration_id,
  CAST(effective_at AS TIMESTAMP) AS effective_at,
  event_type,
  
  -- DSO
  CAST(JSON_VALUE(payload, '$.dso') AS STRING) AS dso_party,
  
  -- Nested amulet structure
  CAST(JSON_VALUE(payload, '$.amulet.owner') AS STRING) AS owner_party,
  CAST(JSON_VALUE(payload, '$.amulet.amount.initialAmount') AS NUMERIC) AS locked_amount,
  CAST(JSON_VALUE(payload, '$.amulet.amount.createdAt.number') AS INT64) AS created_at_round,
  CAST(JSON_VALUE(payload, '$.amulet.amount.ratePerRound.rate') AS NUMERIC) AS holding_fee_rate,
  
  -- Lock structure
  ARRAY(
    SELECT CAST(h AS STRING)
    FROM UNNEST(JSON_VALUE_ARRAY(payload, '$.lock.holders')) AS h
  ) AS lock_holders,
  CAST(JSON_VALUE(payload, '$.lock.expiresAt.number') AS INT64) AS expires_at_round,
  
  -- Lock type detection
  CASE 
    WHEN JSON_VALUE(payload, '$.lock.expiresAt') IS NOT NULL THEN 'time_locked'
    WHEN ARRAY_LENGTH(JSON_VALUE_ARRAY(payload, '$.lock.holders')) > 0 THEN 'holder_locked'
    ELSE 'unknown'
  END AS lock_type

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE event_type = 'created'
  AND (template_id LIKE '%:LockedAmulet');


-- ============================================================================
-- AMULET LIFECYCLE - Complete contract lifecycle view
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.amulet_lifecycle`
PARTITION BY DATE(created_at)
CLUSTER BY owner_party, contract_id
AS
WITH creates AS (
  SELECT
    contract_id,
    event_id AS create_event_id,
    update_id AS create_update_id,
    effective_at AS created_at,
    CAST(JSON_VALUE(payload, '$.owner') AS STRING) AS owner_party,
    CAST(JSON_VALUE(payload, '$.amount.initialAmount') AS NUMERIC) AS initial_amount,
    CAST(JSON_VALUE(payload, '$.amount.createdAt.number') AS INT64) AS created_at_round,
    CAST(JSON_VALUE(payload, '$.amount.ratePerRound.rate') AS NUMERIC) AS holding_fee_rate,
    migration_id
  FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
  WHERE event_type = 'created'
    AND (template_id LIKE '%:Amulet')
    AND template_id NOT LIKE '%LockedAmulet%'
),
archives AS (
  SELECT
    contract_id,
    event_id AS archive_event_id,
    update_id AS archive_update_id,
    effective_at AS archived_at
  FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
  WHERE event_type = 'archived'
    AND (template_id LIKE '%:Amulet')
    AND template_id NOT LIKE '%LockedAmulet%'
)
SELECT
  c.contract_id,
  c.create_event_id,
  c.create_update_id,
  c.created_at,
  c.owner_party,
  c.initial_amount,
  c.created_at_round,
  c.holding_fee_rate,
  c.migration_id,
  a.archive_event_id,
  a.archive_update_id,
  a.archived_at,
  CASE WHEN a.contract_id IS NOT NULL THEN 'archived' ELSE 'active' END AS status,
  TIMESTAMP_DIFF(COALESCE(a.archived_at, CURRENT_TIMESTAMP()), c.created_at, SECOND) AS lifespan_seconds
FROM creates c
LEFT JOIN archives a ON c.contract_id = a.contract_id;


-- ============================================================================
-- DAILY SUPPLY METRICS - Aggregated supply changes
-- ============================================================================
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.canton_silver.daily_supply_metrics`
PARTITION BY supply_date
AS
SELECT
  DATE(effective_at) AS supply_date,
  migration_id,
  
  -- Minting metrics
  COUNTIF(event_type = 'created') AS contracts_minted,
  SUM(CASE 
    WHEN event_type = 'created' 
    THEN CAST(JSON_VALUE(payload, '$.amount.initialAmount') AS NUMERIC)
    ELSE 0 
  END) AS amount_minted,
  
  -- Burning metrics  
  COUNTIF(event_type = 'archived') AS contracts_burned,
  
  -- Unique owners
  COUNT(DISTINCT CASE 
    WHEN event_type = 'created' 
    THEN JSON_VALUE(payload, '$.owner')
  END) AS unique_mint_recipients,
  
  -- Round range
  MIN(CAST(JSON_VALUE(payload, '$.amount.createdAt.number') AS INT64)) AS min_round,
  MAX(CAST(JSON_VALUE(payload, '$.amount.createdAt.number') AS INT64)) AS max_round

FROM `YOUR_PROJECT_ID.canton_ledger.events_raw`
WHERE (template_id LIKE '%:Amulet')
  AND template_id NOT LIKE '%LockedAmulet%'
GROUP BY 1, 2;
