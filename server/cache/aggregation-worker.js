/**
 * Background worker for refreshing aggregated statistics
 * Called periodically and after ACS snapshots complete
 */

import db from '../duckdb/connection.js';
import { setCache, invalidateCache } from './stats-cache.js';
import path from 'path';
import fs from 'fs';

// ACS data path - use the centralized path from duckdb connection
const ACS_DATA_PATH = db.ACS_DATA_PATH;

// Cache TTL for pre-computed aggregations (longer since we control refresh)
const AGGREGATION_TTL = 30 * 60 * 1000; // 30 minutes

function findACSFiles() {
  try {
    if (!fs.existsSync(ACS_DATA_PATH)) return [];
    const allFiles = fs.readdirSync(ACS_DATA_PATH, { recursive: true });
    return allFiles
      .map(f => String(f))
      .filter(f => f.endsWith('.jsonl') || f.endsWith('.jsonl.gz') || f.endsWith('.jsonl.zst'))
      .map(f => path.join(ACS_DATA_PATH, f).replace(/\\/g, '/'));
  } catch {
    return [];
  }
}

function getACSSource() {
  const files = findACSFiles();
  if (files.length === 0) {
    return `(SELECT NULL as placeholder WHERE false)`;
  }
  
  if (files.length <= 100) {
    const selects = files.map(f => 
      `SELECT * FROM read_json_auto('${f}', union_by_name=true, ignore_errors=true)`
    );
    return `(${selects.join(' UNION ALL ')})`;
  }
  
  const hasJsonl = files.some(f => f.endsWith('.jsonl') && !f.endsWith('.jsonl.gz') && !f.endsWith('.jsonl.zst'));
  const hasGz = files.some(f => f.endsWith('.jsonl.gz'));
  const hasZst = files.some(f => f.endsWith('.jsonl.zst'));
  const acsPath = ACS_DATA_PATH.replace(/\\/g, '/');
  
  const parts = [];
  if (hasJsonl) parts.push(`SELECT * FROM read_json_auto('${acsPath}/**/*.jsonl', union_by_name=true, ignore_errors=true)`);
  if (hasGz) parts.push(`SELECT * FROM read_json_auto('${acsPath}/**/*.jsonl.gz', union_by_name=true, ignore_errors=true)`);
  if (hasZst) parts.push(`SELECT * FROM read_json_auto('${acsPath}/**/*.jsonl.zst', union_by_name=true, ignore_errors=true)`);
  
  return parts.length > 0 ? `(${parts.join(' UNION ALL ')})` : `(SELECT NULL as placeholder WHERE false)`;
}

/**
 * Refresh holder balances aggregation
 */
async function refreshHolderBalances() {
  const acsSource = getACSSource();
  
  const sql = `
    WITH latest_snapshot AS (
      SELECT MAX(snapshot_time) as snapshot_time FROM ${acsSource}
    ),
    amulet_balances AS (
      SELECT 
        json_extract_string(payload, '$.owner') as owner,
        CAST(COALESCE(json_extract_string(payload, '$.amount.initialAmount'), '0') AS DOUBLE) as amount
      FROM ${acsSource} acs
      WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
        AND (entity_name = 'Amulet' OR template_id LIKE '%:Amulet:%' OR template_id LIKE '%:Amulet')
        AND json_extract_string(payload, '$.owner') IS NOT NULL
    ),
    locked_balances AS (
      SELECT 
        COALESCE(json_extract_string(payload, '$.amulet.owner'), json_extract_string(payload, '$.owner')) as owner,
        CAST(COALESCE(
          json_extract_string(payload, '$.amulet.amount.initialAmount'),
          json_extract_string(payload, '$.amount.initialAmount'),
          '0'
        ) AS DOUBLE) as amount
      FROM ${acsSource} acs
      WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
        AND (entity_name = 'LockedAmulet' OR template_id LIKE '%:LockedAmulet:%' OR template_id LIKE '%:LockedAmulet')
        AND (json_extract_string(payload, '$.amulet.owner') IS NOT NULL OR json_extract_string(payload, '$.owner') IS NOT NULL)
    ),
    combined AS (
      SELECT owner, amount, 0.0 as locked FROM amulet_balances
      UNION ALL
      SELECT owner, 0.0 as amount, amount as locked FROM locked_balances
    )
    SELECT 
      owner,
      SUM(amount) as unlocked_balance,
      SUM(locked) as locked_balance,
      SUM(amount) + SUM(locked) as total_balance
    FROM combined
    WHERE owner IS NOT NULL AND owner != ''
    GROUP BY owner
    ORDER BY total_balance DESC
  `;

  const rows = await db.safeQuery(sql);
  
  // Also compute totals
  const totalSupply = rows.reduce((sum, r) => sum + r.total_balance, 0);
  const unlockedSupply = rows.reduce((sum, r) => sum + r.unlocked_balance, 0);
  const lockedSupply = rows.reduce((sum, r) => sum + r.locked_balance, 0);
  
  const result = {
    holders: rows,
    totalSupply,
    unlockedSupply,
    lockedSupply,
    holderCount: rows.length,
    refreshedAt: new Date().toISOString(),
  };
  
  setCache('aggregation:holder-balances', result, AGGREGATION_TTL);
  return result;
}

/**
 * Refresh supply totals
 */
async function refreshSupplyTotals() {
  const acsSource = getACSSource();
  
  const sql = `
    WITH latest_snapshot AS (
      SELECT MAX(snapshot_time) as snapshot_time FROM ${acsSource}
    ),
    amulet_total AS (
      SELECT COALESCE(SUM(
        CAST(COALESCE(json_extract_string(payload, '$.amount.initialAmount'), '0') AS DOUBLE)
      ), 0) as total
      FROM ${acsSource} acs
      WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
        AND (entity_name = 'Amulet' OR template_id LIKE '%:Amulet:%')
    ),
    locked_total AS (
      SELECT COALESCE(SUM(
        CAST(COALESCE(
          json_extract_string(payload, '$.amulet.amount.initialAmount'),
          json_extract_string(payload, '$.amount.initialAmount'),
          '0'
        ) AS DOUBLE)
      ), 0) as total
      FROM ${acsSource} acs
      WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
        AND (entity_name = 'LockedAmulet' OR template_id LIKE '%:LockedAmulet:%')
    )
    SELECT 
      amulet_total.total as unlocked_supply,
      locked_total.total as locked_supply,
      amulet_total.total + locked_total.total as total_supply
    FROM amulet_total, locked_total
  `;

  const rows = await db.safeQuery(sql);
  const result = {
    ...rows[0],
    refreshedAt: new Date().toISOString(),
  };
  
  setCache('aggregation:supply-totals', result, AGGREGATION_TTL);
  return result;
}

/**
 * Refresh mining rounds summary
 */
async function refreshMiningRounds() {
  const acsSource = getACSSource();
  
  const sql = `
    WITH latest_snapshot AS (
      SELECT MAX(snapshot_time) as snapshot_time FROM ${acsSource}
    ),
    all_rounds AS (
      SELECT 
        contract_id,
        entity_name,
        template_id,
        -- Try multiple extraction paths for round number
        COALESCE(
          NULLIF(json_extract_string(payload, '$.round.number'), ''),
          NULLIF(CAST(json_extract(payload, '$.round.number') AS VARCHAR), ''),
          NULLIF(json_extract_string(payload, '$.round'), ''),
          NULLIF(CAST(json_extract(payload, '$.round') AS VARCHAR), '')
        ) as round_number,
        json_extract_string(payload, '$.opensAt') as opens_at,
        json_extract_string(payload, '$.targetClosesAt') as target_closes_at,
        json_extract_string(payload, '$.amuletPrice') as amulet_price,
        payload
      FROM ${acsSource} acs
      WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
        AND (entity_name IN ('OpenMiningRound', 'IssuingMiningRound', 'ClosedMiningRound')
             OR template_id LIKE '%MiningRound%')
    )
    SELECT * FROM all_rounds
    ORDER BY entity_name, CAST(COALESCE(NULLIF(round_number, ''), '0') AS BIGINT) DESC
  `;

  const rows = await db.safeQuery(sql);
  
  const openRounds = rows.filter(r => r.entity_name === 'OpenMiningRound' || r.template_id?.includes('OpenMiningRound'));
  const issuingRounds = rows.filter(r => r.entity_name === 'IssuingMiningRound' || r.template_id?.includes('IssuingMiningRound'));
  const closedRounds = rows.filter(r => r.entity_name === 'ClosedMiningRound' || r.template_id?.includes('ClosedMiningRound'));
  
  const result = {
    openRounds,
    issuingRounds,
    closedRounds,
    counts: {
      open: openRounds.length,
      issuing: issuingRounds.length,
      closed: closedRounds.length,
    },
    refreshedAt: new Date().toISOString(),
  };
  
  setCache('aggregation:mining-rounds', result, AGGREGATION_TTL);
  return result;
}

/**
 * Refresh all aggregations
 */
export async function refreshAllAggregations() {
  const files = findACSFiles();
  if (files.length === 0) {
    console.log('⏭️ No ACS files found, skipping aggregation refresh');
    return;
  }
  
  console.log('🔄 Refreshing all aggregations...');
  const startTime = Date.now();
  
  try {
    // Run all aggregations in parallel
    const [holders, supply, rounds] = await Promise.all([
      refreshHolderBalances().catch(err => {
        console.error('Failed to refresh holder balances:', err.message);
        return null;
      }),
      refreshSupplyTotals().catch(err => {
        console.error('Failed to refresh supply totals:', err.message);
        return null;
      }),
      refreshMiningRounds().catch(err => {
        console.error('Failed to refresh mining rounds:', err.message);
        return null;
      }),
    ]);
    
    const duration = Date.now() - startTime;
    console.log(`✅ Aggregations refreshed in ${duration}ms`);
    console.log(`   - Holders: ${holders?.holderCount || 0}`);
    console.log(`   - Supply: ${supply?.total_supply?.toFixed(2) || 0} CC`);
    console.log(`   - Rounds: ${rounds?.counts?.open || 0} open, ${rounds?.counts?.issuing || 0} issuing, ${rounds?.counts?.closed || 0} closed`);
    
    return { holders, supply, rounds, duration };
  } catch (err) {
    console.error('❌ Aggregation refresh failed:', err.message);
    throw err;
  }
}

/**
 * Invalidate all ACS-related caches (call after new snapshot)
 */
export function invalidateACSCache() {
  invalidateCache('aggregation:');
  invalidateCache('acs:');
  console.log('🗑️ ACS caches invalidated');
}

export default { 
  refreshAllAggregations, 
  invalidateACSCache,
  refreshHolderBalances,
  refreshSupplyTotals,
  refreshMiningRounds,
};
