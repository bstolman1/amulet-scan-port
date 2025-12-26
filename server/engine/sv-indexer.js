/**
 * SV Membership Indexer
 * 
 * Tracks Super Validator membership using ONLY SvOnboardingConfirmed template.
 * 
 * This is the ONLY authoritative source of SV membership:
 * - Splice.SvOnboarding:SvOnboardingConfirmed
 * 
 * ❌ EXPLICITLY DO NOT:
 * - Scan DsoRules files
 * - Infer SVs from votes or DsoRules
 * - Hardcode thresholds (9 / 5)
 * - Infer SV count from current network state
 * - Scan 70k DSO files
 * 
 * ✅ CORRECT BEHAVIOR:
 * - Uses template-file-index to scan only ~10 files containing SvOnboardingConfirmed
 * - Builds SV active intervals (active_from, active_until)
 * - active_from = created_at of SvOnboardingConfirmed
 * - active_until = archive time OR SvOnboardingConfirmed_Expire exercise time OR ∞
 * - Exposes getActiveSvCountAt(timestamp) for dynamic voting thresholds
 */

import { query, queryOne, DATA_PATH } from '../duckdb/connection.js';
import * as binaryReader from '../duckdb/binary-reader.js';
import { getFilesForTemplate, isTemplateIndexPopulated } from './template-file-index.js';

let indexingInProgress = false;
let indexingProgress = null;

/**
 * Ensure SV membership tables exist with active interval schema
 */
export async function ensureSvTables() {
  // SV active intervals table - tracks when each SV was active
  await query(`
    CREATE TABLE IF NOT EXISTS sv_active_intervals (
      sv_party VARCHAR NOT NULL,
      sv_name VARCHAR,
      sv_reward_weight INTEGER DEFAULT 1,
      sv_participant_id VARCHAR,
      contract_id VARCHAR PRIMARY KEY,
      active_from TIMESTAMP NOT NULL,
      active_until TIMESTAMP,
      dso VARCHAR,
      reason VARCHAR,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Create unique index for upserts
  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_sv_active_intervals_contract
    ON sv_active_intervals(contract_id)
  `);

  // Index for fast lookups by sv_party
  await query(`
    CREATE INDEX IF NOT EXISTS idx_sv_active_intervals_party
    ON sv_active_intervals(sv_party)
  `);

  // Index for time-based queries
  await query(`
    CREATE INDEX IF NOT EXISTS idx_sv_active_intervals_time
    ON sv_active_intervals(active_from, active_until)
  `);

  // Drop old sv_index_state table if it has wrong schema and recreate
  try {
    // Check if files_scanned column exists
    await query(`SELECT files_scanned FROM sv_index_state LIMIT 1`);
  } catch (err) {
    if (err.message?.includes('files_scanned')) {
      console.log('   Migrating sv_index_state table to new schema...');
      await query(`DROP TABLE IF EXISTS sv_index_state`);
    }
  }

  // Index state with correct schema
  await query(`
    CREATE TABLE IF NOT EXISTS sv_index_state (
      id INTEGER PRIMARY KEY DEFAULT 1,
      last_indexed_at TIMESTAMP,
      total_svs INTEGER DEFAULT 0,
      files_scanned INTEGER DEFAULT 0
    )
  `);
  
  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_sv_index_state_id
    ON sv_index_state(id)
  `);
}

/**
 * Get SV membership index statistics
 */
export async function getSvIndexStats() {
  try {
    await ensureSvTables();
    
    const totalIntervals = await queryOne(`SELECT COUNT(*) as count FROM sv_active_intervals`);
    const currentActive = await queryOne(`
      SELECT COUNT(DISTINCT sv_party) as count 
      FROM sv_active_intervals 
      WHERE active_until IS NULL OR active_until > CURRENT_TIMESTAMP
    `);
    const state = await queryOne(`SELECT last_indexed_at, total_svs, files_scanned FROM sv_index_state WHERE id = 1`);
    
    return {
      totalIntervals: Number(totalIntervals?.count || 0),
      currentActiveSvs: Number(currentActive?.count || 0),
      lastIndexedAt: state?.last_indexed_at || null,
      totalSvs: Number(state?.total_svs || 0),
      filesScanned: Number(state?.files_scanned || 0),
      isPopulated: Number(totalIntervals?.count || 0) > 0,
      isIndexing: indexingInProgress,
      indexing: indexingInProgress ? indexingProgress : null,
    };
  } catch (err) {
    console.error('Error getting SV index stats:', err);
    return { totalIntervals: 0, currentActiveSvs: 0, isPopulated: false, isIndexing: indexingInProgress, indexing: null };
  }
}

/**
 * Get count of active SVs at a specific point in time.
 * This is the ONLY function vote-request-indexer should use for threshold calculation.
 * 
 * @param {Date|string} dateTime - The timestamp to check
 * @returns {number} Count of active SVs at that time
 */
export async function getActiveSvCountAt(dateTime) {
  try {
    await ensureSvTables();
    
    const timestamp = new Date(dateTime).toISOString();
    
    // Count unique SVs that were active at this timestamp
    // active_from <= timestamp AND (active_until IS NULL OR active_until > timestamp)
    const result = await queryOne(`
      SELECT COUNT(DISTINCT sv_party) as count
      FROM sv_active_intervals
      WHERE active_from <= '${timestamp}'
        AND (active_until IS NULL OR active_until > '${timestamp}')
    `);
    
    return Number(result?.count || 0);
  } catch (err) {
    console.error('Error getting SV count at date:', err);
    return 0;
  }
}

/**
 * Legacy alias for backwards compatibility
 */
export async function getSvCountAt(dateTime) {
  return getActiveSvCountAt(dateTime);
}

/**
 * Get list of active SVs at a specific date/time with details
 */
export async function getActiveSvsAt(dateTime) {
  try {
    await ensureSvTables();
    
    const timestamp = new Date(dateTime).toISOString();
    
    const result = await query(`
      SELECT sv_party, sv_name, sv_reward_weight, active_from, active_until, contract_id
      FROM sv_active_intervals
      WHERE active_from <= '${timestamp}'
        AND (active_until IS NULL OR active_until > '${timestamp}')
      ORDER BY active_from ASC
    `);
    
    return result.map(r => ({
      svParty: r.sv_party,
      svName: r.sv_name,
      svRewardWeight: Number(r.sv_reward_weight || 1),
      activeFrom: r.active_from,
      activeUntil: r.active_until,
      contractId: r.contract_id,
    }));
  } catch (err) {
    console.error('Error getting active SVs at date:', err);
    return [];
  }
}

/**
 * Get SV membership timeline (all intervals)
 */
export async function getSvMembershipTimeline(limit = 100) {
  try {
    await ensureSvTables();
    
    const intervals = await query(`
      SELECT sv_party, sv_name, sv_reward_weight, 
             active_from, active_until, contract_id, dso, reason
      FROM sv_active_intervals
      ORDER BY active_from DESC
      LIMIT ${limit}
    `);
    
    return intervals.map(e => ({
      svParty: e.sv_party,
      svName: e.sv_name,
      svRewardWeight: Number(e.sv_reward_weight || 1),
      activeFrom: e.active_from,
      activeUntil: e.active_until,
      contractId: e.contract_id,
      dso: e.dso,
      reason: e.reason,
    }));
  } catch (err) {
    console.error('Error getting SV timeline:', err);
    return [];
  }
}

/**
 * Calculate voting threshold based on SV count
 * Canton governance requires 2/3 majority (rounded up) for pass/reject
 * 
 * ❌ DO NOT hardcode thresholds like 9 or 5
 * ✅ ALWAYS use ceil(2/3 * svCount)
 */
export function calculateVotingThreshold(svCount) {
  // 2/3 majority rounded up - this is the threshold for both accept AND reject
  const twoThirdsThreshold = Math.ceil((svCount * 2) / 3);
  
  return {
    svCount,
    twoThirdsThreshold,  // Votes needed to pass OR reject
    // Simple majority for reference
    simpleMajority: Math.floor(svCount / 2) + 1,
  };
}

/**
 * Build SV membership index by scanning ONLY SvOnboardingConfirmed files
 * 
 * Uses template-file-index to find only the ~10 files containing this template.
 * Does NOT scan DsoRules or any other files.
 */
export async function buildSvMembershipIndex({ force = false } = {}) {
  if (indexingInProgress) {
    console.log('⏳ SV membership indexing already in progress');
    return { status: 'in_progress' };
  }

  indexingInProgress = true;
  indexingProgress = { phase: 'starting', current: 0, total: 0 };
  console.log('\n👥 Starting SV membership index build...');
  console.log('   Source: ONLY Splice.SvOnboarding:SvOnboardingConfirmed');

  try {
    await ensureSvTables();

    // Check if already populated
    if (!force) {
      const stats = await getSvIndexStats();
      if (stats.totalIntervals > 0) {
        console.log(`✅ SV index already populated (${stats.totalIntervals} intervals), use force=true to rebuild`);
        indexingInProgress = false;
        return { status: 'skipped', intervals: stats.totalIntervals };
      }
    }

    if (force) {
      console.log('   🗑️ Force rebuild - clearing existing SV data...');
      await query(`DELETE FROM sv_active_intervals`);
    }

    // Check if template index is available
    const templateIndexPopulated = await isTemplateIndexPopulated();
    
    if (!templateIndexPopulated) {
      console.log('   ⚠️ Template index not available, build it first for fast SV indexing');
      indexingInProgress = false;
      return { status: 'error', message: 'Template index required. Build template index first.' };
    }

    // Get ONLY files containing SvOnboardingConfirmed - should be ~10 files
    const svOnboardingFiles = await getFilesForTemplate('SvOnboardingConfirmed');
    console.log(`   📂 Found ${svOnboardingFiles.length} files with SvOnboardingConfirmed events`);
    
    if (svOnboardingFiles.length === 0) {
      console.log('   ⚠️ No SvOnboardingConfirmed files found in template index');
      indexingInProgress = false;
      return { status: 'error', message: 'No SvOnboardingConfirmed files found' };
    }

    indexingProgress = { phase: 'scanning', current: 0, total: svOnboardingFiles.length };

    // Map to track SV intervals by contract_id
    const svIntervals = new Map();
    
    // Helper to check if template_id matches SvOnboardingConfirmed (handles @hash suffixes)
    const isSvOnboardingConfirmed = (templateId) => {
      if (!templateId) return false;
      // Match "SvOnboardingConfirmed" anywhere, handling @hash suffixes like @123abc
      // Template format: "Splice.SvOnboarding:SvOnboardingConfirmed@abc123"
      return templateId.includes('SvOnboardingConfirmed');
    };
    
    let svOnboardingEventsFound = 0;
    
    // Scan all SvOnboardingConfirmed files
    for (let i = 0; i < svOnboardingFiles.length; i++) {
      indexingProgress.current = i + 1;
      const filePath = svOnboardingFiles[i];
      
      try {
        const result = await binaryReader.readBinaryFile(filePath);
        const events = result.records || result || [];
        
        // Filter to only SvOnboardingConfirmed events first
        const svEvents = events.filter(e => isSvOnboardingConfirmed(e.template_id));
        
        // Debug: log first file's SvOnboardingConfirmed events
        if (i === 0) {
          console.log(`   🔍 Debug - File has ${events.length} total events, ${svEvents.length} SvOnboardingConfirmed`);
          if (svEvents.length > 0) {
            const sample = svEvents.slice(0, 3);
            for (const e of sample) {
              console.log(`      type=${e.type}, template=${e.template_id?.split(':').pop()}, svParty=${e.payload?.svParty?.slice(0,20) || 'N/A'}`);
            }
          }
        }
        
        svOnboardingEventsFound += svEvents.length;
        
        for (const event of svEvents) {
          const contractId = event.contract_id;
          if (!contractId) continue;
          
          if (event.type === 'created') {
            // SV onboarding - set active_from
            const payload = event.payload || {};
            const svParty = payload.svParty || null;
            const svName = payload.svName || null;
            
            if (!svParty) continue;
            
            svIntervals.set(contractId, {
              sv_party: svParty,
              sv_name: svName,
              sv_reward_weight: payload.svRewardWeight || 1,
              sv_participant_id: payload.svParticipantId || null,
              contract_id: contractId,
              active_from: event.effective_at || event.ledger_time || new Date().toISOString(),
              active_until: null, // Active until archived/expired
              dso: payload.dso || null,
              reason: payload.reason ? String(payload.reason).slice(0, 500) : null,
            });
          } else if (event.type === 'archived' || (event.type === 'exercised' && event.consuming === true)) {
            // SV offboarding or expiry - set active_until
            // Only consuming exercised events terminate the contract
            const existing = svIntervals.get(contractId);
            if (existing) {
              existing.active_until = event.effective_at || event.ledger_time || new Date().toISOString();
            } else {
              // We saw archive before create - will be merged later if create appears
              svIntervals.set(contractId, {
                sv_party: null,
                sv_name: null,
                sv_reward_weight: 1,
                sv_participant_id: null,
                contract_id: contractId,
                active_from: null,
                active_until: event.effective_at || event.ledger_time || new Date().toISOString(),
                dso: null,
                reason: null,
              });
            }
          }
        }
      } catch (fileErr) {
        console.warn(`   ⚠️ Error reading ${filePath}:`, fileErr.message);
      }
    }

    console.log(`   📊 Found ${svOnboardingEventsFound} SvOnboardingConfirmed events → ${svIntervals.size} SV intervals`);

    // Insert all intervals into the database
    let insertedCount = 0;
    for (const interval of svIntervals.values()) {
      // Skip intervals without sv_party (incomplete data)
      if (!interval.sv_party || !interval.active_from) continue;
      
      try {
        await query(`
          INSERT INTO sv_active_intervals 
            (sv_party, sv_name, sv_reward_weight, sv_participant_id, contract_id, 
             active_from, active_until, dso, reason)
          VALUES (
            '${interval.sv_party.replace(/'/g, "''")}',
            ${interval.sv_name ? `'${interval.sv_name.replace(/'/g, "''")}'` : 'NULL'},
            ${interval.sv_reward_weight},
            ${interval.sv_participant_id ? `'${interval.sv_participant_id.replace(/'/g, "''")}'` : 'NULL'},
            '${interval.contract_id.replace(/'/g, "''")}',
            '${interval.active_from}',
            ${interval.active_until ? `'${interval.active_until}'` : 'NULL'},
            ${interval.dso ? `'${interval.dso.replace(/'/g, "''")}'` : 'NULL'},
            ${interval.reason ? `'${interval.reason.replace(/'/g, "''")}'` : 'NULL'}
          )
          ON CONFLICT (contract_id) DO UPDATE SET
            active_until = EXCLUDED.active_until
        `);
        insertedCount++;
      } catch (insertErr) {
        console.warn(`   ⚠️ Insert error for ${interval.contract_id}:`, insertErr.message);
      }
    }

    // Calculate current SV count
    const currentSvCount = await getActiveSvCountAt(new Date());
    
    // Update index state
    const now = new Date().toISOString();
    await query(`
      INSERT INTO sv_index_state (id, last_indexed_at, total_svs, files_scanned)
      VALUES (1, '${now}', ${currentSvCount}, ${svOnboardingFiles.length})
      ON CONFLICT (id) DO UPDATE SET
        last_indexed_at = '${now}',
        total_svs = ${currentSvCount},
        files_scanned = ${svOnboardingFiles.length}
    `);

    console.log(`\n✅ SV membership index complete:`);
    console.log(`   - Files scanned: ${svOnboardingFiles.length} (ONLY SvOnboardingConfirmed)`);
    console.log(`   - SV intervals indexed: ${insertedCount}`);
    console.log(`   - Current active SVs: ${currentSvCount}`);

    indexingInProgress = false;
    indexingProgress = null;
    
    return {
      status: 'complete',
      filesScanned: svOnboardingFiles.length,
      intervalsIndexed: insertedCount,
      currentActiveSvs: currentSvCount,
    };
  } catch (err) {
    console.error('❌ SV membership index build failed:', err);
    indexingInProgress = false;
    indexingProgress = null;
    throw err;
  }
}

export function isIndexingInProgress() {
  return indexingInProgress;
}

export function getIndexingProgress() {
  return indexingProgress;
}
