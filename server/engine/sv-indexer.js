/**
 * SV Membership Indexer
 * 
 * Tracks Super Validator onboarding/offboarding events over time to:
 * 1. Build historical SV membership snapshots
 * 2. Calculate SV count at any point in time
 * 3. Determine voting thresholds for proposal outcome analysis
 * 
 * Templates tracked:
 * - Splice.SvOnboarding:SvOnboardingConfirmed (onboard)
 * - Splice.SvOnboarding:SvOnboardingRequest (pending)
 * - DsoRules_OffboardSv actions (offboard)
 */

import { query, queryOne, DATA_PATH } from '../duckdb/connection.js';
import * as binaryReader from '../duckdb/binary-reader.js';
import { getFilesForTemplate, isTemplateIndexPopulated } from './template-file-index.js';

let indexingInProgress = false;
let indexingProgress = null;

/**
 * Ensure SV membership tables exist
 */
export async function ensureSvTables() {
  // SV membership events table
  await query(`
    CREATE TABLE IF NOT EXISTS sv_membership_events (
      event_id VARCHAR PRIMARY KEY,
      event_type VARCHAR NOT NULL,
      sv_party VARCHAR,
      sv_name VARCHAR,
      sv_reward_weight INTEGER,
      sv_participant_id VARCHAR,
      effective_at TIMESTAMP,
      sponsor VARCHAR,
      reason VARCHAR,
      dso VARCHAR,
      contract_id VARCHAR,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Create unique index for upserts
  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_sv_membership_events_id 
    ON sv_membership_events(event_id)
  `);

  // SV count snapshots table (for fast lookups)
  await query(`
    CREATE TABLE IF NOT EXISTS sv_count_snapshots (
      snapshot_date DATE PRIMARY KEY,
      sv_count INTEGER NOT NULL,
      sv_parties VARCHAR,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Index state
  await query(`
    CREATE TABLE IF NOT EXISTS sv_index_state (
      id INTEGER PRIMARY KEY DEFAULT 1,
      last_indexed_at TIMESTAMP,
      total_events INTEGER DEFAULT 0,
      total_svs INTEGER DEFAULT 0
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
    
    const totalEvents = await queryOne(`SELECT COUNT(*) as count FROM sv_membership_events`);
    const onboardCount = await queryOne(`SELECT COUNT(*) as count FROM sv_membership_events WHERE event_type = 'onboard'`);
    const offboardCount = await queryOne(`SELECT COUNT(*) as count FROM sv_membership_events WHERE event_type = 'offboard'`);
    const state = await queryOne(`SELECT last_indexed_at, total_svs FROM sv_index_state WHERE id = 1`);
    
    return {
      totalEvents: Number(totalEvents?.count || 0),
      onboardCount: Number(onboardCount?.count || 0),
      offboardCount: Number(offboardCount?.count || 0),
      lastIndexedAt: state?.last_indexed_at || null,
      currentSvCount: Number(state?.total_svs || 0),
      isPopulated: Number(totalEvents?.count || 0) > 0,
      isIndexing: indexingInProgress,
      indexing: indexingInProgress ? indexingProgress : null,
    };
  } catch (err) {
    console.error('Error getting SV index stats:', err);
    return { totalEvents: 0, onboardCount: 0, offboardCount: 0, isPopulated: false, isIndexing: indexingInProgress, indexing: null };
  }
}

/**
 * Get SV count at a specific date/time
 */
export async function getSvCountAt(dateTime) {
  try {
    await ensureSvTables();
    
    const timestamp = new Date(dateTime).toISOString();
    
    // Count unique SVs that were onboarded before this time and not offboarded
    const result = await query(`
      WITH onboarded AS (
        SELECT DISTINCT sv_party, MIN(effective_at) as onboard_time
        FROM sv_membership_events
        WHERE event_type = 'onboard' AND effective_at <= '${timestamp}'
        GROUP BY sv_party
      ),
      offboarded AS (
        SELECT sv_party, MIN(effective_at) as offboard_time
        FROM sv_membership_events
        WHERE event_type = 'offboard' AND effective_at <= '${timestamp}'
        GROUP BY sv_party
      )
      SELECT COUNT(*) as count
      FROM onboarded o
      LEFT JOIN offboarded off ON o.sv_party = off.sv_party
      WHERE off.offboard_time IS NULL OR off.offboard_time > o.onboard_time
    `);
    
    return Number(result?.[0]?.count || 0);
  } catch (err) {
    console.error('Error getting SV count at date:', err);
    return 0;
  }
}

/**
 * Get list of active SVs at a specific date/time
 */
export async function getActiveSvsAt(dateTime) {
  try {
    await ensureSvTables();
    
    const timestamp = new Date(dateTime).toISOString();
    
    const result = await query(`
      WITH onboarded AS (
        SELECT sv_party, sv_name, sv_reward_weight, MIN(effective_at) as onboard_time
        FROM sv_membership_events
        WHERE event_type = 'onboard' AND effective_at <= '${timestamp}'
        GROUP BY sv_party, sv_name, sv_reward_weight
      ),
      offboarded AS (
        SELECT sv_party, MIN(effective_at) as offboard_time
        FROM sv_membership_events
        WHERE event_type = 'offboard' AND effective_at <= '${timestamp}'
        GROUP BY sv_party
      )
      SELECT o.sv_party, o.sv_name, o.sv_reward_weight, o.onboard_time
      FROM onboarded o
      LEFT JOIN offboarded off ON o.sv_party = off.sv_party
      WHERE off.offboard_time IS NULL OR off.offboard_time > o.onboard_time
      ORDER BY o.onboard_time ASC
    `);
    
    return result.map(r => ({
      svParty: r.sv_party,
      svName: r.sv_name,
      svRewardWeight: Number(r.sv_reward_weight || 1),
      onboardTime: r.onboard_time,
    }));
  } catch (err) {
    console.error('Error getting active SVs at date:', err);
    return [];
  }
}

/**
 * Get SV membership timeline
 */
export async function getSvMembershipTimeline(limit = 100) {
  try {
    await ensureSvTables();
    
    const events = await query(`
      SELECT event_id, event_type, sv_party, sv_name, sv_reward_weight, 
             effective_at, sponsor, reason, contract_id
      FROM sv_membership_events
      ORDER BY effective_at DESC
      LIMIT ${limit}
    `);
    
    return events.map(e => ({
      eventId: e.event_id,
      eventType: e.event_type,
      svParty: e.sv_party,
      svName: e.sv_name,
      svRewardWeight: Number(e.sv_reward_weight || 1),
      effectiveAt: e.effective_at,
      sponsor: e.sponsor,
      reason: e.reason,
      contractId: e.contract_id,
    }));
  } catch (err) {
    console.error('Error getting SV timeline:', err);
    return [];
  }
}

/**
 * Calculate voting threshold based on SV count
 * Canton governance requires 2/3 majority (rounded up) for pass/reject
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
 * Build SV membership index by scanning binary files
 */
export async function buildSvMembershipIndex({ force = false } = {}) {
  if (indexingInProgress) {
    console.log('⏳ SV membership indexing already in progress');
    return { status: 'in_progress' };
  }

  indexingInProgress = true;
  indexingProgress = { phase: 'starting', current: 0, total: 0 };
  console.log('\n👥 Starting SV membership index build...');

  try {
    await ensureSvTables();

    // Check if already populated
    if (!force) {
      const stats = await getSvIndexStats();
      if (stats.totalEvents > 0) {
        console.log(`✅ SV index already populated (${stats.totalEvents} events), use force=true to rebuild`);
        indexingInProgress = false;
        return { status: 'skipped', events: stats.totalEvents };
      }
    }

    if (force) {
      console.log('   🗑️ Force rebuild - clearing existing SV data...');
      await query(`DELETE FROM sv_membership_events`);
    }

    let totalOnboards = 0;
    let totalOffboards = 0;

    // Check if template index is available
    const templateIndexPopulated = await isTemplateIndexPopulated();
    
    if (templateIndexPopulated) {
      // FAST PATH: Use template index
      console.log('   📋 Using template index for fast scanning...');
      
      // Scan SvOnboardingConfirmed for onboard events
      const onboardFiles = await getFilesForTemplate('SvOnboardingConfirmed');
      console.log(`   📂 Found ${onboardFiles.length} files with SvOnboardingConfirmed events`);
      
      indexingProgress = { phase: 'scan:onboard', current: 0, total: onboardFiles.length, filesScanned: 0, totalFiles: onboardFiles.length + (await getFilesForTemplate('DsoRules')).length };
      
      for (let i = 0; i < onboardFiles.length; i++) {
        indexingProgress.current = i + 1;
        indexingProgress.filesScanned = i + 1;
        const filePath = onboardFiles[i];
        
        try {
          const result = await binaryReader.readBinaryFile(filePath);
          const events = result.records || result || [];
          
          for (const event of events) {
            if (event.type !== 'created') continue;
            if (!event.template_id?.includes('SvOnboardingConfirmed')) continue;
            
            const payload = event.payload || {};
            const svParty = payload.svParty || null;
            const svName = payload.svName || null;
            
            if (!svParty) continue;
            
            try {
              await query(`
                INSERT INTO sv_membership_events 
                  (event_id, event_type, sv_party, sv_name, sv_reward_weight, 
                   sv_participant_id, effective_at, sponsor, reason, dso, contract_id)
                VALUES (
                  '${event.event_id?.replace(/'/g, "''")}',
                  'onboard',
                  '${svParty.replace(/'/g, "''")}',
                  ${svName ? `'${svName.replace(/'/g, "''")}'` : 'NULL'},
                  ${payload.svRewardWeight || 1},
                  ${payload.svParticipantId ? `'${payload.svParticipantId.replace(/'/g, "''")}'` : 'NULL'},
                  '${event.effective_at || new Date().toISOString()}',
                  NULL,
                  ${payload.reason ? `'${String(payload.reason).slice(0, 500).replace(/'/g, "''")}'` : 'NULL'},
                  ${payload.dso ? `'${payload.dso.replace(/'/g, "''")}'` : 'NULL'},
                  '${event.contract_id?.replace(/'/g, "''")}' 
                )
                ON CONFLICT (event_id) DO NOTHING
              `);
              totalOnboards++;
            } catch (insertErr) {
              // Skip duplicates
            }
          }
        } catch (fileErr) {
          console.warn(`   ⚠️ Error reading ${filePath}:`, fileErr.message);
        }
      }
      
      console.log(`   ✓ Found ${totalOnboards} SvOnboardingConfirmed events`);
      
      // Scan for offboard events in DsoRules files
      const dsoRulesFiles = await getFilesForTemplate('DsoRules');
      console.log(`   📂 Found ${dsoRulesFiles.length} files with DsoRules events`);
      
      const onboardFilesCount = onboardFiles.length;
      indexingProgress = { phase: 'scan:offboard', current: 0, total: dsoRulesFiles.length, filesScanned: onboardFilesCount, totalFiles: onboardFilesCount + dsoRulesFiles.length };
      
      for (let i = 0; i < dsoRulesFiles.length; i++) {
        indexingProgress.current = i + 1;
        indexingProgress.filesScanned = onboardFilesCount + i + 1;
        const filePath = dsoRulesFiles[i];
        
        try {
          const result = await binaryReader.readBinaryFile(filePath);
          const events = result.records || result || [];
          
          for (const event of events) {
            if (event.type !== 'exercised') continue;
            const choice = event.choice || '';
            if (!choice.includes('OffboardSv')) continue;
            
            const args = event.exercise_argument || event.exerciseArgument || {};
            const svParty = args.sv || args.svParty || null;
            
            if (!svParty) continue;
            
            try {
              await query(`
                INSERT INTO sv_membership_events 
                  (event_id, event_type, sv_party, sv_name, effective_at, contract_id)
                VALUES (
                  '${event.event_id?.replace(/'/g, "''")}',
                  'offboard',
                  '${svParty.replace(/'/g, "''")}',
                  NULL,
                  '${event.effective_at || new Date().toISOString()}',
                  '${event.contract_id?.replace(/'/g, "''")}' 
                )
                ON CONFLICT (event_id) DO NOTHING
              `);
              totalOffboards++;
            } catch (insertErr) {
              // Skip duplicates
            }
          }
        } catch (fileErr) {
          // Skip file errors
        }
      }
      
      console.log(`   ✓ Found ${totalOffboards} OffboardSv events`);
      
    } else {
      console.log('   ⚠️ Template index not available, build it first for faster SV indexing');
      indexingInProgress = false;
      return { status: 'error', message: 'Template index required. Build template index first.' };
    }

    // Calculate current SV count
    const currentSvCount = await getSvCountAt(new Date());
    
    // Update index state
    await query(`
      INSERT INTO sv_index_state (id, last_indexed_at, total_events, total_svs)
      VALUES (1, CURRENT_TIMESTAMP, ${totalOnboards + totalOffboards}, ${currentSvCount})
      ON CONFLICT (id) DO UPDATE SET
        last_indexed_at = CURRENT_TIMESTAMP,
        total_events = ${totalOnboards + totalOffboards},
        total_svs = ${currentSvCount}
    `);

    console.log(`\n✅ SV membership index complete:`);
    console.log(`   - Onboard events: ${totalOnboards}`);
    console.log(`   - Offboard events: ${totalOffboards}`);
    console.log(`   - Current SV count: ${currentSvCount}`);

    indexingInProgress = false;
    indexingProgress = null;
    
    return {
      status: 'complete',
      onboardEvents: totalOnboards,
      offboardEvents: totalOffboards,
      currentSvCount,
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
