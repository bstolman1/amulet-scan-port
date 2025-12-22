/**
 * VoteRequest Indexer - Builds persistent DuckDB index for VoteRequest events
 * 
 * Scans binary files for VoteRequest created/exercised events and maintains
 * a persistent table for instant historical queries.
 * 
 * Uses template-to-file index when available to dramatically reduce scan time.
 */

import { query, queryOne, DATA_PATH } from '../duckdb/connection.js';
import * as binaryReader from '../duckdb/binary-reader.js';
import { 
  getFilesForTemplate, 
  isTemplateIndexPopulated,
  getTemplateIndexStats
} from './template-file-index.js';

let indexingInProgress = false;

/**
 * Get current indexing state
 */
export async function getIndexState() {
  try {
    const state = await queryOne(`
      SELECT last_indexed_file, last_indexed_at, total_indexed 
      FROM vote_request_index_state 
      WHERE id = 1
    `);
    return state || { last_indexed_file: null, last_indexed_at: null, total_indexed: 0 };
  } catch (err) {
    return { last_indexed_file: null, last_indexed_at: null, total_indexed: 0 };
  }
}

/**
 * Get vote request counts from the index
 */
export async function getVoteRequestStats() {
  try {
    const total = await queryOne(`SELECT COUNT(*) as count FROM vote_requests`);
    const active = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status = 'active'`);
    const historical = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status = 'historical'`);
    const closed = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE is_closed = true`);
    
    return {
      total: Number(total?.count || 0),
      active: Number(active?.count || 0),
      historical: Number(historical?.count || 0),
      closed: Number(closed?.count || 0),
    };
  } catch (err) {
    console.error('Error getting vote request stats:', err);
    return { total: 0, active: 0, historical: 0, closed: 0 };
  }
}

/**
 * Query vote requests from the persistent index
 */
export async function queryVoteRequests({ limit = 100, status = 'all', offset = 0 } = {}) {
  let whereClause = '';
  if (status === 'active') {
    whereClause = 'WHERE status = \'active\'';
  } else if (status === 'historical') {
    whereClause = 'WHERE status = \'historical\'';
  }
  
  const results = await query(`
    SELECT 
      event_id, contract_id, template_id, effective_at,
      status, is_closed, action_tag, action_value,
      requester, reason, votes, vote_count,
      vote_before, target_effective_at, tracking_cid, dso,
      payload
    FROM vote_requests
    ${whereClause}
    ORDER BY effective_at DESC
    LIMIT ${limit} OFFSET ${offset}
  `);
  
  const safeJsonParse = (val) => {
    if (val === null || val === undefined) return null;
    if (typeof val !== 'string') return val;
    try {
      return JSON.parse(val);
    } catch {
      return val;
    }
  };

  // Parse JSON fields (DuckDB may return either JSON objects or strings depending on insertion/casting)
  return results.map(r => ({
    ...r,
    action_value: safeJsonParse(r.action_value),
    votes: Array.isArray(r.votes) ? r.votes : (safeJsonParse(r.votes) || []),
    payload: safeJsonParse(r.payload),
  }));
}

/**
 * Check if index is populated
 */
export async function isIndexPopulated() {
  const stats = await getVoteRequestStats();
  return stats.total > 0;
}

/**
 * Ensure index tables exist - delegates to engine schema
 */
async function ensureIndexTables() {
  try {
    // Use the centralized engine schema which creates vote_requests and vote_request_index_state
    const { initEngineSchema } = await import('./schema.js');
    await initEngineSchema();
    console.log('   ✓ Index tables ensured via engine schema');
  } catch (err) {
    console.error('Error ensuring index tables:', err);
    throw err;
  }
}

/**
 * Build or update the VoteRequest index by scanning binary files
 * Uses template-to-file index when available for much faster scanning
 */
export async function buildVoteRequestIndex({ force = false } = {}) {
  if (indexingInProgress) {
    console.log('⏳ VoteRequest indexing already in progress');
    return { status: 'in_progress' };
  }
  
  indexingInProgress = true;
  console.log('\n🗳️ Starting VoteRequest index build...');
  
  try {
    const startTime = Date.now();
    
    // Ensure tables exist first
    await ensureIndexTables();
    
    // Check if template index is available for faster scanning
    const templateIndexPopulated = await isTemplateIndexPopulated();
    let createdResult, exercisedResult;
    
    if (templateIndexPopulated) {
      // FAST PATH: Use template index to scan only relevant files
      const templateIndexStats = await getTemplateIndexStats();
      console.log(`   📋 Using template index (${templateIndexStats.totalFiles} files indexed)`);
      
      const voteRequestFiles = await getFilesForTemplate('VoteRequest');
      console.log(`   📂 Found ${voteRequestFiles.length} files containing VoteRequest events`);
      
      if (voteRequestFiles.length === 0) {
        console.log('   ⚠️ No VoteRequest files found in index, falling back to full scan');
        createdResult = await scanAllFilesForVoteRequests('created');
        exercisedResult = await scanAllFilesForVoteRequests('exercised');
      } else {
        // Scan only the relevant files
        console.log('   Scanning VoteRequest files for created events...');
        createdResult = await scanFilesForVoteRequests(voteRequestFiles, 'created');
        
        console.log('   Scanning VoteRequest files for exercised events...');
        exercisedResult = await scanFilesForVoteRequests(voteRequestFiles, 'exercised');
      }
    } else {
      // SLOW PATH: Full scan (template index not built yet)
      console.log('   ⚠️ Template index not available, using full scan (this will be slow)');
      console.log('   💡 Run template index build first for faster VoteRequest indexing');
      
      createdResult = await scanAllFilesForVoteRequests('created');
      exercisedResult = await scanAllFilesForVoteRequests('exercised');
    }
    
    console.log(`   Found ${createdResult.records.length} VoteRequest created events`);
    console.log(`   Found ${exercisedResult.records.length} VoteRequest exercised events`);
    
    // Build set of closed contract IDs
    const closedContractIds = new Set(
      exercisedResult.records.map(r => r.contract_id).filter(Boolean)
    );
    
    const now = new Date();
    
    // Clear existing data if force rebuild
    if (force) {
      try {
        await query('DELETE FROM vote_requests');
        console.log('   Cleared existing index');
      } catch (err) {
        // Table might not exist yet, ignore
      }
    }
    
    // Insert vote requests
    let inserted = 0;
    let updated = 0;
    
    for (const event of createdResult.records) {
      const voteBefore = event.payload?.voteBefore;
      const voteBeforeDate = voteBefore ? new Date(voteBefore) : null;
      const isClosed = !!event.contract_id && closedContractIds.has(event.contract_id);
      const isActive = !isClosed && (voteBeforeDate ? voteBeforeDate > now : true);
      
      // Normalize reason - can be string or object
      const rawReason = event.payload?.reason;
      const reasonStr = rawReason 
        ? (typeof rawReason === 'string' ? rawReason : JSON.stringify(rawReason))
        : null;
      
      const voteRequest = {
        event_id: event.event_id,
        stable_id: event.contract_id || event.event_id,
        contract_id: event.contract_id,
        template_id: event.template_id,
        effective_at: event.effective_at,
        status: isActive ? 'active' : 'historical',
        is_closed: isClosed,
        action_tag: event.payload?.action?.tag || null,
        action_value: event.payload?.action?.value ? JSON.stringify(event.payload.action.value) : null,
        requester: event.payload?.requester || null,
        reason: reasonStr,
        votes: event.payload?.votes ? JSON.stringify(event.payload.votes) : '[]',
        vote_count: event.payload?.votes?.length || 0,
        vote_before: voteBefore || null,
        target_effective_at: event.payload?.targetEffectiveAt || null,
        tracking_cid: event.payload?.trackingCid || null,
        dso: event.payload?.dso || null,
        payload: event.payload ? JSON.stringify(event.payload) : null,
      };
      
      try {
        // Escape helper for safe SQL string values
        const escapeStr = (val) => (val === null || val === undefined) ? null : String(val).replace(/'/g, "''");
        const payloadStr = voteRequest.payload ? escapeStr(voteRequest.payload) : null;
        const stableIdSql = voteRequest.stable_id ? `'${escapeStr(voteRequest.stable_id)}'` : 'NULL';
        
        // Upsert - insert or update on conflict
        await query(`
          INSERT INTO vote_requests (
            event_id, stable_id, contract_id, template_id, effective_at,
            status, is_closed, action_tag, action_value,
            requester, reason, votes, vote_count,
            vote_before, target_effective_at, tracking_cid, dso,
            payload, updated_at
          ) VALUES (
            '${escapeStr(voteRequest.event_id)}',
            ${stableIdSql},
            ${voteRequest.contract_id ? `'${escapeStr(voteRequest.contract_id)}'` : 'NULL'},
            ${voteRequest.template_id ? `'${escapeStr(voteRequest.template_id)}'` : 'NULL'},
            ${voteRequest.effective_at ? `'${escapeStr(voteRequest.effective_at)}'` : 'NULL'},
            '${escapeStr(voteRequest.status)}',
            ${voteRequest.is_closed},
            ${voteRequest.action_tag ? `'${escapeStr(voteRequest.action_tag)}'` : 'NULL'},
            ${voteRequest.action_value ? `'${escapeStr(voteRequest.action_value)}'` : 'NULL'},
            ${voteRequest.requester ? `'${escapeStr(voteRequest.requester)}'` : 'NULL'},
            ${voteRequest.reason ? `'${escapeStr(voteRequest.reason)}'` : 'NULL'},
            '${escapeStr(voteRequest.votes)}',
            ${voteRequest.vote_count},
            ${voteRequest.vote_before ? `'${escapeStr(voteRequest.vote_before)}'` : 'NULL'},
            ${voteRequest.target_effective_at ? `'${escapeStr(voteRequest.target_effective_at)}'` : 'NULL'},
            ${voteRequest.tracking_cid ? `'${escapeStr(voteRequest.tracking_cid)}'` : 'NULL'},
            ${voteRequest.dso ? `'${escapeStr(voteRequest.dso)}'` : 'NULL'},
            ${payloadStr ? `'${payloadStr}'` : 'NULL'},
            now()
          )
          ON CONFLICT (event_id) DO UPDATE SET
            stable_id = EXCLUDED.stable_id,
            status = EXCLUDED.status,
            is_closed = EXCLUDED.is_closed,
            payload = EXCLUDED.payload,
            updated_at = now()
        `);
        inserted++;
      } catch (err) {
        if (!err.message?.includes('duplicate')) {
          console.error(`   Error inserting vote request ${voteRequest.event_id}:`, err.message);
        } else {
          updated++;
        }
      }
    }
    
    // Update index state
    await query(`
      INSERT INTO vote_request_index_state (id, last_indexed_at, total_indexed)
      VALUES (1, now(), ${inserted})
      ON CONFLICT (id) DO UPDATE SET
        last_indexed_at = now(),
        total_indexed = ${inserted}
    `);
    
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`✅ VoteRequest index built: ${inserted} inserted, ${updated} updated in ${elapsed}s`);
    
    indexingInProgress = false;
    
    return {
      status: 'complete',
      inserted,
      updated,
      closedCount: closedContractIds.size,
      elapsedSeconds: parseFloat(elapsed),
    };
    
  } catch (err) {
    console.error('❌ VoteRequest index build failed:', err);
    indexingInProgress = false;
    throw err;
  }
}

/**
 * Check if indexing is in progress
 */
export function isIndexingInProgress() {
  return indexingInProgress;
}

/**
 * Scan specific files for VoteRequest events (fast path using template index)
 */
async function scanFilesForVoteRequests(files, eventType) {
  const records = [];
  let filesProcessed = 0;
  const startTime = Date.now();
  let lastLogTime = startTime;

  const readWithTimeout = async (file, timeoutMs = 30000) => {
    const timeout = new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`timeout after ${timeoutMs}ms`)), timeoutMs)
    );
    return Promise.race([binaryReader.readBinaryFile(file), timeout]);
  };

  for (const file of files) {
    try {
      const result = await readWithTimeout(file, 30000);
      const fileRecords = result.records || [];

      for (const record of fileRecords) {
        if (!record.template_id?.includes('VoteRequest')) continue;

        if (eventType === 'created' && record.event_type === 'created') {
          records.push(record);
        } else if (eventType === 'exercised' && record.event_type === 'exercised') {
          if (record.choice === 'Archive' || (typeof record.choice === 'string' && record.choice.startsWith('VoteRequest_'))) {
            records.push(record);
          }
        }
      }

      filesProcessed++;

      // Log progress every 50 files or every 5 seconds
      const now = Date.now();
      if (filesProcessed % 50 === 0 || (now - lastLogTime > 5000)) {
        const elapsed = (now - startTime) / 1000;
        const pct = ((filesProcessed / files.length) * 100).toFixed(0);
        console.log(`   📂 [${pct}%] ${filesProcessed}/${files.length} files | ${records.length} ${eventType} events | ${elapsed.toFixed(1)}s`);
        lastLogTime = now;
      }
    } catch (err) {
      // Skip unreadable/hanging files
      console.warn(`   ⚠️ Skipping VoteRequest file due to read error: ${file} (${err?.message || err})`);
    }
  }

  return { records, filesScanned: filesProcessed };
}

/**
 * Scan all files for VoteRequest events (slow fallback path)
 */
async function scanAllFilesForVoteRequests(eventType) {
  const filter = eventType === 'created'
    ? (e) => e.template_id?.includes('VoteRequest') && e.event_type === 'created'
    : (e) => {
        if (!e.template_id?.includes('VoteRequest')) return false;
        if (e.event_type !== 'exercised') return false;
        return e.choice === 'Archive' || (typeof e.choice === 'string' && e.choice.startsWith('VoteRequest_'));
      };
  
  console.log(`   Scanning for VoteRequest ${eventType} events (full scan)...`);
  return binaryReader.streamRecords(DATA_PATH, 'events', {
    limit: Number.MAX_SAFE_INTEGER,
    offset: 0,
    fullScan: true,
    sortBy: 'effective_at',
    filter
  });
}
