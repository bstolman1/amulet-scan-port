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
      stable_id, event_id, contract_id, template_id, effective_at,
      status, is_closed, action_tag, action_value,
      requester, reason, votes, vote_count,
      vote_before, target_effective_at, tracking_cid, dso
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
 * Ensure index tables exist
 */
/**
 * Generate a stable identifier for a VoteRequest that persists across migrations.
 * Uses action_tag + action_value + requester since these define what the vote is about.
 */
function generateStableVoteRequestId(payload) {
  const actionTag = payload?.action?.tag || '';
  const actionValue = payload?.action?.value ? JSON.stringify(payload.action.value) : '';
  const requester = payload?.requester || '';
  
  // Create a stable composite key
  const composite = `${actionTag}::${actionValue}::${requester}`;
  
  // Simple hash to create a shorter identifier
  let hash = 0;
  for (let i = 0; i < composite.length; i++) {
    const char = composite.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32bit integer
  }
  
  // Use the hash combined with action_tag for readability
  return `${actionTag}-${Math.abs(hash).toString(36)}`;
}

async function ensureIndexTables() {
  try {
    // Create vote_requests table if it doesn't exist
    // Use stable_id as primary key - a composite of action+requester that persists across migrations
    // This correctly deduplicates VoteRequests that get new contract_ids during ledger upgrades
    await query(`
      CREATE TABLE IF NOT EXISTS vote_requests (
        stable_id VARCHAR PRIMARY KEY,
        contract_id VARCHAR,
        event_id VARCHAR,
        template_id VARCHAR,
        effective_at TIMESTAMP,
        status VARCHAR,
        is_closed BOOLEAN,
        action_tag VARCHAR,
        action_value VARCHAR,
        requester VARCHAR,
        reason VARCHAR,
        votes VARCHAR,
        vote_count INTEGER,
        vote_before VARCHAR,
        target_effective_at VARCHAR,
        tracking_cid VARCHAR,
        dso VARCHAR,
        updated_at TIMESTAMP
      )
    `);
    
    // Create state table if it doesn't exist
    await query(`
      CREATE TABLE IF NOT EXISTS vote_request_index_state (
        id INTEGER PRIMARY KEY,
        last_indexed_file VARCHAR,
        last_indexed_at TIMESTAMP,
        total_indexed INTEGER
      )
    `);
    
    console.log('   ✓ Index tables ensured');
  } catch (err) {
    console.error('Error creating index tables:', err);
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
    
    // If force, drop and recreate tables to apply schema changes
    if (force) {
      try {
        await query('DROP TABLE IF EXISTS vote_requests');
        await query('DROP TABLE IF EXISTS vote_request_index_state');
        console.log('   Dropped existing tables for schema update');
      } catch (err) {
        // Ignore drop errors
      }
    }
    
    // Ensure tables exist (will create with new schema if dropped)
    await ensureIndexTables();
    
    // Check if template index is available for faster scanning
    const templateIndexPopulated = await isTemplateIndexPopulated();
    let createdRecords = [];
    let exercisedRecords = [];
    
    if (templateIndexPopulated) {
      // FAST PATH: Use template index to scan only relevant files
      const templateIndexStats = await getTemplateIndexStats();
      console.log(`   📋 Using template index (${templateIndexStats.totalFiles} files indexed)`);
      
      const voteRequestFiles = await getFilesForTemplate('VoteRequest');
      console.log(`   📂 Found ${voteRequestFiles.length} files containing VoteRequest events`);
      
      if (voteRequestFiles.length === 0) {
        console.log('   ⚠️ No VoteRequest files found in index, falling back to full scan');
        const result = await scanAllFilesForVoteRequestsSinglePass();
        createdRecords = result.created;
        exercisedRecords = result.exercised;
      } else {
        // SINGLE PASS: Scan files once and collect both event types
        console.log('   📊 Scanning files for VoteRequest events (single pass)...');
        const result = await scanFilesForVoteRequestsSinglePass(voteRequestFiles);
        createdRecords = result.created;
        exercisedRecords = result.exercised;
      }
    } else {
      // SLOW PATH: Full scan (template index not built yet)
      console.log('   ⚠️ Template index not available, using full scan (this will be slow)');
      console.log('   💡 Run template index build first for faster VoteRequest indexing');
      
      const result = await scanAllFilesForVoteRequestsSinglePass();
      createdRecords = result.created;
      exercisedRecords = result.exercised;
    }
    
    // Dedupe created records by STABLE ID (action + requester), keeping newest by effective_at
    // This correctly handles the same VoteRequest getting new contract_ids during ledger migrations
    const stableIdMap = new Map();
    for (const event of createdRecords) {
      const stableId = generateStableVoteRequestId(event.payload);
      if (!stableId || stableId === '-0') continue; // Skip if no meaningful payload
      
      // Attach stableId to event for later use
      event._stableId = stableId;
      
      const existing = stableIdMap.get(stableId);
      if (!existing || new Date(event.effective_at) > new Date(existing.effective_at)) {
        stableIdMap.set(stableId, event);
      }
    }
    const dedupedRecords = Array.from(stableIdMap.values());
    
    console.log(`   📊 Deduplication: ${createdRecords.length} total → ${dedupedRecords.length} unique (by action+requester)`);
    
    console.log(`   ✓ Found ${createdRecords.length} created (${dedupedRecords.length} unique), ${exercisedRecords.length} exercised`);
    
    // Build set of closed stable IDs (not contract IDs, since those change across migrations)
    // We need to look at the exercised events and match them to their original VoteRequest
    const closedStableIds = new Set();
    for (const exercised of exercisedRecords) {
      // For exercised events, we need to find the original VoteRequest by contract_id
      // and then use its stable_id
      const cid = exercised.contract_id;
      if (!cid) continue;
      
      // Find the created event with this contract_id to get its stable_id
      const matchingCreated = createdRecords.find(c => c.contract_id === cid);
      if (matchingCreated) {
        const stableId = generateStableVoteRequestId(matchingCreated.payload);
        if (stableId && stableId !== '-0') {
          closedStableIds.add(stableId);
        }
      }
    }
    
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
    
    for (const event of dedupedRecords) {
      const stableId = event._stableId;
      const voteBefore = event.payload?.voteBefore;
      const voteBeforeDate = voteBefore ? new Date(voteBefore) : null;
      const isClosed = closedStableIds.has(stableId);
      const isActive = !isClosed && (voteBeforeDate ? voteBeforeDate > now : true);
      
      // Normalize reason - can be string or object
      const rawReason = event.payload?.reason;
      const reasonStr = rawReason 
        ? (typeof rawReason === 'string' ? rawReason : JSON.stringify(rawReason))
        : null;
      
      const voteRequest = {
        stable_id: stableId,
        event_id: event.event_id,
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
      };
      
      try {
        // Upsert - insert or update on conflict by stable_id (unique per logical VoteRequest)
        await query(`
          INSERT INTO vote_requests (
            stable_id, contract_id, event_id, template_id, effective_at,
            status, is_closed, action_tag, action_value,
            requester, reason, votes, vote_count,
            vote_before, target_effective_at, tracking_cid, dso,
            updated_at
          ) VALUES (
            '${voteRequest.stable_id}',
            '${voteRequest.contract_id}',
            '${voteRequest.event_id}',
            ${voteRequest.template_id ? `'${voteRequest.template_id}'` : 'NULL'},
            ${voteRequest.effective_at ? `'${voteRequest.effective_at}'` : 'NULL'},
            '${voteRequest.status}',
            ${voteRequest.is_closed},
            ${voteRequest.action_tag ? `'${voteRequest.action_tag.replace(/'/g, "''")}'` : 'NULL'},
            ${voteRequest.action_value ? `'${voteRequest.action_value.replace(/'/g, "''")}'` : 'NULL'},
            ${voteRequest.requester ? `'${voteRequest.requester}'` : 'NULL'},
            ${voteRequest.reason ? `'${voteRequest.reason.replace(/'/g, "''")}'` : 'NULL'},
            '${voteRequest.votes.replace(/'/g, "''")}',
            ${voteRequest.vote_count},
            ${voteRequest.vote_before ? `'${voteRequest.vote_before}'` : 'NULL'},
            ${voteRequest.target_effective_at ? `'${voteRequest.target_effective_at}'` : 'NULL'},
            ${voteRequest.tracking_cid ? `'${voteRequest.tracking_cid}'` : 'NULL'},
            ${voteRequest.dso ? `'${voteRequest.dso}'` : 'NULL'},
            now()
          )
          ON CONFLICT (stable_id) DO UPDATE SET
            contract_id = EXCLUDED.contract_id,
            status = EXCLUDED.status,
            is_closed = EXCLUDED.is_closed,
            votes = EXCLUDED.votes,
            vote_count = EXCLUDED.vote_count,
            updated_at = now()
        `);
        inserted++;
      } catch (err) {
        if (!err.message?.includes('duplicate')) {
          console.error(`   Error inserting vote request ${voteRequest.contract_id}:`, err.message);
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
    console.log(`✅ VoteRequest index built: ${inserted} unique VoteRequests indexed in ${elapsed}s`);
    
    indexingInProgress = false;
    
    return {
      status: 'complete',
      uniqueVoteRequests: inserted,
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
 * Scan specific files for VoteRequest events - SINGLE PASS collecting both created and exercised
 */
async function scanFilesForVoteRequestsSinglePass(files) {
  const created = [];
  const exercised = [];
  let filesProcessed = 0;
  const startTime = Date.now();
  let lastLogTime = startTime;
  
  for (const file of files) {
    try {
      const result = await binaryReader.readBinaryFile(file);
      const fileRecords = result.records || [];
      
      for (const record of fileRecords) {
        if (!record.template_id?.includes('VoteRequest')) continue;
        
        if (record.event_type === 'created') {
          created.push(record);
        } else if (record.event_type === 'exercised') {
          if (record.choice === 'Archive' || (typeof record.choice === 'string' && record.choice.startsWith('VoteRequest_'))) {
            exercised.push(record);
          }
        }
      }
      
      filesProcessed++;
      
      // Log progress every 50 files or every 5 seconds
      const now = Date.now();
      if (filesProcessed % 50 === 0 || (now - lastLogTime > 5000)) {
        const elapsed = (now - startTime) / 1000;
        const pct = ((filesProcessed / files.length) * 100).toFixed(0);
        console.log(`   📂 [${pct}%] ${filesProcessed}/${files.length} files | ${created.length} created, ${exercised.length} exercised | ${elapsed.toFixed(1)}s`);
        lastLogTime = now;
      }
    } catch (err) {
      // Skip unreadable files
    }
  }
  
  return { created, exercised, filesScanned: filesProcessed };
}

/**
 * Scan all files for VoteRequest events - SINGLE PASS (slow fallback path)
 */
async function scanAllFilesForVoteRequestsSinglePass() {
  console.log(`   Scanning all files for VoteRequest events (full scan, single pass)...`);
  
  const created = [];
  const exercised = [];
  
  // Use streamRecords with a filter that captures both event types
  const allResults = await binaryReader.streamRecords(DATA_PATH, 'events', {
    limit: Number.MAX_SAFE_INTEGER,
    offset: 0,
    fullScan: true,
    sortBy: 'effective_at',
    filter: (e) => e.template_id?.includes('VoteRequest')
  });
  
  for (const record of allResults.records || []) {
    if (record.event_type === 'created') {
      created.push(record);
    } else if (record.event_type === 'exercised') {
      if (record.choice === 'Archive' || (typeof record.choice === 'string' && record.choice.startsWith('VoteRequest_'))) {
        exercised.push(record);
      }
    }
  }
  
  return { created, exercised, filesScanned: allResults.filesScanned || 0 };
}
