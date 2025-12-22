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
import fs from 'fs';
import path from 'path';
import {
  getFilesForTemplate,
  isTemplateIndexPopulated,
  getTemplateIndexStats
} from './template-file-index.js';

let indexingInProgress = false;
let indexingProgress = null;
let lockRelease = null;

async function acquireIndexLock() {
  // Prevent multi-process index builds (e.g., two servers running, or the process restarting mid-build)
  const lockDir = path.join(DATA_PATH, '.locks');
  const lockPath = path.join(lockDir, 'vote_request_index.lock');

  await fs.promises.mkdir(lockDir, { recursive: true });

  try {
    // 'wx' = create file exclusively; fail if exists
    const handle = await fs.promises.open(lockPath, 'wx');
    await handle.writeFile(JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() }));
    await handle.close();

    return async () => {
      try {
        await fs.promises.unlink(lockPath);
      } catch {
        // ignore
      }
    };
  } catch {
    return null;
  }
}

/**
 * Force clear a stale lock (e.g., from a crashed process)
 */
export async function clearStaleLock() {
  const lockDir = path.join(DATA_PATH, '.locks');
  const lockPath = path.join(lockDir, 'vote_request_index.lock');
  
  try {
    const stat = await fs.promises.stat(lockPath);
    const lockAge = Date.now() - stat.mtimeMs;
    const lockData = JSON.parse(await fs.promises.readFile(lockPath, 'utf8'));
    
    await fs.promises.unlink(lockPath);
    console.log(`🔓 Cleared stale vote request index lock (age: ${(lockAge / 1000 / 60).toFixed(1)}min, pid: ${lockData.pid})`);
    
    return { 
      cleared: true, 
      lockAge: lockAge, 
      previousPid: lockData.pid,
      startedAt: lockData.startedAt 
    };
  } catch (err) {
    if (err.code === 'ENOENT') {
      return { cleared: false, reason: 'No lock file exists' };
    }
    throw err;
  }
}

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
 * Status breakdown: in_progress, executed, rejected, expired
 */
export async function getVoteRequestStats() {
  try {
    const total = await queryOne(`SELECT COUNT(*) as count FROM vote_requests`);
    const inProgress = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status = 'in_progress'`);
    const executed = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status = 'executed'`);
    const rejected = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status = 'rejected'`);
    const expired = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status = 'expired'`);
    
    // Legacy fields for backwards compatibility
    const active = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status = 'active' OR status = 'in_progress'`);
    const historical = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status IN ('executed', 'rejected', 'expired', 'historical')`);
    const closed = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE is_closed = true`);
    
    return {
      total: Number(total?.count || 0),
      inProgress: Number(inProgress?.count || 0),
      executed: Number(executed?.count || 0),
      rejected: Number(rejected?.count || 0),
      expired: Number(expired?.count || 0),
      // Legacy fields
      active: Number(active?.count || 0),
      historical: Number(historical?.count || 0),
      closed: Number(closed?.count || 0),
    };
  } catch (err) {
    console.error('Error getting vote request stats:', err);
    return { total: 0, inProgress: 0, executed: 0, rejected: 0, expired: 0, active: 0, historical: 0, closed: 0 };
  }
}

/**
 * Get the last successful build summary
 */
export async function getLastSuccessfulBuild() {
  try {
    const build = await queryOne(`
      SELECT 
        build_id, started_at, completed_at, duration_seconds,
        total_indexed, inserted, updated, closed_count,
        in_progress_count, executed_count, rejected_count, expired_count
      FROM vote_request_build_history
      WHERE success = true
      ORDER BY completed_at DESC
      LIMIT 1
    `);
    return build || null;
  } catch (err) {
    // Table might not exist yet
    return null;
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

  // Cross-process lock: if another server instance is building, don't start a second build.
  lockRelease = await acquireIndexLock();
  if (!lockRelease) {
    console.log('⏳ VoteRequest index lock present — another process is indexing');
    return { status: 'in_progress' };
  }

  // Check if index is already populated (skip unless force=true)
  if (!force) {
    const stats = await getVoteRequestStats();
    if (stats.total > 0) {
      console.log(`✅ VoteRequest index already populated (${stats.total} records), skipping rebuild`);
      console.log('   Use force=true to rebuild from scratch');
      await lockRelease();
      lockRelease = null;
      return { status: 'already_populated', totalIndexed: stats.total };
    }
  }

  indexingInProgress = true;
  indexingProgress = { phase: 'starting', current: 0, total: 0, records: 0, startedAt: new Date().toISOString() };
  console.log('\n🗳️ Starting VoteRequest index build...');

  try {
    const startTime = Date.now();

    // Ensure tables exist first
    await ensureIndexTables();

    // Heartbeat: mark "last indexed" as now() so the UI doesn't look stale while building.
    try {
      await query(`
        INSERT INTO vote_request_index_state (id, last_indexed_at, total_indexed)
        VALUES (1, now(), 0)
        ON CONFLICT (id) DO UPDATE SET
          last_indexed_at = now()
      `);
    } catch {
      // ignore
    }

    // Check if template index is available for faster scanning
    const templateIndexPopulated = await isTemplateIndexPopulated();
    let createdResult, exercisedResult;

    if (templateIndexPopulated) {
      // FAST PATH: Use template index to scan only relevant files
      const templateIndexStats = await getTemplateIndexStats();
      console.log(`   📋 Using template index (${templateIndexStats.totalFiles} files indexed)`);

      const voteRequestFiles = await getFilesForTemplate('VoteRequest');
      const dsoRulesFiles = await getFilesForTemplate('DsoRules');
      console.log(`   📂 Found ${voteRequestFiles.length} files containing VoteRequest events`);
      console.log(`   📂 Found ${dsoRulesFiles.length} files containing DsoRules events`);

      if (voteRequestFiles.length === 0) {
        console.log('   ⚠️ No VoteRequest files found in index, falling back to full scan');
        indexingProgress = { ...indexingProgress, phase: 'scan:created (full)', current: 0, total: 0, records: 0 };
        createdResult = await scanAllFilesForVoteRequests('created');
        indexingProgress = { ...indexingProgress, phase: 'scan:exercised (full)', current: 0, total: 0, records: 0 };
        exercisedResult = await scanAllFilesForVoteRequests('exercised');
      } else {
        // Scan only the relevant files
        console.log('   Scanning VoteRequest files for created events...');
        indexingProgress = { ...indexingProgress, phase: 'scan:created', current: 0, total: voteRequestFiles.length, records: 0 };
        createdResult = await scanFilesForVoteRequests(voteRequestFiles, 'created');

        // Scan VoteRequest files for direct exercised events
        console.log('   Scanning VoteRequest files for exercised events...');
        indexingProgress = { ...indexingProgress, phase: 'scan:exercised (VoteRequest)', current: 0, total: voteRequestFiles.length, records: 0 };
        exercisedResult = await scanFilesForVoteRequests(voteRequestFiles, 'exercised');

        // CRITICAL: Also scan DsoRules files for DsoRules_CloseVoteRequest exercises
        // These are the events that ACTUALLY close VoteRequests when votes pass/fail
        if (dsoRulesFiles.length > 0) {
          console.log('   Scanning DsoRules files for CloseVoteRequest exercises...');
          indexingProgress = { ...indexingProgress, phase: 'scan:exercised (DsoRules)', current: 0, total: dsoRulesFiles.length, records: 0 };
          const dsoCloseResult = await scanFilesForDsoCloseVoteRequests(dsoRulesFiles);
          console.log(`   Found ${dsoCloseResult.records.length} DsoRules_CloseVoteRequest events`);
          // Merge with exercised results
          exercisedResult.records.push(...dsoCloseResult.records);
        }
      }
    } else {
      // SLOW PATH: Full scan (template index not built yet)
      console.log('   ⚠️ Template index not available, using full scan (this will be slow)');
      console.log('   💡 Run template index build first for faster VoteRequest indexing');

      indexingProgress = { ...indexingProgress, phase: 'scan:created (full)', current: 0, total: 0, records: 0 };
      createdResult = await scanAllFilesForVoteRequests('created');
      indexingProgress = { ...indexingProgress, phase: 'scan:exercised (full)', current: 0, total: 0, records: 0 };
      exercisedResult = await scanAllFilesForVoteRequests('exercised');
    }

    console.log(`   Found ${createdResult.records.length} VoteRequest created events`);
    console.log(`   Found ${exercisedResult.records.length} VoteRequest exercised/closed events total`);

    // Build map of closed contract IDs -> archived event data (with final vote counts)
    const archivedEventsMap = new Map();
    for (const record of exercisedResult.records) {
      if (record.contract_id) {
        archivedEventsMap.set(record.contract_id, record);
      }
    }
    const closedContractIds = new Set(archivedEventsMap.keys());

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
    indexingProgress = { ...indexingProgress, phase: 'upsert', current: 0, total: createdResult.records.length, records: 0 };
    let inserted = 0;
    let updated = 0;

    for (const event of createdResult.records) {
      const isClosed = !!event.contract_id && closedContractIds.has(event.contract_id);

      // Use archived event data for final vote counts if available
      const archivedEvent = event.contract_id ? archivedEventsMap.get(event.contract_id) : null;
      const finalVotes = archivedEvent?.payload?.votes || event.payload?.votes;
      const finalVoteCount = finalVotes?.length || 0;

      const voteBefore = event.payload?.voteBefore;
      const voteBeforeDate = voteBefore ? new Date(voteBefore) : null;
      const isExpired = voteBeforeDate && voteBeforeDate < now;

      // Determine proper status matching the target UI:
      // - in_progress: not closed, vote deadline not passed
      // - executed: closed AND reached voting threshold (we use votesFor >= threshold, but since we don't have threshold here, use choice == 'VoteRequest_Accept' or high vote count)
      // - rejected: closed AND did not reach threshold
      // - expired: vote deadline passed AND not closed (or closed with 0 votes)
      let status = 'in_progress';
      
      // Count accept vs reject votes
      const votesArray = Array.isArray(finalVotes) ? finalVotes : [];
      let acceptCount = 0;
      let rejectCount = 0;
      for (const vote of votesArray) {
        const [, voteData] = Array.isArray(vote) ? vote : ['', vote];
        if (voteData?.accept === true || voteData?.Accept === true) acceptCount++;
        else if (voteData?.accept === false || voteData?.reject === true || voteData?.Reject === true) rejectCount++;
      }
      
      // Check if this was an accepted/executed proposal by looking at the archived event choice
      // Choices can be: VoteRequest_Accept, DsoRules_CloseVoteRequest, Archive, etc.
      const archivedChoice = archivedEvent?.choice || '';
      const choiceLower = archivedChoice.toLowerCase();
      
      // DsoRules_CloseVoteRequest means the vote completed - determine outcome from vote counts
      const isCloseVoteRequest = archivedChoice === 'DsoRules_CloseVoteRequest' || 
                                  archivedChoice === 'DsoRules_CloseVoteRequestResult' ||
                                  archivedChoice === 'DsoRules_ExecuteConfirmedAction';
      
      // Traditional choice-based detection
      const wasExecutedByChoice = choiceLower.includes('accept') && !choiceLower.includes('reject');
      const wasRejectedByChoice = choiceLower.includes('reject');
      const wasExpiredByChoice = choiceLower.includes('expire');
      
      // Vote-based detection for CloseVoteRequest (all accepts = executed, any rejects or expired = rejected/expired)
      const wasExecutedByVotes = isCloseVoteRequest && acceptCount > 0 && rejectCount === 0;
      const wasRejectedByVotes = isCloseVoteRequest && rejectCount > 0;
      
      const wasExecuted = wasExecutedByChoice || wasExecutedByVotes;
      const wasRejected = wasRejectedByChoice || wasRejectedByVotes;
      const wasExpired = wasExpiredByChoice;
      
      if (isClosed) {
        if (wasExecuted) {
          status = 'executed';
        } else if (wasRejected) {
          status = 'rejected';
        } else if (wasExpired || (finalVoteCount === 0 && isExpired)) {
          status = 'expired';
        } else if (isExpired) {
          // Closed after expiry without explicit action
          status = 'expired';
        } else if (isCloseVoteRequest) {
          // CloseVoteRequest with no clear outcome - check if expired
          status = isExpired ? 'expired' : 'executed'; // Default to executed if closed via CloseVoteRequest
        } else {
          // Closed but no clear accept/reject - likely expired or archived
          status = 'rejected';
        }
      } else if (isExpired) {
        status = 'expired';
      } else {
        status = 'in_progress';
      }

      // Normalize reason - can be string or object
      const rawReason = event.payload?.reason;
      const reasonStr = rawReason
        ? (typeof rawReason === 'string' ? rawReason : JSON.stringify(rawReason))
        : null;

      const voteRequest = {
        event_id: event.event_id,
        // Always set stable_id to a non-null identifier (some older records may lack contract_id)
        stable_id: event.contract_id || event.event_id || event.update_id,
        contract_id: event.contract_id,
        template_id: event.template_id,
        effective_at: event.effective_at,
        status,
        is_closed: isClosed,
        action_tag: event.payload?.action?.tag || null,
        action_value: event.payload?.action?.value ? JSON.stringify(event.payload.action.value) : null,
        requester: event.payload?.requester || null,
        reason: reasonStr,
        // Use final votes from archived event if available, otherwise use created event votes
        votes: finalVotes ? JSON.stringify(finalVotes) : '[]',
        vote_count: finalVoteCount,
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
        // stable_id should never be NULL; fall back to event_id if needed
        const stableIdSql = `'${escapeStr(voteRequest.stable_id ?? voteRequest.event_id)}'`;

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
      } finally {
        const current = inserted + updated;
        indexingProgress = { ...indexingProgress, current, records: current };
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

    // Persist successful build summary for audit trail
    const buildId = `build_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const finalStats = await getVoteRequestStats();
    try {
      await query(`
        INSERT INTO vote_request_build_history (
          build_id, started_at, completed_at, duration_seconds,
          total_indexed, inserted, updated, closed_count,
          in_progress_count, executed_count, rejected_count, expired_count, success
        ) VALUES (
          '${buildId}',
          '${indexingProgress?.startedAt || new Date().toISOString()}',
          now(),
          ${parseFloat(elapsed)},
          ${inserted},
          ${inserted},
          ${updated},
          ${closedContractIds.size},
          ${finalStats.inProgress},
          ${finalStats.executed},
          ${finalStats.rejected},
          ${finalStats.expired},
          true
        )
      `);
      console.log(`   📋 Build summary saved: ${buildId}`);
    } catch (histErr) {
      console.warn('   ⚠️ Failed to save build history:', histErr.message);
    }

    indexingInProgress = false;
    indexingProgress = null;

    if (lockRelease) {
      await lockRelease();
      lockRelease = null;
    }

    return {
      status: 'complete',
      buildId,
      inserted,
      updated,
      closedCount: closedContractIds.size,
      elapsedSeconds: parseFloat(elapsed),
      totalIndexed: inserted,
      stats: finalStats,
    };

  } catch (err) {
    console.error('❌ VoteRequest index build failed:', err);
    indexingInProgress = false;
    indexingProgress = null;

    if (lockRelease) {
      await lockRelease();
      lockRelease = null;
    }

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
 * Get current indexing progress (null when not indexing)
 */
export function getIndexingProgress() {
  return indexingProgress;
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
    const fileStart = Date.now();
    try {
      const result = await readWithTimeout(file, 30000);
      const fileRecords = result.records || [];

      for (const record of fileRecords) {
        // Match only the actual VoteRequest template, not VoteRequestResult, VoteRequestTrackingCid, etc.
        // Template IDs look like: "Splice.DsoRules:VoteRequest"
        if (!record.template_id?.endsWith(':VoteRequest')) continue;

        if (eventType === 'created' && record.event_type === 'created') {
          records.push(record);
        } else if (eventType === 'exercised' && record.event_type === 'exercised') {
          // Capture all exercise choices that close VoteRequests:
          // - Archive (explicit archive)
          // - VoteRequest_Accept, VoteRequest_Reject, VoteRequest_Expire (direct choices)
          // - ARC_DsoRules_VoteRequest_Accept, etc. (DsoRules arc variants)
          // - Any choice containing VoteRequest for safety
          const choice = record.choice || '';
          if (choice === 'Archive' || choice.includes('VoteRequest') || choice.includes('Accept') || choice.includes('Reject') || choice.includes('Expire')) {
            records.push(record);
          }
        }
      }
    } catch (err) {
      // Skip unreadable/hanging files (but still advance progress)
      console.warn(`   ⚠️ Skipping VoteRequest file due to read error: ${file} (${err?.message || err})`);
    } finally {
      filesProcessed++;

      // update shared progress state for status endpoint/UI
      if (indexingProgress) {
        indexingProgress = {
          ...indexingProgress,
          current: filesProcessed,
          total: files.length,
          records: records.length,
        };
      }

      // Log progress every 50 files or every 5 seconds
      const now = Date.now();
      if (filesProcessed % 50 === 0 || (now - lastLogTime > 5000)) {
        const elapsed = (now - startTime) / 1000;
        const pct = ((filesProcessed / files.length) * 100).toFixed(0);
        console.log(`   📂 [${pct}%] ${filesProcessed}/${files.length} files | ${records.length} ${eventType} events | ${elapsed.toFixed(1)}s`);
        lastLogTime = now;
      }

      // If a single file takes a long time, surface it so we know where it stalls
      const tookMs = Date.now() - fileStart;
      if (tookMs > 15000) {
        console.log(`   🐢 Slow VoteRequest file: ${file} (${(tookMs / 1000).toFixed(1)}s)`);
      }
    }
  }

  return { records, filesScanned: filesProcessed };
}

/**
 * Scan all files for VoteRequest events (slow fallback path)
 * Includes DsoRules_CloseVoteRequest exercises for closed votes
 */
async function scanAllFilesForVoteRequests(eventType) {
  const filter = eventType === 'created'
    ? (e) => e.template_id?.endsWith(':VoteRequest') && e.event_type === 'created'
    : (e) => {
        // For exercised events, capture both:
        // 1. Direct exercises on VoteRequest template
        // 2. DsoRules_CloseVoteRequest exercises (the actual closing event)
        if (e.event_type !== 'exercised') return false;
        
        // DsoRules_CloseVoteRequest is the key choice that closes VoteRequests
        const choice = e.choice || '';
        if (choice === 'DsoRules_CloseVoteRequest' || choice === 'DsoRules_CloseVoteRequestResult') {
          return true;
        }
        
        // Also check VoteRequest direct exercises (only the actual VoteRequest template)
        if (!e.template_id?.endsWith(':VoteRequest')) return false;
        return choice === 'Archive' || choice.includes('VoteRequest') || choice.includes('Accept') || choice.includes('Reject') || choice.includes('Expire');
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

/**
 * Scan DsoRules files specifically for CloseVoteRequest exercises
 * These events close VoteRequests and determine their final status
 */
async function scanFilesForDsoCloseVoteRequests(files) {
  const records = [];
  let filesProcessed = 0;
  const startTime = Date.now();
  let lastLogTime = startTime;

  const readWithTimeout = async (file, timeoutMs = 30000) => {
    return Promise.race([
      binaryReader.readBinaryFile(file),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Read timeout')), timeoutMs)
      )
    ]);
  };

  for (const file of files) {
    const fileStart = Date.now();
    try {
      const result = await readWithTimeout(file, 30000);
      const fileRecords = result.records || [];

      for (const record of fileRecords) {
        if (record.event_type !== 'exercised') continue;
        
        const choice = record.choice || '';
        // Only collect DsoRules_CloseVoteRequest - this specifically closes VoteRequests
        // Do NOT include DsoRules_ExecuteConfirmedAction as that's used for many other 
        // governance actions (amulet price updates, validator confirmations, etc.)
        if (choice === 'DsoRules_CloseVoteRequest' || choice === 'DsoRules_CloseVoteRequestResult') {
          records.push(record);
        }
      }
    } catch (err) {
      console.warn(`   ⚠️ Skipping DsoRules file due to read error: ${file} (${err?.message || err})`);
    } finally {
      filesProcessed++;

      if (indexingProgress) {
        indexingProgress = {
          ...indexingProgress,
          current: filesProcessed,
          total: files.length,
          records: records.length,
        };
      }

      const now = Date.now();
      if (filesProcessed % 100 === 0 || (now - lastLogTime > 5000)) {
        const elapsed = (now - startTime) / 1000;
        const pct = ((filesProcessed / files.length) * 100).toFixed(0);
        console.log(`   📂 [${pct}%] ${filesProcessed}/${files.length} DsoRules files | ${records.length} close events | ${elapsed.toFixed(1)}s`);
        lastLogTime = now;
      }

      const tookMs = Date.now() - fileStart;
      if (tookMs > 15000) {
        console.log(`   🐢 Slow DsoRules file: ${file} (${(tookMs / 1000).toFixed(1)}s)`);
      }
    }
  }

  return { records, filesScanned: filesProcessed };
}
