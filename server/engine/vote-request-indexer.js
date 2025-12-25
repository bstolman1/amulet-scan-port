/**
 * VoteRequest Indexer - Builds persistent DuckDB index for VoteRequest events
 * 
 * STATUS DETERMINATION MODEL:
 * - A proposal's outcome is determined by a CONSUMING Exercised event on the 
 *   proposal's root contract (VoteRequest) — nothing else.
 * - Status is derived from (template_id, choice) on the consuming exercise.
 * - Vote counts are stored for DISPLAY ONLY, not for status determination.
 * - If no consuming exercise exists: in_progress (if before deadline) or expired.
 * 
 * Uses template-to-file index when available to dramatically reduce scan time.
 * 
 * Semantic Key: Each proposal gets a semantic_key that combines action_type + subject
 * to link re-submitted proposals together across different contract IDs.
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

/**
 * Extract the subject/target of a governance action for semantic grouping.
 * This is the "who" or "what" being acted upon.
 * 
 * Examples:
 * - GrantFeaturedAppRight → provider party
 * - UpdateSvRewardWeight → svParty
 * - RevokeFeaturedAppRight → rightCid
 * - SetConfig → config hash
 */
function extractActionSubject(actionDetails) {
  if (!actionDetails) return null;
  
  try {
    // Navigate to the inner action value
    const dsoAction = actionDetails.value?.dsoAction?.value || 
                      actionDetails.value?.dsoAction ||
                      actionDetails.value?.value || 
                      actionDetails.value ||
                      actionDetails;
    
    // GrantFeaturedAppRight - provider is the subject
    if (dsoAction?.provider) {
      return `provider:${dsoAction.provider}`;
    }
    
    // RevokeFeaturedAppRight - rightCid is the subject
    if (dsoAction?.rightCid) {
      return `right:${dsoAction.rightCid}`;
    }
    
    // UpdateSvRewardWeight - svParty is the subject
    if (dsoAction?.svParty) {
      return `sv:${dsoAction.svParty}`;
    }
    
    // CreateUnallocatedUnclaimedActivityRecord - beneficiary is the subject
    if (dsoAction?.beneficiary) {
      return `beneficiary:${dsoAction.beneficiary}`;
    }
    
    // OnboardValidator / OffboardValidator - validator party
    if (dsoAction?.validator) {
      return `validator:${dsoAction.validator}`;
    }
    
    // SetConfig or AddFutureAmuletConfigSchedule - hash the config for grouping
    if (dsoAction?.newSchedule || dsoAction?.config) {
      const configStr = JSON.stringify(dsoAction.newSchedule || dsoAction.config);
      return `config:${simpleHash(configStr)}`;
    }
    
    // Election-related - election request CID
    if (dsoAction?.electionRequestCid) {
      return `election:${dsoAction.electionRequestCid}`;
    }
    
    // Fallback: no specific subject found
    return null;
  } catch (e) {
    return null;
  }
}

/**
 * Build a semantic key for a proposal that survives re-submissions.
 * Format: action_type::subject
 * 
 * This allows grouping proposals that target the same thing even if
 * they have different contract IDs.
 */
function buildSemanticKey(actionTag, actionDetails, requester) {
  const subject = extractActionSubject(actionDetails);
  
  // If we have a specific subject, use it
  if (subject) {
    return `${actionTag || 'unknown'}::${subject}`;
  }
  
  // Fallback: use requester as part of the key (less precise but still groups)
  if (requester) {
    return `${actionTag || 'unknown'}::requester:${requester}`;
  }
  
  // Last resort: just the action type
  return actionTag || 'unknown';
}

/**
 * Simple hash function for deduplication
 */
function simpleHash(str) {
  if (!str) return '0';
  let hash = 0;
  for (let i = 0; i < Math.min(str.length, 200); i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return Math.abs(hash).toString(36);
}

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
    whereClause = "WHERE status IN ('active', 'in_progress')";
  } else if (status === 'historical') {
    // Historical = completed votes
    whereClause = "WHERE status IN ('executed', 'rejected', 'expired', 'historical')";
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
 * CANONICAL MODEL: Query governance proposals collapsed by proposal_id.
 * proposal_id = COALESCE(tracking_cid, contract_id)
 * 
 * Returns the latest VoteRequest for each unique proposal_id, with:
 * - is_human: true for explorer-visible proposals
 * - accept_count, reject_count: final vote tallies
 * - related_count: number of contract_ids for this proposal (migrations/re-submissions)
 * 
 * @param {Object} options
 * @param {number} options.limit - Max results (default 100)
 * @param {number} options.offset - Pagination offset (default 0)
 * @param {string} options.status - Filter: 'all', 'active', 'executed', 'rejected', 'expired'
 * @param {boolean} options.humanOnly - If true, only return is_human=true proposals (default true)
 */
export async function queryCanonicalProposals({ limit = 100, status = 'all', offset = 0, humanOnly = true } = {}) {
  let whereConditions = [];
  
  // Status filter
  if (status === 'active' || status === 'in_progress') {
    whereConditions.push("status IN ('active', 'in_progress')");
  } else if (status === 'executed' || status === 'approved') {
    whereConditions.push("status = 'executed'");
  } else if (status === 'rejected') {
    whereConditions.push("status = 'rejected'");
  } else if (status === 'expired') {
    whereConditions.push("status = 'expired'");
  } else if (status === 'historical' || status === 'completed') {
    whereConditions.push("status IN ('executed', 'rejected', 'expired', 'historical')");
  }
  
  // Human filter - this is the key canonical filter
  if (humanOnly) {
    whereConditions.push("is_human = true");
  }
  
  const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

  // Group by proposal_id to collapse lifecycle duplicates (migrations, re-submissions)
  const results = await query(`
    WITH base AS (
      SELECT * FROM vote_requests ${whereClause}
    ),
    ranked AS (
      SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY COALESCE(proposal_id, contract_id) ORDER BY effective_at DESC) as rn,
        COUNT(*) OVER (PARTITION BY COALESCE(proposal_id, contract_id)) as related_count,
        MIN(effective_at) OVER (PARTITION BY COALESCE(proposal_id, contract_id)) as first_seen,
        MAX(effective_at) OVER (PARTITION BY COALESCE(proposal_id, contract_id)) as last_seen,
        MAX(accept_count) OVER (PARTITION BY COALESCE(proposal_id, contract_id)) as max_accept_count,
        MAX(reject_count) OVER (PARTITION BY COALESCE(proposal_id, contract_id)) as max_reject_count
      FROM base
    )
    SELECT 
      proposal_id,
      event_id,
      stable_id,
      contract_id,
      template_id,
      effective_at,
      status,
      is_closed,
      action_tag,
      action_value,
      requester,
      reason,
      reason_url,
      votes,
      vote_count,
      max_accept_count as accept_count,
      max_reject_count as reject_count,
      vote_before,
      target_effective_at,
      tracking_cid,
      dso,
      semantic_key,
      action_subject,
      is_human,
      related_count,
      first_seen,
      last_seen
    FROM ranked
    WHERE rn = 1
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

  return results.map(r => ({
    ...r,
    action_value: safeJsonParse(r.action_value),
    votes: Array.isArray(r.votes) ? r.votes : (safeJsonParse(r.votes) || []),
    accept_count: Number(r.accept_count) || 0,
    reject_count: Number(r.reject_count) || 0,
    related_count: Number(r.related_count) || 1,
    first_seen: r.first_seen,
    last_seen: r.last_seen,
    is_human: r.is_human === true || r.is_human === 1,
  }));
}

/**
 * Get canonical proposal counts matching explorer semantics
 * Returns counts at each layer of the model
 */
export async function getCanonicalProposalStats() {
  try {
    // Raw events (all VoteRequest records in index)
    const rawTotal = await queryOne(`SELECT COUNT(*) as count FROM vote_requests`);
    
    // Lifecycle proposals (unique proposal_id)
    const lifecycleTotal = await queryOne(`
      SELECT COUNT(DISTINCT COALESCE(proposal_id, contract_id)) as count FROM vote_requests
    `);
    
    // Human proposals (explorer-visible)
    const humanTotal = await queryOne(`
      SELECT COUNT(DISTINCT COALESCE(proposal_id, contract_id)) as count 
      FROM vote_requests 
      WHERE is_human = true
    `);
    
    // Status breakdown for human proposals
    const humanByStatus = await query(`
      WITH latest AS (
        SELECT 
          COALESCE(proposal_id, contract_id) as pid,
          status,
          ROW_NUMBER() OVER (PARTITION BY COALESCE(proposal_id, contract_id) ORDER BY effective_at DESC) as rn
        FROM vote_requests
        WHERE is_human = true
      )
      SELECT status, COUNT(*) as count
      FROM latest
      WHERE rn = 1
      GROUP BY status
    `);
    
    const byStatus = {
      in_progress: 0,
      executed: 0,
      rejected: 0,
      expired: 0,
    };
    for (const row of humanByStatus) {
      if (row.status === 'in_progress' || row.status === 'active') {
        byStatus.in_progress += Number(row.count);
      } else if (row.status === 'executed') {
        byStatus.executed = Number(row.count);
      } else if (row.status === 'rejected') {
        byStatus.rejected = Number(row.count);
      } else if (row.status === 'expired') {
        byStatus.expired = Number(row.count);
      }
    }
    
    return {
      rawEvents: Number(rawTotal?.count || 0),
      lifecycleProposals: Number(lifecycleTotal?.count || 0),
      humanProposals: Number(humanTotal?.count || 0),
      byStatus,
    };
  } catch (err) {
    console.error('Error getting canonical proposal stats:', err);
    return {
      rawEvents: 0,
      lifecycleProposals: 0,
      humanProposals: 0,
      byStatus: { in_progress: 0, executed: 0, rejected: 0, expired: 0 },
    };
  }
}

/**
 * Query governance proposals grouped by semantic_key (legacy).
 * Returns the latest VoteRequest for each unique semantic_key, with timeline info.
 * @deprecated Use queryCanonicalProposals instead for explorer-matching semantics
 */
export async function queryGovernanceProposals({ limit = 100, status = 'all', offset = 0 } = {}) {
  let statusFilter = '';
  if (status === 'active' || status === 'in_progress') {
    statusFilter = "WHERE status IN ('active', 'in_progress')";
  } else if (status === 'executed' || status === 'approved') {
    statusFilter = "WHERE status = 'executed'";
  } else if (status === 'rejected') {
    statusFilter = "WHERE status = 'rejected'";
  } else if (status === 'expired') {
    statusFilter = "WHERE status = 'expired'";
  } else if (status === 'historical' || status === 'completed') {
    statusFilter = "WHERE status IN ('executed', 'rejected', 'expired', 'historical')";
  }

  // Use window functions to get the latest record per semantic_key
  // along with count of related proposals (re-submissions)
  const results = await query(`
    WITH ranked AS (
      SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY COALESCE(semantic_key, stable_id) ORDER BY effective_at DESC) as rn,
        COUNT(*) OVER (PARTITION BY COALESCE(semantic_key, stable_id)) as related_count,
        MIN(effective_at) OVER (PARTITION BY COALESCE(semantic_key, stable_id)) as first_seen,
        MAX(effective_at) OVER (PARTITION BY COALESCE(semantic_key, stable_id)) as last_seen
      FROM vote_requests
      ${statusFilter}
    )
    SELECT 
      event_id,
      stable_id,
      contract_id,
      template_id,
      effective_at,
      status,
      is_closed,
      action_tag,
      action_value,
      requester,
      reason,
      reason_url,
      votes,
      vote_count,
      accept_count,
      reject_count,
      vote_before,
      target_effective_at,
      tracking_cid,
      dso,
      payload,
      semantic_key,
      action_subject,
      proposal_id,
      is_human,
      related_count,
      first_seen,
      last_seen
    FROM ranked
    WHERE rn = 1
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

  return results.map(r => ({
    ...r,
    action_value: safeJsonParse(r.action_value),
    votes: Array.isArray(r.votes) ? r.votes : (safeJsonParse(r.votes) || []),
    payload: safeJsonParse(r.payload),
    accept_count: Number(r.accept_count) || 0,
    reject_count: Number(r.reject_count) || 0,
    is_human: r.is_human === true || r.is_human === 1,
    // Timeline info
    timeline: {
      firstSeen: r.first_seen,
      lastSeen: r.last_seen,
      relatedCount: Number(r.related_count) || 1,
    }
  }));
}

/**
 * Get all VoteRequests related to a semantic_key (proposal history/timeline)
 */
export async function queryProposalTimeline(semanticKey) {
  const results = await query(`
    SELECT 
      event_id,
      stable_id,
      contract_id,
      template_id,
      effective_at,
      status,
      is_closed,
      action_tag,
      action_value,
      requester,
      reason,
      reason_url,
      votes,
      vote_count,
      accept_count,
      reject_count,
      vote_before,
      target_effective_at,
      tracking_cid,
      dso,
      payload,
      semantic_key,
      action_subject,
      proposal_id,
      is_human
    FROM vote_requests
    WHERE semantic_key = '${semanticKey.replace(/'/g, "''")}'
    ORDER BY effective_at DESC
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

  return results.map(r => ({
    ...r,
    action_value: safeJsonParse(r.action_value),
    votes: Array.isArray(r.votes) ? r.votes : (safeJsonParse(r.votes) || []),
    payload: safeJsonParse(r.payload),
    accept_count: Number(r.accept_count) || 0,
    reject_count: Number(r.reject_count) || 0,
    is_human: r.is_human === true || r.is_human === 1,
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
      console.log(`   📂 Found ${voteRequestFiles.length} files containing VoteRequest events`);

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

        // Scan VoteRequest files for archive/exercised events
        console.log('   Scanning VoteRequest files for archive events...');
        indexingProgress = { ...indexingProgress, phase: 'scan:archived', current: 0, total: voteRequestFiles.length, records: 0 };
        exercisedResult = await scanFilesForVoteRequests(voteRequestFiles, 'exercised');

        // NOTE: DsoRules_CloseVoteRequest scanning removed - status is derived from
        // vote counts in the VoteRequest payload (9-of-13 SV supermajority threshold).
        // DsoRules closing events were found absent/unreliable in backfill data.
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

    // Build map of closed contract IDs -> archived event data (with final vote counts and status)
    const archivedEventsMap = new Map();
    let closedViaDsoRules = 0;
    let closedViaDirectArchive = 0;
    
    for (const record of exercisedResult.records) {
      const choice = String(record.choice || '');
      
      // For DsoRules_CloseVoteRequest, extract the VoteRequest contract_id from the exercise arguments
      // The voteRequestCid field contains the contract being closed
      if (choice === 'DsoRules_CloseVoteRequest' || choice === 'DsoRules_CloseVoteRequestResult') {
        // Extract VoteRequest contract ID from exercise arguments
        const voteRequestCid = 
          record.exercise_argument?.voteRequestCid || 
          record.payload?.voteRequestCid ||
          record.exercise_argument?.voteRequest ||
          record.payload?.voteRequest;
        
        if (voteRequestCid) {
          // DsoRules_CloseVoteRequestResult contains the authoritative outcome
          // The result is in exercise_result and contains:
          // - request: VoteRequest (the original vote request)
          // - completedAt: Time
          // - offboardedVoters: [Text]
          // - abstainingSvs: [Text]  
          // - outcome: VoteRequestOutcome (the AUTHORITATIVE final result)
          const exerciseResult = record.exercise_result || record.payload?.exercise_result || {};
          const outcome = 
            exerciseResult.outcome ||
            record.exercise_argument?.outcome ||
            record.payload?.outcome;
          
          // Extract outcome tag (e.g., "VRO_Accepted", "VRO_Rejected", "VRO_Expired")
          // Outcome can be:
          // - { tag: 'VRO_Accepted' } or { VRO_Accepted: {...} }
          // - { tag: 'VRO_Rejected' } or { VRO_Rejected: {...} }
          // - { tag: 'VRO_Expired' } or { VRO_Expired: {...} }
          let outcomeTag = null;
          if (outcome) {
            if (typeof outcome === 'string') {
              outcomeTag = outcome;
            } else if (outcome.tag) {
              outcomeTag = outcome.tag;
            } else {
              // Check for variant-as-key encoding: { VRO_Accepted: {...} }
              const keys = Object.keys(outcome);
              if (keys.length > 0 && keys[0].startsWith('VRO_')) {
                outcomeTag = keys[0];
              }
            }
          }
          
          archivedEventsMap.set(voteRequestCid, {
            ...record,
            contract_id: voteRequestCid,
            dso_close_outcome: outcome,
            dso_close_outcome_tag: outcomeTag, // Parsed outcome tag for easy status detection
            completedAt: exerciseResult.completedAt,
            abstainingSvs: exerciseResult.abstainingSvs,
            offboardedVoters: exerciseResult.offboardedVoters,
            choice: choice, // Preserve the choice for status detection
            close_source: 'dso_rules',
          });
          closedViaDsoRules++;
        }
      } else if (record.contract_id) {
        // Direct archive on VoteRequest template
        archivedEventsMap.set(record.contract_id, {
          ...record,
          close_source: 'direct_archive',
        });
        closedViaDirectArchive++;
      }
    }
    const closedContractIds = new Set(archivedEventsMap.keys());
    
    console.log(`   📊 Close source breakdown:`);
    console.log(`      - DsoRules_CloseVoteRequest: ${closedViaDsoRules}`);
    console.log(`      - Direct VoteRequest archive: ${closedViaDirectArchive}`);
    console.log(`      - Unique closed contracts: ${closedContractIds.size}`);

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
    
    // Track status detection stats (based on consuming exercised events)
    const statusStats = {
      executed: 0,
      rejected: 0,
      expired: 0,
      in_progress: 0,
      detectedByOutcome: 0,  // From DsoRules_CloseVoteRequestResult outcome tag
      detectedByChoice: 0,   // From exercise choice name
      detectedByExpiry: 0,   // From deadline without consuming exercise
      totalAcceptVotes: 0,   // For display only
      totalRejectVotes: 0    // For display only
    };

    // =============================================================================
    // PAYLOAD NORMALIZATION: Handle both DAML record format and normalized format
    // =============================================================================
    // Raw DAML format: { record: { fields: [ { value: {...} }, ... ] } }
    // Normalized format: { requester: "...", action: { tag: "...", value: {...} }, reason: {...}, ... }
    // =============================================================================
    const normalizePayload = (payload) => {
      if (!payload) return {};
      
      // Already normalized format - has direct properties like 'requester', 'action', etc.
      if (payload.requester || payload.action || payload.reason) {
        return payload;
      }
      
      // Raw DAML record format: { record: { fields: [...] } }
      if (payload.record?.fields && Array.isArray(payload.record.fields)) {
        const fields = payload.record.fields;
        const normalized = {};
        
        // VoteRequest field order (from DAML schema):
        // 0: dso (party)
        // 1: requester (text)
        // 2: action (variant - ARC_DsoRules, ARC_AmuletRules, etc.)
        // 3: reason (record with url and body)
        // 4: voteBefore (timestamp)
        // 5: votes (genMap)
        // 6: trackingCid (optional contractId)
        
        // Extract dso (field 0)
        const dsoField = fields[0]?.value;
        if (dsoField?.party) normalized.dso = dsoField.party;
        else if (dsoField?.text) normalized.dso = dsoField.text;
        
        // Extract requester (field 1)
        const requesterField = fields[1]?.value;
        if (requesterField?.text) normalized.requester = requesterField.text;
        else if (typeof requesterField === 'string') normalized.requester = requesterField;
        
        // Extract action (field 2) - this is the critical one for action_tag
        const actionField = fields[2]?.value;
        if (actionField?.variant) {
          // { variant: { constructor: "ARC_DsoRules", value: { record: { fields: [...] } } } }
          normalized.action = {
            tag: actionField.variant.constructor,
            value: actionField.variant.value
          };
        } else if (actionField?.constructor) {
          // Alternative format: { constructor: "ARC_DsoRules", value: {...} }
          normalized.action = {
            tag: actionField.constructor,
            value: actionField.value
          };
        }
        
        // Extract reason (field 3) - record with url and body
        const reasonField = fields[3]?.value;
        if (reasonField?.record?.fields && Array.isArray(reasonField.record.fields)) {
          const reasonFields = reasonField.record.fields;
          const urlField = reasonFields[0]?.value;
          const bodyField = reasonFields[1]?.value;
          normalized.reason = {
            url: urlField?.text || urlField || null,
            body: bodyField?.text || bodyField || null
          };
        } else if (typeof reasonField === 'object') {
          normalized.reason = reasonField;
        }
        
        // Extract voteBefore (field 4)
        const voteBeforeField = fields[4]?.value;
        if (voteBeforeField?.timestamp) {
          // Convert microseconds to ISO string
          const microTs = parseInt(voteBeforeField.timestamp, 10);
          if (!isNaN(microTs)) {
            normalized.voteBefore = new Date(microTs / 1000).toISOString();
          }
        } else if (typeof voteBeforeField === 'string') {
          normalized.voteBefore = voteBeforeField;
        }
        
        // Extract votes (field 5) - genMap
        const votesField = fields[5]?.value;
        if (votesField?.genMap?.entries) {
          normalized.votes = votesField.genMap.entries;
        } else if (Array.isArray(votesField)) {
          normalized.votes = votesField;
        }
        
        // Extract trackingCid (field 6, optional)
        if (fields[6]) {
          const trackingField = fields[6]?.value;
          if (trackingField?.contractId) normalized.trackingCid = trackingField.contractId;
          else if (trackingField?.Some?.contractId) normalized.trackingCid = trackingField.Some.contractId;
        }
        
        return normalized;
      }
      
      // Unknown format - return as-is
      return payload;
    };

    // ============================================================
    // PAYLOAD SHAPE PROBE: Detect payload structure before normalization
    // ============================================================
    const detectPayloadShape = (payload) => {
      if (!payload) return 'null';
      if (payload.action && payload.requester) return 'normalized';
      if (payload.record?.fields && Array.isArray(payload.record.fields)) return 'daml_record';
      return 'unknown';
    };
    
    const shapeStats = {
      normalized: 0,
      daml_record: 0,
      unknown: 0,
      null: 0,
    };
    const shapeSamples = {
      normalized: [],
      daml_record: [],
      unknown: [],
      null: [],
    };
    const MAX_SAMPLES = 5;

    // ============================================================
    // DIAGNOSTIC LOGGING: Track payload extraction during build
    // ============================================================
    let nullPayloadCount = 0;
    let validPayloadCount = 0;
    let emptyPayloadCount = 0;
    const nullPayloadSamples = [];
    
    const totalEvents = createdResult.records.length;
    console.log(`\n   🔬 Starting payload shape probe for ${totalEvents} created events...`);
    
    let processedCount = 0;
    const PROBE_LOG_INTERVAL = 100; // Log every 100 events
    
    for (const event of createdResult.records) {
      processedCount++;
      
      // Progress logging
      if (processedCount % PROBE_LOG_INTERVAL === 0 || processedCount === totalEvents) {
        const pct = Math.round((processedCount / totalEvents) * 100);
        console.log(`   🔬 [${pct}%] Processed ${processedCount}/${totalEvents} events | valid: ${validPayloadCount} | null: ${nullPayloadCount} | empty: ${emptyPayloadCount}`);
      }
      // ============================================================
      // PAYLOAD SHAPE PROBE: Detect and track shape before normalization
      // ============================================================
      const shape = detectPayloadShape(event.payload);
      shapeStats[shape]++;
      
      // Collect samples for each shape (max 5 each)
      if (shapeSamples[shape].length < MAX_SAMPLES) {
        const sample = {
          event_id: event.event_id,
          contract_id: event.contract_id,
          trackingCid: event.payload?.trackingCid || event.payload?.record?.fields?.[6]?.value?.contractId || null,
          shape,
        };
        
        // Add shape-specific structure info
        if (shape === 'normalized') {
          sample.topLevelKeys = Object.keys(event.payload || {}).slice(0, 10);
        } else if (shape === 'daml_record') {
          // Extract field labels from DAML record structure
          sample.fieldLabels = (event.payload?.record?.fields || [])
            .map((f, i) => f.label || `field_${i}`)
            .slice(0, 10);
          sample.fieldCount = event.payload?.record?.fields?.length || 0;
        } else if (shape === 'unknown' && event.payload) {
          sample.topLevelKeys = Object.keys(event.payload).slice(0, 10);
          sample.payloadType = typeof event.payload;
        }
        
        shapeSamples[shape].push(sample);
      }
      
      // NORMALIZE PAYLOAD EARLY - handle both DAML record format and normalized format
      const normalizedPayload = normalizePayload(event.payload);
      
      // Diagnostic: check payload status
      if (!event.payload) {
        nullPayloadCount++;
        if (nullPayloadSamples.length < 5) {
          nullPayloadSamples.push({
            event_id: event.event_id,
            contract_id: event.contract_id,
            hasPayloadField: 'payload' in event,
            payloadType: typeof event.payload,
            rawKeys: event.raw ? Object.keys(event.raw).slice(0, 10) : [],
          });
        }
      } else if (Object.keys(event.payload).length === 0) {
        emptyPayloadCount++;
        if (nullPayloadSamples.length < 5) {
          nullPayloadSamples.push({
            event_id: event.event_id,
            contract_id: event.contract_id,
            payloadType: 'empty_object',
            rawKeys: event.raw ? Object.keys(event.raw).slice(0, 10) : [],
          });
        }
      } else {
        validPayloadCount++;
      }

      const isClosed = !!event.contract_id && closedContractIds.has(event.contract_id);

      // Use archived event data for final vote counts if available
      const archivedEvent = event.contract_id ? archivedEventsMap.get(event.contract_id) : null;
      const archivedNormalized = archivedEvent ? normalizePayload(archivedEvent.payload) : null;
      const finalVotes = archivedNormalized?.votes || normalizedPayload.votes || archivedEvent?.payload?.votes || event.payload?.votes;
      const finalVoteCount = finalVotes?.length || 0;

      const voteBefore = normalizedPayload.voteBefore || event.payload?.voteBefore;
      const voteBeforeDate = voteBefore ? new Date(voteBefore) : null;
      const isExpired = voteBeforeDate && voteBeforeDate < now;

      // ============================================================
      // STATUS DETERMINATION: Consuming Exercised Event Model
      // ============================================================
      // The ONLY authoritative source of proposal status is:
      // 1. If a consuming Exercised event exists on the proposal root contract:
      //    - Status is FINALIZED
      //    - Outcome is determined by (template_id, choice) on that exercise
      // 2. If no consuming Exercised exists:
      //    - If now < vote_before deadline → IN_PROGRESS
      //    - If now >= vote_before deadline → EXPIRED
      //
      // Vote counts are stored for DISPLAY ONLY, not status determination.
      // ============================================================
      
      let status = 'in_progress';
      
      // Count votes for display purposes only (not for status determination)
      const votesArray = Array.isArray(finalVotes) ? finalVotes : [];
      let acceptCount = 0;
      let rejectCount = 0;

      const normalizeVote = (voteData) => {
        if (!voteData || typeof voteData !== 'object') return null;
        if (voteData.accept === true || voteData.Accept === true) return 'accept';
        if (voteData.reject === true || voteData.Reject === true) return 'reject';
        if (voteData.accept === false) return 'reject';
        const tag = voteData.tag || voteData.Tag || voteData.vote?.tag || voteData.vote?.Tag;
        if (typeof tag === 'string') {
          const t = tag.toLowerCase();
          if (t === 'accept') return 'accept';
          if (t === 'reject') return 'reject';
        }
        if (Object.prototype.hasOwnProperty.call(voteData, 'Accept')) return 'accept';
        if (Object.prototype.hasOwnProperty.call(voteData, 'Reject')) return 'reject';
        return null;
      };

      for (const vote of votesArray) {
        const [, voteData] = Array.isArray(vote) ? vote : ['', vote];
        const normalized = normalizeVote(voteData);
        if (normalized === 'accept') acceptCount++;
        else if (normalized === 'reject') rejectCount++;
      }

      // Get the archived/exercised event for this contract (if it exists)
      const archivedEvent = event.contract_id ? archivedEventsMap.get(event.contract_id) : null;
      
      // STATUS DETERMINATION: Based on consuming exercised events
      if (archivedEvent) {
        // A consuming exercised event exists - proposal is FINALIZED
        // Determine outcome from (template_id, choice)
        const choice = String(archivedEvent.choice || '').toLowerCase();
        
        // Check for authoritative outcome from DsoRules_CloseVoteRequestResult first
        const outcomeTag = archivedEvent.dso_close_outcome_tag;
        if (outcomeTag) {
          const tagLower = outcomeTag.toLowerCase();
          if (tagLower.includes('accepted') || tagLower === 'vro_accepted') {
            status = 'executed';
            statusStats.detectedByOutcome = (statusStats.detectedByOutcome || 0) + 1;
          } else if (tagLower.includes('rejected') || tagLower === 'vro_rejected') {
            status = 'rejected';
            statusStats.detectedByOutcome = (statusStats.detectedByOutcome || 0) + 1;
          } else if (tagLower.includes('expired') || tagLower === 'vro_expired') {
            status = 'expired';
            statusStats.detectedByOutcome = (statusStats.detectedByOutcome || 0) + 1;
          }
        }
        
        // If outcome tag didn't resolve status, use the choice name
        if (status === 'in_progress') {
          if (choice.includes('accept') && !choice.includes('reject')) {
            status = 'executed';
            statusStats.detectedByChoice = (statusStats.detectedByChoice || 0) + 1;
          } else if (choice.includes('reject')) {
            status = 'rejected';
            statusStats.detectedByChoice = (statusStats.detectedByChoice || 0) + 1;
          } else if (choice.includes('expire')) {
            status = 'expired';
            statusStats.detectedByChoice = (statusStats.detectedByChoice || 0) + 1;
          } else if (choice === 'archive') {
            // Generic archive - check if deadline passed to distinguish
            if (isExpired) {
              status = 'expired';
            } else {
              // Archived before deadline = likely executed (successful action)
              status = 'executed';
            }
            statusStats.detectedByChoice = (statusStats.detectedByChoice || 0) + 1;
          } else {
            // Unknown consuming choice - mark as finalized with unknown outcome
            // Default to expired if past deadline, otherwise executed
            status = isExpired ? 'expired' : 'executed';
            statusStats.detectedByChoice = (statusStats.detectedByChoice || 0) + 1;
          }
        }
      } else {
        // No consuming exercised event - proposal may still be active or expired by time
        if (isExpired) {
          status = 'expired';
          statusStats.detectedByExpiry = (statusStats.detectedByExpiry || 0) + 1;
        } else {
          status = 'in_progress';
        }
      }

      // Track detection statistics
      statusStats[status]++;
      statusStats.totalAcceptVotes += acceptCount;
      statusStats.totalRejectVotes += rejectCount;

      // Normalize reason - can be string or object with body and url
      const rawReason = normalizedPayload.reason || event.payload?.reason;
      let reasonStr = null;
      let reasonUrl = null;
      if (rawReason) {
        if (typeof rawReason === 'string') {
          reasonStr = rawReason;
        } else if (typeof rawReason === 'object') {
          reasonStr = rawReason.body || null;
          reasonUrl = rawReason.url || null;
        }
      }

      // Build semantic key for proposal grouping
      // Use normalized payload for extraction, fallback to raw payload
      const actionTag = normalizedPayload.action?.tag || event.payload?.action?.tag || null;
      const actionDetails = normalizedPayload.action || event.payload?.action || null;
      const requester = normalizedPayload.requester || event.payload?.requester || null;
      const semanticKey = buildSemanticKey(actionTag, actionDetails, requester);
      const actionSubject = extractActionSubject(actionDetails);
      
      // CANONICAL MODEL: proposal_id = COALESCE(trackingCid, contract_id)
      // This is the authoritative proposal identity that survives migrations/re-submissions
      const trackingCid = normalizedPayload.trackingCid || event.payload?.trackingCid || null;
      const proposalId = trackingCid || event.contract_id || null;
      
      // Extract dso from normalized payload
      const dso = normalizedPayload.dso || event.payload?.dso || null;
      
      // CANONICAL MODEL: is_human classification
      // Explorers only show human-readable governance proposals, excluding:
      // 1. Config maintenance: SRARC_SetConfig, CRARC_SetConfig
      // 2. No narrative AND no votes: (reason IS NULL) AND (vote_count = 0)
      // 3. No mailing list link AND no meaningful participation
      const isConfigMaintenance = actionTag && (
        actionTag === 'SRARC_SetConfig' || 
        actionTag === 'CRARC_SetConfig' ||
        actionTag.includes('SetConfig')
      );
      const hasReason = reasonStr && typeof reasonStr === 'string' && reasonStr.trim().length > 0;
      const hasMailingListLink = reasonUrl && typeof reasonUrl === 'string' && reasonUrl.includes('lists.sync.global');
      const hasVotes = finalVoteCount > 0;
      const hasNarrative = hasReason || hasMailingListLink;
      
      // is_human = NOT config maintenance AND (has narrative OR has votes)
      const isHuman = !isConfigMaintenance && (hasNarrative || hasVotes);

      const voteRequest = {
        event_id: event.event_id,
        // Always set stable_id to a non-null identifier (some older records may lack contract_id)
        stable_id: event.contract_id || event.event_id || event.update_id,
        contract_id: event.contract_id,
        template_id: event.template_id,
        effective_at: event.effective_at,
        status,
        is_closed: isClosed,
        action_tag: actionTag,
        action_value: actionDetails?.value ? JSON.stringify(actionDetails.value) : null,
        requester: requester,
        reason: reasonStr,
        reason_url: reasonUrl,
        // Use final votes from archived event if available, otherwise use created event votes
        votes: finalVotes ? JSON.stringify(finalVotes) : '[]',
        vote_count: finalVoteCount,
        accept_count: acceptCount,
        reject_count: rejectCount,
        vote_before: normalizedPayload.voteBefore || voteBefore || null,
        target_effective_at: normalizedPayload.targetEffectiveAt || event.payload?.targetEffectiveAt || null,
        tracking_cid: trackingCid,
        dso: dso,
        payload: event.payload ? JSON.stringify(event.payload) : null,
        semantic_key: semanticKey,
        action_subject: actionSubject,
        proposal_id: proposalId,
        is_human: isHuman,
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
            requester, reason, reason_url, votes, vote_count, accept_count, reject_count,
            vote_before, target_effective_at, tracking_cid, dso,
            payload, semantic_key, action_subject, proposal_id, is_human, updated_at
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
            ${voteRequest.reason_url ? `'${escapeStr(voteRequest.reason_url)}'` : 'NULL'},
            '${escapeStr(voteRequest.votes)}',
            ${voteRequest.vote_count},
            ${voteRequest.accept_count},
            ${voteRequest.reject_count},
            ${voteRequest.vote_before ? `'${escapeStr(voteRequest.vote_before)}'` : 'NULL'},
            ${voteRequest.target_effective_at ? `'${escapeStr(voteRequest.target_effective_at)}'` : 'NULL'},
            ${voteRequest.tracking_cid ? `'${escapeStr(voteRequest.tracking_cid)}'` : 'NULL'},
            ${voteRequest.dso ? `'${escapeStr(voteRequest.dso)}'` : 'NULL'},
            ${payloadStr ? `'${payloadStr}'` : 'NULL'},
            ${voteRequest.semantic_key ? `'${escapeStr(voteRequest.semantic_key)}'` : 'NULL'},
            ${voteRequest.action_subject ? `'${escapeStr(voteRequest.action_subject)}'` : 'NULL'},
            ${voteRequest.proposal_id ? `'${escapeStr(voteRequest.proposal_id)}'` : 'NULL'},
            ${voteRequest.is_human},
            now()
          )
          ON CONFLICT (event_id) DO UPDATE SET
            stable_id = EXCLUDED.stable_id,
            contract_id = EXCLUDED.contract_id,
            template_id = EXCLUDED.template_id,
            effective_at = EXCLUDED.effective_at,
            status = EXCLUDED.status,
            is_closed = EXCLUDED.is_closed,
            action_tag = EXCLUDED.action_tag,
            action_value = EXCLUDED.action_value,
            requester = EXCLUDED.requester,
            reason = EXCLUDED.reason,
            reason_url = EXCLUDED.reason_url,
            votes = EXCLUDED.votes,
            vote_count = EXCLUDED.vote_count,
            accept_count = EXCLUDED.accept_count,
            reject_count = EXCLUDED.reject_count,
            vote_before = EXCLUDED.vote_before,
            target_effective_at = EXCLUDED.target_effective_at,
            tracking_cid = EXCLUDED.tracking_cid,
            dso = EXCLUDED.dso,
            payload = EXCLUDED.payload,
            semantic_key = EXCLUDED.semantic_key,
            action_subject = EXCLUDED.action_subject,
            proposal_id = EXCLUDED.proposal_id,
            is_human = EXCLUDED.is_human,
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

    // ============================================================
    // PAYLOAD SHAPE PROBE SUMMARY
    // ============================================================
    const shapeTotal = shapeStats.normalized + shapeStats.daml_record + shapeStats.unknown + shapeStats.null;
    console.log(`\n   🔬 PAYLOAD SHAPE PROBE SUMMARY:`);
    console.log(`      Total VoteRequest CREATED events: ${shapeTotal}`);
    console.log(`      - normalized:   ${shapeStats.normalized} (${(shapeStats.normalized / shapeTotal * 100).toFixed(1)}%)`);
    console.log(`      - daml_record:  ${shapeStats.daml_record} (${(shapeStats.daml_record / shapeTotal * 100).toFixed(1)}%)`);
    console.log(`      - unknown:      ${shapeStats.unknown} (${(shapeStats.unknown / shapeTotal * 100).toFixed(1)}%)`);
    console.log(`      - null:         ${shapeStats.null} (${(shapeStats.null / shapeTotal * 100).toFixed(1)}%)`);
    
    // Log samples for each shape
    for (const shapeType of ['normalized', 'daml_record', 'unknown', 'null']) {
      if (shapeSamples[shapeType].length > 0) {
        console.log(`\n      📋 ${shapeType.toUpperCase()} samples (${shapeSamples[shapeType].length}):`);
        shapeSamples[shapeType].forEach((sample, i) => {
          console.log(`         ${i + 1}. event_id: ${sample.event_id}`);
          console.log(`            contract_id: ${sample.contract_id}`);
          if (sample.trackingCid) console.log(`            trackingCid: ${sample.trackingCid}`);
          if (sample.topLevelKeys) console.log(`            topLevelKeys: [${sample.topLevelKeys.join(', ')}]`);
          if (sample.fieldLabels) console.log(`            fieldLabels: [${sample.fieldLabels.join(', ')}]`);
          if (sample.fieldCount !== undefined) console.log(`            fieldCount: ${sample.fieldCount}`);
          if (sample.payloadType) console.log(`            payloadType: ${sample.payloadType}`);
        });
      }
    }

    // ============================================================
    // DIAGNOSTIC SUMMARY: Log payload extraction results
    // ============================================================
    console.log(`\n   📊 PAYLOAD DIAGNOSTIC SUMMARY:`);
    console.log(`      - Valid payloads: ${validPayloadCount}`);
    console.log(`      - Null payloads: ${nullPayloadCount}`);
    console.log(`      - Empty payloads ({}): ${emptyPayloadCount}`);
    console.log(`      - Total processed: ${validPayloadCount + nullPayloadCount + emptyPayloadCount}`);
    if (nullPayloadSamples.length > 0) {
      console.log(`      - Sample problematic events:`);
      nullPayloadSamples.forEach((sample, i) => {
        console.log(`        ${i + 1}. event_id: ${sample.event_id}`);
        console.log(`           contract_id: ${sample.contract_id}`);
        console.log(`           payloadType: ${sample.payloadType}`);
        console.log(`           rawKeys: ${sample.rawKeys?.join(', ') || 'none'}`);
      });
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
    
    // Log status detection statistics (based on consuming exercised events)
    console.log(`   📊 Status detection breakdown (consuming exercised event model):`);
    console.log(`      - Executed: ${statusStats.executed} (${statusStats.detectedByOutcome || 0} by outcome tag, ${statusStats.detectedByChoice || 0} by choice)`);
    console.log(`      - Rejected: ${statusStats.rejected}`);
    console.log(`      - Expired: ${statusStats.expired} (${statusStats.detectedByExpiry || 0} by deadline)`);
    console.log(`      - In Progress: ${statusStats.in_progress}`);
    console.log(`      - Votes (display only): ${statusStats.totalAcceptVotes} accepts, ${statusStats.totalRejectVotes} rejects`);

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
          // CONSUMING EXERCISED EVENT MODEL:
          // Any consuming exercised event on the VoteRequest contract means the proposal is finalized.
          // The outcome is determined by the (template_id, choice) pair.
          //
          // If consuming flag is available, use it. Otherwise, fall back to choice-based heuristics.
          const choice = String(record.choice || '');
          const consuming = record.consuming === true || record.consuming === 'true';
          
          // Primary: Use consuming flag if available
          if (consuming) {
            records.push(record);
          } else if (record.consuming === undefined || record.consuming === null) {
            // Fallback: If consuming flag not available, use choice-based heuristics
            // These are the known closing choices:
            const isClosingChoice =
              choice === 'Archive' ||
              /(^|_)VoteRequest_(Accept|Reject|Expire)/.test(choice) ||
              choice === 'VoteRequest_ExpireVoteRequest';

            if (isClosingChoice) {
              records.push(record);
            }
          }
          // Note: consuming=false means non-consuming exercise (e.g., CastVote) - skip
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
        if (e.event_type !== 'exercised') return false;
        if (!e.template_id?.endsWith(':VoteRequest')) return false;

        const choice = String(e.choice || '');
        const consuming = e.consuming === true || e.consuming === 'true';
        
        // Primary: Use consuming flag if available
        if (consuming) return true;
        
        // Fallback: If consuming flag not available, use choice-based heuristics
        if (e.consuming === undefined || e.consuming === null) {
          return (
            choice === 'Archive' ||
            /(^|_)VoteRequest_(Accept|Reject|Expire)/.test(choice) ||
            choice === 'VoteRequest_ExpireVoteRequest'
          );
        }
        
        return false;
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
  
  // DEBUG: Track all unique choice names found
  const uniqueChoices = new Set();
  const choiceCounts = {};

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
        
        // DEBUG: Collect all unique choice names
        uniqueChoices.add(choice);
        choiceCounts[choice] = (choiceCounts[choice] || 0) + 1;
        
        // Only collect DsoRules_CloseVoteRequest - this specifically closes VoteRequests
        // Do NOT include DsoRules_ExecuteConfirmedAction as that's used for many other 
        // governance actions (amulet price updates, validator confirmations, etc.)
        if (choice === 'DsoRules_CloseVoteRequest' || choice === 'DsoRules_CloseVoteRequestResult') {
          records.push(record);
          
          // DEBUG: Log first few events with full structure to verify exercise_result/outcome
          if (records.length <= 3) {
            console.log(`\n   🔍 DEBUG: DsoRules_CloseVoteRequest event #${records.length}:`);
            console.log(`      - choice: ${record.choice}`);
            console.log(`      - contract_id: ${record.contract_id}`);
            console.log(`      - Top-level keys: ${Object.keys(record).join(', ')}`);
            console.log(`      - Has exercise_result: ${!!record.exercise_result}`);
            console.log(`      - Has exercise_argument: ${!!record.exercise_argument}`);
            console.log(`      - Has payload: ${!!record.payload}`);
            if (record.exercise_result) {
              console.log(`      - exercise_result keys: ${Object.keys(record.exercise_result).join(', ')}`);
              console.log(`      - exercise_result.outcome: ${JSON.stringify(record.exercise_result.outcome)}`);
            }
            if (record.exercise_argument) {
              console.log(`      - exercise_argument keys: ${Object.keys(record.exercise_argument).join(', ')}`);
            }
            if (record.payload) {
              console.log(`      - payload keys: ${Object.keys(record.payload).join(', ')}`);
              if (record.payload.exercise_result) {
                console.log(`      - payload.exercise_result keys: ${Object.keys(record.payload.exercise_result).join(', ')}`);
              }
            }
          }
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

        // Surface "are we even seeing vote/close-ish choices" early (not only at the very end)
        const voteRelatedSoFar = [...uniqueChoices].filter((c) => {
          const cl = String(c || '').toLowerCase();
          return (
            cl.includes('vote') ||
            cl.includes('close') ||
            cl.includes('expire') ||
            cl.includes('reject') ||
            cl.includes('accept')
          );
        });

        console.log(
          `   📂 [${pct}%] ${filesProcessed}/${files.length} DsoRules files | ${records.length} close events | ${elapsed.toFixed(1)}s | ${voteRelatedSoFar.length} vote/close-ish choices seen`
        );
        if (voteRelatedSoFar.length === 0) {
          console.log('      (none yet — likely different choice naming, or closes are not in DsoRules for this migration)');
        }

        lastLogTime = now;
      }

      const tookMs = Date.now() - fileStart;
      if (tookMs > 15000) {
        console.log(`   🐢 Slow DsoRules file: ${file} (${(tookMs / 1000).toFixed(1)}s)`);
      }
    }
  }

  // DEBUG: Log all unique choice names found in DsoRules
  console.log(`\n   🔍 DEBUG: Found ${uniqueChoices.size} unique choice names in DsoRules files:`);
  const sortedChoices = Object.entries(choiceCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 50); // Top 50 choices
  for (const [choice, count] of sortedChoices) {
    console.log(`      - ${choice}: ${count} events`);
  }
  
  // Also log any choices containing "vote" or "close" (case-insensitive)
  const voteRelated = [...uniqueChoices].filter(c => 
    c.toLowerCase().includes('vote') || 
    c.toLowerCase().includes('close') ||
    c.toLowerCase().includes('expire') ||
    c.toLowerCase().includes('reject') ||
    c.toLowerCase().includes('accept')
  );
  if (voteRelated.length > 0) {
    console.log(`\n   🗳️ DEBUG: Vote/Close-related choices found:`);
    for (const choice of voteRelated) {
      console.log(`      - ${choice}: ${choiceCounts[choice]} events`);
    }
  } else {
    console.log(`   ⚠️ DEBUG: No vote/close-related choices found in DsoRules!`);
  }

  return { records, filesScanned: filesProcessed };
}
