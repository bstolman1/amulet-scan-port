/**
 * VoteRequest Indexer - Builds persistent DuckDB index for VoteRequest events
 * 
 * STATUS DETERMINATION MODEL (AUTHORITATIVE):
 * ============================================
 * Governance outcomes are derived from VoteRequest state + time, NOT from execution events.
 * 
 * Threshold calculation (DYNAMIC):
 *   threshold = ceil(2/3 × number_of_active_SVs_at_vote_time)
 *   Same threshold applies to both acceptance AND rejection.
 * 
 * For each VoteRequest:
 *   1. If now < voteBefore → status = "in_progress" (voting still open)
 *   2. If now >= voteBefore (voting closed), compute status from vote tallies:
 *      - Count acceptedVotes, rejectedVotes from votes array
 *      - If acceptedVotes >= threshold → status = "accepted"
 *      - If rejectedVotes >= threshold → status = "rejected"
 *      - Else → status = "expired" (deadline passed without threshold met)
 * 
 * Execution tracking (OPTIONAL, SEPARATE):
 *   - If a consuming DsoRules event exists after acceptance → executed = true
 *   - If not → executed = false (but status is still "accepted")
 * 
 * ❌ INCORRECT ASSUMPTION (removed):
 *    "A vote is finalized only when a consuming DsoRules event exists"
 *    This caused the 98-only ceiling instead of matching ccview's ~220-235.
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
import * as svIndexer from './sv-indexer.js';
import * as dsoRulesIndexer from './dso-rules-indexer.js';

// Default SV count if DSO Rules index is not populated (fallback)
const DEFAULT_SV_COUNT = 13;

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
    // NEW: Status is now 'accepted' (not 'executed') for proposals that passed vote threshold
    const accepted = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status = 'accepted'`);
    const rejected = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status = 'rejected'`);
    const expired = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status = 'expired'`);
    // NEW: Executed is a separate flag tracking whether accepted proposals were executed via DsoRules
    const executed = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE is_executed = true`);
    
    // Legacy fields for backwards compatibility
    const active = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status IN ('in_progress', 'pending')`);
    const historical = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status IN ('accepted', 'rejected', 'expired')`);
    const closed = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE is_closed = true`);
    
    return {
      total: Number(total?.count || 0),
      inProgress: Number(inProgress?.count || 0),
      // NEW: Use 'accepted' as primary status name
      accepted: Number(accepted?.count || 0),
      rejected: Number(rejected?.count || 0),
      expired: Number(expired?.count || 0),
      // Legacy: 'executed' now means "accepted and executed via DsoRules"
      executed: Number(executed?.count || 0),
      // Legacy fields
      active: Number(active?.count || 0),
      historical: Number(historical?.count || 0),
      closed: Number(closed?.count || 0),
    };
  } catch (err) {
    console.error('Error getting vote request stats:', err);
    return { total: 0, inProgress: 0, accepted: 0, rejected: 0, expired: 0, executed: 0, active: 0, historical: 0, closed: 0, directGovernance: 0 };
  }
}

/**
 * Get direct governance action stats (Path B - no VoteRequest)
 */
export async function getDirectGovernanceStats() {
  try {
    const total = await queryOne(`SELECT COUNT(*) as count FROM direct_governance_actions`);
    const byChoice = await query(`
      SELECT choice, COUNT(*) as count 
      FROM direct_governance_actions 
      GROUP BY choice 
      ORDER BY count DESC 
      LIMIT 10
    `);
    
    return {
      total: Number(total?.count || 0),
      byChoice: byChoice || [],
    };
  } catch (err) {
    console.error('Error getting direct governance stats:', err);
    return { total: 0, byChoice: [] };
  }
}

/**
 * Get combined governance stats (VoteRequests + Direct DsoRules)
 */
export async function getCombinedGovernanceStats() {
  const voteRequestStats = await getVoteRequestStats();
  const directStats = await getDirectGovernanceStats();
  
  return {
    voteRequestBacked: {
      total: voteRequestStats.total,
      // NEW: Use 'accepted' as primary status
      accepted: voteRequestStats.accepted,
      rejected: voteRequestStats.rejected,
      expired: voteRequestStats.expired,
      inProgress: voteRequestStats.inProgress,
      // Legacy: executed now means "accepted AND executed via DsoRules"
      executed: voteRequestStats.executed,
    },
    directDsoRules: {
      total: directStats.total,
      byChoice: directStats.byChoice,
    },
    combined: {
      total: voteRequestStats.total + directStats.total,
      // Finalized = accepted + rejected + expired (vote deadline passed)
      finalized: voteRequestStats.accepted + voteRequestStats.rejected + voteRequestStats.expired + directStats.total,
    }
  };
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
  if (status === 'active' || status === 'in_progress' || status === 'pending') {
    // Active/pending = vote deadline not yet passed
    whereClause = "WHERE status IN ('in_progress', 'pending')";
  } else if (status === 'historical' || status === 'completed') {
    // Historical/completed = vote deadline passed (accepted, rejected, expired)
    whereClause = "WHERE status IN ('accepted', 'rejected', 'expired')";
  } else if (status === 'accepted') {
    whereClause = "WHERE status = 'accepted'";
  } else if (status === 'rejected') {
    whereClause = "WHERE status = 'rejected'";
  } else if (status === 'expired') {
    whereClause = "WHERE status = 'expired'";
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
  
  // Status filter - using NEW model: in_progress, accepted, rejected, expired
  if (status === 'active' || status === 'in_progress' || status === 'pending') {
    whereConditions.push("status IN ('in_progress', 'pending')");
  } else if (status === 'accepted' || status === 'executed' || status === 'approved') {
    whereConditions.push("status = 'accepted'");
  } else if (status === 'rejected') {
    whereConditions.push("status = 'rejected'");
  } else if (status === 'expired') {
    whereConditions.push("status = 'expired'");
  } else if (status === 'historical' || status === 'completed') {
    whereConditions.push("status IN ('accepted', 'rejected', 'expired')");
  }
  
  if (humanOnly) {
    whereConditions.push("is_human = true");
  }
  
  const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

  // NOW: Each row IS one proposal (already grouped at index time)
  const results = await query(`
    SELECT 
      proposal_id,
      event_id,
      stable_id,
      contract_id,
      template_id,
      effective_at,
      effective_at as first_seen,
      effective_at as last_seen,
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
      semantic_key,
      action_subject,
      is_human,
      1 as related_count
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
 * NOW: Each row IS one proposal (already grouped at index time)
 */
export async function getCanonicalProposalStats() {
  try {
    // Total proposals in index (each row = one proposal)
    const rawTotal = await queryOne(`SELECT COUNT(*) as count FROM vote_requests`);
    
    // Human proposals (explorer-visible)
    const humanTotal = await queryOne(`
      SELECT COUNT(*) as count FROM vote_requests WHERE is_human = true
    `);
    
    // Status breakdown for human proposals (direct count, no grouping needed)
    const humanByStatus = await query(`
      SELECT status, COUNT(*) as count
      FROM vote_requests
      WHERE is_human = true
      GROUP BY status
    `);
    
    const byStatus = {
      in_progress: 0,
      accepted: 0,
      rejected: 0,
      expired: 0,
      executed: 0,
    };
    for (const row of humanByStatus) {
      if (row.status === 'in_progress' || row.status === 'active') {
        byStatus.in_progress += Number(row.count);
      } else if (row.status === 'accepted') {
        byStatus.accepted += Number(row.count);
      } else if (row.status === 'executed') {
        byStatus.accepted += Number(row.count);
        byStatus.executed = Number(row.count);
      } else if (row.status === 'rejected') {
        byStatus.rejected = Number(row.count);
      } else if (row.status === 'expired') {
        byStatus.expired = Number(row.count);
      }
    }
    
    return {
      rawEvents: Number(rawTotal?.count || 0),
      lifecycleProposals: Number(rawTotal?.count || 0), // Same as rawEvents now
      humanProposals: Number(humanTotal?.count || 0),
      byStatus,
    };
  } catch (err) {
    console.error('Error getting canonical proposal stats:', err);
    return {
      rawEvents: 0,
      lifecycleProposals: 0,
      humanProposals: 0,
      byStatus: { in_progress: 0, accepted: 0, rejected: 0, expired: 0 },
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
 * Query direct governance actions (Path B - no VoteRequest contract)
 */
export async function queryDirectGovernanceActions({ limit = 100, offset = 0 } = {}) {
  const results = await query(`
    SELECT 
      event_id,
      contract_id,
      template_id,
      effective_at,
      choice,
      status,
      action_subject,
      exercise_argument,
      exercise_result,
      dso,
      source
    FROM direct_governance_actions
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
    exercise_argument: safeJsonParse(r.exercise_argument),
    exercise_result: safeJsonParse(r.exercise_result),
    governance_type: 'direct_dso_rules', // Mark as Path B
  }));
}

/**
 * Query combined governance history (VoteRequests + Direct DsoRules)
 * Returns a unified view merging both governance paths
 */
export async function queryCombinedGovernance({ limit = 100, offset = 0, status = 'all' } = {}) {
  let statusFilter = '';
  if (status === 'executed') {
    statusFilter = "WHERE status = 'executed'";
  } else if (status === 'rejected') {
    statusFilter = "WHERE status = 'rejected'";
  } else if (status === 'expired') {
    statusFilter = "WHERE status = 'expired'";
  } else if (status === 'in_progress') {
    statusFilter = "WHERE status = 'in_progress'";
  } else if (status === 'historical' || status === 'completed') {
    statusFilter = "WHERE status IN ('executed', 'rejected', 'expired')";
  }

  // Use UNION ALL to combine both tables
  const results = await query(`
    WITH combined AS (
      SELECT 
        event_id,
        contract_id,
        template_id,
        effective_at,
        status,
        action_tag as action_subject,
        reason,
        requester,
        vote_count,
        accept_count,
        reject_count,
        'vote_request' as governance_type
      FROM vote_requests
      ${statusFilter}
      
      UNION ALL
      
      SELECT 
        event_id,
        contract_id,
        template_id,
        effective_at,
        status,
        action_subject,
        NULL as reason,
        NULL as requester,
        0 as vote_count,
        0 as accept_count,
        0 as reject_count,
        'direct_dso_rules' as governance_type
      FROM direct_governance_actions
      ${statusFilter ? statusFilter.replace('WHERE', 'WHERE') : ''}
    )
    SELECT * FROM combined
    ORDER BY effective_at DESC
    LIMIT ${limit} OFFSET ${offset}
  `);

  return results;
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
  
  // 1️⃣ MODEL BANNER - confirms the binary and model in use
  console.log('\n🗳️ [VoteRequestIndexer] Starting index build...');
  console.log('   Status model: VoteRequest state + time (2/3 SV threshold at vote time)');

  try {
    const startTime = Date.now();

    // Ensure tables exist first
    await ensureIndexTables();

    // Ensure DSO Rules SV membership index is available (needed for correct thresholds)
    // If missing, we build it once up-front to avoid misclassifying proposals as expired.
    try {
      const dsoStats = await dsoRulesIndexer.getDsoIndexStats();
      if (!dsoStats?.isPopulated) {
        console.log('   📜 DSO Rules index not populated — building it for accurate vote thresholds...');
        await dsoRulesIndexer.buildDsoRulesIndex({ force: false });
      }
    } catch (e) {
      console.warn('   ⚠️ Unable to ensure DSO Rules index (will fall back to default SV count):', e?.message || e);
    }

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
    let createdResult;

    if (templateIndexPopulated) {
      // FAST PATH: Use template index to scan only relevant files
      const templateIndexStats = await getTemplateIndexStats();
      console.log(`   📋 Using template index (${templateIndexStats.totalFiles} files indexed)`);

      const voteRequestFiles = await getFilesForTemplate('VoteRequest');
      const totalEventFiles = binaryReader.findBinaryFiles(DATA_PATH, 'events')?.length || 0;
      console.log(`   📂 Found ${voteRequestFiles.length} files containing VoteRequest events (of ~${totalEventFiles} event files)`);

      // Safety check
      const suspiciouslySmall = totalEventFiles > 2000 && voteRequestFiles.length < 50;

      if (voteRequestFiles.length === 0 || suspiciouslySmall) {
        if (suspiciouslySmall) {
          console.warn('   ⚠️ Template index appears incomplete — falling back to full scan');
        } else {
          console.log('   ⚠️ No VoteRequest files found in index, falling back to full scan');
        }
        indexingProgress = { ...indexingProgress, phase: 'scan:created (full)', current: 0, total: 0, records: 0 };
        createdResult = await scanAllFilesForVoteRequests('created');
      } else {
        console.log('   Scanning VoteRequest files for created events...');
        indexingProgress = { ...indexingProgress, phase: 'scan:created', current: 0, total: voteRequestFiles.length, records: 0 };
        createdResult = await scanFilesForVoteRequests(voteRequestFiles, 'created');
      }

    } else {
      // SLOW PATH: Full scan (template index not built yet)
      console.log('   ⚠️ Template index not available, using full scan (this will be slow)');
      console.log('   💡 Run template index build first for faster VoteRequest indexing');

      indexingProgress = { ...indexingProgress, phase: 'scan:created (full)', current: 0, total: 0, records: 0 };
      createdResult = await scanAllFilesForVoteRequests('created');
    }

    console.log(`   ✅ Found ${createdResult.records.length} VoteRequest created events`);
    
    // SKIP DsoRules exercised scan - not needed for status determination
    // Status is now computed from votes + voteBefore time
    // is_executed flag will be false (can be added in a separate pass if needed)
    const archivedEventsMap = new Map();
    const directGovernanceActions = [];

    const now = new Date();

    // Clear existing data if force rebuild
    if (force) {
      try {
        await query('DELETE FROM vote_requests');
        await query('DELETE FROM direct_governance_actions');
        console.log('   Cleared existing indices (vote_requests + direct_governance_actions)');
      } catch (err) {
        // Tables might not exist yet, ignore
      }
    }

    // ============================================================
    // PAYLOAD NORMALIZATION HELPER
    // ============================================================
    const normalizePayload = (payload) => {
      if (!payload) return {};
      if (payload.requester || payload.action || payload.reason) return payload;
      
      const unwrapScalar = (v) => {
        if (v === null || v === undefined) return null;
        if (typeof v !== 'object') return v;
        if (v.party) return v.party;
        if (v.text) return v.text;
        if (v.timestamp) return new Date(parseInt(v.timestamp, 10) / 1000).toISOString();
        if (v.contractId) return v.contractId;
        return v;
      };
      
      if (payload.record?.fields && Array.isArray(payload.record.fields)) {
        const fields = payload.record.fields;
        const normalized = {};
        normalized.dso = unwrapScalar(fields[0]?.value);
        normalized.requester = unwrapScalar(fields[1]?.value);
        const actionField = fields[2]?.value;
        if (actionField?.variant) {
          normalized.action = { tag: actionField.variant.constructor, value: actionField.variant.value };
        }
        const reasonField = fields[3]?.value;
        if (reasonField?.record?.fields) {
          normalized.reason = { url: unwrapScalar(reasonField.record.fields[0]?.value), body: unwrapScalar(reasonField.record.fields[1]?.value) };
        }
        normalized.voteBefore = unwrapScalar(fields[4]?.value);
        const votesField = fields[5]?.value;
        if (votesField?.genMap?.entries) {
          normalized.votes = votesField.genMap.entries.map(e => [unwrapScalar(e?.key), e?.value]);
        }
        normalized.trackingCid = unwrapScalar(fields[6]?.value?.contractId || fields[6]?.value);
        return normalized;
      }
      return payload;
    };

    // ============================================================
    // STEP 1: Build proposal_key for each VoteRequest
    // proposal_key = normalized(action JSON) + mailing_list_url
    // ============================================================
    console.log('   📋 Building proposal keys...');
    const buildProposalKey = (payload) => {
      const normalized = normalizePayload(payload);
      
      // Extract action for normalization
      const action = normalized.action || {};
      const actionTag = action.tag || '';
      const actionValue = action.value || {};
      
      // Sort keys and strip metadata for consistent hashing
      const normalizeObj = (obj) => {
        if (!obj || typeof obj !== 'object') return obj;
        if (Array.isArray(obj)) return obj.map(normalizeObj);
        const sorted = {};
        for (const key of Object.keys(obj).sort()) {
          // Strip metadata fields that change between events
          if (['timestamp', 'createdAt', 'updatedAt', 'recordTime'].includes(key)) continue;
          sorted[key] = normalizeObj(obj[key]);
        }
        return sorted;
      };
      
      const normalizedAction = normalizeObj(actionValue);
      const actionStr = JSON.stringify(normalizedAction);
      
      // Extract mailing list URL if present
      const reason = normalized.reason || {};
      const mailingListUrl = (reason.url && reason.url.includes('lists.sync.global')) 
        ? reason.url 
        : null;
      
      // Combine for proposal key
      const keyParts = [actionTag, actionStr];
      if (mailingListUrl) {
        // Normalize URL by removing trailing slashes and query params
        const cleanUrl = mailingListUrl.split('?')[0].replace(/\/+$/, '');
        keyParts.push(cleanUrl);
      }
      
      return simpleHash(keyParts.join('::'));
    };

    // ============================================================
    // STEP 2: Group ALL VoteRequest events by proposal_key
    // ============================================================
    console.log('   📋 Grouping events by proposal key...');
    
    const proposalGroups = new Map(); // proposal_key -> { events: [], latestEvent, votes: Map }
    
    for (const event of createdResult.records) {
      const payload = event.payload || {};
      const proposalKey = buildProposalKey(payload);
      
      if (!proposalGroups.has(proposalKey)) {
        proposalGroups.set(proposalKey, {
          events: [],
          latestEvent: null,
          allVotes: new Map(), // svParty -> latest vote
          effectiveAt: null,
        });
      }
      
      const group = proposalGroups.get(proposalKey);
      group.events.push(event);
      
      // Track latest event by effective_at
      const eventTime = event.effective_at || event.timestamp || event.record_time || event.created_at_ts;
      if (!group.effectiveAt || (eventTime && eventTime > group.effectiveAt)) {
        group.effectiveAt = eventTime;
        group.latestEvent = event;
      }
      
      // Merge votes from this event
      const normalized = normalizePayload(payload);
      const votes = normalized.votes || [];
      const voteArray = Array.isArray(votes) ? votes : [];
      
      for (const vote of voteArray) {
        const [svParty, voteData] = Array.isArray(vote) ? vote : ['', vote];
        if (svParty && voteData) {
          // Later events overwrite earlier votes (captures vote changes)
          group.allVotes.set(svParty, { ...voteData, castAt: eventTime });
        }
      }
    }
    
    console.log(`   📋 Grouped ${createdResult.records.length} events into ${proposalGroups.size} unique proposals`);

    // ============================================================
    // STEP 3: Compute status ONCE per proposal group (using latest state)
    // ============================================================
    indexingProgress = { ...indexingProgress, phase: 'upsert', current: 0, total: proposalGroups.size, records: 0 };
    let upserted = 0;
    
    const statusStats = {
      accepted: 0,
      rejected: 0,
      expired: 0,
      in_progress: 0,
      executed: 0,
      finalized: 0,
      totalAcceptVotes: 0,
      totalRejectVotes: 0
    };
    
    const sampleFinalized = [];
    const svCountCache = new Map();
    let usedFallbackSvCount = false;

    for (const [proposalKey, group] of proposalGroups) {
      const event = group.latestEvent;
      if (!event) continue;
      
      const normalized = normalizePayload(event.payload);
      
      // Use merged votes from all events in the group
      const mergedVotes = Array.from(group.allVotes.entries()).map(([sv, data]) => [sv, data]);
      const finalVoteCount = mergedVotes.length;
      
      // Determine effective_at (prefer earliest for first_seen semantics)
      const earliestEvent = group.events.reduce((earliest, e) => {
        const t = e.effective_at || e.timestamp || e.record_time || e.created_at_ts;
        const et = earliest.effective_at || earliest.timestamp || earliest.record_time || earliest.created_at_ts;
        return (t && (!et || t < et)) ? e : earliest;
      }, group.events[0]);
      const effectiveAt = earliestEvent.effective_at || earliestEvent.timestamp || earliestEvent.record_time || earliestEvent.created_at_ts;
      
      // Check for execution (any event in group was consumed by DsoRules)
      let hasBeenExecuted = false;
      for (const e of group.events) {
        if (e.contract_id && archivedEventsMap.has(e.contract_id)) {
          hasBeenExecuted = true;
          break;
        }
      }
      
      // Get voteBefore from latest event
      const voteBefore = normalized.voteBefore || event.payload?.voteBefore;
      const voteBeforeDate = voteBefore ? new Date(voteBefore) : null;
      const isVoteBeforeValid = voteBeforeDate && !isNaN(voteBeforeDate.getTime());
      const isExpired = isVoteBeforeValid && voteBeforeDate < now;
      
      // Dynamic threshold calculation
      const voteTime = (isVoteBeforeValid ? voteBefore : null) || effectiveAt;
      let svCountAtVoteTime = svCountCache.get(voteTime);
      if (svCountAtVoteTime === undefined) {
        svCountAtVoteTime = await dsoRulesIndexer.getDsoSvCountAt(voteTime);
        if (svCountAtVoteTime === 0) {
          svCountAtVoteTime = await svIndexer.getSvCountAt(voteTime);
        }
        if (svCountAtVoteTime === 0) {
          svCountAtVoteTime = DEFAULT_SV_COUNT;
          usedFallbackSvCount = true;
        }
        svCountCache.set(voteTime, svCountAtVoteTime);
      }
      const SUPERMAJORITY_THRESHOLD = Math.ceil((svCountAtVoteTime * 2) / 3);
      
      // Count votes from merged votes
      let acceptCount = 0;
      let rejectCount = 0;
      
      const normalizeVote = (voteData) => {
        if (!voteData || typeof voteData !== 'object') return null;
        if (voteData.accept === true) return 'accept';
        if (voteData.accept === false) return 'reject';
        const tag = voteData.tag || voteData.Tag || voteData.vote?.tag || voteData.variant?.constructor;
        if (typeof tag === 'string') {
          const t = tag.toLowerCase();
          if (t.includes('accept')) return 'accept';
          if (t.includes('reject')) return 'reject';
        }
        if (voteData.Accept === true || Object.prototype.hasOwnProperty.call(voteData, 'Accept')) return 'accept';
        if (voteData.Reject === true || Object.prototype.hasOwnProperty.call(voteData, 'Reject')) return 'reject';
        return null;
      };
      
      for (const [, voteData] of mergedVotes) {
        const result = normalizeVote(voteData);
        if (result === 'accept') acceptCount++;
        else if (result === 'reject') rejectCount++;
      }
      
      // COMPUTE STATUS (once per proposal, not per event)
      let status = 'in_progress';
      if (!isExpired) {
        status = 'in_progress';
        statusStats.in_progress++;
      } else {
        if (acceptCount >= SUPERMAJORITY_THRESHOLD) {
          status = 'accepted';
          statusStats.accepted++;
          statusStats.finalized++;
          if (hasBeenExecuted) statusStats.executed++;
        } else if (rejectCount >= SUPERMAJORITY_THRESHOLD) {
          status = 'rejected';
          statusStats.rejected++;
          statusStats.finalized++;
        } else {
          status = 'expired';
          statusStats.expired++;
          statusStats.finalized++;
        }
        
        if (sampleFinalized.length < 3) {
          sampleFinalized.push({
            proposalKey,
            eventCount: group.events.length,
            acceptCount,
            rejectCount,
            svCount: svCountAtVoteTime,
            threshold: SUPERMAJORITY_THRESHOLD,
            status
          });
        }
      }
      
      statusStats.totalAcceptVotes += acceptCount;
      statusStats.totalRejectVotes += rejectCount;
      
      // Build the canonical row for this proposal
      const rawReason = normalized.reason || event.payload?.reason;
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
      
      const actionTag = normalized.action?.tag || event.payload?.action?.tag || null;
      const actionDetails = normalized.action || event.payload?.action || null;
      const requester = normalized.requester || event.payload?.requester || null;
      const semanticKey = buildSemanticKey(actionTag, actionDetails, requester);
      const actionSubject = extractActionSubject(actionDetails);
      
      const trackingCid = normalized.trackingCid || event.payload?.trackingCid || null;
      // Use proposal_key as the canonical ID
      const proposalId = proposalKey;
      
      const dso = normalized.dso || event.payload?.dso || null;
      
      // is_human classification
      const isConfigMaintenance = actionTag && (
        actionTag === 'SRARC_SetConfig' || 
        actionTag === 'CRARC_SetConfig' ||
        actionTag.includes('SetConfig')
      );
      const hasReason = reasonStr && typeof reasonStr === 'string' && reasonStr.trim().length > 0;
      const hasMailingListLink = reasonUrl && typeof reasonUrl === 'string' && reasonUrl.includes('lists.sync.global');
      const hasVotes = finalVoteCount > 0;
      const isHuman = !isConfigMaintenance && (hasReason || hasMailingListLink || hasVotes);
      
      // is_closed: any event in group was consumed
      const isClosed = hasBeenExecuted;
      
      const voteRequest = {
        event_id: event.event_id,
        stable_id: proposalKey, // Use proposal_key as stable identifier
        contract_id: event.contract_id,
        template_id: event.template_id,
        effective_at: effectiveAt,
        status,
        is_closed: isClosed,
        is_executed: hasBeenExecuted,
        action_tag: actionTag,
        action_value: actionDetails?.value ? JSON.stringify(actionDetails.value) : null,
        requester,
        reason: reasonStr,
        reason_url: reasonUrl,
        votes: JSON.stringify(mergedVotes),
        vote_count: finalVoteCount,
        accept_count: acceptCount,
        reject_count: rejectCount,
        vote_before: isVoteBeforeValid ? voteBefore : null,
        target_effective_at: normalized.targetEffectiveAt || event.payload?.targetEffectiveAt || null,
        tracking_cid: trackingCid,
        dso,
        payload: event.payload ? JSON.stringify(event.payload) : null,
        semantic_key: semanticKey,
        action_subject: actionSubject,
        proposal_id: proposalId,
        is_human: isHuman,
        related_count: group.events.length,
      };
      
      try {
        const escapeStr = (val) => (val === null || val === undefined) ? null : String(val).replace(/'/g, "''");
        const payloadStr = voteRequest.payload ? escapeStr(voteRequest.payload) : null;
        const stableIdSql = `'${escapeStr(voteRequest.stable_id)}'`;

        await query(`
          INSERT INTO vote_requests (
            event_id, stable_id, contract_id, template_id, effective_at,
            status, is_closed, is_executed, action_tag, action_value,
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
            ${voteRequest.is_executed},
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
            is_executed = EXCLUDED.is_executed,
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
        upserted++;
      } catch (err) {
        if (!err.message?.includes('duplicate')) {
          console.error(`   Error upserting proposal ${proposalKey}:`, err.message);
        }
      } finally {
        indexingProgress = { ...indexingProgress, current: upserted, records: upserted };
      }
    }

    // ============================================================
    // GROUPING SUMMARY
    // ============================================================
    console.log(`\n   📊 PROPOSAL GROUPING SUMMARY:`);
    console.log(`      - Raw VoteRequest events: ${createdResult.records.length}`);
    console.log(`      - Unique proposals (by proposal_key): ${proposalGroups.size}`);
    console.log(`      - Deduplication rate: ${((1 - proposalGroups.size / createdResult.records.length) * 100).toFixed(1)}%`);

    // ============================================================
    // INSERT DIRECT GOVERNANCE ACTIONS (Path B)
    // ============================================================
    let directUpserted = 0;
    if (directGovernanceActions.length > 0) {
      console.log(`\n   📋 Inserting ${directGovernanceActions.length} direct governance actions...`);
      
      for (const action of directGovernanceActions) {
        try {
          const escapeStr = (val) => (val === null || val === undefined) ? null : String(val).replace(/'/g, "''");
          
          await query(`
            INSERT INTO direct_governance_actions (
              event_id, contract_id, template_id, effective_at,
              choice, status, action_subject,
              exercise_argument, exercise_result, dso, source, updated_at
            ) VALUES (
              '${escapeStr(action.event_id)}',
              ${action.contract_id ? `'${escapeStr(action.contract_id)}'` : 'NULL'},
              ${action.template_id ? `'${escapeStr(action.template_id)}'` : 'NULL'},
              ${action.effective_at ? `'${escapeStr(action.effective_at)}'` : 'NULL'},
              ${action.choice ? `'${escapeStr(action.choice)}'` : 'NULL'},
              '${escapeStr(action.status)}',
              ${action.action_subject ? `'${escapeStr(action.action_subject)}'` : 'NULL'},
              ${action.exercise_argument ? `'${escapeStr(action.exercise_argument)}'` : 'NULL'},
              ${action.exercise_result ? `'${escapeStr(action.exercise_result)}'` : 'NULL'},
              ${action.dso ? `'${escapeStr(action.dso)}'` : 'NULL'},
              '${escapeStr(action.source)}',
              now()
            )
            ON CONFLICT (event_id) DO UPDATE SET
              contract_id = EXCLUDED.contract_id,
              template_id = EXCLUDED.template_id,
              effective_at = EXCLUDED.effective_at,
              choice = EXCLUDED.choice,
              status = EXCLUDED.status,
              action_subject = EXCLUDED.action_subject,
              exercise_argument = EXCLUDED.exercise_argument,
              exercise_result = EXCLUDED.exercise_result,
              dso = EXCLUDED.dso,
              source = EXCLUDED.source,
              updated_at = now()
          `);
          directUpserted++;
        } catch (err) {
          if (!err.message?.includes('duplicate')) {
            console.error(`   Error upserting direct governance action ${action.event_id}:`, err.message);
          }
        }
      }
      console.log(`   ✅ Direct governance actions indexed: ${directUpserted}`);
    }

    // Update index state
    await query(`
      INSERT INTO vote_request_index_state (id, last_indexed_at, total_indexed)
      VALUES (1, now(), ${upserted})
      ON CONFLICT (id) DO UPDATE SET
        last_indexed_at = now(),
        total_indexed = ${upserted}
    `);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n✅ [VoteRequestIndexer] Index built: ${upserted} proposals indexed in ${elapsed}s`);
    
    // ============================================================
    // 1️⃣ AUTHORITATIVE GOVERNANCE SUMMARY LOG
    // ============================================================
    const acceptedNotExecuted = statusStats.accepted - statusStats.executed;
    console.log(`\n[GovernanceIndexer] VoteRequest lifecycle summary:`);
    console.log(JSON.stringify({
      totalVoteRequests: upserted,
      pending: statusStats.in_progress,
      accepted: statusStats.accepted,
      rejected: statusStats.rejected,
      expired: statusStats.expired,
      acceptedExecuted: statusStats.executed,
      acceptedNotExecuted: acceptedNotExecuted
    }, null, 2));
    
    // ============================================================
    // 2️⃣ SANITY-CHECK COMPARISON VS EXPLORERS
    // ============================================================
    const totalCompleted = statusStats.accepted + statusStats.rejected + statusStats.expired;
    const explorerExpectedMidpoint = 227;
    const explorerExpectedRange = { min: 220, max: 235 };
    const delta = totalCompleted - explorerExpectedMidpoint;
    
    console.log(`\n[GovernanceIndexer] Sanity check:`);
    console.log(`- totalCompleted = accepted + rejected + expired = ${totalCompleted}`);
    console.log(`- explorerExpectedRange = ${explorerExpectedRange.min}–${explorerExpectedRange.max}`);
    console.log(`- delta = totalCompleted - explorerExpectedMidpoint = ${delta}`);
    
    if (Math.abs(delta) <= 10) {
      console.log(`✅ Governance counts consistent with external explorers`);
    } else {
      console.warn(`⚠️ Governance counts diverge from explorers — investigate`);
    }
    
    // ============================================================
    // 3️⃣ MODEL NOTE (DsoRules scan skipped for speed)
    // ============================================================
    console.log(`\n[GovernanceIndexer] Note: DsoRules execution scan skipped for speed.`);
    console.log(`   is_executed flag not populated. Status based on votes + voteBefore time.`);
    
    // ============================================================
    // 4️⃣ MODEL EXPLANATION LOG (ONCE PER RUN)
    // ============================================================
    console.log(`\n[GovernanceIndexer] Model note:`);
    console.log(`Threshold = ceil(2/3 × SV count at vote time). Same threshold for accept AND reject.`);
    console.log(`accepted: acceptVotes >= threshold`);
    console.log(`rejected: rejectVotes >= threshold`);
    console.log(`expired: voteBefore passed and neither threshold met`);
    console.log(`in_progress: voting still open (now < voteBefore)`);
    console.log(`DsoRules execution is tracked separately (is_executed flag) but does not affect status.`);
    
    if (usedFallbackSvCount) {
      console.log(`\n⚠️ WARNING: SV membership index was empty - used fallback SV count (${DEFAULT_SV_COUNT}).`);
      console.log(`   For accurate thresholds, rebuild SV index first, then rebuild VoteRequest index.`);
    }

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
          ${upserted},
          ${upserted},
          0,
          ${totalCompleted},
          ${finalStats.inProgress},
          ${finalStats.accepted},
          ${finalStats.rejected},
          ${finalStats.expired},
          true
        )
      `);
      console.log(`\n   📋 Build summary saved: ${buildId}`);
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
      upserted,
      directGovernanceUpserted: directUpserted,
      totalGovernance: upserted + directUpserted,
      closedCount: 0, // DsoRules scan skipped
      elapsedSeconds: parseFloat(elapsed),
      totalIndexed: upserted,
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
  let missingConsumingCount = 0; // Track exercised events lacking consuming flag
  const startTime = Date.now();
  let lastLogTime = startTime;

  const readWithTimeout = async (file, timeoutMs = 120000) => {
    const timeout = new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`timeout after ${timeoutMs}ms`)), timeoutMs)
    );
    return Promise.race([binaryReader.readBinaryFile(file), timeout]);
  };

  let skippedFiles = 0;

  for (const file of files) {
    const fileStart = Date.now();
    try {
      // Try once with a reasonable timeout; retry once with a longer timeout before skipping.
      let result;
      try {
        result = await readWithTimeout(file, 120000);
      } catch (e) {
        result = await readWithTimeout(file, 300000);
      }

      const fileRecords = result.records || [];

      for (const record of fileRecords) {
        if (eventType === 'created' && record.event_type === 'created') {
          // Match only the actual VoteRequest template, not VoteRequestResult, VoteRequestTrackingCid, etc.
          // Template IDs look like: "Splice.DsoRules:VoteRequest"
          if (record.template_id?.endsWith(':VoteRequest')) {
            records.push(record);
          }
        } else if (eventType === 'exercised' && record.event_type === 'exercised') {
          // NOTE: exercised scan is used only for optional vote-final snapshotting/closure detection.
          // We keep it conservative; it should not prevent created events from being indexed.
          if (!record.template_id?.endsWith(':DsoRules')) continue;

          const consuming = record.consuming === true || record.consuming === 'true';
          if (consuming) {
            records.push(record);
          } else if (record.consuming === undefined || record.consuming === null) {
            missingConsumingCount++;
            if (missingConsumingCount <= 5) {
              console.warn(`   ⚠️ Exercised event lacks 'consuming' flag: ${record.contract_id} choice=${record.choice}`);
            }
          }
        }
      }
    } catch (err) {
      skippedFiles++;
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
  // Log summary of missing consuming flags
  if (missingConsumingCount > 0) {
    console.warn(`   ⚠️ Total exercised events lacking 'consuming' flag: ${missingConsumingCount} (treated as non-terminal)`);
  }
  if (skippedFiles > 0) {
    console.warn(`   ⚠️ Skipped ${skippedFiles}/${filesProcessed} VoteRequest files due to read errors/timeouts`);
  }

  return { records, filesScanned: filesProcessed, missingConsumingCount, skippedFiles };
}

/**
 * Scan all files for VoteRequest events (slow fallback path)
 * Includes DsoRules_CloseVoteRequest exercises for closed votes
 */
async function scanAllFilesForVoteRequests(eventType) {
  const filter = eventType === 'created'
    ? (e) => e.template_id?.endsWith(':VoteRequest') && e.event_type === 'created'
    : (e) => {
        // CANTON GOVERNANCE MODEL:
        // There is NO explicit "vote closed" event on Canton.
        // The winning rule execution IS the vote outcome.
        // ANY consuming exercised event on :DsoRules template = finalized proposal.
        if (e.event_type !== 'exercised') return false;
        if (!e.template_id?.endsWith(':DsoRules')) return false;
        const consuming = e.consuming === true || e.consuming === 'true';
        return consuming;
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
// NOTE: scanFilesForDsoCloseVoteRequests removed - no longer needed.
// DsoRules_CloseVoteRequest events are now detected within the main exercised scan
// via consuming === true filtering on VoteRequest template.
