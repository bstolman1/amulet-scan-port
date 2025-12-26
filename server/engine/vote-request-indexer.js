/**
 * VoteRequest Indexer - Builds persistent DuckDB index for VoteRequest events
 * 
 * STATUS DETERMINATION MODEL (AUTHORITATIVE):
 * ============================================
 * Governance outcomes are derived from VoteRequest state + time, NOT from execution events.
 * 
 * For each VoteRequest:
 *   1. If now < voteBefore → status = "in_progress" (voting still open)
 *   2. If now >= voteBefore (voting closed), compute status from vote tallies:
 *      - Calculate acceptedVotes, rejectedVotes from votes array
 *      - Apply 2/3 supermajority threshold (configurable, default 9/13 SVs)
 *      - If acceptedVotes >= threshold → status = "accepted"
 *      - Else if rejectedVotes >= rejectionThreshold → status = "rejected"  
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
    
    // Legacy fields for backwards compatibility - map 'accepted' to 'executed' for old UI
    const active = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status = 'active' OR status = 'in_progress'`);
    const historical = await queryOne(`SELECT COUNT(*) as count FROM vote_requests WHERE status IN ('accepted', 'rejected', 'expired', 'historical')`);
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
      executed: voteRequestStats.executed,
      rejected: voteRequestStats.rejected,
      expired: voteRequestStats.expired,
      inProgress: voteRequestStats.inProgress,
    },
    directDsoRules: {
      total: directStats.total,
      byChoice: directStats.byChoice,
    },
    combined: {
      total: voteRequestStats.total + directStats.total,
      finalized: voteRequestStats.executed + voteRequestStats.rejected + voteRequestStats.expired + directStats.total,
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
      accepted: 0,  // NEW: Use 'accepted' instead of 'executed'
      rejected: 0,
      expired: 0,
      // Legacy alias for backwards compatibility
      executed: 0,
    };
    for (const row of humanByStatus) {
      if (row.status === 'in_progress' || row.status === 'active') {
        byStatus.in_progress += Number(row.count);
      } else if (row.status === 'accepted') {
        byStatus.accepted += Number(row.count);
      } else if (row.status === 'executed') {
        // Legacy status name - treat as accepted
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
  console.log('   Status model: FINAL iff consuming Exercised event exists on proposal root contract');

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

        // Status determined by consuming exercised events only
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

    // 2️⃣ EXERCISED SCAN SUMMARY
    const missingCount = exercisedResult.missingConsumingCount || 0;
    console.log(`   [VoteRequestIndexer] Exercised scan summary:`, {
      totalCreated: createdResult.records.length,
      consumingExercised: exercisedResult.records.length,
      missingConsumingFlag: missingCount
    });

    // Build map of proposal root contract_id -> consuming exercised event
    // This is keyed by the proposal root contract_id, not by any other identifier.
    // The exercised event scanner already filters for consuming === true.
    const archivedEventsMap = new Map();
    let closedViaDsoRules = 0;
    
    // PATH B: Direct DsoRules governance actions (no VoteRequest contract)
    const directGovernanceActions = [];
    
    // Helper: Extract action subject from DsoRules choice name
    function extractActionSubjectFromChoice(choice) {
      if (!choice) return null;
      // Common patterns: DsoRules_UpdateSvRewardWeight, DsoRules_RevokeFeaturedAppRight, etc.
      const match = choice.match(/DsoRules_(\w+)/);
      if (match) return match[1];
      return choice;
    }
    
    // =============================================================================
    // GENERIC DAML RECORD WALKER
    // Recursively extracts all contractId values from nested DAML structures
    // =============================================================================
    function extractContractIds(node, depth = 0) {
      if (!node || depth > 20) return []; // Prevent infinite recursion
      
      const ids = [];
      
      // Direct contractId field
      if (node.contractId) {
        ids.push(node.contractId);
      }
      
      // DAML record: { record: { fields: [ { value: {...} }, ... ] } }
      if (node.record?.fields && Array.isArray(node.record.fields)) {
        for (const field of node.record.fields) {
          if (field.value) {
            ids.push(...extractContractIds(field.value, depth + 1));
          }
        }
      }
      
      // DAML variant: { variant: { value: {...} } }
      if (node.variant?.value) {
        ids.push(...extractContractIds(node.variant.value, depth + 1));
      }
      
      // DAML optional: { optional: { value: {...} } }
      if (node.optional?.value) {
        ids.push(...extractContractIds(node.optional.value, depth + 1));
      }
      
      // DAML list: { list: [ {...}, ... ] }
      if (node.list && Array.isArray(node.list)) {
        for (const item of node.list) {
          ids.push(...extractContractIds(item, depth + 1));
        }
      }
      
      // Generic object traversal (for non-DAML structures)
      if (typeof node === 'object' && !Array.isArray(node)) {
        for (const key of Object.keys(node)) {
          if (key !== 'record' && key !== 'variant' && key !== 'optional' && key !== 'list') {
            const val = node[key];
            if (typeof val === 'object' && val !== null) {
              ids.push(...extractContractIds(val, depth + 1));
            }
          }
        }
      }
      
      return ids;
    }
    
    // Build set of known VoteRequest contract IDs for matching
    const knownVoteRequestIds = new Set(createdResult.records.map(r => r.contract_id));
    
    let unmatchedCount = 0;
    let voteRequestMatchCount = 0; // Debug counter for successful matches
    const DEBUG_MATCH_LIMIT = 5;
    
    for (const record of exercisedResult.records) {
      // CANTON GOVERNANCE MODEL:
      // The winning rule execution IS the vote outcome.
      // ANY consuming exercised event on :DsoRules template = finalized proposal.
      // The scanner already guarantees consuming === true AND template_id.endsWith(':DsoRules').
      
      const choice = String(record.choice || '');
      
      // =============================================================================
      // EXTENDED DAML WALKER: Extract contractIds from ALL relevant event fields
      // Traverses: exercise_argument, exercise_result, child_event_ids
      // =============================================================================
      const allContractIds = [];
      
      // 1. Extract from exercise_argument
      const exerciseArg = record.exercise_argument || record.payload || record.raw?.choice_argument || {};
      allContractIds.push(...extractContractIds(exerciseArg));
      
      // 2. Extract from exercise_result
      const exerciseResult = record.exercise_result || record.raw?.exercise_result || {};
      allContractIds.push(...extractContractIds(exerciseResult));
      
      // 3. Extract from child_event_ids (may contain references to consumed VoteRequest)
      if (record.child_event_ids && Array.isArray(record.child_event_ids)) {
        for (const childId of record.child_event_ids) {
          if (typeof childId === 'string') {
            allContractIds.push(childId);
          } else if (typeof childId === 'object') {
            allContractIds.push(...extractContractIds(childId));
          }
        }
      }
      
      // 4. Extract from raw event structure (additional coverage)
      if (record.raw) {
        allContractIds.push(...extractContractIds(record.raw));
      }
      
      // Find the VoteRequest contract ID by matching against known created VoteRequests
      let voteRequestCid = null;
      for (const cid of allContractIds) {
        if (knownVoteRequestIds.has(cid)) {
          voteRequestCid = cid;
          break;
        }
      }
      
      // Fallback: try direct field access patterns (legacy support)
      if (!voteRequestCid) {
        voteRequestCid = 
          exerciseArg.voteRequestCid || 
          exerciseArg.voteRequest ||
          record.raw?.choice_argument?.voteRequestCid ||
          record.raw?.choice_argument?.voteRequest;
      }
      
      if (voteRequestCid) {
        // DEBUG: Log first few successful VoteRequest matches
        voteRequestMatchCount++;
        if (voteRequestMatchCount <= DEBUG_MATCH_LIMIT) {
          console.log(`   🎯 VoteRequest match #${voteRequestMatchCount}: choice=${choice}, voteRequestCid=${voteRequestCid.substring(0, 40)}...`);
        }
        
        // Extract outcome from exercise result if available
        const exerciseResultData = record.exercise_result || record.payload?.exercise_result || {};
        const outcome = 
          exerciseResultData.outcome ||
          record.exercise_argument?.outcome ||
          record.payload?.outcome;
        
        let outcomeTag = null;
        if (outcome) {
          if (typeof outcome === 'string') {
            outcomeTag = outcome;
          } else if (outcome.tag) {
            outcomeTag = outcome.tag;
          } else {
            const keys = Object.keys(outcome);
            if (keys.length > 0 && keys[0].startsWith('VRO_')) {
              outcomeTag = keys[0];
            }
          }
        }
        
        // Key by the proposal root contract_id (voteRequestCid)
        archivedEventsMap.set(voteRequestCid, {
          ...record,
          contract_id: voteRequestCid, // Normalize to proposal root
          dso_close_outcome: outcome,
          dso_close_outcome_tag: outcomeTag,
          completedAt: exerciseResultData.completedAt,
          abstainingSvs: exerciseResultData.abstainingSvs,
          offboardedVoters: exerciseResultData.offboardedVoters,
          choice: choice,
          close_source: 'dso_rules',
        });
        closedViaDsoRules++;
      } else {
        // ============================================================
        // PATH B: DIRECT DSORULES GOVERNANCE (no VoteRequest contract)
        // These are standalone governance actions without a formal vote request
        // ============================================================
        unmatchedCount++;
        
        // Collect unmatched events for insertion into direct_governance_actions
        directGovernanceActions.push({
          event_id: record.event_id,
          contract_id: record.contract_id,
          template_id: record.template_id,
          effective_at: record.effective_at,
          choice: choice,
          status: 'executed', // Direct DsoRules executions are always executed
          action_subject: extractActionSubjectFromChoice(choice), 
          exercise_argument: record.exercise_argument ? JSON.stringify(record.exercise_argument) : null,
          exercise_result: record.exercise_result ? JSON.stringify(record.exercise_result) : null,
          dso: record.payload?.dso || null,
          source: 'dso_rules_direct',
        });
        
        // Reduced logging - first 3 only
        if (unmatchedCount <= 3) {
          console.log(`   📋 Direct DsoRules governance: choice=${choice}`);
        }
      }
    }
    
    if (unmatchedCount > 3) {
      console.log(`   📋 ... and ${unmatchedCount - 3} more direct DsoRules governance actions`);
    }
    
    const closedContractIds = new Set(archivedEventsMap.keys());
    
    // INFORMATIONAL: DsoRules execution summary (does NOT affect VoteRequest status)
    console.log(`   [GovernanceIndexer] DsoRules execution scan complete:`, {
      consumingDsoRulesEvents: exercisedResult.records.length,
      executionsLinkedToVoteRequests: closedViaDsoRules,
      directGovernance: directGovernanceActions.length
    });

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

    // Upsert vote requests (ON CONFLICT doesn't throw, so we just count upserts)
    indexingProgress = { ...indexingProgress, phase: 'upsert', current: 0, total: createdResult.records.length, records: 0 };
    let upserted = 0;
    
    // Track status counts (VoteRequest state + time model)
    // - accepted: voteBefore passed AND acceptVotes >= threshold
    // - rejected: voteBefore passed AND rejectVotes >= rejectionThreshold  
    // - expired: voteBefore passed AND neither threshold met
    // - in_progress: voteBefore not yet reached
    // - executed: has consuming DsoRules event (optional, separate from status)
    const statusStats = {
      accepted: 0,            // Voted to accept (met 2/3 threshold)
      rejected: 0,            // Voted to reject (met rejection threshold)
      expired: 0,             // Deadline passed without threshold met
      in_progress: 0,         // Voting still open
      // Execution tracking (separate from status)
      executed: 0,            // Has consuming DsoRules event after acceptance
      // Legacy names for backwards compatibility
      finalized: 0,           // Alias for accepted + rejected + expired
      expired_unfinalized: 0, // Legacy - now just 'expired'
      expired_final: 0,       // Legacy - now just 'expired'
      // Vote totals (for display)
      totalAcceptVotes: 0,
      totalRejectVotes: 0
    };
    
    // 5️⃣ SAMPLE FINALIZED PROPOSALS (first 3)
    const sampleFinalized = [];

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

      // is_closed indicates ledger consumption (consuming exercised event exists), not semantic completion
      const isClosed = !!event.contract_id && closedContractIds.has(event.contract_id);

      // Get archived/exercised event for this contract (single declaration, used for votes and status)
      const archivedEvent = event.contract_id ? archivedEventsMap.get(event.contract_id) : null;
      const archivedNormalized = archivedEvent ? normalizePayload(archivedEvent.payload) : null;
      const finalVotes = archivedNormalized?.votes || normalizedPayload.votes || archivedEvent?.payload?.votes || event.payload?.votes;
      const finalVoteCount = finalVotes?.length || 0;

      const voteBefore = normalizedPayload.voteBefore || event.payload?.voteBefore;
      const voteBeforeDate = voteBefore ? new Date(voteBefore) : null;
      const isExpired = voteBeforeDate && voteBeforeDate < now;

      // ============================================================
      // STATUS DETERMINATION: VoteRequest State + Time Model
      // ============================================================
      // Governance outcomes are derived from VoteRequest state + time, NOT execution events.
      //
      // 1. If now < voteBefore → in_progress (voting open)
      // 2. If now >= voteBefore (voting closed):
      //    a. Count votes: acceptedVotes, rejectedVotes
      //    b. Apply 2/3 supermajority threshold
      //    c. acceptedVotes >= threshold → accepted
      //    d. rejectedVotes >= threshold → rejected
      //    e. Neither threshold → expired
      //
      // Execution tracking is SEPARATE: if consuming DsoRules event exists → executed=true
      // ============================================================
      
      // 2/3 supermajority threshold (default: 9 out of 13 SVs)
      // This is configurable but we use a reasonable default
      const SUPERMAJORITY_THRESHOLD = 9;
      const REJECTION_THRESHOLD = 5; // More than 1/3 = rejection
      
      let status = 'in_progress';
      let hasBeenExecuted = false;
      
      // Count votes from the votes array
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
      
      // Check if execution event exists (for executed flag, not status)
      const executionEvent = event.contract_id ? archivedEventsMap.get(event.contract_id) : null;
      if (executionEvent) {
        hasBeenExecuted = true;
      }

      // STATUS DETERMINATION: Based on VoteRequest state + time
      if (!isExpired) {
        // Voting still open
        status = 'in_progress';
        statusStats.in_progress++;
      } else {
        // Voting closed (voteBefore has passed) - determine outcome from vote tallies
        if (acceptCount >= SUPERMAJORITY_THRESHOLD) {
          status = 'accepted';
          statusStats.accepted++;
          statusStats.finalized++;
          
          // Track if it was also executed
          if (hasBeenExecuted) {
            statusStats.executed++;
          }
        } else if (rejectCount >= REJECTION_THRESHOLD) {
          status = 'rejected';
          statusStats.rejected++;
          statusStats.finalized++;
        } else {
          // Neither threshold met - expired
          status = 'expired';
          statusStats.expired++;
          statusStats.finalized++;
        }
        
        // Collect sample for debug logging
        if (sampleFinalized.length < 3) {
          sampleFinalized.push({
            contract_id: event.contract_id,
            acceptCount,
            rejectCount,
            threshold: SUPERMAJORITY_THRESHOLD,
            hasBeenExecuted,
            status
          });
        }
      }

      // Track vote totals for display purposes
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
        // NEW: Track execution separately from status
        is_executed: hasBeenExecuted,
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
          console.error(`   Error upserting vote request ${voteRequest.event_id}:`, err.message);
        }
        // ON CONFLICT handles duplicates silently, so this path is rarely hit
      } finally {
        indexingProgress = { ...indexingProgress, current: upserted, records: upserted };
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
    // 3️⃣ DSORULES EXECUTION STATS (INFORMATIONAL ONLY)
    // ============================================================
    console.log(`\n[GovernanceIndexer] Execution stats:`);
    console.log(JSON.stringify({
      consumingDsoRulesEvents: exercisedResult.records.length,
      executionsLinkedToVoteRequests: closedViaDsoRules
    }, null, 2));
    
    // ============================================================
    // 4️⃣ MODEL EXPLANATION LOG (ONCE PER RUN)
    // ============================================================
    console.log(`\n[GovernanceIndexer] Model note:`);
    console.log(`VoteRequest status is finalized at voteBefore.`);
    console.log(`DsoRules execution is optional and does not determine accepted/rejected/expired.`);

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
      closedCount: closedContractIds.size,
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
        if (eventType === 'created' && record.event_type === 'created') {
          // Match only the actual VoteRequest template, not VoteRequestResult, VoteRequestTrackingCid, etc.
          // Template IDs look like: "Splice.DsoRules:VoteRequest"
          if (record.template_id?.endsWith(':VoteRequest')) {
            records.push(record);
          }
        } else if (eventType === 'exercised' && record.event_type === 'exercised') {
          // CANTON GOVERNANCE MODEL:
          // There is NO explicit "vote closed" event on Canton.
          // The winning rule execution IS the vote outcome.
          // ANY consuming exercised event on :DsoRules template = finalized proposal.
          // Examples: DsoRules_UpdateSvRewardWeight, DsoRules_RevokeFeaturedAppRight, etc.
          
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

  return { records, filesScanned: filesProcessed, missingConsumingCount };
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
