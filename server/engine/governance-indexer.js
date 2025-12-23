/**
 * Governance Proposal Indexer
 * 
 * Queries the vote_requests table (populated by vote-request-indexer) and groups
 * VoteRequest events into unique proposals by (action type + reason URL).
 * 
 * PERSISTENT: Stores proposals in DuckDB so index survives restarts.
 * RELIABLE: Uses already-indexed vote_requests table for complete data.
 */

import { query, queryOne } from '../duckdb/connection.js';

let indexingInProgress = false;
let indexingProgress = null;

/**
 * Ensure governance tables exist
 */
async function ensureGovernanceTables() {
  try {
    const { initEngineSchema } = await import('./schema.js');
    await initEngineSchema();
  } catch (err) {
    console.error('Error ensuring governance tables:', err);
    throw err;
  }
}

/**
 * Simple hash function for string content (djb2)
 */
function hashString(str) {
  if (!str) return 'empty';
  let hash = 5381;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) + hash) + str.charCodeAt(i);
    hash = hash & hash; // Convert to 32bit integer
  }
  return Math.abs(hash).toString(36);
}

/**
 * Generate a unique proposal key from action type and reason URL/body/contract
 * 
 * Priority:
 * 1. If reasonUrl exists → use it (same URL = same proposal)
 * 2. Else if reasonBody exists → use hash of body
 * 3. Else → use contract_id as fallback (each contract is unique proposal)
 */
function getProposalKey(actionType, reasonUrl, reasonBody, contractId) {
  const type = actionType || 'unknown';
  
  // If we have a URL, use it (best case - same URL = same proposal)
  if (reasonUrl && reasonUrl.trim() !== '') {
    return `${type}::url::${reasonUrl}`;
  }
  
  // If we have body text, use a hash of it
  if (reasonBody && reasonBody.trim() !== '') {
    return `${type}::body::${hashString(reasonBody)}`;
  }
  
  // Fallback: use contract ID (each contract becomes its own proposal)
  if (contractId) {
    return `${type}::cid::${contractId}`;
  }
  
  // Last resort (should rarely happen)
  return `${type}::unknown::${Date.now()}`;
}

/**
 * Extract the specific action type from action_tag and action_value
 */
function extractActionType(actionTag, actionValue) {
  // Check for nested action types in action_value
  if (actionValue) {
    const parsed = typeof actionValue === 'string' ? JSON.parse(actionValue) : actionValue;
    
    // Check for dsoAction with specific tag
    const dsoAction = parsed?.dsoAction;
    if (dsoAction?.tag) {
      return dsoAction.tag;
    }
    
    // Check for amuletRulesAction
    const amuletAction = parsed?.amuletRulesAction;
    if (amuletAction?.tag) {
      return amuletAction.tag;
    }
    
    // Check for direct tag in value
    if (parsed?.tag) {
      return parsed.tag;
    }
  }
  
  // Fall back to outer action tag
  return actionTag || 'unknown';
}

/**
 * Extract reason URL from the reason column
 */
function extractReasonUrl(reason) {
  if (!reason) return '';
  
  try {
    const parsed = typeof reason === 'string' ? JSON.parse(reason) : reason;
    return parsed?.url || parsed?.text || '';
  } catch {
    return typeof reason === 'string' ? reason : '';
  }
}

/**
 * Extract reason body from the reason column
 */
function extractReasonBody(reason) {
  if (!reason) return '';
  
  try {
    const parsed = typeof reason === 'string' ? JSON.parse(reason) : reason;
    return parsed?.body || parsed?.description || '';
  } catch {
    return '';
  }
}

/**
 * Parse votes from the votes column
 */
function parseVotes(votes) {
  if (!votes) return [];
  
  try {
    const parsed = typeof votes === 'string' ? JSON.parse(votes) : votes;
    if (!Array.isArray(parsed)) return [];
    
    return parsed.map(vote => {
      if (Array.isArray(vote) && vote.length >= 2) {
        const [svName, voteData] = vote;
        return {
          svName,
          sv: voteData?.sv || svName,
          accept: voteData?.accept === true,
          reasonUrl: voteData?.reason?.url || '',
          reasonBody: voteData?.reason?.body || '',
          castAt: voteData?.optCastAt || null,
        };
      }
      return null;
    }).filter(Boolean);
  } catch {
    return [];
  }
}

/**
 * Determine proposal status based on votes and expiry
 */
function determineStatus(proposal, now = new Date()) {
  const voteBeforeDate = proposal.vote_before_timestamp
    ? new Date(proposal.vote_before_timestamp)
    : null;

  if (proposal.tracking_cid && proposal.votes_for > proposal.votes_against) {
    return 'approved';
  }

  if (voteBeforeDate && voteBeforeDate < now) {
    if (proposal.votes_for > proposal.votes_against) {
      return 'approved';
    } else if (proposal.votes_against > 0) {
      return 'rejected';
    } else {
      return 'expired';
    }
  }

  return 'pending';
}

/**
 * Get indexing progress
 */
export function getIndexingProgress() {
  return indexingProgress;
}

/**
 * Check if indexing is in progress
 */
export function isGovernanceIndexingInProgress() {
  return indexingInProgress;
}

/**
 * Clear the proposal cache (no-op for persistent, but triggers re-query)
 */
export function invalidateCache() {
  console.log('🗳️ Governance proposal cache invalidated (will re-query from DB)');
}

// SQL helpers
function sqlStr(val) {
  if (val === null || val === undefined) return 'NULL';
  return `'${String(val).replace(/'/g, "''")}'`;
}

function sqlJson(val) {
  if (val === null || val === undefined) return 'NULL';
  try {
    const json = typeof val === 'string' ? val : JSON.stringify(val);
    return `'${json.replace(/'/g, "''")}'`;
  } catch {
    return 'NULL';
  }
}

/**
 * Build the governance proposal index by querying vote_requests table
 * Persists results to DuckDB
 */
export async function buildGovernanceIndex({ limit = 10000, forceRefresh = false } = {}) {
  if (indexingInProgress) {
    console.log('⏳ Governance indexing already in progress');
    return { status: 'in_progress', progress: indexingProgress };
  }

  // Check if already indexed (unless force)
  if (!forceRefresh) {
    const stats = await getProposalStats();
    if (stats && stats.total > 0) {
      console.log(`✅ Governance index already populated (${stats.total} proposals), skipping rebuild`);
      return { status: 'already_populated', stats };
    }
  }

  indexingInProgress = true;
  indexingProgress = {
    phase: 'starting',
    current: 0,
    total: 0,
    records: 0,
    proposals: 0,
    startedAt: new Date().toISOString(),
  };
  console.log('\n🗳️ Building governance proposal index (from vote_requests table)...');

  try {
    await ensureGovernanceTables();

    const startTime = Date.now();

    // Query vote_requests table for all VoteRequest records
    indexingProgress = { ...indexingProgress, phase: 'querying' };
    
    const rows = await query(`
      SELECT 
        event_id,
        contract_id,
        template_id,
        effective_at,
        requester,
        action_tag,
        action_value,
        reason,
        votes,
        vote_count,
        vote_before,
        tracking_cid,
        payload
      FROM vote_requests
      ORDER BY effective_at DESC
      LIMIT ${limit}
    `);

    console.log(`   Queried ${rows.length} vote request records from table`);
    indexingProgress = { ...indexingProgress, phase: 'scanning', total: rows.length, records: rows.length };

    // Group by unique proposal (action type + reason URL)
    indexingProgress = { ...indexingProgress, phase: 'grouping' };
    const proposalMap = new Map();

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      
      const actionType = extractActionType(row.action_tag, row.action_value);
      const reasonUrl = extractReasonUrl(row.reason);
      const reasonBody = extractReasonBody(row.reason);
      const votes = parseVotes(row.votes);
      
      let votesFor = 0;
      let votesAgainst = 0;
      for (const v of votes) {
        if (v.accept) votesFor++;
        else votesAgainst++;
      }

      // Parse voteBefore timestamp
      let voteBeforeTimestamp = null;
      if (row.vote_before) {
        const vb = String(row.vote_before);
        if (/^\d+$/.test(vb)) {
          voteBeforeTimestamp = parseInt(vb.slice(0, 13), 10);
        } else {
          const parsedVb = new Date(vb).getTime();
          voteBeforeTimestamp = Number.isFinite(parsedVb) ? parsedVb : null;
        }
      }

      const key = getProposalKey(actionType, reasonUrl, reasonBody, row.contract_id);
      const timestamp = row.effective_at ? new Date(row.effective_at).getTime() : 0;
      const existing = proposalMap.get(key);

      if (!existing || timestamp > existing.latest_timestamp) {
        proposalMap.set(key, {
          proposal_key: key,
          latest_timestamp: timestamp,
          latest_contract_id: row.contract_id,
          latest_event_id: row.event_id,
          requester: row.requester,
          action_type: actionType,
          action_details: row.action_value,
          reason_url: reasonUrl,
          reason_body: reasonBody,
          vote_before: row.vote_before,
          vote_before_timestamp: voteBeforeTimestamp,
          votes,
          votes_for: votesFor,
          votes_against: votesAgainst,
          tracking_cid: row.tracking_cid,
        });
      }

      indexingProgress.current = i + 1;
      indexingProgress.proposals = proposalMap.size;
    }

    // Convert to array and determine status
    indexingProgress = { ...indexingProgress, phase: 'persisting', proposals: proposalMap.size };
    const now = new Date();
    const proposals = Array.from(proposalMap.values()).map((p) => ({
      ...p,
      status: determineStatus(p, now),
    }));

    console.log(`   📦 Persisting ${proposals.length} unique proposals to DuckDB...`);

    // Clear existing and insert new
    await query(`DELETE FROM governance_proposals`);

    // Insert in batches
    const BATCH_SIZE = 100;
    for (let i = 0; i < proposals.length; i += BATCH_SIZE) {
      const batch = proposals.slice(i, i + BATCH_SIZE);

      const values = batch.map((p) => `(
        ${sqlStr(p.proposal_key)},
        ${sqlStr(p.latest_event_id)},
        ${sqlStr(p.latest_contract_id)},
        ${p.latest_timestamp ? `TIMESTAMP '${new Date(p.latest_timestamp).toISOString()}'` : 'NULL'},
        ${sqlStr(p.requester)},
        ${sqlStr(p.action_type)},
        ${sqlJson(p.action_details)},
        ${sqlStr(p.reason_url)},
        ${sqlStr(p.reason_body)},
        ${sqlStr(p.vote_before)},
        ${p.vote_before_timestamp || 'NULL'},
        ${sqlJson(p.votes)},
        ${p.votes_for},
        ${p.votes_against},
        ${sqlStr(p.tracking_cid)},
        ${sqlStr(p.status)},
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
      )`).join(',\n');

      await query(`
        INSERT INTO governance_proposals (
          proposal_key, latest_event_id, latest_contract_id, latest_timestamp,
          requester, action_type, action_details, reason_url, reason_body,
          vote_before, vote_before_timestamp, votes, votes_for, votes_against,
          tracking_cid, status, created_at, updated_at
        ) VALUES ${values}
      `);
    }

    // Update index state
    const stats = {
      total: proposals.length,
      approved: proposals.filter((p) => p.status === 'approved').length,
      rejected: proposals.filter((p) => p.status === 'rejected').length,
      pending: proposals.filter((p) => p.status === 'pending').length,
      expired: proposals.filter((p) => p.status === 'expired').length,
    };

    await query(`
      INSERT INTO governance_index_state (id, last_indexed_at, total_indexed, files_scanned, approved_count, rejected_count, pending_count, expired_count)
      VALUES (1, now(), ${stats.total}, 0, ${stats.approved}, ${stats.rejected}, ${stats.pending}, ${stats.expired})
      ON CONFLICT (id) DO UPDATE SET
        last_indexed_at = now(),
        total_indexed = ${stats.total},
        files_scanned = 0,
        approved_count = ${stats.approved},
        rejected_count = ${stats.rejected},
        pending_count = ${stats.pending},
        expired_count = ${stats.expired}
    `);

    const duration = Date.now() - startTime;
    console.log(`   ✅ Persisted ${proposals.length} unique proposals in ${duration}ms`);
    console.log(`   📊 Status: ${stats.approved} approved, ${stats.rejected} rejected, ${stats.pending} pending, ${stats.expired} expired`);

    return {
      summary: {
        recordsQueried: rows.length,
        uniqueProposals: proposals.length,
        duration,
      },
      stats,
    };
  } finally {
    indexingInProgress = false;
    indexingProgress = null;
  }
}

/**
 * Get proposal stats from persistent storage
 */
export async function getProposalStats() {
  try {
    const state = await queryOne(`
      SELECT total_indexed, approved_count, rejected_count, pending_count, expired_count, last_indexed_at
      FROM governance_index_state
      WHERE id = 1
    `);

    if (!state) {
      // Check if we have any proposals even without state
      const count = await queryOne(`SELECT COUNT(*) as count FROM governance_proposals`);
      if (count && Number(count.count) > 0) {
        // Count by status
        const approved = await queryOne(`SELECT COUNT(*) as count FROM governance_proposals WHERE status = 'approved'`);
        const rejected = await queryOne(`SELECT COUNT(*) as count FROM governance_proposals WHERE status = 'rejected'`);
        const pending = await queryOne(`SELECT COUNT(*) as count FROM governance_proposals WHERE status = 'pending'`);
        const expired = await queryOne(`SELECT COUNT(*) as count FROM governance_proposals WHERE status = 'expired'`);

        return {
          total: Number(count.count),
          approved: Number(approved?.count || 0),
          rejected: Number(rejected?.count || 0),
          pending: Number(pending?.count || 0),
          expired: Number(expired?.count || 0),
        };
      }
      return null;
    }

    return {
      total: Number(state.total_indexed || 0),
      approved: Number(state.approved_count || 0),
      rejected: Number(state.rejected_count || 0),
      pending: Number(state.pending_count || 0),
      expired: Number(state.expired_count || 0),
      lastIndexedAt: state.last_indexed_at,
    };
  } catch (err) {
    console.error('Error getting proposal stats:', err);
    return null;
  }
}

/**
 * Query proposals with filters from persistent storage
 */
export async function queryProposals({
  limit = 100,
  offset = 0,
  status = null,
  actionType = null,
  requester = null,
  search = null,
} = {}) {
  try {
    const conditions = [];

    if (status) {
      conditions.push(`status = ${sqlStr(status)}`);
    }

    if (actionType) {
      conditions.push(`action_type = ${sqlStr(actionType)}`);
    }

    if (requester) {
      conditions.push(`LOWER(requester) LIKE LOWER('%' || ${sqlStr(requester)} || '%')`);
    }

    if (search) {
      conditions.push(`(
        LOWER(reason_body) LIKE LOWER('%' || ${sqlStr(search)} || '%') OR
        LOWER(reason_url) LIKE LOWER('%' || ${sqlStr(search)} || '%') OR
        LOWER(requester) LIKE LOWER('%' || ${sqlStr(search)} || '%') OR
        LOWER(action_type) LIKE LOWER('%' || ${sqlStr(search)} || '%')
      )`);
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    // Get total count
    const countResult = await queryOne(`SELECT COUNT(*) as count FROM governance_proposals ${whereClause}`);
    const total = Number(countResult?.count || 0);

    // Get proposals
    const rows = await query(`
      SELECT * FROM governance_proposals
      ${whereClause}
      ORDER BY latest_timestamp DESC
      LIMIT ${limit} OFFSET ${offset}
    `);

    // Parse JSON fields
    const toSafeNumber = (v) => (typeof v === 'bigint' ? Number(v) : v);

    const proposals = rows.map((row) => {
      let actionDetails = row.action_details;
      let votes = row.votes;

      try {
        if (typeof actionDetails === 'string') actionDetails = JSON.parse(actionDetails);
      } catch { /* ignore */ }

      try {
        if (typeof votes === 'string') votes = JSON.parse(votes);
      } catch { /* ignore */ }

      return {
        proposalKey: row.proposal_key,
        latestEventId: row.latest_event_id,
        latestContractId: row.latest_contract_id,
        latestTimestamp: row.latest_timestamp ? new Date(row.latest_timestamp).getTime() : null,
        rawTimestamp: row.latest_timestamp,
        requester: row.requester,
        actionType: row.action_type,
        actionDetails,
        reasonUrl: row.reason_url,
        reasonBody: row.reason_body,
        voteBefore: row.vote_before,
        voteBeforeTimestamp: row.vote_before_timestamp != null ? toSafeNumber(row.vote_before_timestamp) : null,
        votes: votes || [],
        votesFor: toSafeNumber(row.votes_for),
        votesAgainst: toSafeNumber(row.votes_against),
        trackingCid: row.tracking_cid,
        status: row.status,
      };
    });

    return {
      proposals,
      pagination: {
        total,
        limit,
        offset,
        hasMore: offset + limit < total,
      },
    };
  } catch (err) {
    console.error('Error querying proposals:', err);
    return { proposals: [], pagination: { total: 0, limit, offset, hasMore: false } };
  }
}

/**
 * Get a single proposal by key
 */
export async function getProposalByKey(proposalKey) {
  try {
    const row = await queryOne(`
      SELECT * FROM governance_proposals
      WHERE proposal_key = ${sqlStr(proposalKey)}
    `);

    if (!row) return null;

    let actionDetails = row.action_details;
    let votes = row.votes;

    try {
      if (typeof actionDetails === 'string') actionDetails = JSON.parse(actionDetails);
    } catch { /* ignore */ }

    try {
      if (typeof votes === 'string') votes = JSON.parse(votes);
    } catch { /* ignore */ }

    const toSafeNumber = (v) => (typeof v === 'bigint' ? Number(v) : v);

    return {
      proposalKey: row.proposal_key,
      latestEventId: row.latest_event_id,
      latestContractId: row.latest_contract_id,
      latestTimestamp: row.latest_timestamp ? new Date(row.latest_timestamp).getTime() : null,
      rawTimestamp: row.latest_timestamp,
      requester: row.requester,
      actionType: row.action_type,
      actionDetails,
      reasonUrl: row.reason_url,
      reasonBody: row.reason_body,
      voteBefore: row.vote_before,
      voteBeforeTimestamp: row.vote_before_timestamp != null ? toSafeNumber(row.vote_before_timestamp) : null,
      votes: votes || [],
      votesFor: toSafeNumber(row.votes_for),
      votesAgainst: toSafeNumber(row.votes_against),
      trackingCid: row.tracking_cid,
      status: row.status,
    };
  } catch (err) {
    console.error('Error getting proposal by key:', err);
    return null;
  }
}

/**
 * Get proposals by contract ID
 */
export async function getProposalByContractId(contractId) {
  try {
    const row = await queryOne(`
      SELECT * FROM governance_proposals
      WHERE latest_contract_id = ${sqlStr(contractId)}
    `);

    if (!row) return null;

    let actionDetails = row.action_details;
    let votes = row.votes;

    try {
      if (typeof actionDetails === 'string') actionDetails = JSON.parse(actionDetails);
    } catch { /* ignore */ }

    try {
      if (typeof votes === 'string') votes = JSON.parse(votes);
    } catch { /* ignore */ }

    const toSafeNumber = (v) => (typeof v === 'bigint' ? Number(v) : v);

    return {
      proposalKey: row.proposal_key,
      latestEventId: row.latest_event_id,
      latestContractId: row.latest_contract_id,
      latestTimestamp: row.latest_timestamp ? new Date(row.latest_timestamp).getTime() : null,
      rawTimestamp: row.latest_timestamp,
      requester: row.requester,
      actionType: row.action_type,
      actionDetails,
      reasonUrl: row.reason_url,
      reasonBody: row.reason_body,
      voteBefore: row.vote_before,
      voteBeforeTimestamp: row.vote_before_timestamp != null ? toSafeNumber(row.vote_before_timestamp) : null,
      votes: votes || [],
      votesFor: toSafeNumber(row.votes_for),
      votesAgainst: toSafeNumber(row.votes_against),
      trackingCid: row.tracking_cid,
      status: row.status,
    };
  } catch (err) {
    console.error('Error getting proposal by contract:', err);
    return null;
  }
}
