/**
 * Governance Proposal Indexer
 * 
 * Builds unique governance proposals FROM the persistent vote_requests DuckDB table.
 * Groups by proposal identifier (action type + reason URL), and tracks vote progress.
 * 
 * PERSISTENT: Stores proposals in DuckDB so index survives restarts.
 * FAST: Reads from vote_requests table instead of rescanning binary files.
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
 * Generate a unique proposal key from action type and reason URL
 */
function getProposalKey(actionType, reasonUrl) {
  return `${actionType || 'unknown'}::${reasonUrl || 'no-url'}`;
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
 * Parse reason from vote_requests table (can be JSON string or plain string)
 * Falls back to parsing from full payload if needed
 */
function parseReasonFromRow(reasonStr, payloadStr) {
  // Try reason column first
  if (reasonStr) {
    if (typeof reasonStr === 'string') {
      try {
        const parsed = JSON.parse(reasonStr);
        if (parsed && typeof parsed === 'object') {
          return {
            reasonUrl: parsed.url || parsed.text || '',
            reasonBody: parsed.body || parsed.description || '',
          };
        }
      } catch {
        // Not JSON, treat as plain text/URL
        return { reasonUrl: reasonStr, reasonBody: '' };
      }
    }
  }

  // Fallback: try extracting from full payload
  if (payloadStr) {
    try {
      const payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr) : payloadStr;
      const reason = payload?.reason;
      if (reason) {
        return {
          reasonUrl: reason.url || reason.text || '',
          reasonBody: reason.body || reason.description || '',
        };
      }
    } catch {
      // ignore
    }
  }

  return { reasonUrl: '', reasonBody: '' };
}

/**
 * Parse votes from JSON string
 */
function parseVotesFromRow(votesStr) {
  if (!votesStr) return [];
  if (typeof votesStr !== 'string') return Array.isArray(votesStr) ? votesStr : [];
  try {
    const parsed = JSON.parse(votesStr);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

/**
 * Extract the specific action type from nested action structure
 * Actions can be nested like: { tag: "ARC_DsoRules", value: { dsoAction: { tag: "SRARC_SetConfig" } } }
 * We want the innermost specific action type for grouping
 */
function extractActionType(actionTag, actionValueStr, payloadStr) {
  // Try to get the inner action type from action_value
  if (actionValueStr) {
    try {
      const actionValue = typeof actionValueStr === 'string' ? JSON.parse(actionValueStr) : actionValueStr;
      
      // Check for dsoAction nested structure
      if (actionValue?.dsoAction?.tag) {
        return actionValue.dsoAction.tag;
      }
      if (actionValue?.dsoAction) {
        // Check for variant-as-key encoding: { SRARC_SetConfig: {...} }
        const keys = Object.keys(actionValue.dsoAction);
        const actionKey = keys.find(k => 
          k.startsWith('SRARC_') || k.startsWith('CRARC_') || k.startsWith('ARC_')
        );
        if (actionKey) return actionKey;
      }
      
      // Check for direct tag in value
      if (actionValue?.tag) {
        return actionValue.tag;
      }
      
      // Check for variant-as-key in value itself
      const valueKeys = Object.keys(actionValue || {});
      const directActionKey = valueKeys.find(k => 
        k.startsWith('SRARC_') || k.startsWith('CRARC_') || k.startsWith('ARC_')
      );
      if (directActionKey) return directActionKey;
    } catch {
      // ignore parsing errors
    }
  }
  
  // Fallback: try parsing from full payload
  if (payloadStr) {
    try {
      const payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr) : payloadStr;
      const action = payload?.action;
      
      if (action?.value?.dsoAction?.tag) {
        return action.value.dsoAction.tag;
      }
      if (action?.value?.dsoAction) {
        const keys = Object.keys(action.value.dsoAction);
        const actionKey = keys.find(k => 
          k.startsWith('SRARC_') || k.startsWith('CRARC_') || k.startsWith('ARC_')
        );
        if (actionKey) return actionKey;
      }
    } catch {
      // ignore
    }
  }
  
  // Fall back to the outer action tag
  return actionTag || 'unknown';
}

/**
 * Build the governance proposal index from persistent vote_requests table
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

    // Build proposals FROM the persistent VoteRequest index (fast + consistent)
    const countRow = await queryOne(`SELECT COUNT(*) as count FROM vote_requests`);
    const totalVoteRequests = Number(countRow?.count || 0);

    indexingProgress = {
      ...indexingProgress,
      phase: 'loading_vote_requests',
      total: Math.min(totalVoteRequests, Number(limit) || totalVoteRequests),
    };

    if (totalVoteRequests === 0) {
      console.log('   ⚠️ vote_requests table is empty; build the VoteRequest index first');
      indexingInProgress = false;
      indexingProgress = null;
      return {
        summary: {
          voteRequestsScanned: 0,
          uniqueProposals: 0,
          duration: Date.now() - startTime,
        },
        stats: { total: 0, approved: 0, rejected: 0, pending: 0, expired: 0 },
      };
    }

    const voteRequestRows = await query(`
      SELECT
        event_id,
        contract_id,
        effective_at,
        requester,
        action_tag,
        action_value,
        reason,
        vote_before,
        votes,
        vote_count,
        tracking_cid,
        payload
      FROM vote_requests
      ORDER BY effective_at DESC
      LIMIT ${Number(limit) || 10000}
    `);

    console.log(`   Loaded ${voteRequestRows.length} VoteRequest records from DuckDB`);

    indexingProgress = { ...indexingProgress, phase: 'grouping' };

    const proposalMap = new Map();
    const now = new Date();

    // Log sample of first few records for debugging
    if (voteRequestRows.length > 0) {
      const sample = voteRequestRows[0];
      const sampleActionType = extractActionType(sample.action_tag, sample.action_value, sample.payload);
      console.log(`   Sample record: outer_tag=${sample.action_tag}, extracted_type=${sampleActionType}`);
    }

    for (let i = 0; i < voteRequestRows.length; i++) {
      const row = voteRequestRows[i];

      if (indexingProgress) {
        indexingProgress.current = i + 1;
        indexingProgress.records = i + 1;
        indexingProgress.proposals = proposalMap.size;
      }

      // Extract the specific inner action type (e.g., SRARC_SetConfig from nested dsoAction)
      const actionType = extractActionType(row.action_tag, row.action_value, row.payload);
      const { reasonUrl, reasonBody } = parseReasonFromRow(row.reason, row.payload);
      const key = getProposalKey(actionType, reasonUrl);

      const ts = row.effective_at ? new Date(row.effective_at).getTime() : 0;
      const existing = proposalMap.get(key);

      if (!existing || ts > existing.latest_timestamp) {
      const votes = parseVotesFromRow(row.votes);
      let votesFor = 0;
      let votesAgainst = 0;
      for (const v of votes) {
        if (v?.accept === true) votesFor++;
        else if (v?.accept === false) votesAgainst++;
      }

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

        proposalMap.set(key, {
          proposal_key: key,
          latest_timestamp: ts,
          latest_contract_id: row.contract_id,
          latest_event_id: row.event_id,
          requester: row.requester,
          action_type: actionType,
          action_details: row.action_value ? (() => {
            try {
              return JSON.parse(row.action_value);
            } catch {
              return row.action_value;
            }
          })() : null,
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
    }

    indexingProgress = { ...indexingProgress, phase: 'persisting', proposals: proposalMap.size };

    const proposals = Array.from(proposalMap.values()).map((p) => ({
      ...p,
      status: determineStatus(p, now),
    }));

    console.log(`   📦 Persisting ${proposals.length} proposals to DuckDB...`);

    await query(`DELETE FROM governance_proposals`);

    const BATCH_SIZE = 100;
    for (let i = 0; i < proposals.length; i += BATCH_SIZE) {
      const batch = proposals.slice(i, i + BATCH_SIZE);

      const values = batch
        .map(
          (p) => `(
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
      )`
        )
        .join(',\n');

      await query(`
        INSERT INTO governance_proposals (
          proposal_key, latest_event_id, latest_contract_id, latest_timestamp,
          requester, action_type, action_details, reason_url, reason_body,
          vote_before, vote_before_timestamp, votes, votes_for, votes_against,
          tracking_cid, status, created_at, updated_at
        ) VALUES ${values}
      `);
    }

    const stats = {
      total: proposals.length,
      approved: proposals.filter((p) => p.status === 'approved').length,
      rejected: proposals.filter((p) => p.status === 'rejected').length,
      pending: proposals.filter((p) => p.status === 'pending').length,
      expired: proposals.filter((p) => p.status === 'expired').length,
    };

    await query(`
      INSERT INTO governance_index_state (id, last_indexed_at, total_indexed, files_scanned, approved_count, rejected_count, pending_count, expired_count)
      VALUES (1, CURRENT_TIMESTAMP, ${stats.total}, ${voteRequestRows.length}, ${stats.approved}, ${stats.rejected}, ${stats.pending}, ${stats.expired})
      ON CONFLICT (id) DO UPDATE SET
        last_indexed_at = CURRENT_TIMESTAMP,
        total_indexed = ${stats.total},
        files_scanned = ${voteRequestRows.length},
        approved_count = ${stats.approved},
        rejected_count = ${stats.rejected},
        pending_count = ${stats.pending},
        expired_count = ${stats.expired}
    `);

    const duration = Date.now() - startTime;
    console.log(`   ✅ Persisted ${proposals.length} unique proposals in ${duration}ms`);

    return {
      summary: {
        voteRequestsScanned: voteRequestRows.length,
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
    const proposals = rows.map(row => {
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
        voteBeforeTimestamp: row.vote_before_timestamp,
        votes: votes || [],
        votesFor: row.votes_for,
        votesAgainst: row.votes_against,
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
      voteBeforeTimestamp: row.vote_before_timestamp,
      votes: votes || [],
      votesFor: row.votes_for,
      votesAgainst: row.votes_against,
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
      voteBeforeTimestamp: row.vote_before_timestamp,
      votes: votes || [],
      votesFor: row.votes_for,
      votesAgainst: row.votes_against,
      trackingCid: row.tracking_cid,
      status: row.status,
    };
  } catch (err) {
    console.error('Error getting proposal by contract:', err);
    return null;
  }
}
