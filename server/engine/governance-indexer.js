/**
 * Governance Proposal Indexer
 * 
 * Extracts unique governance proposals from VoteRequest created events,
 * groups by proposal identifier (action type + reason URL), and tracks
 * vote progress and final outcomes.
 * 
 * PERSISTENT: Stores proposals in DuckDB so index survives restarts.
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
 * Parse VoteRequest payload to extract structured proposal data
 */
function parseVoteRequestPayload(payload) {
  if (!payload) return null;

  try {
    const fields = payload.record?.fields || payload.fields || payload;
    
    if (payload.dso && payload.requester) {
      return parseNamedFields(payload);
    }
    
    if (Array.isArray(fields)) {
      return parseArrayFields(fields);
    }
    
    return null;
  } catch (err) {
    console.error('Error parsing VoteRequest payload:', err.message);
    return null;
  }
}

/**
 * Parse named field format
 */
function parseNamedFields(payload) {
  const action = payload.action;
  const reason = payload.reason;
  const voteBefore = payload.voteBefore;
  const votes = payload.votes;
  const trackingCid = payload.trackingCid;
  
  const actionType = action?.tag || action?.constructor || 
    Object.keys(action || {}).find(k => k.startsWith('ARC_') || k.startsWith('SRARC_') || k.startsWith('CRARC_'));
  
  let actionDetails = action;
  let innerActionType = actionType;
  
  if (action?.value?.dsoAction) {
    innerActionType = action.value.dsoAction.tag || 
      Object.keys(action.value.dsoAction).find(k => k.startsWith('SRARC_'));
    actionDetails = action.value.dsoAction;
  } else if (action?.dsoAction) {
    innerActionType = action.dsoAction.tag || 
      Object.keys(action.dsoAction).find(k => k.startsWith('SRARC_'));
    actionDetails = action.dsoAction;
  }
  
  const parsedVotes = parseVotes(votes);
  
  return {
    dso: payload.dso,
    requester: payload.requester,
    actionType: innerActionType || actionType,
    actionDetails,
    reasonUrl: reason?.url || reason?.text || '',
    reasonBody: reason?.body || reason?.description || '',
    voteBefore,
    votes: parsedVotes,
    trackingCid: trackingCid?.value || trackingCid || null,
  };
}

/**
 * Parse array field format (older protobuf-style)
 */
function parseArrayFields(fields) {
  const getField = (idx) => fields[idx];
  
  const dso = getField(0);
  const requester = getField(1);
  const action = getField(2);
  const reason = getField(3);
  const voteBefore = getField(4);
  const votes = getField(5);
  const trackingCid = getField(6);
  
  const actionType = action?.tag || (typeof action === 'object' ? Object.keys(action)[0] : null);
  
  return {
    dso,
    requester,
    actionType,
    actionDetails: action,
    reasonUrl: reason?.url || '',
    reasonBody: reason?.body || '',
    voteBefore,
    votes: parseVotes(votes),
    trackingCid,
  };
}

/**
 * Parse votes from various formats
 */
function parseVotes(votes) {
  if (!votes) return [];
  
  const result = [];
  
  if (Array.isArray(votes)) {
    for (const vote of votes) {
      if (Array.isArray(vote) && vote.length >= 2) {
        const [svName, voteData] = vote;
        const sv = voteData?.sv || svName;
        const accept = voteData?.accept === true;
        const reasonUrl = voteData?.reason?.url || '';
        const reasonBody = voteData?.reason?.body || '';
        const castAt = voteData?.optCastAt || null;
        
        result.push({
          svName,
          sv,
          accept: Boolean(accept),
          reasonUrl: reasonUrl || '',
          reasonBody: reasonBody || '',
          castAt,
        });
      }
    }
  } else if (typeof votes === 'object') {
    for (const [svName, voteData] of Object.entries(votes)) {
      if (voteData && typeof voteData === 'object') {
        const sv = voteData.sv || svName;
        const accept = voteData.accept === true;
        const reasonUrl = voteData.reason?.url || '';
        const reasonBody = voteData.reason?.body || '';
        const castAt = voteData.optCastAt || null;
        
        result.push({
          svName,
          sv,
          accept: Boolean(accept),
          reasonUrl: reasonUrl || '',
          reasonBody: reasonBody || '',
          castAt,
        });
      }
    }
  }
  
  return result;
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
  const voteBeforeDate = proposal.vote_before_timestamp ? 
    new Date(proposal.vote_before_timestamp) : null;
  
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

/**
 * Scan binary files for VoteRequest created events
 */
async function scanFilesForVoteRequests(files) {
  const records = [];
  let filesScanned = 0;
  
  for (const filePath of files) {
    try {
      const fullPath = path.isAbsolute(filePath) ? filePath : path.join(DATA_PATH, filePath);
      
      if (!fs.existsSync(fullPath)) continue;
      
      const events = await binaryReader.readBinaryFile(fullPath);
      filesScanned++;
      
      if (indexingProgress) {
        indexingProgress.current = filesScanned;
        indexingProgress.records = records.length;
      }
      
      for (const event of events) {
        if (event.event_type !== 'created') continue;
        
        const templateId = event.template_id || '';
        if (!templateId.includes('VoteRequest')) continue;
        
        const payload = event.create_arguments || event.payload;
        if (!payload) continue;
        
        const parsed = parseVoteRequestPayload(payload);
        if (!parsed) continue;
        
        records.push({
          event_id: event.event_id,
          contract_id: event.contract_id,
          template_id: templateId,
          timestamp: event.created_at || event.timestamp,
          ...parsed,
        });
      }
    } catch (err) {
      console.error(`Error scanning file ${filePath}:`, err.message);
    }
  }
  
  return { records, filesScanned };
}

/**
 * Full scan of all binary files (fallback when template index unavailable)
 */
async function scanAllFilesForVoteRequests() {
  const dataDir = DATA_PATH;
  const allFiles = [];
  
  const findFiles = async (dir) => {
    try {
      const entries = await fs.promises.readdir(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory() && !entry.name.startsWith('.')) {
          await findFiles(fullPath);
        } else if (entry.name.endsWith('.pb.zst')) {
          allFiles.push(fullPath);
        }
      }
    } catch (err) {
      // Ignore permission errors
    }
  };
  
  await findFiles(dataDir);
  console.log(`   Found ${allFiles.length} binary files to scan`);
  
  return scanFilesForVoteRequests(allFiles);
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
 * Build the governance proposal index by scanning VoteRequest events
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
  console.log('\n🗳️ Building governance proposal index (persistent)...');

  try {
    await ensureGovernanceTables();

    const startTime = Date.now();

    // Build proposals FROM the persistent VoteRequest index (fast + consistent)
    // rather than rescanning binary files.
    const countRow = await queryOne(`SELECT COUNT(*) as count FROM vote_requests`);
    const totalVoteRequests = Number(countRow?.count || 0);

    indexingProgress = {
      ...indexingProgress,
      phase: 'loading_vote_requests',
      total: Math.min(totalVoteRequests, Number(limit) || totalVoteRequests),
    };

    if (totalVoteRequests === 0) {
      console.log('   ⚠️ vote_requests table is empty; build the VoteRequest index first');
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
        tracking_cid
      FROM vote_requests
      ORDER BY effective_at DESC
      LIMIT ${Number(limit) || 10000}
    `);

    console.log(`   Loaded ${voteRequestRows.length} VoteRequest records from DuckDB`);

    indexingProgress = { ...indexingProgress, phase: 'grouping' };

    const proposalMap = new Map();
    const now = new Date();

    const parseReason = (reasonStr) => {
      if (!reasonStr) return { reasonUrl: '', reasonBody: '' };
      if (typeof reasonStr !== 'string') return { reasonUrl: String(reasonStr), reasonBody: '' };

      // Often stored as JSON string: { url, body }
      try {
        const parsed = JSON.parse(reasonStr);
        if (parsed && typeof parsed === 'object') {
          return {
            reasonUrl: parsed.url || parsed.text || '',
            reasonBody: parsed.body || parsed.description || '',
          };
        }
      } catch {
        // ignore
      }

      // Fallback: treat as URL/text
      return { reasonUrl: reasonStr, reasonBody: '' };
    };

    const parseVotes = (votesStr) => {
      if (!votesStr) return [];
      if (typeof votesStr !== 'string') return Array.isArray(votesStr) ? votesStr : [];
      try {
        const parsed = JSON.parse(votesStr);
        return Array.isArray(parsed) ? parsed : [];
      } catch {
        return [];
      }
    };

    for (let i = 0; i < voteRequestRows.length; i++) {
      const row = voteRequestRows[i];

      if (indexingProgress) {
        indexingProgress.current = i + 1;
        indexingProgress.records = i + 1;
        indexingProgress.proposals = proposalMap.size;
      }

      const actionType = row.action_tag || 'unknown';
      const { reasonUrl, reasonBody } = parseReason(row.reason);
      const key = getProposalKey(actionType, reasonUrl);

      const ts = row.effective_at ? new Date(row.effective_at).getTime() : 0;
      const existing = proposalMap.get(key);

      if (!existing || ts > existing.latest_timestamp) {
        const votes = parseVotes(row.votes);
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
