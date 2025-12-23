/**
 * Governance Proposal Indexer
 * 
 * Scans binary files for VoteRequest created events, extracts unique proposals
 * by grouping on (action type + reason URL), and tracks vote progress.
 * 
 * PERSISTENT: Stores proposals in DuckDB so index survives restarts.
 * RELIABLE: Scans raw binary files directly for complete payload access.
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
 * Generate a unique proposal key from action type and reason URL
 */
function getProposalKey(actionType, reasonUrl) {
  return `${actionType || 'unknown'}::${reasonUrl || 'no-url'}`;
}

/**
 * Extract the specific action type from a VoteRequest payload
 * Handles nested structures like: { tag: "ARC_DsoRules", value: { dsoAction: { tag: "SRARC_SetConfig" } } }
 */
function extractActionType(payload) {
  if (!payload) return 'unknown';
  
  const action = payload.action;
  if (!action) return 'unknown';
  
  // Get outer tag
  const outerTag = action.tag || Object.keys(action).find(k => 
    k.startsWith('ARC_') || k.startsWith('SRARC_') || k.startsWith('CRARC_')
  );
  
  // Check for nested dsoAction with specific action type
  const dsoAction = action.value?.dsoAction || action.dsoAction;
  if (dsoAction) {
    const innerTag = dsoAction.tag || Object.keys(dsoAction).find(k => 
      k.startsWith('SRARC_') || k.startsWith('CRARC_') || k.startsWith('ARC_')
    );
    if (innerTag) return innerTag;
  }
  
  // Check for value.tag
  if (action.value?.tag) {
    return action.value.tag;
  }
  
  return outerTag || 'unknown';
}

/**
 * Extract reason URL and body from payload
 */
function extractReason(payload) {
  if (!payload) return { reasonUrl: '', reasonBody: '' };
  
  const reason = payload.reason;
  if (!reason) return { reasonUrl: '', reasonBody: '' };
  
  if (typeof reason === 'string') {
    return { reasonUrl: reason, reasonBody: '' };
  }
  
  return {
    reasonUrl: reason.url || reason.text || '',
    reasonBody: reason.body || reason.description || '',
  };
}

/**
 * Parse votes from payload
 */
function parseVotes(votes) {
  if (!votes) return [];
  
  const result = [];
  
  if (Array.isArray(votes)) {
    for (const vote of votes) {
      if (Array.isArray(vote) && vote.length >= 2) {
        const [svName, voteData] = vote;
        result.push({
          svName,
          sv: voteData?.sv || svName,
          accept: voteData?.accept === true,
          reasonUrl: voteData?.reason?.url || '',
          reasonBody: voteData?.reason?.body || '',
          castAt: voteData?.optCastAt || null,
        });
      }
    }
  } else if (typeof votes === 'object') {
    for (const [svName, voteData] of Object.entries(votes)) {
      if (voteData && typeof voteData === 'object') {
        result.push({
          svName,
          sv: voteData.sv || svName,
          accept: voteData.accept === true,
          reasonUrl: voteData.reason?.url || '',
          reasonBody: voteData.reason?.body || '',
          castAt: voteData.optCastAt || null,
        });
      }
    }
  }
  
  return result;
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
        
        const actionType = extractActionType(payload);
        const { reasonUrl, reasonBody } = extractReason(payload);
        const votes = parseVotes(payload.votes);
        
        let votesFor = 0;
        let votesAgainst = 0;
        for (const v of votes) {
          if (v.accept) votesFor++;
          else votesAgainst++;
        }
        
        // Parse voteBefore timestamp
        let voteBeforeTimestamp = null;
        if (payload.voteBefore) {
          const vb = String(payload.voteBefore);
          if (/^\d+$/.test(vb)) {
            voteBeforeTimestamp = parseInt(vb.slice(0, 13), 10);
          } else {
            const parsedVb = new Date(vb).getTime();
            voteBeforeTimestamp = Number.isFinite(parsedVb) ? parsedVb : null;
          }
        }
        
        records.push({
          event_id: event.event_id,
          contract_id: event.contract_id,
          template_id: templateId,
          timestamp: event.effective_at || event.created_at || event.timestamp,
          requester: payload.requester,
          actionType,
          actionDetails: payload.action,
          reasonUrl,
          reasonBody,
          voteBefore: payload.voteBefore,
          voteBeforeTimestamp,
          votes,
          votesFor,
          votesAgainst,
          trackingCid: payload.trackingCid?.value || payload.trackingCid || null,
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

/**
 * Build the governance proposal index by scanning binary files
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
  console.log('\n🗳️ Building governance proposal index (scanning binary files)...');

  try {
    await ensureGovernanceTables();

    const startTime = Date.now();
    let scanResult;

    // Check if template index is available for faster scanning
    const templateIndexPopulated = await isTemplateIndexPopulated();

    if (templateIndexPopulated) {
      const templateIndexStats = await getTemplateIndexStats();
      console.log(`   📋 Using template index (${templateIndexStats.totalFiles} files indexed)`);

      const voteRequestFiles = await getFilesForTemplate('VoteRequest');
      console.log(`   📂 Found ${voteRequestFiles.length} files containing VoteRequest events`);

      if (voteRequestFiles.length === 0) {
        console.log('   ⚠️ No VoteRequest files in index, using full scan');
        indexingProgress = { ...indexingProgress, phase: 'full_scan', total: 0 };
        scanResult = await scanAllFilesForVoteRequests();
      } else {
        indexingProgress = { ...indexingProgress, phase: 'scanning', total: voteRequestFiles.length };
        scanResult = await scanFilesForVoteRequests(voteRequestFiles);
      }
    } else {
      console.log('   ⚠️ Template index not available, using full scan');
      indexingProgress = { ...indexingProgress, phase: 'full_scan', total: 0 };
      scanResult = await scanAllFilesForVoteRequests();
    }

    console.log(`   Found ${scanResult.records.length} VoteRequest created events from ${scanResult.filesScanned} files`);

    // Log sample for debugging
    if (scanResult.records.length > 0) {
      const sample = scanResult.records[0];
      console.log(`   Sample: actionType=${sample.actionType}, reasonUrl=${sample.reasonUrl?.substring(0, 50)}...`);
    }

    // Group by unique proposal (action type + reason URL)
    indexingProgress = { ...indexingProgress, phase: 'grouping' };
    const proposalMap = new Map();

    for (const record of scanResult.records) {
      const key = getProposalKey(record.actionType, record.reasonUrl);

      const timestamp = record.timestamp ? new Date(record.timestamp).getTime() : 0;
      const existing = proposalMap.get(key);

      if (!existing || timestamp > existing.latest_timestamp) {
        proposalMap.set(key, {
          proposal_key: key,
          latest_timestamp: timestamp,
          latest_contract_id: record.contract_id,
          latest_event_id: record.event_id,
          requester: record.requester,
          action_type: record.actionType,
          action_details: record.actionDetails,
          reason_url: record.reasonUrl,
          reason_body: record.reasonBody,
          vote_before: record.voteBefore,
          vote_before_timestamp: record.voteBeforeTimestamp,
          votes: record.votes,
          votes_for: record.votesFor,
          votes_against: record.votesAgainst,
          tracking_cid: record.trackingCid,
        });
      }

      if (indexingProgress) {
        indexingProgress.proposals = proposalMap.size;
      }
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
      VALUES (1, CURRENT_TIMESTAMP, ${stats.total}, ${scanResult.filesScanned}, ${stats.approved}, ${stats.rejected}, ${stats.pending}, ${stats.expired})
      ON CONFLICT (id) DO UPDATE SET
        last_indexed_at = CURRENT_TIMESTAMP,
        total_indexed = ${stats.total},
        files_scanned = ${scanResult.filesScanned},
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
        filesScanned: scanResult.filesScanned,
        totalVoteRequests: scanResult.records.length,
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
