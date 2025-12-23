/**
 * Governance Proposal Indexer
 * 
 * Extracts unique governance proposals from VoteRequest created events,
 * groups by proposal identifier (action type + reason URL), and tracks
 * vote progress and final outcomes.
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

// In-memory proposal cache (refreshed on demand)
let proposalCache = null;
let cacheTimestamp = null;
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

/**
 * Parse VoteRequest payload to extract structured proposal data
 */
function parseVoteRequestPayload(payload) {
  if (!payload) return null;

  try {
    // Handle both formats: { record: { fields: [...] } } and named fields
    const fields = payload.record?.fields || payload.fields || payload;
    
    // Named field format (newer)
    if (payload.dso && payload.requester) {
      return parseNamedFields(payload);
    }
    
    // Array field format (older)
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
  
  // Extract action type and details
  const actionType = action?.tag || action?.constructor || 
    Object.keys(action || {}).find(k => k.startsWith('ARC_') || k.startsWith('SRARC_') || k.startsWith('CRARC_'));
  
  let actionDetails = action;
  let innerActionType = actionType;
  
  // Unwrap nested action structure
  if (action?.value?.dsoAction) {
    innerActionType = action.value.dsoAction.tag || 
      Object.keys(action.value.dsoAction).find(k => k.startsWith('SRARC_'));
    actionDetails = action.value.dsoAction;
  } else if (action?.dsoAction) {
    innerActionType = action.dsoAction.tag || 
      Object.keys(action.dsoAction).find(k => k.startsWith('SRARC_'));
    actionDetails = action.dsoAction;
  }
  
  // Parse votes
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
  const extractValue = (field) => {
    if (!field?.value) return null;
    const v = field.value;
    return v.party || v.text || v.timestamp || v.int64 || v.numeric || v.bool || v.contractId || v;
  };
  
  const dso = extractValue(fields[0]);
  const requester = extractValue(fields[1]);
  const action = fields[2]?.value?.variant || fields[2]?.value;
  const reason = fields[3]?.value?.record?.fields || fields[3]?.value;
  const voteBefore = fields[4]?.value?.timestamp;
  const votes = fields[5]?.value?.genMap?.entries || fields[5]?.value?.list?.elements || [];
  const trackingCid = fields[6]?.value?.optional?.value?.contractId || fields[6]?.value?.contractId;
  
  // Extract action type from variant
  let actionType = action?.constructor;
  let actionDetails = action;
  
  // Unwrap nested action structure for DsoRules actions
  if (action?.constructor === 'ARC_DsoRules' || action?.tag === 'ARC_DsoRules') {
    const innerAction = action?.value?.record?.fields?.[0]?.value?.variant || 
                        action?.value?.dsoAction;
    if (innerAction) {
      actionType = innerAction.constructor || innerAction.tag;
      actionDetails = innerAction;
    }
  } else if (action?.constructor === 'ARC_AmuletRules' || action?.tag === 'ARC_AmuletRules') {
    const innerAction = action?.value?.record?.fields?.[0]?.value?.variant ||
                        action?.value?.amuletRulesAction;
    if (innerAction) {
      actionType = innerAction.constructor || innerAction.tag;
      actionDetails = innerAction;
    }
  }
  
  // Parse reason
  let reasonUrl = '';
  let reasonBody = '';
  if (Array.isArray(reason)) {
    reasonUrl = extractValue(reason[0]) || '';
    reasonBody = extractValue(reason[1]) || '';
  } else if (reason) {
    reasonUrl = reason.url || reason.text || '';
    reasonBody = reason.body || reason.description || '';
  }
  
  // Parse votes
  const parsedVotes = parseVotes(votes);
  
  return {
    dso,
    requester,
    actionType,
    actionDetails,
    reasonUrl,
    reasonBody,
    voteBefore,
    votes: parsedVotes,
    trackingCid,
  };
}

/**
 * Parse votes from various formats
 */
function parseVotes(votes) {
  if (!votes) return [];
  
  const result = [];
  
  // genMap format: { entries: [{ key, value }] }
  if (Array.isArray(votes)) {
    for (const entry of votes) {
      if (entry.key && entry.value) {
        // genMap entry
        const svName = entry.key.text || entry.key;
        const voteData = entry.value.record?.fields || entry.value;
        
        let sv, accept, reasonUrl, reasonBody, castAt;
        
        if (Array.isArray(voteData)) {
          sv = voteData[0]?.value?.party;
          accept = voteData[1]?.value?.bool;
          const voteReason = voteData[2]?.value?.record?.fields;
          if (voteReason) {
            reasonUrl = voteReason[0]?.value?.text || '';
            reasonBody = voteReason[1]?.value?.text || '';
          }
          castAt = voteData[3]?.value?.timestamp;
        } else {
          sv = voteData.sv;
          accept = voteData.accept;
          reasonUrl = voteData.reason?.url || '';
          reasonBody = voteData.reason?.body || '';
          castAt = voteData.castAt;
        }
        
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
  const voteBeforeDate = proposal.voteBeforeTimestamp ? 
    new Date(proposal.voteBeforeTimestamp) : null;
  
  // If we have tracking CID and majority accepts, it's approved
  if (proposal.trackingCid && proposal.votesFor > proposal.votesAgainst) {
    return 'approved';
  }
  
  // Check if vote deadline has passed
  if (voteBeforeDate && voteBeforeDate < now) {
    // Deadline passed - check vote outcome
    if (proposal.votesFor > proposal.votesAgainst) {
      return 'approved';
    } else if (proposal.votesAgainst > 0) {
      return 'rejected';
    } else {
      return 'expired';
    }
  }
  
  // Still within voting period
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
 * Clear the proposal cache
 */
export function invalidateCache() {
  proposalCache = null;
  cacheTimestamp = null;
  console.log('🗳️ Governance proposal cache invalidated');
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
        // Only created events
        if (event.event_type !== 'created') continue;
        
        // Check for VoteRequest template
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
  
  // Recursively find all .pb.zst files
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
 * Build the governance proposal index by scanning VoteRequest events
 */
export async function buildGovernanceIndex({ limit = 10000, forceRefresh = false } = {}) {
  // Check cache first
  if (!forceRefresh && proposalCache && cacheTimestamp) {
    const age = Date.now() - cacheTimestamp;
    if (age < CACHE_TTL_MS) {
      console.log(`🗳️ Using cached governance proposals (age: ${(age / 1000).toFixed(1)}s)`);
      return proposalCache;
    }
  }
  
  if (indexingInProgress) {
    console.log('⏳ Governance indexing already in progress');
    return { status: 'in_progress', progress: indexingProgress };
  }
  
  indexingInProgress = true;
  indexingProgress = { phase: 'starting', current: 0, total: 0, records: 0, startedAt: new Date().toISOString() };
  console.log('\n🗳️ Building governance proposal index...');
  
  try {
    const startTime = Date.now();
    let scanResult;
    
    // Check if template index is available
    const templateIndexPopulated = await isTemplateIndexPopulated();
    
    if (templateIndexPopulated) {
      const templateIndexStats = await getTemplateIndexStats();
      console.log(`   📋 Using template index (${templateIndexStats.totalFiles} files indexed)`);
      
      const voteRequestFiles = await getFilesForTemplate('VoteRequest');
      console.log(`   📂 Found ${voteRequestFiles.length} files containing VoteRequest events`);
      
      if (voteRequestFiles.length === 0) {
        console.log('   ⚠️ No VoteRequest files in index, using full scan');
        indexingProgress = { ...indexingProgress, phase: 'full_scan' };
        scanResult = await scanAllFilesForVoteRequests();
      } else {
        indexingProgress = { ...indexingProgress, phase: 'scanning', total: voteRequestFiles.length };
        scanResult = await scanFilesForVoteRequests(voteRequestFiles);
      }
    } else {
      console.log('   ⚠️ Template index not available, using full scan');
      indexingProgress = { ...indexingProgress, phase: 'full_scan' };
      scanResult = await scanAllFilesForVoteRequests();
    }
    
    console.log(`   Found ${scanResult.records.length} VoteRequest created events`);
    
    // Group by unique proposal (action type + reason URL)
    indexingProgress = { ...indexingProgress, phase: 'grouping' };
    const proposalMap = new Map();
    
    for (const record of scanResult.records) {
      const key = getProposalKey(record.actionType, record.reasonUrl);
      
      const timestamp = record.timestamp ? new Date(record.timestamp).getTime() : 0;
      const existing = proposalMap.get(key);
      
      // Keep the latest version of each proposal
      if (!existing || timestamp > existing.latestTimestamp) {
        // Parse voteBefore timestamp
        let voteBeforeTimestamp = null;
        if (record.voteBefore) {
          // Handle microsecond timestamp strings
          const vb = String(record.voteBefore);
          if (/^\d+$/.test(vb)) {
            voteBeforeTimestamp = parseInt(vb.slice(0, 13), 10); // Convert to milliseconds
          } else {
            voteBeforeTimestamp = new Date(vb).getTime();
          }
        }
        
        // Count votes
        let votesFor = 0;
        let votesAgainst = 0;
        for (const vote of record.votes || []) {
          if (vote.accept) votesFor++;
          else votesAgainst++;
        }
        
        proposalMap.set(key, {
          proposalKey: key,
          latestTimestamp: timestamp,
          latestContractId: record.contract_id,
          latestEventId: record.event_id,
          requester: record.requester,
          actionType: record.actionType,
          actionDetails: record.actionDetails,
          reasonUrl: record.reasonUrl,
          reasonBody: record.reasonBody,
          voteBefore: record.voteBefore,
          voteBeforeTimestamp,
          votes: record.votes || [],
          votesFor,
          votesAgainst,
          trackingCid: record.trackingCid,
          rawTimestamp: record.timestamp,
        });
      }
    }
    
    // Convert to array and determine status
    const now = new Date();
    const proposals = Array.from(proposalMap.values()).map(p => ({
      ...p,
      status: determineStatus(p, now),
    }));
    
    // Sort by latest timestamp descending
    proposals.sort((a, b) => b.latestTimestamp - a.latestTimestamp);
    
    // Limit results
    const limitedProposals = proposals.slice(0, limit);
    
    // Calculate stats
    const stats = {
      total: proposals.length,
      byActionType: {},
      byStatus: {
        approved: 0,
        rejected: 0,
        pending: 0,
        expired: 0,
      },
    };
    
    for (const p of proposals) {
      // By action type
      const actionType = p.actionType || 'unknown';
      stats.byActionType[actionType] = (stats.byActionType[actionType] || 0) + 1;
      
      // By status
      stats.byStatus[p.status] = (stats.byStatus[p.status] || 0) + 1;
    }
    
    const duration = Date.now() - startTime;
    console.log(`   ✅ Indexed ${proposals.length} unique proposals in ${duration}ms`);
    console.log(`   📊 Status: ${stats.byStatus.approved} approved, ${stats.byStatus.rejected} rejected, ${stats.byStatus.pending} pending, ${stats.byStatus.expired} expired`);
    
    const result = {
      summary: {
        filesScanned: scanResult.filesScanned,
        totalVoteRequests: scanResult.records.length,
        uniqueProposals: proposals.length,
        duration,
      },
      stats,
      proposals: limitedProposals,
    };
    
    // Update cache
    proposalCache = result;
    cacheTimestamp = Date.now();
    
    return result;
    
  } finally {
    indexingInProgress = false;
    indexingProgress = null;
  }
}

/**
 * Get proposal stats without full rebuild
 */
export async function getProposalStats() {
  if (proposalCache) {
    return proposalCache.stats;
  }
  
  // Build index if not cached
  const result = await buildGovernanceIndex();
  return result.stats;
}

/**
 * Query proposals with filters
 */
export async function queryProposals({ 
  limit = 100, 
  offset = 0,
  status = null,
  actionType = null,
  requester = null,
  search = null,
} = {}) {
  // Ensure cache is populated
  if (!proposalCache) {
    await buildGovernanceIndex();
  }
  
  let proposals = [...(proposalCache?.proposals || [])];
  
  // Apply filters
  if (status) {
    proposals = proposals.filter(p => p.status === status);
  }
  
  if (actionType) {
    proposals = proposals.filter(p => p.actionType === actionType);
  }
  
  if (requester) {
    proposals = proposals.filter(p => 
      p.requester?.toLowerCase().includes(requester.toLowerCase())
    );
  }
  
  if (search) {
    const searchLower = search.toLowerCase();
    proposals = proposals.filter(p => 
      p.reasonBody?.toLowerCase().includes(searchLower) ||
      p.reasonUrl?.toLowerCase().includes(searchLower) ||
      p.requester?.toLowerCase().includes(searchLower) ||
      p.actionType?.toLowerCase().includes(searchLower)
    );
  }
  
  // Apply pagination
  const total = proposals.length;
  const paginatedProposals = proposals.slice(offset, offset + limit);
  
  return {
    proposals: paginatedProposals,
    pagination: {
      total,
      limit,
      offset,
      hasMore: offset + limit < total,
    },
  };
}

/**
 * Get a single proposal by key
 */
export async function getProposalByKey(proposalKey) {
  if (!proposalCache) {
    await buildGovernanceIndex();
  }
  
  return proposalCache?.proposals?.find(p => p.proposalKey === proposalKey) || null;
}

/**
 * Get proposals by contract ID
 */
export async function getProposalByContractId(contractId) {
  if (!proposalCache) {
    await buildGovernanceIndex();
  }
  
  return proposalCache?.proposals?.find(p => p.latestContractId === contractId) || null;
}
