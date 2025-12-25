/**
 * Vote Outcome Analyzer
 * 
 * Analyzes proposal outcomes using 2/3 majority threshold:
 * 
 * - IN PROGRESS: Has NOT met 2/3 accept OR 2/3 reject threshold AND has NOT expired
 * - EXECUTED: Met 2/3 accept threshold before expiration  
 * - REJECTED: Met 2/3 reject threshold before expiration
 * - EXPIRED: Did NOT meet either 2/3 threshold AND vote_before time has passed
 */

import { query, queryOne } from '../duckdb/connection.js';
import { getSvCountAt, getActiveSvsAt, calculateVotingThreshold } from './vote-request-indexer.js';

/**
 * Analyze a single proposal's outcome using 2/3 threshold
 */
export async function analyzeProposalOutcome(proposalId) {
  try {
    // Get the proposal from the index
    const proposal = await queryOne(`
      SELECT 
        COALESCE(proposal_id, contract_id) as proposal_id,
        contract_id,
        action_tag,
        requester,
        reason,
        votes,
        vote_count,
        accept_count,
        reject_count,
        status,
        is_closed,
        effective_at,
        vote_before
      FROM vote_requests
      WHERE COALESCE(proposal_id, contract_id) = '${proposalId.replace(/'/g, "''")}'
      ORDER BY effective_at DESC
      LIMIT 1
    `);
    
    if (!proposal) {
      return { error: 'Proposal not found' };
    }
    
    // Get SV count at the time of voting
    const voteTime = proposal.vote_before || proposal.effective_at;
    const svCount = await getSvCountAt(voteTime);
    const activeSvs = await getActiveSvsAt(voteTime);
    const thresholds = calculateVotingThreshold(svCount);
    
    const acceptCount = Number(proposal.accept_count || 0);
    const rejectCount = Number(proposal.reject_count || 0);
    const totalVotes = Number(proposal.vote_count || 0);
    
    // 2/3 threshold for both accept and reject
    const threshold = thresholds.twoThirdsThreshold;
    
    // Check if thresholds are met
    const meetsAcceptThreshold = acceptCount >= threshold;
    const meetsRejectThreshold = rejectCount >= threshold;
    
    // Check if expired (vote_before has passed)
    const now = new Date();
    const voteBefore = proposal.vote_before ? new Date(proposal.vote_before) : null;
    const isExpired = voteBefore && voteBefore < now;
    
    let calculatedOutcome = 'in_progress';
    let outcomeReason = '';
    
    if (meetsAcceptThreshold) {
      calculatedOutcome = 'executed';
      outcomeReason = `Met 2/3 accept threshold: ${acceptCount} accepts >= ${threshold} (${svCount} SVs × 2/3)`;
    } else if (meetsRejectThreshold) {
      calculatedOutcome = 'rejected';
      outcomeReason = `Met 2/3 reject threshold: ${rejectCount} rejects >= ${threshold} (${svCount} SVs × 2/3)`;
    } else if (isExpired || proposal.is_closed) {
      calculatedOutcome = 'expired';
      outcomeReason = `Did not meet 2/3 threshold before expiration: ${acceptCount} accepts, ${rejectCount} rejects (needed ${threshold})`;
    } else {
      calculatedOutcome = 'in_progress';
      outcomeReason = `Voting ongoing: ${acceptCount}/${threshold} accepts or ${rejectCount}/${threshold} rejects needed`;
    }
    
    return {
      proposalId: proposal.proposal_id,
      contractId: proposal.contract_id,
      actionTag: proposal.action_tag,
      requester: proposal.requester,
      reason: proposal.reason,
      
      // Vote counts
      acceptCount,
      rejectCount,
      totalVotes,
      
      // SV context at vote time
      svCountAtVote: svCount,
      activeSvsAtVote: activeSvs.length,
      
      // Thresholds (2/3 majority)
      threshold,
      thresholdFormula: `ceil(${svCount} × 2/3) = ${threshold}`,
      
      // Outcome analysis
      recordedStatus: proposal.status,
      calculatedOutcome,
      outcomeReason,
      
      // Threshold checks
      meetsAcceptThreshold,
      meetsRejectThreshold,
      acceptProgress: `${acceptCount}/${threshold}`,
      rejectProgress: `${rejectCount}/${threshold}`,
      
      // Timing
      effectiveAt: proposal.effective_at,
      voteBefore: proposal.vote_before,
      isExpired,
      isClosed: proposal.is_closed,
      
      // Mismatch detection
      statusMismatch: calculatedOutcome !== proposal.status && 
                      !(calculatedOutcome === 'in_progress' && proposal.status === 'active'),
    };
  } catch (err) {
    console.error('Error analyzing proposal outcome:', err);
    return { error: err.message };
  }
}

/**
 * Analyze all proposals and flag any with mismatched outcomes
 */
export async function analyzeAllProposalOutcomes({ limit = 100, offset = 0 } = {}) {
  try {
    // Get proposals grouped by proposal_id
    const proposals = await query(`
      SELECT 
        COALESCE(proposal_id, contract_id) as proposal_id,
        MAX(contract_id) as contract_id,
        MAX(action_tag) as action_tag,
        MAX(requester) as requester,
        MAX(reason) as reason,
        MAX(vote_count) as vote_count,
        MAX(accept_count) as accept_count,
        MAX(reject_count) as reject_count,
        MAX(status) as status,
        MAX(CASE WHEN is_closed THEN 1 ELSE 0 END) as is_closed,
        MAX(effective_at) as effective_at,
        MAX(vote_before) as vote_before
      FROM vote_requests
      GROUP BY COALESCE(proposal_id, contract_id)
      ORDER BY effective_at DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `);
    
    const results = [];
    const now = new Date();
    
    for (const proposal of proposals) {
      const voteTime = proposal.vote_before || proposal.effective_at;
      const svCount = await getSvCountAt(voteTime);
      const thresholds = calculateVotingThreshold(svCount);
      const threshold = thresholds.twoThirdsThreshold;
      
      const acceptCount = Number(proposal.accept_count || 0);
      const rejectCount = Number(proposal.reject_count || 0);
      
      const meetsAcceptThreshold = acceptCount >= threshold;
      const meetsRejectThreshold = rejectCount >= threshold;
      
      const voteBefore = proposal.vote_before ? new Date(proposal.vote_before) : null;
      const isExpired = voteBefore && voteBefore < now;
      const isClosed = proposal.is_closed === 1 || proposal.is_closed === true;
      
      let calculatedOutcome = 'in_progress';
      if (meetsAcceptThreshold) calculatedOutcome = 'executed';
      else if (meetsRejectThreshold) calculatedOutcome = 'rejected';
      else if (isExpired || isClosed) calculatedOutcome = 'expired';
      
      const recordedStatus = proposal.status;
      const mismatch = calculatedOutcome !== recordedStatus && 
                       !(calculatedOutcome === 'in_progress' && recordedStatus === 'active');
      
      results.push({
        proposalId: proposal.proposal_id,
        actionTag: proposal.action_tag,
        acceptCount,
        rejectCount,
        svCountAtVote: svCount,
        threshold,
        thresholdFormula: `ceil(${svCount} × 2/3)`,
        acceptProgress: `${acceptCount}/${threshold}`,
        rejectProgress: `${rejectCount}/${threshold}`,
        recordedStatus,
        calculatedOutcome,
        mismatch,
        effectiveAt: proposal.effective_at,
        voteBefore: proposal.vote_before,
        isExpired,
      });
    }
    
    const mismatches = results.filter(r => r.mismatch);
    
    // Summary by outcome
    const summary = {
      in_progress: results.filter(r => r.calculatedOutcome === 'in_progress').length,
      executed: results.filter(r => r.calculatedOutcome === 'executed').length,
      rejected: results.filter(r => r.calculatedOutcome === 'rejected').length,
      expired: results.filter(r => r.calculatedOutcome === 'expired').length,
    };
    
    return {
      total: results.length,
      mismatchCount: mismatches.length,
      summary,
      results,
      mismatches,
    };
  } catch (err) {
    console.error('Error analyzing all proposal outcomes:', err);
    return { error: err.message };
  }
}

/**
 * Get raw proposals grouped by tracking_cid/contract_id WITHOUT is_human filtering
 * This is for manual review to determine what should be human-readable
 */
export async function getRawProposalsGrouped({ limit = 200, offset = 0 } = {}) {
  try {
    const proposals = await query(`
      WITH grouped AS (
        SELECT 
          COALESCE(tracking_cid, contract_id) as group_id,
          COUNT(*) as event_count,
          MAX(action_tag) as action_tag,
          MAX(requester) as requester,
          MAX(reason) as reason,
          MAX(reason_url) as reason_url,
          MAX(vote_count) as vote_count,
          MAX(accept_count) as accept_count,
          MAX(reject_count) as reject_count,
          MAX(status) as status,
          MAX(CASE WHEN is_closed THEN 1 ELSE 0 END) as is_closed,
          MAX(is_human) as current_is_human,
          MIN(effective_at) as first_seen,
          MAX(effective_at) as last_seen,
          MAX(vote_before) as vote_before,
          MAX(votes) as votes
        FROM vote_requests
        GROUP BY COALESCE(tracking_cid, contract_id)
      )
      SELECT * FROM grouped
      ORDER BY last_seen DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `);
    
    const total = await queryOne(`
      SELECT COUNT(DISTINCT COALESCE(tracking_cid, contract_id)) as count FROM vote_requests
    `);
    
    return {
      total: Number(total?.count || 0),
      offset,
      limit,
      proposals: proposals.map(p => ({
        groupId: p.group_id,
        eventCount: Number(p.event_count),
        actionTag: p.action_tag,
        requester: p.requester,
        reason: p.reason,
        reasonUrl: p.reason_url,
        voteCount: Number(p.vote_count || 0),
        acceptCount: Number(p.accept_count || 0),
        rejectCount: Number(p.reject_count || 0),
        status: p.status,
        isClosed: p.is_closed === 1 || p.is_closed === true,
        currentIsHuman: p.current_is_human === 1 || p.current_is_human === true,
        firstSeen: p.first_seen,
        lastSeen: p.last_seen,
        voteBefore: p.vote_before,
        votes: p.votes,
        // Suggestion for human-readability based on heuristics
        suggestedHuman: !!(p.reason || p.reason_url || Number(p.vote_count || 0) > 0),
        hasReason: !!p.reason,
        hasReasonUrl: !!p.reason_url,
        hasVotes: Number(p.vote_count || 0) > 0,
        isSetConfig: p.action_tag?.includes('SetConfig') || false,
      })),
    };
  } catch (err) {
    console.error('Error getting raw proposals:', err);
    return { error: err.message, proposals: [] };
  }
}

/**
 * Get summary stats for raw proposals
 */
export async function getRawProposalStats() {
  try {
    const total = await queryOne(`
      SELECT COUNT(DISTINCT COALESCE(tracking_cid, contract_id)) as count FROM vote_requests
    `);
    
    const withReason = await queryOne(`
      SELECT COUNT(DISTINCT COALESCE(tracking_cid, contract_id)) as count 
      FROM vote_requests 
      WHERE reason IS NOT NULL AND reason != ''
    `);
    
    const withVotes = await queryOne(`
      SELECT COUNT(DISTINCT COALESCE(tracking_cid, contract_id)) as count 
      FROM vote_requests 
      WHERE vote_count > 0
    `);
    
    const setConfig = await queryOne(`
      SELECT COUNT(DISTINCT COALESCE(tracking_cid, contract_id)) as count 
      FROM vote_requests 
      WHERE action_tag LIKE '%SetConfig%'
    `);
    
    const markedHuman = await queryOne(`
      SELECT COUNT(DISTINCT COALESCE(tracking_cid, contract_id)) as count 
      FROM vote_requests 
      WHERE is_human = true
    `);
    
    return {
      totalGroups: Number(total?.count || 0),
      withReason: Number(withReason?.count || 0),
      withVotes: Number(withVotes?.count || 0),
      setConfigCount: Number(setConfig?.count || 0),
      currentlyMarkedHuman: Number(markedHuman?.count || 0),
    };
  } catch (err) {
    console.error('Error getting raw proposal stats:', err);
    return { error: err.message };
  }
}
