/**
 * Vote Outcome Analyzer
 * 
 * Analyzes proposal outcomes by:
 * 1. Getting SV count at the time of voting
 * 2. Calculating if quorum was reached
 * 3. Determining pass/fail based on vote counts and thresholds
 */

import { query, queryOne } from '../duckdb/connection.js';
import { getSvCountAt, getActiveSvsAt, calculateVotingThreshold } from './sv-indexer.js';

/**
 * Analyze a single proposal's outcome
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
    
    // Determine outcome
    const meetsAcceptThreshold = acceptCount >= thresholds.fixedThreshold;
    const meetsRejectThreshold = rejectCount >= thresholds.fixedThreshold;
    const meetsTwoThirdsMajority = acceptCount >= thresholds.twoThirdsMajority;
    
    let calculatedOutcome = 'unknown';
    let outcomeReason = '';
    
    if (meetsAcceptThreshold) {
      calculatedOutcome = 'passed';
      outcomeReason = `${acceptCount} accepts >= ${thresholds.fixedThreshold} threshold`;
    } else if (meetsRejectThreshold) {
      calculatedOutcome = 'rejected';
      outcomeReason = `${rejectCount} rejects >= ${thresholds.fixedThreshold} threshold`;
    } else if (proposal.is_closed && proposal.status === 'expired') {
      calculatedOutcome = 'expired';
      outcomeReason = `Vote closed without meeting thresholds (${acceptCount} accepts, ${rejectCount} rejects)`;
    } else if (!proposal.is_closed) {
      calculatedOutcome = 'pending';
      outcomeReason = `Voting still open (${acceptCount}/${thresholds.fixedThreshold} accepts needed)`;
    } else {
      calculatedOutcome = 'indeterminate';
      outcomeReason = `Closed but outcome unclear (${acceptCount} accepts, ${rejectCount} rejects, threshold: ${thresholds.fixedThreshold})`;
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
      
      // Thresholds
      thresholds: {
        fixed: thresholds.fixedThreshold,
        twoThirds: thresholds.twoThirdsMajority,
        simpleMajority: thresholds.simpleMajority,
      },
      
      // Outcome analysis
      recordedStatus: proposal.status,
      calculatedOutcome,
      outcomeReason,
      
      // Checks
      meetsAcceptThreshold,
      meetsRejectThreshold,
      meetsTwoThirdsMajority,
      
      // Timing
      effectiveAt: proposal.effective_at,
      voteBefore: proposal.vote_before,
      isClosed: proposal.is_closed,
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
    
    for (const proposal of proposals) {
      const voteTime = proposal.vote_before || proposal.effective_at;
      const svCount = await getSvCountAt(voteTime);
      const thresholds = calculateVotingThreshold(svCount);
      
      const acceptCount = Number(proposal.accept_count || 0);
      const rejectCount = Number(proposal.reject_count || 0);
      
      const meetsAcceptThreshold = acceptCount >= thresholds.fixedThreshold;
      const meetsRejectThreshold = rejectCount >= thresholds.fixedThreshold;
      
      let calculatedOutcome = 'pending';
      if (meetsAcceptThreshold) calculatedOutcome = 'executed';
      else if (meetsRejectThreshold) calculatedOutcome = 'rejected';
      else if (proposal.is_closed) calculatedOutcome = 'expired';
      
      const recordedStatus = proposal.status;
      const mismatch = calculatedOutcome !== recordedStatus && 
                       !(calculatedOutcome === 'pending' && recordedStatus === 'in_progress');
      
      results.push({
        proposalId: proposal.proposal_id,
        actionTag: proposal.action_tag,
        acceptCount,
        rejectCount,
        svCountAtVote: svCount,
        threshold: thresholds.fixedThreshold,
        recordedStatus,
        calculatedOutcome,
        mismatch,
        effectiveAt: proposal.effective_at,
      });
    }
    
    const mismatches = results.filter(r => r.mismatch);
    
    return {
      total: results.length,
      mismatchCount: mismatches.length,
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
