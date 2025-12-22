import { useMemo } from "react";
import { GovernanceAction, useGovernanceHistory } from "./use-governance-history";

export interface UniqueProposal {
  proposalId: string; // hash + actionType
  proposalHash: string;
  actionType: string;
  title: string;
  status: 'approved' | 'rejected' | 'pending' | 'expired';
  latestEventTime: string;
  createdAt: string | null;
  voteBefore: string | null;
  requester: string | null;
  reason: string | null;
  reasonUrl: string | null;
  votesFor: number;
  votesAgainst: number;
  totalVotes: number;
  contractId: string;
  cipReference: string | null;
  eventCount: number;
  lastEventType: 'created' | 'archived';
  rawData: GovernanceAction; // Include raw data for display
}

// Format action tag to human-readable title
const formatActionTitle = (actionTag: string | null): string => {
  if (!actionTag) return 'Unknown Action';
  return actionTag
    .replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, '')
    .replace(/([A-Z])/g, ' $1')
    .trim();
};

// Extract proposal hash from contract_id (first 12 chars)
const extractProposalHash = (contractId: string): string => {
  return contractId?.slice(0, 12) || 'unknown';
};

export function useUniqueProposals(votingThreshold = 10) {
  const { data: governanceActions, isLoading, error } = useGovernanceHistory(2000);

  const uniqueProposals = useMemo(() => {
    if (!governanceActions?.length) return [];

    // Group events by proposal hash + action type
    const proposalMap = new Map<string, {
      events: typeof governanceActions;
      latestEvent: typeof governanceActions[0];
    }>();

    for (const event of governanceActions) {
      const proposalHash = extractProposalHash(event.contractId);
      const actionType = event.actionTag || 'Unknown';
      const proposalId = `${proposalHash}•${actionType}`;

      const existing = proposalMap.get(proposalId);
      if (!existing) {
        proposalMap.set(proposalId, {
          events: [event],
          latestEvent: event,
        });
      } else {
        existing.events.push(event);
        // Keep the latest event by effectiveAt
        const currentTime = new Date(event.effectiveAt).getTime();
        const latestTime = new Date(existing.latestEvent.effectiveAt).getTime();
        if (currentTime > latestTime) {
          existing.latestEvent = event;
        }
      }
    }

    // Convert to unique proposals array
    const proposals: UniqueProposal[] = [];
    
    for (const [proposalId, { events, latestEvent }] of proposalMap) {
      const proposalHash = extractProposalHash(latestEvent.contractId);
      const actionType = latestEvent.actionTag || 'Unknown';
      
      // Status is already determined correctly in useGovernanceHistory
      // Map the status from GovernanceAction to UniqueProposal status
      let status: UniqueProposal['status'];
      switch (latestEvent.status) {
        case 'passed':
          status = 'approved';
          break;
        case 'failed':
          status = 'rejected';
          break;
        case 'expired':
          status = 'expired';
          break;
        case 'executed':
          // Executed means it was approved and enacted
          status = 'approved';
          break;
        default:
          // Fallback - check votes
          if (latestEvent.votesFor >= votingThreshold) {
            status = 'approved';
          } else if (latestEvent.type === 'vote_completed') {
            status = 'rejected';
          } else {
            status = 'expired'; // Historical data without clear status
          }
      }

      // Find earliest created event
      const sortedEvents = [...events].sort(
        (a, b) => new Date(a.effectiveAt).getTime() - new Date(b.effectiveAt).getTime()
      );
      const createdAt = sortedEvents[0]?.effectiveAt || null;

      proposals.push({
        proposalId,
        proposalHash,
        actionType,
        title: formatActionTitle(actionType),
        status,
        latestEventTime: latestEvent.effectiveAt,
        createdAt,
        voteBefore: null,
        requester: latestEvent.requester,
        reason: latestEvent.reason,
        reasonUrl: latestEvent.reasonUrl,
        votesFor: latestEvent.votesFor,
        votesAgainst: latestEvent.votesAgainst,
        totalVotes: latestEvent.totalVotes,
        contractId: latestEvent.contractId,
        cipReference: latestEvent.cipReference,
        eventCount: events.length,
        lastEventType: latestEvent.type === 'vote_completed' ? 'archived' : 'created',
        rawData: latestEvent,
      });
    }

    // Sort by latest event time descending
    return proposals.sort(
      (a, b) => new Date(b.latestEventTime).getTime() - new Date(a.latestEventTime).getTime()
    );
  }, [governanceActions, votingThreshold]);

  const stats = useMemo(() => {
    const total = uniqueProposals.length;
    const approved = uniqueProposals.filter(p => p.status === 'approved').length;
    const rejected = uniqueProposals.filter(p => p.status === 'rejected').length;
    const pending = uniqueProposals.filter(p => p.status === 'pending').length;
    const expired = uniqueProposals.filter(p => p.status === 'expired').length;
    const duplicatesRemoved = (governanceActions?.length || 0) - total;

    return { total, approved, rejected, pending, expired, duplicatesRemoved };
  }, [uniqueProposals, governanceActions]);

  return {
    proposals: uniqueProposals,
    stats,
    isLoading,
    error,
    rawEventCount: governanceActions?.length || 0,
  };
}
