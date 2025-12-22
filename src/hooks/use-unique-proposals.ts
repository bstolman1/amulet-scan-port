import { useMemo } from "react";
import { GovernanceHistoryEvent, useGovernanceHistory } from "./use-governance-history";

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
  eventCount: number; // How many events this proposal has
  lastEventType: 'created' | 'archived';
}

// Extract CIP reference from reason
const extractCipReference = (reason: { url?: string; body?: string } | null): string | null => {
  if (!reason) return null;
  const text = `${reason.body || ''} ${reason.url || ''}`;
  const match = text.match(/CIP[#\-\s]?0*(\d+)/i);
  return match ? `CIP-${match[1].padStart(4, '0')}` : null;
};

// Parse votes array to count for/against
const parseVotes = (votes: GovernanceHistoryEvent['votes']): { votesFor: number; votesAgainst: number } => {
  let votesFor = 0;
  let votesAgainst = 0;
  
  for (const vote of votes || []) {
    const [, voteData] = Array.isArray(vote) ? vote : ['', vote];
    const isAccept = voteData?.accept === true || (voteData as any)?.Accept === true;
    if (isAccept) votesFor++;
    else votesAgainst++;
  }
  
  return { votesFor, votesAgainst };
};

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
  const { data: governanceActions, isLoading, error } = useGovernanceHistory(1000);

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
      
      // Determine status based on latest event
      let status: UniqueProposal['status'] = 'pending';
      if (latestEvent.status === 'passed') {
        status = 'approved';
      } else if (latestEvent.status === 'failed') {
        status = 'rejected';
      } else if (latestEvent.status === 'expired') {
        status = 'expired';
      } else if (latestEvent.votesFor >= votingThreshold) {
        status = 'approved';
      } else if (latestEvent.type === 'vote_completed') {
        status = latestEvent.votesFor >= votingThreshold ? 'approved' : 'rejected';
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
        voteBefore: null, // Not available in processed actions
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
