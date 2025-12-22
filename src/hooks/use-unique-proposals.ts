import { useMemo } from "react";
import { useGovernanceEvents } from "./use-governance-events";

export interface GovernanceAction {
  id: string;
  type: 'vote_completed' | 'rule_change' | 'confirmation';
  actionTag: string;
  templateType: 'VoteRequest' | 'DsoRules' | 'AmuletRules' | 'Confirmation';
  status: 'passed' | 'failed' | 'expired' | 'executed';
  effectiveAt: string;
  requester: string | null;
  reason: string | null;
  reasonUrl: string | null;
  votesFor: number;
  votesAgainst: number;
  totalVotes: number;
  contractId: string;
  cipReference: string | null;
}

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
  rawData: GovernanceAction;
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

// Extract CIP reference from reason text
const extractCipReference = (reason: string | null, reasonUrl: string | null): string | null => {
  if (!reason && !reasonUrl) return null;
  const text = `${reason || ''} ${reasonUrl || ''}`;
  const match = text.match(/CIP[#\-\s]?0*(\d+)/i);
  return match ? match[1].padStart(4, '0') : null;
};

// Parse votes array to count for/against
const parseVotes = (votes: unknown): { votesFor: number; votesAgainst: number } => {
  let votesFor = 0;
  let votesAgainst = 0;
  
  if (!Array.isArray(votes)) return { votesFor, votesAgainst };
  
  for (const vote of votes) {
    const [, voteData] = Array.isArray(vote) ? vote : ['', vote];
    const isAccept = voteData?.accept === true || (voteData as any)?.Accept === true;
    if (isAccept) votesFor++;
    else votesAgainst++;
  }
  
  return { votesFor, votesAgainst };
};

export function useUniqueProposals(votingThreshold = 10) {
  const { data: rawEvents, isLoading, error } = useGovernanceEvents();

  // Transform raw events from indexed data into GovernanceAction format
  const governanceActions = useMemo(() => {
    if (!rawEvents?.length) return [];
    
    const actions: GovernanceAction[] = [];
    
    for (const event of rawEvents) {
      const templateId = event.template_id || '';
      if (!templateId.includes('VoteRequest')) continue;
      
      // Only process archived events (completed votes) for historical data
      if (event.event_type !== 'archived') continue;
      
      const payload = event.payload as Record<string, unknown> | undefined;
      const actionTag = (payload?.action as any)?.tag || 'Unknown';
      const reason = payload?.reason as { url?: string; body?: string } | string | null;
      const reasonBody = typeof reason === 'string' ? reason : (reason?.body || null);
      const reasonUrl = typeof reason === 'object' ? (reason?.url || null) : null;
      const votes = payload?.votes;
      const { votesFor, votesAgainst } = parseVotes(votes);
      const totalVotes = votesFor + votesAgainst;
      
      let status: GovernanceAction['status'] = 'failed';
      if (votesFor >= votingThreshold) status = 'passed';
      else if (totalVotes === 0) status = 'expired';
      
      actions.push({
        id: event.event_id || event.contract_id || '',
        type: 'vote_completed',
        actionTag,
        templateType: 'VoteRequest',
        status,
        effectiveAt: event.effective_at || event.timestamp || '',
        requester: (payload?.requester as string) || null,
        reason: reasonBody,
        reasonUrl,
        votesFor,
        votesAgainst,
        totalVotes,
        contractId: event.contract_id || '',
        cipReference: extractCipReference(reasonBody, reasonUrl),
      });
    }
    
    return actions.sort((a, b) => 
      new Date(b.effectiveAt).getTime() - new Date(a.effectiveAt).getTime()
    );
  }, [rawEvents, votingThreshold]);

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
