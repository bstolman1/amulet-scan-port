import { useMemo } from "react";
import { useGovernanceEvents } from "./use-governance-events";

// Uses fast indexed VoteRequest data from DuckDB
export interface GovernanceAction {
  id: string;
  type: 'vote_completed' | 'rule_change' | 'confirmation';
  actionTag: string;
  actionTitle: string;
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
  voteBefore: string | null;
  targetEffectiveAt: string | null;
  actionDetails: Record<string, unknown> | null;
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
  actionDetails: Record<string, unknown> | null;
  rawData: GovernanceAction;
}

// Helper to parse action structure and extract meaningful title (same as Governance History)
const parseAction = (action: any): { title: string; actionType: string; actionDetails: any } => {
  if (!action) return { title: "Unknown Action", actionType: "Unknown", actionDetails: null };
  
  // Handle nested tag/value structure: { tag: "ARC_DsoRules", value: { dsoAction: { tag: "SRARC_...", value: {...} } } }
  const outerTag = action.tag || Object.keys(action)[0] || "Unknown";
  const outerValue = action.value || action[outerTag] || action;
  
  // Extract inner action (e.g., dsoAction)
  const innerAction = outerValue?.dsoAction || outerValue?.amuletRulesAction || outerValue;
  const innerTag = innerAction?.tag || "";
  const innerValue = innerAction?.value || innerAction;
  
  // Build human-readable title
  const actionType = innerTag || outerTag;
  const title = actionType
    .replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, "")
    .replace(/([A-Z])/g, " $1")
    .trim();
  
  return { title, actionType, actionDetails: innerValue };
};

// Parse votes array to count for/against (same as Governance History)
const parseVotes = (votes: unknown): { votesFor: number; votesAgainst: number } => {
  if (!votes) return { votesFor: 0, votesAgainst: 0 };
  
  // Handle array of tuples format: [["SV Name", { sv, accept, reason, optCastAt }], ...]
  const votesArray = Array.isArray(votes) ? votes : Object.entries(votes);
  
  let votesFor = 0;
  let votesAgainst = 0;
  
  for (const vote of votesArray) {
    const [, voteData] = Array.isArray(vote) ? vote : ['', vote];
    const isAccept = voteData?.accept === true || (voteData as any)?.Accept === true;
    const isReject = voteData?.accept === false || voteData?.reject === true || (voteData as any)?.Reject === true;
    
    if (isAccept) votesFor++;
    else if (isReject) votesAgainst++;
  }
  
  return { votesFor, votesAgainst };
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

export function useUniqueProposals(votingThreshold = 10) {
  const { data: rawEvents, isLoading, error } = useGovernanceEvents();

  // Transform raw events from indexed data into GovernanceAction format
  // Use the SAME parsing logic as the Governance History tab
  const governanceActions = useMemo(() => {
    if (!rawEvents?.length) return [];
    
    const actions: GovernanceAction[] = [];
    
    for (const event of rawEvents) {
      const templateId = event.template_id || '';
      if (!templateId.includes('VoteRequest')) continue;
      
      // Process ALL events (not just archived) to get complete data
      const payload = event.payload as Record<string, unknown> | undefined;
      if (!payload) continue;
      
      // Parse action using same logic as Governance History
      const action = payload.action || {};
      const { title, actionType, actionDetails } = parseAction(action);
      
      // Parse votes using same logic as Governance History
      const votesRaw = payload.votes;
      const { votesFor, votesAgainst } = parseVotes(votesRaw);
      const totalVotes = votesFor + votesAgainst;
      
      // Extract requester
      const requester = (payload.requester as string) || null;
      
      // Extract reason (has url and body)
      const reasonObj = payload.reason as { url?: string; body?: string } | string | null;
      const reasonBody = typeof reasonObj === 'string' ? reasonObj : (reasonObj?.body || null);
      const reasonUrl = typeof reasonObj === 'object' ? (reasonObj?.url || null) : null;
      
      // Extract timing fields
      const voteBefore = (payload.voteBefore as string) || null;
      const targetEffectiveAt = (payload.targetEffectiveAt as string) || null;
      
      // Determine status based on votes and deadline
      const now = new Date();
      const voteDeadline = voteBefore ? new Date(voteBefore) : null;
      const isExpired = voteDeadline && voteDeadline < now;
      const isClosed = event.event_type === 'archived';
      
      let status: GovernanceAction['status'] = 'failed';
      if (votesFor >= votingThreshold) {
        status = 'passed';
      } else if (isClosed || (isExpired && votesFor < votingThreshold)) {
        status = totalVotes === 0 ? 'expired' : 'failed';
      }
      
      actions.push({
        id: event.event_id || event.contract_id || '',
        type: 'vote_completed',
        actionTag: actionType,
        actionTitle: title || 'Unknown',
        templateType: 'VoteRequest',
        status,
        effectiveAt: event.effective_at || event.timestamp || '',
        requester,
        reason: reasonBody,
        reasonUrl,
        votesFor,
        votesAgainst,
        totalVotes,
        contractId: event.contract_id || '',
        cipReference: extractCipReference(reasonBody, reasonUrl),
        voteBefore,
        targetEffectiveAt,
        actionDetails,
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
          status = 'approved';
          break;
        default:
          if (latestEvent.votesFor >= votingThreshold) {
            status = 'approved';
          } else if (latestEvent.type === 'vote_completed') {
            status = 'rejected';
          } else {
            status = 'expired';
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
        title: latestEvent.actionTitle || 'Unknown',
        status,
        latestEventTime: latestEvent.effectiveAt,
        createdAt,
        voteBefore: latestEvent.voteBefore,
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
        actionDetails: latestEvent.actionDetails,
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
