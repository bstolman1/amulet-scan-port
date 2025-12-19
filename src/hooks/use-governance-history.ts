import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

export interface GovernanceHistoryEvent {
  event_id: string;
  event_type: 'created' | 'archived';
  contract_id: string;
  template_id: string;
  effective_at: string;
  timestamp: string;
  payload: any; // Full payload like Active Proposals
  signatories?: string[];
  observers?: string[];
}

interface HistoryResponse {
  data: GovernanceHistoryEvent[];
  count: number;
  hasMore?: boolean;
  source?: string;
}

// Processed governance action for display - matching Active Proposals structure
export interface GovernanceAction {
  id: string;
  contractId: string;
  trackingCid: string;
  title: string;
  actionType: string;
  actionDetails: any;
  action: any; // Full action for detailed display
  reasonBody: string;
  reasonUrl: string;
  requester: string;
  status: 'in_progress' | 'executed' | 'rejected' | 'expired';
  votesFor: number;
  votesAgainst: number;
  votedSvs: Array<{
    party: string;
    sv: string;
    vote: 'accept' | 'reject' | 'abstain';
    reason: string;
    reasonUrl: string;
    castAt: string | null;
  }>;
  voteBefore: string | null;
  targetEffectiveAt: string | null;
  templateType: 'VoteRequest' | 'DsoRules' | 'AmuletRules' | 'Confirmation';
  eventType: 'created' | 'archived';
  effectiveAt: string;
  rawData: any; // Full event for JSON display
}

export interface GovernanceHistorySummary {
  totalRequests: number;
  inProgress: number;
  executed: number;
  rejected: number;
  expired: number;
}

export interface GovernanceHistoryResult {
  actions: GovernanceAction[];
  summary: GovernanceHistorySummary;
  totalRawEvents: number;
  hasMore: boolean;
}

// Helper to parse action structure and extract meaningful title
// COPIED FROM Governance.tsx to ensure identical processing
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

// Helper to parse votes array (format: [[svName, voteObj], ...])
// COPIED FROM Governance.tsx to ensure identical processing
const parseVotes = (votes: any): { votesFor: number; votesAgainst: number; votedSvs: GovernanceAction['votedSvs'] } => {
  if (!votes) return { votesFor: 0, votesAgainst: 0, votedSvs: [] };
  
  // Handle array of tuples format: [["SV Name", { sv, accept, reason, optCastAt }], ...]
  const votesArray = Array.isArray(votes) ? votes : Object.entries(votes);
  
  let votesFor = 0;
  let votesAgainst = 0;
  const votedSvs: GovernanceAction['votedSvs'] = [];
  
  for (const vote of votesArray) {
    const [svName, voteData] = Array.isArray(vote) ? vote : [vote.sv || "Unknown", vote];
    const isAccept = voteData?.accept === true || voteData?.Accept === true;
    const isReject = voteData?.accept === false || voteData?.reject === true || voteData?.Reject === true;
    
    if (isAccept) votesFor++;
    else if (isReject) votesAgainst++;
    
    votedSvs.push({
      party: svName,
      sv: voteData?.sv || svName,
      vote: isAccept ? "accept" : isReject ? "reject" : "abstain",
      reason: voteData?.reason?.body || voteData?.reason || "",
      reasonUrl: voteData?.reason?.url || "",
      castAt: voteData?.optCastAt || null,
    });
  }
  
  return { votesFor, votesAgainst, votedSvs };
};

// Get template type from template_id
const getTemplateType = (templateId: string): GovernanceAction['templateType'] => {
  if (templateId.includes('VoteRequest')) return 'VoteRequest';
  if (templateId.includes('DsoRules')) return 'DsoRules';
  if (templateId.includes('AmuletRules')) return 'AmuletRules';
  if (templateId.includes('Confirmation')) return 'Confirmation';
  return 'VoteRequest';
};

// Determine VoteRequest status based on votes, deadline, and event type
const determineStatus = (
  eventType: string,
  votesFor: number,
  totalVotes: number,
  threshold: number,
  voteBefore: string | null
): GovernanceAction['status'] => {
  const now = new Date();
  const voteDeadline = voteBefore ? new Date(voteBefore) : null;
  const isExpired = voteDeadline && voteDeadline < now;
  
  // If this is a created event (not archived), check if still in progress
  if (eventType === 'created') {
    if (votesFor >= threshold) return 'executed';
    if (isExpired) {
      if (totalVotes === 0) return 'expired';
      return 'rejected';
    }
    return 'in_progress';
  }
  
  // Archived event - vote is completed
  if (votesFor >= threshold) return 'executed';
  if (totalVotes === 0) return 'expired';
  return 'rejected';
};

export function useGovernanceHistory(limit = 50, offset = 0) {
  return useQuery({
    queryKey: ["governanceHistory", limit, offset],
    queryFn: async (): Promise<GovernanceHistoryResult> => {
      const response = await apiFetch<HistoryResponse>(`/api/events/governance-history?limit=${limit}&offset=${offset}`);
      const events = response.data || [];
      const hasMore = response.hasMore ?? false;
      
      // Process events - backend already filters for VoteRequest CREATED events
      // and groups by trackingCid to get latest vote state
      const actions: GovernanceAction[] = [];
      const threshold = 9; // Standard threshold from DSO (voting_threshold)
      
      for (const event of events) {
        const templateType = getTemplateType(event.template_id);
        const payload = event.payload || {};
        
        if (templateType === 'VoteRequest') {
          // VoteRequest created events have full vote data
          const actionRaw = payload.action || {};
          const { title, actionType, actionDetails } = parseAction(actionRaw);
          
          // Parse votes
          const votesRaw = payload.votes || [];
          const { votesFor, votesAgainst, votedSvs } = parseVotes(votesRaw);
          const totalVotes = votesFor + votesAgainst;
          
          // Extract fields
          const requester = payload.requester || "Unknown";
          const reasonObj = payload.reason || {};
          const reasonBody = reasonObj?.body || (typeof reasonObj === "string" ? reasonObj : "");
          const reasonUrl = reasonObj?.url || "";
          const voteBefore = payload.voteBefore || null;
          const targetEffectiveAt = payload.targetEffectiveAt || null;
          const trackingCid = payload.trackingCid || event.contract_id;
          
          // Determine status based on votes and deadline
          const status = determineStatus('created', votesFor, totalVotes, threshold, voteBefore);
          
          actions.push({
            id: trackingCid?.slice(0, 12) || event.contract_id.slice(0, 12),
            contractId: event.contract_id,
            trackingCid,
            title,
            actionType,
            actionDetails,
            action: actionRaw,
            reasonBody,
            reasonUrl,
            requester,
            status,
            votesFor,
            votesAgainst,
            votedSvs,
            voteBefore,
            targetEffectiveAt,
            templateType: 'VoteRequest',
            eventType: 'created',
            effectiveAt: event.effective_at || event.timestamp,
            rawData: event,
          });
        } else if (templateType === 'Confirmation') {
          // Confirmation events (executed actions)
          const actionRaw = payload.action || {};
          const { title } = parseAction(actionRaw);
          
          actions.push({
            id: event.contract_id.slice(0, 12),
            contractId: event.contract_id,
            trackingCid: event.contract_id,
            title: title || 'Confirmation',
            actionType: 'Confirmation',
            actionDetails: payload,
            action: actionRaw,
            reasonBody: '',
            reasonUrl: '',
            requester: payload.requester || '',
            status: 'executed',
            votesFor: 0,
            votesAgainst: 0,
            votedSvs: [],
            voteBefore: null,
            targetEffectiveAt: null,
            templateType: 'Confirmation',
            eventType: 'created',
            effectiveAt: event.effective_at || event.timestamp,
            rawData: event,
          });
        }
      }
      
      // Calculate summary stats
      const voteRequests = actions.filter(a => a.templateType === 'VoteRequest');
      const summary: GovernanceHistorySummary = {
        totalRequests: voteRequests.length,
        inProgress: voteRequests.filter(a => a.status === 'in_progress').length,
        executed: voteRequests.filter(a => a.status === 'executed').length,
        rejected: voteRequests.filter(a => a.status === 'rejected').length,
        expired: voteRequests.filter(a => a.status === 'expired').length,
      };
      
      // Already sorted by backend
      return {
        actions,
        summary,
        totalRawEvents: response.count,
        hasMore,
      };
    },
    staleTime: 60_000, // 1 minute
  });
}
