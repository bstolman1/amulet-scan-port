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
      
      // Process events exactly like Active Proposals does
      const actions: GovernanceAction[] = [];
      const seenContracts = new Set<string>();
      const threshold = 9; // Standard threshold from DSO (voting_threshold)
      
      // Group events by contract_id to prefer archived over created
      const contractEvents = new Map<string, { created?: GovernanceHistoryEvent; archived?: GovernanceHistoryEvent }>();
      
      for (const event of events) {
        const templateType = getTemplateType(event.template_id);
        if (templateType !== 'VoteRequest') continue;
        
        const state = contractEvents.get(event.contract_id) || {};
        if (event.event_type === 'created') state.created = event;
        if (event.event_type === 'archived') state.archived = event;
        contractEvents.set(event.contract_id, state);
      }
      
      // Process VoteRequests - prefer archived (completed) over created (in-progress)
      for (const [contractId, state] of contractEvents) {
        if (seenContracts.has(contractId)) continue;
        seenContracts.add(contractId);
        
        const event = state.archived || state.created;
        if (!event) continue;
        
        // Process exactly like Active Proposals
        const payload = event.payload || {};
        
        // Parse action
        const actionRaw = payload.action || {};
        const { title, actionType, actionDetails } = parseAction(actionRaw);
        
        // Parse votes
        const votesRaw = payload.votes || [];
        const { votesFor, votesAgainst, votedSvs } = parseVotes(votesRaw);
        const totalVotes = votesFor + votesAgainst;
        
        // Extract requester information
        const requester = payload.requester || "Unknown";
        
        // Extract reason (has url and body)
        const reasonObj = payload.reason || {};
        const reasonBody = reasonObj?.body || (typeof reasonObj === "string" ? reasonObj : "");
        const reasonUrl = reasonObj?.url || "";
        
        // Extract timing fields
        const voteBefore = payload.voteBefore || null;
        const targetEffectiveAt = payload.targetEffectiveAt || null;
        const trackingCid = payload.trackingCid || contractId;
        
        // Determine status
        const status = determineStatus(event.event_type, votesFor, totalVotes, threshold, voteBefore);
        
        actions.push({
          id: trackingCid?.slice(0, 12) || contractId.slice(0, 12),
          contractId,
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
          eventType: event.event_type as 'created' | 'archived',
          effectiveAt: event.effective_at || event.timestamp,
          rawData: event,
        });
      }
      
      // Process non-VoteRequest events (DsoRules, AmuletRules, Confirmation)
      for (const event of events) {
        const templateType = getTemplateType(event.template_id);
        
        // Skip VoteRequests (already processed above)
        if (templateType === 'VoteRequest') continue;
        
        // For DsoRules/AmuletRules, we care about created events (rule changes)
        if ((templateType === 'DsoRules' || templateType === 'AmuletRules') && event.event_type === 'created') {
          if (seenContracts.has(event.contract_id)) continue;
          seenContracts.add(event.contract_id);
          
          actions.push({
            id: event.contract_id.slice(0, 12),
            contractId: event.contract_id,
            trackingCid: event.contract_id,
            title: templateType === 'DsoRules' ? 'DSO Rules Update' : 'Amulet Rules Update',
            actionType: templateType,
            actionDetails: event.payload,
            action: null,
            reasonBody: '',
            reasonUrl: '',
            requester: '',
            status: 'executed',
            votesFor: 0,
            votesAgainst: 0,
            votedSvs: [],
            voteBefore: null,
            targetEffectiveAt: null,
            templateType,
            eventType: 'created',
            effectiveAt: event.effective_at || event.timestamp,
            rawData: event,
          });
        }
        
        // For Confirmations (executed actions)
        if (templateType === 'Confirmation' && event.event_type === 'created') {
          if (seenContracts.has(event.contract_id)) continue;
          seenContracts.add(event.contract_id);
          
          const payload = event.payload || {};
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
      
      // Sort by effective date descending
      const sortedActions = actions.sort((a, b) => 
        new Date(b.effectiveAt).getTime() - new Date(a.effectiveAt).getTime()
      );
      
      return {
        actions: sortedActions,
        summary,
        totalRawEvents: response.count,
        hasMore,
      };
    },
    staleTime: 60_000, // 1 minute
  });
}
