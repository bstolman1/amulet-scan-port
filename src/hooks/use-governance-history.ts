import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

export interface GovernanceHistoryEvent {
  event_id: string;
  event_type: 'created' | 'archived';
  contract_id: string;
  template_id: string;
  effective_at: string;
  timestamp: string;
  action_tag: string | null;
  requester: string | null;
  reason: { url?: string; body?: string } | null;
  votes: Array<[string, { accept?: boolean; reason?: { url?: string; body?: string } }]>;
  vote_before: string | null;
}

interface HistoryResponse {
  data: GovernanceHistoryEvent[];
  count: number;
  hasMore?: boolean;
  source?: string;
}

// Processed governance action for display
export interface GovernanceAction {
  id: string;
  type: 'vote_request' | 'vote_completed' | 'rule_change' | 'confirmation';
  actionTag: string;
  templateType: 'VoteRequest' | 'DsoRules' | 'AmuletRules' | 'Confirmation';
  status: 'in_progress' | 'executed' | 'rejected' | 'expired';
  eventType: 'created' | 'archived';
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

// Extract CIP reference from reason text
const extractCipReference = (reason: { url?: string; body?: string } | null): string | null => {
  if (!reason) return null;
  const text = `${reason.body || ''} ${reason.url || ''}`;
  const match = text.match(/CIP[#\-\s]?0*(\d+)/i);
  return match ? match[1].padStart(4, '0') : null;
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

// Get template type from template_id
const getTemplateType = (templateId: string): GovernanceAction['templateType'] => {
  if (templateId.includes('VoteRequest')) return 'VoteRequest';
  if (templateId.includes('DsoRules')) return 'DsoRules';
  if (templateId.includes('AmuletRules')) return 'AmuletRules';
  if (templateId.includes('Confirmation')) return 'Confirmation';
  return 'VoteRequest';
};

// Determine VoteRequest status based on votes, deadline, and event type
const determineVoteRequestStatus = (
  event: GovernanceHistoryEvent,
  votesFor: number,
  totalVotes: number,
  threshold: number
): GovernanceAction['status'] => {
  // If this is a created event (not archived), it's still in progress
  if (event.event_type === 'created') {
    // Check if voting deadline has passed
    const now = new Date();
    const voteBefore = event.vote_before ? new Date(event.vote_before) : null;
    
    if (voteBefore && voteBefore < now) {
      // Deadline passed - determine outcome
      if (votesFor >= threshold) return 'executed';
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
      
      // Process events into governance actions
      // Track contract states to handle create/archive pairs
      const contractStates = new Map<string, { created?: GovernanceHistoryEvent; archived?: GovernanceHistoryEvent }>();
      
      // First pass: group events by contract_id
      for (const event of events) {
        const templateType = getTemplateType(event.template_id);
        if (templateType !== 'VoteRequest') continue;
        
        const state = contractStates.get(event.contract_id) || {};
        if (event.event_type === 'created') state.created = event;
        if (event.event_type === 'archived') state.archived = event;
        contractStates.set(event.contract_id, state);
      }
      
      const actions: GovernanceAction[] = [];
      const seenContracts = new Set<string>();
      const threshold = 9; // Standard threshold from DSO (voting_threshold from API)
      
      // Process VoteRequests - prefer archived (completed) over created (in-progress)
      for (const [contractId, state] of contractStates) {
        if (seenContracts.has(contractId)) continue;
        seenContracts.add(contractId);
        
        // Use archived event if available (completed vote), otherwise created (in-progress)
        const event = state.archived || state.created;
        if (!event) continue;
        
        const { votesFor, votesAgainst } = parseVotes(event.votes);
        const totalVotes = votesFor + votesAgainst;
        const status = determineVoteRequestStatus(event, votesFor, totalVotes, threshold);
        
        actions.push({
          id: event.event_id || contractId,
          type: state.archived ? 'vote_completed' : 'vote_request',
          actionTag: event.action_tag || 'Unknown Action',
          templateType: 'VoteRequest',
          status,
          eventType: event.event_type as 'created' | 'archived',
          effectiveAt: event.effective_at || event.timestamp,
          requester: event.requester,
          reason: event.reason?.body || null,
          reasonUrl: event.reason?.url || null,
          votesFor,
          votesAgainst,
          totalVotes,
          contractId,
          cipReference: extractCipReference(event.reason),
          voteBefore: event.vote_before,
        });
      }
      
      // Process non-VoteRequest events
      for (const event of events) {
        const templateType = getTemplateType(event.template_id);
        
        // For DsoRules/AmuletRules, we care about created events (rule changes)
        if ((templateType === 'DsoRules' || templateType === 'AmuletRules') && event.event_type === 'created') {
          if (seenContracts.has(event.contract_id)) continue;
          seenContracts.add(event.contract_id);
          
          actions.push({
            id: event.event_id || event.contract_id,
            type: 'rule_change',
            actionTag: templateType === 'DsoRules' ? 'DSO Rules Update' : 'Amulet Rules Update',
            templateType,
            status: 'executed',
            eventType: 'created',
            effectiveAt: event.effective_at || event.timestamp,
            requester: null,
            reason: null,
            reasonUrl: null,
            votesFor: 0,
            votesAgainst: 0,
            totalVotes: 0,
            contractId: event.contract_id,
            cipReference: null,
            voteBefore: null,
          });
        }
        
        // For Confirmations (executed actions)
        if (templateType === 'Confirmation' && event.event_type === 'created') {
          if (seenContracts.has(event.contract_id)) continue;
          seenContracts.add(event.contract_id);
          
          actions.push({
            id: event.event_id || event.contract_id,
            type: 'confirmation',
            actionTag: event.action_tag || 'Confirmation',
            templateType,
            status: 'executed',
            eventType: 'created',
            effectiveAt: event.effective_at || event.timestamp,
            requester: event.requester,
            reason: event.reason?.body || null,
            reasonUrl: event.reason?.url || null,
            votesFor: 0,
            votesAgainst: 0,
            totalVotes: 0,
            contractId: event.contract_id,
            cipReference: extractCipReference(event.reason),
            voteBefore: null,
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
