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

export function useGovernanceHistory(limit = 500) {
  return useQuery({
    queryKey: ["governanceHistory", limit],
    queryFn: async (): Promise<GovernanceAction[]> => {
      const response = await apiFetch<HistoryResponse>(`/api/events/governance-history?limit=${limit}`);
      const events = response.data || [];
      
      // Process events into governance actions
      // Focus on archived VoteRequests (completed votes) and created DsoRules/AmuletRules
      const actions: GovernanceAction[] = [];
      const seenContracts = new Set<string>();
      
      for (const event of events) {
        const templateType = getTemplateType(event.template_id);
        
        // For VoteRequests, we care about archived events (completed votes)
        if (templateType === 'VoteRequest' && event.event_type === 'archived') {
          if (seenContracts.has(event.contract_id)) continue;
          seenContracts.add(event.contract_id);
          
          const { votesFor, votesAgainst } = parseVotes(event.votes);
          const totalVotes = votesFor + votesAgainst;
          const threshold = 10; // Standard threshold
          
          let status: GovernanceAction['status'] = 'failed';
          if (votesFor >= threshold) status = 'passed';
          else if (totalVotes === 0) status = 'expired';
          
          actions.push({
            id: event.event_id || event.contract_id,
            type: 'vote_completed',
            actionTag: event.action_tag || 'Unknown',
            templateType,
            status,
            effectiveAt: event.effective_at || event.timestamp,
            requester: event.requester,
            reason: event.reason?.body || null,
            reasonUrl: event.reason?.url || null,
            votesFor,
            votesAgainst,
            totalVotes,
            contractId: event.contract_id,
            cipReference: extractCipReference(event.reason),
          });
        }
        
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
            effectiveAt: event.effective_at || event.timestamp,
            requester: null,
            reason: null,
            reasonUrl: null,
            votesFor: 0,
            votesAgainst: 0,
            totalVotes: 0,
            contractId: event.contract_id,
            cipReference: null,
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
            effectiveAt: event.effective_at || event.timestamp,
            requester: event.requester,
            reason: event.reason?.body || null,
            reasonUrl: event.reason?.url || null,
            votesFor: 0,
            votesAgainst: 0,
            totalVotes: 0,
            contractId: event.contract_id,
            cipReference: extractCipReference(event.reason),
          });
        }
      }
      
      // Sort by effective date descending
      return actions.sort((a, b) => 
        new Date(b.effectiveAt).getTime() - new Date(a.effectiveAt).getTime()
      );
    },
    staleTime: 60_000, // 1 minute
  });
}
