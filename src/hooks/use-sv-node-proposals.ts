import { useQuery, useMutation } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

const SV_NODE_BASE = "/api/events/sv-node";

// Types for SV node responses
export interface SVNodeVoteRequest {
  contract_id: string;
  template_id: string;
  payload: {
    dso: string;
    requester: string;
    action: {
      tag: string;
      value: unknown;
    };
    reason: {
      url: string;
      body: string;
    };
    voteBefore: string;
    votes: Array<[string, { sv: string; accept: boolean; reason: { url: string; body: string } }]>;
    trackingCid: string | null;
  };
  created_at: string;
}

export interface SVNodeVoteResult {
  request: {
    tracking_cid?: string;
    contract_id?: string;
    template_id?: string;
    requester?: string;
    action?: unknown;
    reason?: { url: string; body: string };
    vote_before?: string;
    votes?: Array<[string, unknown]>;
  };
  outcome: {
    accepted: boolean;
    effective_at?: string;
  };
}

export interface SVNodeProposal {
  contract_id: string;
  template_id: string;
  status: 'in_progress' | 'executed' | 'rejected';
  payload: unknown;
  outcome?: {
    accepted: boolean;
    effective_at?: string;
  };
  created_at?: string;
  effective_at?: string;
  source_type: 'active_request' | 'vote_result';
}

export interface AllProposalsResponse {
  proposals: SVNodeProposal[];
  stats: {
    total: number;
    active: number;
    accepted: number;
    rejected: number;
    in_progress: number;
    executed: number;
  };
  source: string;
  fetched_at: string;
}

export interface VoteResultsResponse {
  vote_results: SVNodeVoteResult[];
  count: number;
  source: string;
  fetched_at: string;
}

export interface ActiveVoteRequestsResponse {
  vote_requests: SVNodeVoteRequest[];
  count: number;
  source: string;
  fetched_at: string;
}

/**
 * Fetch ALL proposals from SV node (active + historical)
 * This is the primary hook for the governance page
 */
export function useSVNodeAllProposals() {
  return useQuery<AllProposalsResponse>({
    queryKey: ["sv-node-all-proposals"],
    queryFn: () => apiFetch<AllProposalsResponse>(`${SV_NODE_BASE}/all-proposals`),
    staleTime: 30 * 1000, // 30 seconds
    retry: 2,
  });
}

/**
 * Fetch active vote requests from SV node
 */
export function useSVNodeActiveRequests() {
  return useQuery<ActiveVoteRequestsResponse>({
    queryKey: ["sv-node-active-requests"],
    queryFn: () => apiFetch<ActiveVoteRequestsResponse>(`${SV_NODE_BASE}/active-vote-requests`),
    staleTime: 30 * 1000,
    retry: 2,
  });
}

/**
 * Query historical vote results with filters
 */
export function useSVNodeVoteResults(filters?: {
  actionName?: string;
  accepted?: boolean;
  requester?: string;
  effectiveFrom?: string;
  effectiveTo?: string;
  limit?: number;
}) {
  return useQuery<VoteResultsResponse>({
    queryKey: ["sv-node-vote-results", filters],
    queryFn: async () => {
      const response = await fetch(`${SV_NODE_BASE}/vote-results`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(filters || {}),
      });
      if (!response.ok) {
        throw new Error(`Failed to fetch vote results: ${response.statusText}`);
      }
      return response.json();
    },
    staleTime: 30 * 1000,
    retry: 2,
  });
}

/**
 * Helper to parse action from SV node payload
 */
export function parseAction(action: unknown): { title: string; actionType: string; actionDetails: unknown } {
  if (!action || typeof action !== 'object') {
    return { title: "Unknown Action", actionType: "Unknown", actionDetails: null };
  }
  
  const actionObj = action as { tag?: string; value?: unknown };
  const outerTag = actionObj.tag || Object.keys(actionObj)[0] || "Unknown";
  const outerValue = actionObj.value || (actionObj as Record<string, unknown>)[outerTag] || actionObj;
  
  // Extract inner action (e.g., dsoAction, amuletRulesAction)
  const innerAction = (outerValue as Record<string, unknown>)?.dsoAction || 
                      (outerValue as Record<string, unknown>)?.amuletRulesAction || 
                      outerValue;
  const innerTag = (innerAction as { tag?: string })?.tag || "";
  const innerValue = (innerAction as { value?: unknown })?.value || innerAction;
  
  // Build human-readable title
  const actionType = innerTag || outerTag;
  const title = actionType
    .replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, "")
    .replace(/([A-Z])/g, " $1")
    .trim();
  
  return { title, actionType, actionDetails: innerValue };
}

/**
 * Helper to parse votes from SV node payload
 */
export function parseVotes(votes: unknown): { votesFor: number; votesAgainst: number; votedSvs: Array<{ party: string; vote: string; reason: string }> } {
  if (!Array.isArray(votes)) {
    return { votesFor: 0, votesAgainst: 0, votedSvs: [] };
  }
  
  let votesFor = 0;
  let votesAgainst = 0;
  const votedSvs: Array<{ party: string; vote: string; reason: string }> = [];
  
  for (const vote of votes) {
    const [svName, voteData] = Array.isArray(vote) ? vote : [String(vote), {}];
    const isAccept = (voteData as { accept?: boolean })?.accept === true;
    const isReject = (voteData as { accept?: boolean })?.accept === false;
    
    if (isAccept) votesFor++;
    else if (isReject) votesAgainst++;
    
    votedSvs.push({
      party: svName,
      vote: isAccept ? "accept" : isReject ? "reject" : "abstain",
      reason: (voteData as { reason?: { body?: string } })?.reason?.body || "",
    });
  }
  
  return { votesFor, votesAgainst, votedSvs };
}
