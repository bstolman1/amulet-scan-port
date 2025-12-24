import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

// Canonical governance proposal matching explorer semantics
export interface CanonicalProposal {
  proposal_id: string;
  event_id: string;
  contract_id: string;
  template_id: string;
  effective_at: string;
  status: 'in_progress' | 'executed' | 'rejected' | 'expired';
  is_closed: boolean;
  action_tag: string | null;
  action_value: unknown;
  requester: string | null;
  reason: string | null;
  reason_url: string | null;
  votes: unknown[];
  vote_count: number;
  accept_count: number;
  reject_count: number;
  vote_before: string | null;
  tracking_cid: string | null;
  semantic_key: string | null;
  action_subject: string | null;
  is_human: boolean;
  related_count: number;
  first_seen: string;
  last_seen: string;
}

export interface CanonicalProposalStats {
  rawEvents: number;
  lifecycleProposals: number;
  humanProposals: number;
  byStatus: {
    in_progress: number;
    executed: number;
    rejected: number;
    expired: number;
  };
}

export interface CanonicalProposalsResponse {
  proposals: CanonicalProposal[];
  total: number;
  stats: CanonicalProposalStats;
  source: string;
  indexedAt: string | null;
}

// Legacy interface for backwards compatibility
export interface GovernanceProposal {
  semantic_key: string;
  action_type: string;
  action_subject: string | null;
  latest_status: 'in_progress' | 'executed' | 'rejected' | 'expired';
  accept_count: number;
  reject_count: number;
  first_seen: string;
  last_seen: string;
  related_count: number;
  latest_requester: string | null;
  latest_reason_body: string | null;
  latest_reason_url: string | null;
  latest_vote_before: string | null;
  latest_contract_id: string;
}

export interface GovernanceProposalsResponse {
  proposals: GovernanceProposal[];
  total: number;
  byStatus: {
    in_progress: number;
    executed: number;
    rejected: number;
    expired: number;
  };
  fromIndex: boolean;
  indexedAt: string | null;
}

export interface ProposalTimelineEntry {
  id: number;
  payload_id: string;
  contract_id: string;
  status: string;
  accept_count: number;
  reject_count: number;
  requester: string | null;
  reason_body: string | null;
  reason_url: string | null;
  vote_before: string | null;
  indexed_at: string;
}

export interface ProposalTimelineResponse {
  semantic_key: string;
  entries: ProposalTimelineEntry[];
  total: number;
}

/**
 * CANONICAL: Fetch governance proposals matching explorer semantics
 * - proposal_id = COALESCE(tracking_cid, contract_id)
 * - is_human = true for explorer-visible proposals
 * - Returns ~200-250 proposals matching major explorers
 */
export function useCanonicalProposals(options?: { status?: string; humanOnly?: boolean }) {
  return useQuery<CanonicalProposalsResponse>({
    queryKey: ["canonical-proposals", options?.status, options?.humanOnly],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (options?.status) params.set("status", options.status);
      if (options?.humanOnly === false) params.set("human", "false");
      const url = `/api/events/canonical-proposals${params.toString() ? `?${params}` : ""}`;
      return apiFetch<CanonicalProposalsResponse>(url);
    },
    staleTime: 30 * 1000,
    retry: 1,
  });
}

/**
 * Fetch canonical proposal statistics
 */
export function useCanonicalProposalStats() {
  return useQuery<CanonicalProposalStats & { source: string; indexedAt: string | null }>({
    queryKey: ["canonical-proposals-stats"],
    queryFn: () => apiFetch("/api/events/canonical-proposals/stats"),
    staleTime: 30 * 1000,
    retry: 1,
  });
}

/**
 * @deprecated Use useCanonicalProposals for explorer-matching semantics
 */
export function useGovernanceProposals(status?: 'active' | 'historical') {
  return useQuery<GovernanceProposalsResponse>({
    queryKey: ["governance-proposals", status],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (status) params.set("status", status);
      const url = `/api/events/governance-proposals${params.toString() ? `?${params}` : ""}`;
      const response = await apiFetch<GovernanceProposalsResponse>(url);
      return response;
    },
    staleTime: 30 * 1000,
    retry: 1,
  });
}

export function useProposalTimeline(semanticKey: string | null) {
  return useQuery<ProposalTimelineResponse>({
    queryKey: ["proposal-timeline", semanticKey],
    queryFn: async () => {
      if (!semanticKey) throw new Error("No semantic key provided");
      const encoded = encodeURIComponent(semanticKey);
      const response = await apiFetch<ProposalTimelineResponse>(
        `/api/events/governance-proposals/${encoded}/timeline`
      );
      return response;
    },
    enabled: !!semanticKey,
    staleTime: 60 * 1000,
    retry: 1,
  });
}
