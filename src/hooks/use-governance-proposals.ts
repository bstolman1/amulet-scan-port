import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

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
