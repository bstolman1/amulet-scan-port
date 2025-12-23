import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

/**
 * Timeline info for proposal re-submissions
 */
export interface ProposalTimeline {
  firstSeen: string | null;
  lastSeen: string | null;
  relatedCount: number;
}

/**
 * Governance proposal from the indexed API with semantic grouping
 */
export interface GovernanceProposal {
  event_id: string;
  stable_id: string;
  contract_id: string;
  template_id: string;
  effective_at: string;
  status: "in_progress" | "executed" | "rejected" | "expired" | "active" | "historical";
  is_closed: boolean;
  action_tag: string | null;
  action_value: unknown;
  requester: string | null;
  reason: string | null;
  votes: Array<[string, { accept: boolean; reason?: { url?: string; body?: string } }]>;
  vote_count: number;
  vote_before: string | null;
  target_effective_at: string | null;
  tracking_cid: string | null;
  dso: string | null;
  payload: unknown;
  semantic_key: string | null;
  action_subject: string | null;
  timeline: ProposalTimeline;
}

export interface GovernanceProposalsStats {
  total: number;
  inProgress: number;
  executed: number;
  rejected: number;
  expired: number;
  active: number;
  historical: number;
  closed: number;
}

export interface GovernanceProposalsResponse {
  data: GovernanceProposal[];
  count: number;
  stats: GovernanceProposalsStats;
  source: string;
  indexedAt: string | null;
  totalIndexed: number;
  _meta?: {
    endpoint: string;
    description: string;
    fields: Record<string, unknown>;
  };
}

export interface GovernanceProposalsResult {
  proposals: GovernanceProposal[];
  stats: GovernanceProposalsStats;
  source: string;
  indexedAt: string | null;
  totalIndexed: number;
  isIndexEmpty: boolean;
}

/**
 * Hook to fetch governance proposals from the indexed API with semantic grouping.
 * This uses the persistent DuckDB index and groups proposals by semantic_key
 * to deduplicate re-submissions.
 * 
 * @param status - Filter by status: 'all' | 'active' | 'executed' | 'rejected' | 'expired' | 'historical'
 * @param limit - Max proposals to return (default 100)
 */
export function useGovernanceProposals(status: string = "all", limit: number = 100) {
  return useQuery({
    queryKey: ["governanceProposals", status, limit],
    queryFn: async (): Promise<GovernanceProposalsResult> => {
      const response = await apiFetch<GovernanceProposalsResponse>(
        `/api/events/governance-proposals?status=${status}&limit=${limit}`
      );
      
      // Check if index is empty
      const isIndexEmpty = response.source === "index-empty" || response.count === 0;
      
      return {
        proposals: response.data || [],
        stats: response.stats || {
          total: 0,
          inProgress: 0,
          executed: 0,
          rejected: 0,
          expired: 0,
          active: 0,
          historical: 0,
          closed: 0,
        },
        source: response.source || "unknown",
        indexedAt: response.indexedAt || null,
        totalIndexed: response.totalIndexed || 0,
        isIndexEmpty,
      };
    },
    staleTime: 30_000, // 30 seconds
    retry: 1,
  });
}

/**
 * Hook to fetch the full timeline for a specific proposal by semantic_key
 */
export function useProposalTimeline(semanticKey: string | null) {
  return useQuery({
    queryKey: ["proposalTimeline", semanticKey],
    queryFn: async () => {
      if (!semanticKey) return null;
      
      const response = await apiFetch<{
        semanticKey: string;
        latestStatus: string;
        latestContractId: string;
        timeline: GovernanceProposal[];
        summary: {
          totalVersions: number;
          firstCreated: string;
          lastUpdated: string;
          finalVoteCount: number;
          actionType: string;
          subject: string | null;
        };
        source: string;
      }>(`/api/events/governance-proposals/${encodeURIComponent(semanticKey)}/timeline`);
      
      return response;
    },
    enabled: !!semanticKey,
    staleTime: 60_000,
  });
}

/**
 * Parse votes from the proposal to count for/against
 */
export function parseProposalVotes(votes: GovernanceProposal["votes"]) {
  let votesFor = 0;
  let votesAgainst = 0;
  const votedSvs: Array<{
    party: string;
    vote: "accept" | "reject";
    reason: string;
    reasonUrl: string;
  }> = [];

  if (!Array.isArray(votes)) return { votesFor, votesAgainst, votedSvs };

  for (const vote of votes) {
    const [svName, voteData] = Array.isArray(vote) ? vote : ["Unknown", vote];
    const isAccept = voteData?.accept === true;

    if (isAccept) votesFor++;
    else votesAgainst++;

    votedSvs.push({
      party: svName,
      vote: isAccept ? "accept" : "reject",
      reason: voteData?.reason?.body || "",
      reasonUrl: voteData?.reason?.url || "",
    });
  }

  return { votesFor, votesAgainst, votedSvs };
}

/**
 * Format action_tag to human-readable title
 */
export function formatActionTitle(actionTag: string | null): string {
  if (!actionTag) return "Unknown Action";
  return actionTag
    .replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, "")
    .replace(/([A-Z])/g, " $1")
    .trim();
}

/**
 * Map indexed status to UI-friendly status
 */
export function mapProposalStatus(status: GovernanceProposal["status"]): "approved" | "rejected" | "pending" | "expired" {
  switch (status) {
    case "executed":
      return "approved";
    case "rejected":
      return "rejected";
    case "expired":
      return "expired";
    case "in_progress":
    case "active":
      return "pending";
    default:
      return "pending";
  }
}
