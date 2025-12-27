import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

export interface CanonicalProposal {
  proposal_id: string | null;
  event_id: string;
  stable_id: string | null;
  contract_id: string;
  template_id: string | null;
  effective_at: string | null;
  status: "in_progress" | "accepted" | "rejected" | "expired" | string;
  is_closed: boolean;
  action_tag: string | null;
  action_value: Record<string, unknown> | null;
  requester: string | null;
  reason: string | Record<string, unknown> | null;
  reason_url: string | null;
  votes: Array<[string, { sv: string; accept: boolean; reason?: { body?: string; url?: string } }]>;
  vote_count: number;
  accept_count: number;
  reject_count: number;
  vote_before: string | null;
  target_effective_at: string | null;
  tracking_cid: string | null;
  dso: string | null;
  semantic_key: string | null;
  action_subject: string | null;
  is_human: boolean;
  related_count: number;
  first_seen: string | null;
  last_seen: string | null;
}

export interface CanonicalProposalStats {
  rawEvents: number;
  lifecycleProposals: number;
  humanProposals: number;
  byStatus: {
    in_progress: number;
    accepted: number;
    rejected: number;
    expired: number;
  };
}

interface CanonicalProposalsResponse {
  proposals: CanonicalProposal[];
  total: number;
  stats: CanonicalProposalStats;
  source: string;
  indexedAt: string | null;
}

interface DedupeStatsResponse {
  totalRows: number;
  uniqueProposals: number;
  duplicateRows: number;
  duplicatePct: number;
  humanProposals: number;
  statusBreakdown: Array<{ status: string; count: number }>;
  explanation: {
    model: string;
    deduplication: string;
    duplicates: string;
    humanFilter: string;
  };
}

/**
 * Hook to fetch canonical governance proposals (deduplicated by proposal_id)
 * This is the PRIMARY hook for governance UIs matching explorer semantics.
 * 
 * Key concept: 1 governance proposal = 1 unique VoteRequest ID (proposal_id)
 * Multiple rows in vote_requests represent state updates, NOT separate proposals.
 */
export function useCanonicalProposals(options?: {
  limit?: number;
  offset?: number;
  status?: "all" | "active" | "accepted" | "rejected" | "expired" | "historical";
  humanOnly?: boolean;
}) {
  const { limit = 500, offset = 0, status = "all", humanOnly = true } = options || {};

  return useQuery({
    queryKey: ["canonicalProposals", limit, offset, status, humanOnly],
    queryFn: async (): Promise<CanonicalProposalsResponse> => {
      const params = new URLSearchParams({
        limit: String(limit),
        offset: String(offset),
        status,
        human: String(humanOnly),
      });
      
      const response = await apiFetch<CanonicalProposalsResponse>(
        `/api/events/canonical-proposals?${params}`
      );
      
      return response;
    },
    staleTime: 30_000,
  });
}

/**
 * Hook to fetch canonical proposal statistics
 */
export function useCanonicalProposalStats() {
  return useQuery({
    queryKey: ["canonicalProposalStats"],
    queryFn: async (): Promise<CanonicalProposalStats & { source: string; indexedAt: string | null }> => {
      return apiFetch("/api/events/canonical-proposals/stats");
    },
    staleTime: 30_000,
  });
}

/**
 * Hook to fetch deduplication diagnostic stats
 * Answers: "How many unique proposals vs duplicate state updates?"
 */
export function useDedupeStats() {
  return useQuery({
    queryKey: ["dedupeStats"],
    queryFn: async (): Promise<DedupeStatsResponse> => {
      return apiFetch("/api/events/vote-requests/dedupe-stats");
    },
    staleTime: 60_000,
  });
}

/**
 * Helper to parse action from canonical proposal
 */
export function parseCanonicalAction(proposal: CanonicalProposal): {
  title: string;
  actionType: string;
  actionDetails: Record<string, unknown> | null;
} {
  const actionTag = proposal.action_tag || "Unknown";
  const actionValue = proposal.action_value;
  
  // Build human-readable title from action tag
  const title = actionTag
    .replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, "")
    .replace(/([A-Z])/g, " $1")
    .trim();
  
  return { title, actionType: actionTag, actionDetails: actionValue };
}

/**
 * Helper to parse votes from canonical proposal
 */
export function parseCanonicalVotes(proposal: CanonicalProposal): {
  votesFor: number;
  votesAgainst: number;
  votedSvs: Array<{
    party: string;
    sv: string;
    vote: "accept" | "reject" | "abstain";
    reason: string;
    reasonUrl: string;
    castAt: string | null;
  }>;
} {
  const votes = proposal.votes || [];
  const votedSvs: Array<{
    party: string;
    sv: string;
    vote: "accept" | "reject" | "abstain";
    reason: string;
    reasonUrl: string;
    castAt: string | null;
  }> = [];

  let votesFor = 0;
  let votesAgainst = 0;

  const toPartyString = (key: unknown): string => {
    if (typeof key === "string") return key;
    if (key && typeof key === "object") {
      const anyKey = key as any;
      return String(anyKey.party || anyKey.text || anyKey.sv || anyKey.voter || "Unknown");
    }
    return "Unknown";
  };

  for (const vote of votes) {
    const tuple = Array.isArray(vote) ? vote : [vote as any, null];
    const svKey = tuple[0];
    const voteData = tuple[1] as any;

    const party = toPartyString(svKey);
    const isAccept = voteData?.accept === true;
    const isReject = voteData?.accept === false;

    if (isAccept) votesFor++;
    else if (isReject) votesAgainst++;

    votedSvs.push({
      party,
      sv: String(voteData?.sv || party),
      vote: isAccept ? "accept" : isReject ? "reject" : "abstain",
      reason: String(voteData?.reason?.body || ""),
      reasonUrl: String(voteData?.reason?.url || ""),
      castAt: null, // Not available in canonical model
    });
  }

  return { votesFor, votesAgainst, votedSvs };
}

/**
 * Helper to get status display properties
 */
export function getProposalStatusDisplay(status: string): {
  label: string;
  color: string;
  bgColor: string;
} {
  switch (status) {
    case "accepted":
      return { label: "Accepted", color: "text-success", bgColor: "bg-success/10" };
    case "rejected":
      return { label: "Rejected", color: "text-destructive", bgColor: "bg-destructive/10" };
    case "expired":
      return { label: "Expired", color: "text-muted-foreground", bgColor: "bg-muted" };
    case "in_progress":
      return { label: "In Progress", color: "text-warning", bgColor: "bg-warning/10" };
    default:
      return { label: status || "Unknown", color: "text-muted-foreground", bgColor: "bg-muted" };
  }
}
