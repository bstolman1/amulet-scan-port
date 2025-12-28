import { useQuery } from "@tanstack/react-query";

const SCAN_API_BASE = "https://scan.sv-1.global.canton.network.sync.global/api/scan";

export interface VoteResultRequest {
  actionName?: string;
  accepted?: boolean;
  requester?: string;
  effectiveFrom?: string;
  effectiveTo?: string;
  limit?: number;
}

export interface VoteResult {
  request_tracking_cid: string;
  request: {
    requester: string;
    action: {
      tag: string;
      value: any;
    };
    reason: {
      url: string;
      body: string;
    };
    vote_before: string;
    votes: Array<[string, {
      sv: string;
      accept: boolean;
      reason: {
        url: string;
        body: string;
      };
    }]>;
    expires_at: string;
  };
  completed_at: string;
  offboarded_voters: string[];
  abstaining_voters: string[];
  outcome: {
    tag: "VRO_Accepted" | "VRO_Rejected" | "VRO_Expired";
    value?: any;
  };
}

export interface VoteResultsResponse {
  dso_rules_vote_results: VoteResult[];
}

// Parsed vote result for display
export interface ParsedVoteResult {
  id: string;
  trackingCid: string;
  actionType: string;
  actionTitle: string;
  actionDetails: any;
  requester: string;
  reasonBody: string;
  reasonUrl: string;
  voteBefore: string;
  completedAt: string;
  expiresAt: string;
  outcome: "accepted" | "rejected" | "expired";
  votesFor: number;
  votesAgainst: number;
  totalVotes: number;
  votes: Array<{
    svName: string;
    svParty: string;
    accept: boolean;
    reasonUrl: string;
    reasonBody: string;
  }>;
  abstainers: string[];
  offboarded: string[];
}

// Parse action tag into readable title
function parseActionTitle(tag: string): string {
  return tag
    .replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, "")
    .replace(/([A-Z])/g, " $1")
    .trim();
}

// Parse vote results into display format
function parseVoteResults(results: VoteResult[]): ParsedVoteResult[] {
  return results.map((result) => {
    const action = result.request.action;
    const votes = result.request.votes || [];
    
    let votesFor = 0;
    let votesAgainst = 0;
    const parsedVotes: ParsedVoteResult["votes"] = [];
    
    for (const [svName, voteData] of votes) {
      if (voteData.accept) {
        votesFor++;
      } else {
        votesAgainst++;
      }
      parsedVotes.push({
        svName,
        svParty: voteData.sv,
        accept: voteData.accept,
        reasonUrl: voteData.reason?.url || "",
        reasonBody: voteData.reason?.body || "",
      });
    }
    
    let outcome: ParsedVoteResult["outcome"] = "expired";
    if (result.outcome.tag === "VRO_Accepted") outcome = "accepted";
    else if (result.outcome.tag === "VRO_Rejected") outcome = "rejected";
    
    return {
      id: result.request_tracking_cid.slice(0, 12),
      trackingCid: result.request_tracking_cid,
      actionType: action.tag,
      actionTitle: parseActionTitle(action.tag),
      actionDetails: action.value,
      requester: result.request.requester,
      reasonBody: result.request.reason?.body || "",
      reasonUrl: result.request.reason?.url || "",
      voteBefore: result.request.vote_before,
      completedAt: result.completed_at,
      expiresAt: result.request.expires_at,
      outcome,
      votesFor,
      votesAgainst,
      totalVotes: votesFor + votesAgainst,
      votes: parsedVotes,
      abstainers: result.abstaining_voters || [],
      offboarded: result.offboarded_voters || [],
    };
  });
}

export function useScanVoteResults(request: VoteResultRequest = {}) {
  return useQuery({
    queryKey: ["scanVoteResults", request],
    queryFn: async (): Promise<ParsedVoteResult[]> => {
      const res = await fetch(`${SCAN_API_BASE}/v0/admin/sv/voteresults`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...request,
          limit: request.limit ?? 500,
        }),
      });
      
      if (!res.ok) {
        throw new Error(`Failed to fetch vote results: ${res.status}`);
      }
      
      const data: VoteResultsResponse = await res.json();
      return parseVoteResults(data.dso_rules_vote_results || []);
    },
    staleTime: 60_000, // 1 minute
    retry: 2,
  });
}

// Hook to fetch all governance history (no filters)
export function useGovernanceVoteHistory(limit = 500) {
  return useScanVoteResults({ limit });
}
