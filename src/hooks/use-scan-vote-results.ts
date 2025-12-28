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

// Parse date from various formats (ISO string, object with microsecondsSinceEpoch, etc.)
function parseDate(value: any): string {
  if (!value) return "";
  
  // Already a string
  if (typeof value === "string") return value;
  
  // Object with microsecondsSinceEpoch (DAML format)
  if (typeof value === "object" && value.microsecondsSinceEpoch) {
    const ms = Number(value.microsecondsSinceEpoch) / 1000;
    return new Date(ms).toISOString();
  }
  
  // Object with unixtime
  if (typeof value === "object" && value.unixtime) {
    return new Date(Number(value.unixtime) * 1000).toISOString();
  }
  
  // Number (assume milliseconds)
  if (typeof value === "number") {
    return new Date(value).toISOString();
  }
  
  return "";
}

// Parse vote results into display format
function parseVoteResults(results: VoteResult[]): ParsedVoteResult[] {
  if (!results || !Array.isArray(results)) return [];
  
  return results.map((result) => {
    // Safely access nested properties with type coercion for safety
    const request = (result?.request || {}) as any;
    const action = request?.action || { tag: "Unknown", value: null };
    const votes = request?.votes || [];
    const trackingCid = result?.request_tracking_cid || (result as any)?.tracking_cid || "";
    
    let votesFor = 0;
    let votesAgainst = 0;
    const parsedVotes: ParsedVoteResult["votes"] = [];
    
    for (const vote of votes) {
      // Handle both array format [svName, voteData] and object format
      const [svName, voteData] = Array.isArray(vote) ? vote : [vote?.sv || "Unknown", vote];
      if (voteData?.accept) {
        votesFor++;
      } else {
        votesAgainst++;
      }
      parsedVotes.push({
        svName: svName || "Unknown",
        svParty: voteData?.sv || "",
        accept: voteData?.accept ?? false,
        reasonUrl: voteData?.reason?.url || "",
        reasonBody: voteData?.reason?.body || "",
      });
    }
    
    let outcome: ParsedVoteResult["outcome"] = "expired";
    const outcomeTag = result?.outcome?.tag || "";
    if (outcomeTag === "VRO_Accepted") outcome = "accepted";
    else if (outcomeTag === "VRO_Rejected") outcome = "rejected";
    
    return {
      id: trackingCid ? trackingCid.slice(0, 12) : "unknown",
      trackingCid: trackingCid,
      actionType: action?.tag || "Unknown",
      actionTitle: parseActionTitle(action?.tag || "Unknown"),
      actionDetails: action?.value,
      requester: request?.requester || "",
      reasonBody: request?.reason?.body || "",
      reasonUrl: request?.reason?.url || "",
      voteBefore: parseDate(request?.vote_before),
      completedAt: parseDate(result?.completed_at),
      expiresAt: parseDate(request?.expires_at),
      outcome,
      votesFor,
      votesAgainst,
      totalVotes: votesFor + votesAgainst,
      votes: parsedVotes,
      abstainers: result?.abstaining_voters || [],
      offboarded: result?.offboarded_voters || [],
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
