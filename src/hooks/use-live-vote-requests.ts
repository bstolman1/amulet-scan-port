import { useQuery } from "@tanstack/react-query";

/**
 * Live Vote Requests Hook
 * 
 * Fetches active vote requests from the SV Node API instead of local ACS snapshots.
 * API: GET /v0/admin/sv/voterequests
 */

// SV Node API Base URL
const SV_API_BASE = "https://sv-1.global.canton.network.sync.global/api/sv";

interface VoteRequestPayload {
  requester?: string;
  action?: {
    tag?: string;
    value?: any;
  };
  reason?: {
    url?: string;
    body?: string;
  } | string;
  votes?: Array<[string, { sv?: string; accept?: boolean; reason?: any; optCastAt?: string }]>;
  voteBefore?: string;
  targetEffectiveAt?: string;
  trackingCid?: string;
  dso?: string;
}

export interface LiveVoteRequest {
  template_id: string;
  contract_id: string;
  payload: VoteRequestPayload;
  created_event_blob?: string;
  created_at?: string;
}

interface LiveVoteRequestsResponse {
  dso_rules_vote_requests: LiveVoteRequest[];
}

export function useLiveVoteRequests() {
  return useQuery({
    queryKey: ["liveVoteRequests"],
    queryFn: async (): Promise<{ data: LiveVoteRequest[]; source: string }> => {
      const response = await fetch(`${SV_API_BASE}/v0/admin/sv/voterequests`, {
        method: "GET",
        headers: {
          "Accept": "application/json",
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to fetch live vote requests: ${response.status} ${response.statusText}`);
      }

      const result: LiveVoteRequestsResponse = await response.json();
      
      // Transform to match the expected data structure
      return {
        data: result.dso_rules_vote_requests || [],
        source: "sv-node-api",
      };
    },
    staleTime: 30_000, // 30 seconds
    refetchInterval: 60_000, // Refresh every minute
    retry: 2,
  });
}
