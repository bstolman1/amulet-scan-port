import { useQuery } from "@tanstack/react-query";
import { getDuckDBApiUrl } from "@/lib/backend-config";

/**
 * Live Vote Requests Hook
 * 
 * Fetches active vote requests from the SV Node API via backend proxy.
 * This avoids CORS issues by routing through our server.
 * API: GET /api/sv-proxy/voterequests -> proxies to SV Node /v0/admin/sv/voterequests
 */

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
      const backendUrl = getDuckDBApiUrl();
      const response = await fetch(`${backendUrl}/api/sv-proxy/voterequests`, {
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
