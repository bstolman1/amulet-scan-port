import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";

/**
 * Hook to fetch active vote requests directly from the Scan API.
 * This is the PRIMARY source for Active Governance data.
 * The /v0/admin/sv/voterequests endpoint returns all open VoteRequest contracts.
 */
export function useActiveVoteRequests() {
  return useQuery({
    queryKey: ["active-vote-requests"],
    queryFn: async () => {
      const response = await scanApi.fetchActiveVoteRequests();
      // Response shape: { dso_rules_vote_requests: VoteRequest[] }
      const voteRequests = response.dso_rules_vote_requests || [];
      
      console.log(`[useActiveVoteRequests] Fetched ${voteRequests.length} active vote requests from Scan API`);
      
      return {
        data: voteRequests,
        source: "scan-api",
      };
    },
    staleTime: 30 * 1000, // 30 seconds
    retry: 2,
    refetchInterval: 60 * 1000, // Refresh every minute
  });
}
