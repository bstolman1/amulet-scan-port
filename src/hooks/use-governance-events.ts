import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

interface GovernanceEvent {
  id?: string;
  event_id?: string;
  event_type: string;
  contract_id?: string;
  template_id?: string;
  package_name?: string;
  round?: number;
  timestamp?: string;
  effective_at?: string;
  created_at?: string;
  payload?: Record<string, unknown>;
  event_data?: Record<string, unknown>;
  exercise_result?: Record<string, unknown>;
  raw?: Record<string, unknown>;
  choice?: string;
  signatories?: string[];
  observers?: string[];
}

interface EventsResponse {
  data: GovernanceEvent[];
  count: number;
  hasMore?: boolean;
  source?: string;
}

export function useGovernanceEvents() {
  return useQuery({
    queryKey: ["governanceEvents"],
    queryFn: async (): Promise<GovernanceEvent[]> => {
      // Fetch VoteRequest events from DuckDB indexed table (instant queries)
      // Falls back to file scanning if index is not populated
      const response = await apiFetch<EventsResponse>("/api/events/vote-requests?status=all&limit=500");
      return response.data || [];
    },
    staleTime: 30_000,
  });
}

export function useRewardClaimEvents() {
  return useQuery({
    queryKey: ["rewardClaimEvents"],
    queryFn: async (): Promise<GovernanceEvent[]> => {
      // Fetch reward claim events from DuckDB
      const response = await apiFetch<EventsResponse>("/api/events/rewards");
      return response.data || [];
    },
    staleTime: 30_000,
  });
}
