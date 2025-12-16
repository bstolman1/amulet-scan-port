import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

interface MemberTrafficEvent {
  id?: string;
  event_id?: string;
  event_type: string;
  contract_id?: string;
  template_id?: string;
  timestamp?: string;
  effective_at?: string;
  payload?: Record<string, unknown>;
}

interface EventsResponse {
  data: MemberTrafficEvent[];
  count: number;
  hasMore?: boolean;
  source?: string;
}

export function useMemberTrafficEvents() {
  return useQuery({
    queryKey: ["memberTrafficEvents"],
    queryFn: async (): Promise<MemberTrafficEvent[]> => {
      const response = await apiFetch<EventsResponse>("/api/events/member-traffic");
      return response.data || [];
    },
    staleTime: 30_000,
  });
}
