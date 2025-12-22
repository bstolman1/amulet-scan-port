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

interface VoteRequestRow {
  event_id: string;
  contract_id: string;
  template_id?: string | null;
  effective_at?: string | null;
  status?: "active" | "historical" | string;
  is_closed?: boolean;
  action_tag?: string | null;
  action_value?: unknown;
  requester?: string | null;
  reason?: unknown;
  votes?: unknown;
  vote_before?: string | null;
  target_effective_at?: string | null;
  tracking_cid?: string | null;
  dso?: string | null;
}

interface EventsResponse<T> {
  data: T[];
  count: number;
  hasMore?: boolean;
  source?: string;
}

export function useGovernanceEvents() {
  return useQuery({
    queryKey: ["governanceEvents"],
    queryFn: async (): Promise<GovernanceEvent[]> => {
      // Governance History needs VoteRequest data shaped like the rest of the UI expects (payload.action, payload.votes, etc.).
      // ensureFresh=true triggers a background index rebuild if the persistent index is stale/partial.
      const response = await apiFetch<EventsResponse<VoteRequestRow>>(
        "/api/events/vote-requests?status=historical&limit=1000&ensureFresh=true",
      );

      // Dedupe by contract_id (each VoteRequest contract is unique) rather than event_id
      // This handles duplicates from multiple migration_ids or ingestion passes
      const seenContracts = new Set<string>();
      const mapped = (response.data || [])
        .filter((r) => {
          if (!r?.contract_id) return false;
          if (seenContracts.has(r.contract_id)) return false;
          seenContracts.add(r.contract_id);
          return true;
        })
        .map((r) => {
          const payload = {
            action: r.action_tag ? { tag: r.action_tag, value: r.action_value } : undefined,
            requester: r.requester ?? undefined,
            reason: r.reason ?? undefined,
            votes: r.votes ?? undefined,
            voteBefore: r.vote_before ?? undefined,
            targetEffectiveAt: r.target_effective_at ?? undefined,
            trackingCid: r.tracking_cid ?? undefined,
            dso: r.dso ?? undefined,
          } satisfies Record<string, unknown>;

          return {
            event_id: r.event_id,
            event_type: r.is_closed ? "archived" : "created",
            contract_id: r.contract_id,
            template_id: r.template_id ?? "Splice.DsoRules:VoteRequest",
            effective_at: r.effective_at ?? undefined,
            timestamp: r.effective_at ?? undefined,
            payload,
          } satisfies GovernanceEvent;
        });

      return mapped;
    },
    staleTime: 30_000,
  });
}

export function useRewardClaimEvents() {
  return useQuery({
    queryKey: ["rewardClaimEvents"],
    queryFn: async (): Promise<GovernanceEvent[]> => {
      // Fetch reward claim events from DuckDB
      const response = await apiFetch<EventsResponse<GovernanceEvent>>("/api/events/rewards");
      return response.data || [];
    },
    staleTime: 30_000,
  });
}
