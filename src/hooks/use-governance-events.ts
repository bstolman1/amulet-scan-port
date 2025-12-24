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
  _summary?: {
    activeCount: number;
    historicalCount: number;
    closedCount: number;
    statusFilter: string;
  };
  _debug?: {
    indexedAt?: string;
    totalIndexed?: number;
    fromIndex?: boolean;
  };
}

export interface GovernanceEventsResult {
  events: GovernanceEvent[];
  source: string | null;
  fromIndex: boolean;
  indexedAt: string | null;
  totalIndexed: number | null;
}

export function useGovernanceEvents() {
  return useQuery({
    queryKey: ["governanceEvents"],
    queryFn: async (): Promise<GovernanceEventsResult> => {
      // Governance History needs VoteRequest data shaped like the rest of the UI expects (payload.action, payload.votes, etc.).
      // verbose=true to get debug info about data source
      // If index is populated, use it. Otherwise, fall back to binary scan (slower).
      // We do NOT set ensureFresh=true to avoid triggering repeated index rebuilds.
      // Use status=all to include both active and historical vote requests
      // The template index contains all VoteRequest events from binary files
      const response = await apiFetch<EventsResponse<VoteRequestRow>>(
        "/api/events/vote-requests?status=all&limit=5000&verbose=true",
      );

      const seen = new Set<string>();
      const mapped = (response.data || [])
        .filter((r) => {
          if (!r?.event_id) return false;
          if (seen.has(r.event_id)) return false;
          seen.add(r.event_id);
          return true;
        })
        .map((r) => {
          // Use full payload if available (new indexer stores complete JSON)
          // Fall back to reconstructed payload for backwards compatibility
          const fullPayload = (r as any).payload;
          
          const payload = fullPayload || {
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
            template_id: r.template_id ?? "Splice:DsoRules:VoteRequest",
            effective_at: r.effective_at ?? undefined,
            timestamp: r.effective_at ?? undefined,
            payload,
          } satisfies GovernanceEvent;
        });

      return {
        events: mapped,
        source: response.source || null,
        fromIndex: response._debug?.fromIndex || response.source === 'duckdb-index',
        indexedAt: response._debug?.indexedAt || null,
        totalIndexed: response._debug?.totalIndexed || null,
      };
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
