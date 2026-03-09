import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";

export interface GovernanceEventsResult {
  events: GovernanceEvent[];
  source: string | null;
  fromIndex: boolean;
  indexedAt: string | null;
  totalIndexed: number | null;
}

export interface GovernanceEvent {
  event_id: string;
  event_type: "created" | "archived";
  contract_id: string;
  template_id: string;
  effective_at?: string;
  timestamp?: string;
  payload: Record<string, unknown>;
}

function mapVoteRequest(vr: any, eventType: "created" | "archived"): GovernanceEvent {
  const contract = vr.contract || vr;
  const payload = contract.payload || vr.payload || vr;
  return {
    event_id: contract.contract_id || vr.contract_id || "",
    event_type: eventType,
    contract_id: contract.contract_id || vr.contract_id || "",
    template_id: contract.template_id || vr.template_id || "Splice.DsoRules:VoteRequest",
    effective_at: contract.created_at || vr.created_at || vr.effective_at || "",
    timestamp: contract.created_at || vr.created_at || vr.effective_at || "",
    payload,
  };
}

export function useGovernanceEvents() {
  return useQuery({
    queryKey: ["governanceEvents-scan"],
    queryFn: async (): Promise<GovernanceEventsResult> => {
      const [activeRes, historicalRes] = await Promise.allSettled([
        scanApi.fetchActiveVoteRequests(),
        scanApi.fetchVoteResults({ limit: 1000 }),
      ]);

      const events: GovernanceEvent[] = [];

      if (activeRes.status === "fulfilled") {
        const requests = activeRes.value.dso_rules_vote_requests || [];
        for (const vr of requests) {
          events.push(mapVoteRequest(vr, "created"));
        }
      }

      if (historicalRes.status === "fulfilled") {
        const results = historicalRes.value.dso_rules_vote_results || [];
        for (const vr of results) {
          const request = vr.request || vr;
          events.push(mapVoteRequest(request, "archived"));
        }
      }

      const seen = new Set<string>();
      const deduped: GovernanceEvent[] = [];
      for (const ev of events) {
        if (!ev.contract_id || seen.has(ev.contract_id)) continue;
        seen.add(ev.contract_id);
        deduped.push(ev);
      }

      console.log(`[useGovernanceEvents] ${deduped.length} events from Scan API`);

      return {
        events: deduped,
        source: "scan-api",
        fromIndex: false,
        indexedAt: null,
        totalIndexed: deduped.length,
      };
    },
    staleTime: 30_000,
    retry: 2,
    refetchInterval: 60_000,
  });
}
