import { useQuery } from "@tanstack/react-query";

const SCAN_API_BASE = "https://scan.sv-1.global.canton.network.sync.global/api/scan/v0";

async function fetchJson<T>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(url, options);

  const contentType = res.headers.get("content-type") || "";
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Scan API ${res.status}: ${text.slice(0, 200) || res.statusText}`);
  }

  if (!contentType.includes("application/json")) {
    const text = await res.text().catch(() => "");
    throw new Error(`Scan API returned non-JSON (${contentType || "unknown"}): ${text.slice(0, 120)}`);
  }

  return res.json();
}

export interface SVNodeVoteRequest {
  template_id: string;
  contract_id: string;
  payload: any;
  created_event_blob?: string;
  created_at: string;
}

export interface SVNodeVoteResult {
  [key: string]: any;
}

export interface SVNodeProposal {
  contract_id: string;
  template_id: string;
  status: "in_progress" | "executed" | "rejected";
  payload: unknown;
  outcome?: { accepted: boolean; effective_at?: string };
  created_at?: string;
  effective_at?: string;
  source_type: "active_request" | "vote_result";
}

export interface AllProposalsResponse {
  proposals: SVNodeProposal[];
  stats: {
    total: number;
    active: number;
    accepted: number;
    rejected: number;
    in_progress: number;
    executed: number;
  };
  source: string;
  fetched_at: string;
}

export interface VoteResultsResponse {
  vote_results: SVNodeVoteResult[];
  count: number;
  source: string;
  fetched_at: string;
}

export interface ActiveVoteRequestsResponse {
  vote_requests: SVNodeVoteRequest[];
  count: number;
  source: string;
  fetched_at: string;
}

interface DsoRulesPayload {
  voteRequests?: Array<[string, any]>;
  svs?: Record<string, any>;
  [key: string]: any;
}

interface DsoInfoResponse {
  dso_rules?: {
    contract: {
      contract_id: string;
      template_id: string;
      payload: DsoRulesPayload;
      created_at: string;
    };
  };
  voting_threshold?: number;
  [key: string]: any;
}

/**
 * Fetch ALL proposals from the public Scan API /v0/dso endpoint.
 * This uses the dso_rules.payload.voteRequests which contains active vote requests.
 */
export function useSVNodeAllProposals() {
  return useQuery<AllProposalsResponse>({
    queryKey: ["sv-node-all-proposals"],
    queryFn: async () => {
      const url = `${SCAN_API_BASE}/v0/dso`;
      const dsoInfo = await fetchJson<DsoInfoResponse>(url);

      const dsoRulesPayload = dsoInfo.dso_rules?.contract?.payload;
      const voteRequests = dsoRulesPayload?.voteRequests || [];

      // voteRequests is an array of tuples: [[trackingCid, voteRequestData], ...]
      const activeProposals: SVNodeProposal[] = voteRequests.map(([trackingCid, voteRequestData]: [string, any]) => ({
        contract_id: trackingCid,
        template_id: "Splice.DsoRules:VoteRequest",
        status: "in_progress" as const,
        payload: voteRequestData,
        created_at: dsoInfo.dso_rules?.contract?.created_at,
        source_type: "active_request" as const,
      }));

      const stats = {
        total: activeProposals.length,
        active: activeProposals.length,
        accepted: 0, // Historical data not available from this endpoint
        rejected: 0,
        in_progress: activeProposals.length,
        executed: 0,
      };

      return {
        proposals: activeProposals,
        stats,
        source: "scan-api-dso-rules",
        fetched_at: new Date().toISOString(),
      };
    },
    staleTime: 30 * 1000,
    retry: 1,
  });
}

export function useSVNodeActiveRequests() {
  return useQuery<ActiveVoteRequestsResponse>({
    queryKey: ["sv-node-active-requests"],
    queryFn: async () => {
      const url = `${SCAN_API_BASE}/v0/dso`;
      const dsoInfo = await fetchJson<DsoInfoResponse>(url);
      const voteRequests = dsoInfo.dso_rules?.contract?.payload?.voteRequests || [];

      const vote_requests: SVNodeVoteRequest[] = voteRequests.map(([trackingCid, voteRequestData]: [string, any]) => ({
        template_id: "Splice.DsoRules:VoteRequest",
        contract_id: trackingCid,
        payload: voteRequestData,
        created_at: dsoInfo.dso_rules?.contract?.created_at || "",
      }));

      return {
        vote_requests,
        count: vote_requests.length,
        source: "scan-api-dso-rules",
        fetched_at: new Date().toISOString(),
      };
    },
    staleTime: 30 * 1000,
    retry: 1,
  });
}

export function useSVNodeVoteResults(filters?: {
  actionName?: string;
  accepted?: boolean;
  requester?: string;
  effectiveFrom?: string;
  effectiveTo?: string;
  limit?: number;
}) {
  return useQuery<VoteResultsResponse>({
    queryKey: ["sv-node-vote-results", filters],
    queryFn: async () => {
      // Historical vote results are not available from the public scan API
      // Return empty results - historical data would require the admin API
      return {
        vote_results: [],
        count: 0,
        source: "scan-api-dso-rules",
        fetched_at: new Date().toISOString(),
      };
    },
    staleTime: 30 * 1000,
    retry: 1,
  });
}

export function parseAction(action: unknown): { title: string; actionType: string; actionDetails: unknown } {
  if (!action || typeof action !== "object") {
    return { title: "Unknown Action", actionType: "Unknown", actionDetails: null };
  }

  const actionObj = action as { tag?: string; value?: unknown };
  const outerTag = actionObj.tag || Object.keys(actionObj)[0] || "Unknown";
  const outerValue = actionObj.value || (actionObj as Record<string, unknown>)[outerTag] || actionObj;

  const innerAction = (outerValue as Record<string, unknown>)?.dsoAction ||
    (outerValue as Record<string, unknown>)?.amuletRulesAction ||
    outerValue;
  const innerTag = (innerAction as { tag?: string })?.tag || "";
  const innerValue = (innerAction as { value?: unknown })?.value || innerAction;

  const actionType = innerTag || outerTag;
  const title = actionType
    .replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, "")
    .replace(/([A-Z])/g, " $1")
    .trim();

  return { title, actionType, actionDetails: innerValue };
}

export function parseVotes(votes: unknown): {
  votesFor: number;
  votesAgainst: number;
  votedSvs: Array<{ party: string; vote: string; reason: string }>;
} {
  if (!Array.isArray(votes)) {
    return { votesFor: 0, votesAgainst: 0, votedSvs: [] };
  }

  let votesFor = 0;
  let votesAgainst = 0;
  const votedSvs: Array<{ party: string; vote: string; reason: string }> = [];

  for (const vote of votes) {
    const [svName, voteData] = Array.isArray(vote) ? vote : [String(vote), {}];
    const isAccept = (voteData as { accept?: boolean })?.accept === true;
    const isReject = (voteData as { accept?: boolean })?.accept === false;

    if (isAccept) votesFor++;
    else if (isReject) votesAgainst++;

    votedSvs.push({
      party: svName,
      vote: isAccept ? "accept" : isReject ? "reject" : "abstain",
      reason: (voteData as { reason?: { body?: string } })?.reason?.body || "",
    });
  }

  return { votesFor, votesAgainst, votedSvs };
}
