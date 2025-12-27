import { useQuery } from "@tanstack/react-query";

const SV_NODE_BASE_URL = "https://scan.sv-1.global.canton.network.sync.global";
const SV_API_BASE = `${SV_NODE_BASE_URL}/api/sv/v0`;

async function fetchJson<T>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(url, options);

  const contentType = res.headers.get("content-type") || "";
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`SV API ${res.status}: ${text.slice(0, 200) || res.statusText}`);
  }

  // Guard against HTML error pages returned with 200
  if (!contentType.includes("application/json")) {
    const text = await res.text().catch(() => "");
    throw new Error(`SV API returned non-JSON (${contentType || "unknown"}): ${text.slice(0, 120)}`);
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

/**
 * Fetch ALL proposals from SV node (active + historical)
 * NOTE: /admin/sv/* endpoints may require authentication depending on deployment.
 */
export function useSVNodeAllProposals() {
  return useQuery<AllProposalsResponse>({
    queryKey: ["sv-node-all-proposals"],
    queryFn: async () => {
      const activeUrl = `${SV_API_BASE}/admin/sv/voterequests`;
      const active = await fetchJson<{ dso_rules_vote_requests?: SVNodeVoteRequest[] }>(activeUrl);
      const activeRequests = active.dso_rules_vote_requests || [];

      const resultsUrl = `${SV_API_BASE}/admin/sv/voteresults`;
      const [accepted, rejected] = await Promise.all([
        fetchJson<{ dso_rules_vote_results?: SVNodeVoteResult[] }>(resultsUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ accepted: true, limit: 1000 }),
        }),
        fetchJson<{ dso_rules_vote_results?: SVNodeVoteResult[] }>(resultsUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ accepted: false, limit: 1000 }),
        }),
      ]);

      const acceptedResults = accepted.dso_rules_vote_results || [];
      const rejectedResults = rejected.dso_rules_vote_results || [];

      const activeProposals: SVNodeProposal[] = activeRequests.map((vr) => ({
        contract_id: vr.contract_id,
        template_id: vr.template_id,
        status: "in_progress",
        payload: vr.payload,
        created_at: vr.created_at,
        source_type: "active_request",
      }));

      const historicalProposals: SVNodeProposal[] = [...acceptedResults, ...rejectedResults].map((vr: any) => ({
        contract_id:
          vr?.request?.tracking_cid ||
          vr?.request?.trackingCid ||
          vr?.request?.contract_id ||
          vr?.contract_id ||
          "unknown",
        template_id: vr?.request?.template_id || vr?.template_id || "unknown",
        status: vr?.outcome?.accepted ? "executed" : "rejected",
        payload: vr?.request || vr,
        outcome: vr?.outcome,
        effective_at: vr?.outcome?.effective_at || vr?.outcome?.effectiveAt,
        source_type: "vote_result",
      }));

      const proposalMap = new Map<string, SVNodeProposal>();
      for (const p of activeProposals) proposalMap.set(p.contract_id, p);
      for (const p of historicalProposals) proposalMap.set(p.contract_id, p);

      const proposals = Array.from(proposalMap.values());

      const stats = {
        total: proposals.length,
        active: activeRequests.length,
        accepted: acceptedResults.length,
        rejected: rejectedResults.length,
        in_progress: proposals.filter((p) => p.status === "in_progress").length,
        executed: proposals.filter((p) => p.status === "executed").length,
      };

      return {
        proposals,
        stats,
        source: "sv-node-live",
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
      const url = `${SV_API_BASE}/admin/sv/voterequests`;
      const data = await fetchJson<{ dso_rules_vote_requests?: SVNodeVoteRequest[] }>(url);
      const vote_requests = data.dso_rules_vote_requests || [];
      return {
        vote_requests,
        count: vote_requests.length,
        source: "sv-node-live",
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
      const url = `${SV_API_BASE}/admin/sv/voteresults`;
      const data = await fetchJson<{ dso_rules_vote_results?: SVNodeVoteResult[] }>(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(filters || {}),
      });
      const vote_results = data.dso_rules_vote_results || [];
      return {
        vote_results,
        count: vote_results.length,
        source: "sv-node-live",
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
