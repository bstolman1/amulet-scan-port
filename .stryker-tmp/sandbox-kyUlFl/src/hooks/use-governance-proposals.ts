// @ts-nocheck
function stryNS_9fa48() {
  var g = typeof globalThis === 'object' && globalThis && globalThis.Math === Math && globalThis || new Function("return this")();
  var ns = g.__stryker__ || (g.__stryker__ = {});
  if (ns.activeMutant === undefined && g.process && g.process.env && g.process.env.__STRYKER_ACTIVE_MUTANT__) {
    ns.activeMutant = g.process.env.__STRYKER_ACTIVE_MUTANT__;
  }
  function retrieveNS() {
    return ns;
  }
  stryNS_9fa48 = retrieveNS;
  return retrieveNS();
}
stryNS_9fa48();
function stryCov_9fa48() {
  var ns = stryNS_9fa48();
  var cov = ns.mutantCoverage || (ns.mutantCoverage = {
    static: {},
    perTest: {}
  });
  function cover() {
    var c = cov.static;
    if (ns.currentTestId) {
      c = cov.perTest[ns.currentTestId] = cov.perTest[ns.currentTestId] || {};
    }
    var a = arguments;
    for (var i = 0; i < a.length; i++) {
      c[a[i]] = (c[a[i]] || 0) + 1;
    }
  }
  stryCov_9fa48 = cover;
  cover.apply(null, arguments);
}
function stryMutAct_9fa48(id) {
  var ns = stryNS_9fa48();
  function isActive(id) {
    if (ns.activeMutant === id) {
      if (ns.hitCount !== void 0 && ++ns.hitCount > ns.hitLimit) {
        throw new Error('Stryker: Hit count limit reached (' + ns.hitCount + ')');
      }
      return true;
    }
    return false;
  }
  stryMutAct_9fa48 = isActive;
  return isActive(id);
}
import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

// Canonical governance proposal matching explorer semantics
export interface CanonicalProposal {
  proposal_id: string;
  event_id: string;
  contract_id: string;
  template_id: string;
  effective_at: string;
  status: 'in_progress' | 'executed' | 'rejected' | 'expired';
  is_closed: boolean;
  action_tag: string | null;
  action_value: unknown;
  requester: string | null;
  reason: string | null;
  reason_url: string | null;
  votes: unknown[];
  vote_count: number;
  accept_count: number;
  reject_count: number;
  vote_before: string | null;
  tracking_cid: string | null;
  semantic_key: string | null;
  action_subject: string | null;
  is_human: boolean;
  related_count: number;
  first_seen: string;
  last_seen: string;
}
export interface CanonicalProposalStats {
  rawEvents: number;
  lifecycleProposals: number;
  humanProposals: number;
  byStatus: {
    in_progress: number;
    executed: number;
    rejected: number;
    expired: number;
  };
}
export interface CanonicalProposalsResponse {
  proposals: CanonicalProposal[];
  total: number;
  stats: CanonicalProposalStats;
  source: string;
  indexedAt: string | null;
}

// Legacy interface for backwards compatibility
export interface GovernanceProposal {
  semantic_key: string;
  action_type: string;
  action_subject: string | null;
  latest_status: 'in_progress' | 'executed' | 'rejected' | 'expired';
  accept_count: number;
  reject_count: number;
  first_seen: string;
  last_seen: string;
  related_count: number;
  latest_requester: string | null;
  latest_reason_body: string | null;
  latest_reason_url: string | null;
  latest_vote_before: string | null;
  latest_contract_id: string;
}
export interface GovernanceProposalsResponse {
  proposals: GovernanceProposal[];
  total: number;
  byStatus: {
    in_progress: number;
    executed: number;
    rejected: number;
    expired: number;
  };
  fromIndex: boolean;
  indexedAt: string | null;
}
export interface ProposalTimelineEntry {
  id: number;
  payload_id: string;
  contract_id: string;
  status: string;
  accept_count: number;
  reject_count: number;
  requester: string | null;
  reason_body: string | null;
  reason_url: string | null;
  vote_before: string | null;
  indexed_at: string;
}
export interface ProposalTimelineResponse {
  semantic_key: string;
  entries: ProposalTimelineEntry[];
  total: number;
}

/**
 * CANONICAL: Fetch governance proposals matching explorer semantics
 * - proposal_id = COALESCE(tracking_cid, contract_id)
 * - is_human = true for explorer-visible proposals
 * - Returns ~200-250 proposals matching major explorers
 */
export function useCanonicalProposals(options?: {
  status?: string;
  humanOnly?: boolean;
}) {
  if (stryMutAct_9fa48("2365")) {
    {}
  } else {
    stryCov_9fa48("2365");
    return useQuery<CanonicalProposalsResponse>({
      queryKey: stryMutAct_9fa48("2367") ? [] : (stryCov_9fa48("2367"), ["canonical-proposals", stryMutAct_9fa48("2369") ? options.status : (stryCov_9fa48("2369"), options?.status), stryMutAct_9fa48("2370") ? options.humanOnly : (stryCov_9fa48("2370"), options?.humanOnly)]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2371")) {
          {}
        } else {
          stryCov_9fa48("2371");
          const params = new URLSearchParams();
          if (stryMutAct_9fa48("2374") ? options.status : stryMutAct_9fa48("2373") ? false : stryMutAct_9fa48("2372") ? true : (stryCov_9fa48("2372", "2373", "2374"), options?.status)) params.set("status", options.status);
          if (stryMutAct_9fa48("2378") ? options?.humanOnly !== false : stryMutAct_9fa48("2377") ? false : stryMutAct_9fa48("2376") ? true : (stryCov_9fa48("2376", "2377", "2378"), (stryMutAct_9fa48("2379") ? options.humanOnly : (stryCov_9fa48("2379"), options?.humanOnly)) === (stryMutAct_9fa48("2380") ? true : (stryCov_9fa48("2380"), false)))) params.set("human", "false");
          const url = `/api/events/canonical-proposals${params.toString() ? `?${params}` : ""}`;
          return apiFetch<CanonicalProposalsResponse>(url);
        }
      },
      staleTime: stryMutAct_9fa48("2386") ? 30 / 1000 : (stryCov_9fa48("2386"), 30 * 1000),
      retry: 1
    });
  }
}

/**
 * Fetch canonical proposal statistics
 */
export function useCanonicalProposalStats() {
  if (stryMutAct_9fa48("2387")) {
    {}
  } else {
    stryCov_9fa48("2387");
    return useQuery<CanonicalProposalStats & {
      source: string;
      indexedAt: string | null;
    }>({
      queryKey: stryMutAct_9fa48("2389") ? [] : (stryCov_9fa48("2389"), ["canonical-proposals-stats"]),
      queryFn: stryMutAct_9fa48("2391") ? () => undefined : (stryCov_9fa48("2391"), () => apiFetch("/api/events/canonical-proposals/stats")),
      staleTime: stryMutAct_9fa48("2393") ? 30 / 1000 : (stryCov_9fa48("2393"), 30 * 1000),
      retry: 1
    });
  }
}

/**
 * @deprecated Use useCanonicalProposals for explorer-matching semantics
 */
export function useGovernanceProposals(status?: 'active' | 'historical') {
  if (stryMutAct_9fa48("2394")) {
    {}
  } else {
    stryCov_9fa48("2394");
    return useQuery<GovernanceProposalsResponse>({
      queryKey: stryMutAct_9fa48("2396") ? [] : (stryCov_9fa48("2396"), ["governance-proposals", status]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2398")) {
          {}
        } else {
          stryCov_9fa48("2398");
          const params = new URLSearchParams();
          if (stryMutAct_9fa48("2400") ? false : stryMutAct_9fa48("2399") ? true : (stryCov_9fa48("2399", "2400"), status)) params.set("status", status);
          const url = `/api/events/governance-proposals${params.toString() ? `?${params}` : ""}`;
          const response = await apiFetch<GovernanceProposalsResponse>(url);
          return response;
        }
      },
      staleTime: stryMutAct_9fa48("2405") ? 30 / 1000 : (stryCov_9fa48("2405"), 30 * 1000),
      retry: 1
    });
  }
}
export function useProposalTimeline(semanticKey: string | null) {
  if (stryMutAct_9fa48("2406")) {
    {}
  } else {
    stryCov_9fa48("2406");
    return useQuery<ProposalTimelineResponse>({
      queryKey: stryMutAct_9fa48("2408") ? [] : (stryCov_9fa48("2408"), ["proposal-timeline", semanticKey]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2410")) {
          {}
        } else {
          stryCov_9fa48("2410");
          if (stryMutAct_9fa48("2413") ? false : stryMutAct_9fa48("2412") ? true : stryMutAct_9fa48("2411") ? semanticKey : (stryCov_9fa48("2411", "2412", "2413"), !semanticKey)) throw new Error("No semantic key provided");
          const encoded = encodeURIComponent(semanticKey);
          const response = await apiFetch<ProposalTimelineResponse>(`/api/events/governance-proposals/${encoded}/timeline`);
          return response;
        }
      },
      enabled: stryMutAct_9fa48("2416") ? !semanticKey : (stryCov_9fa48("2416"), !(stryMutAct_9fa48("2417") ? semanticKey : (stryCov_9fa48("2417"), !semanticKey))),
      staleTime: stryMutAct_9fa48("2418") ? 60 / 1000 : (stryCov_9fa48("2418"), 60 * 1000),
      retry: 1
    });
  }
}