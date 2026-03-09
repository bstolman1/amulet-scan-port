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
  if (stryMutAct_9fa48("2027")) {
    {}
  } else {
    stryCov_9fa48("2027");
    return useQuery({
      queryKey: stryMutAct_9fa48("2029") ? [] : (stryCov_9fa48("2029"), ["governanceEvents"]),
      queryFn: async (): Promise<GovernanceEventsResult> => {
        if (stryMutAct_9fa48("2031")) {
          {}
        } else {
          stryCov_9fa48("2031");
          // Governance History needs VoteRequest data shaped like the rest of the UI expects (payload.action, payload.votes, etc.).
          // verbose=true to get debug info about data source
          // If index is populated, use it. Otherwise, fall back to binary scan (slower).
          // We do NOT set ensureFresh=true to avoid triggering repeated index rebuilds.
          // Use status=all to include both active and historical vote requests
          // The template index contains all VoteRequest events from binary files
          const response = await apiFetch<EventsResponse<VoteRequestRow>>("/api/events/vote-requests?status=all&limit=5000&verbose=true");
          const seen = new Set<string>();
          const mapped = stryMutAct_9fa48("2033") ? (response.data || []).map(r => {
            // Helper to safely parse JSON if it's a string
            const safeJsonParse = (val: unknown): unknown => {
              if (val === null || val === undefined) return null;
              if (typeof val !== 'string') return val;
              try {
                return JSON.parse(val);
              } catch {
                return val;
              }
            };

            // Use full payload if available (new indexer stores complete JSON)
            // Parse it if it's a string (DuckDB may return VARCHAR as string)
            // Fall back to reconstructed payload for backwards compatibility
            const rawPayload = (r as any).payload;
            const parsedPayload = safeJsonParse(rawPayload);

            // Also parse votes and reason if they are strings
            const parsedVotes = safeJsonParse(r.votes);
            const parsedReason = safeJsonParse(r.reason);
            const parsedActionValue = safeJsonParse(r.action_value);
            const payload = parsedPayload && typeof parsedPayload === 'object' ? parsedPayload as Record<string, unknown> : {
              action: r.action_tag ? {
                tag: r.action_tag,
                value: parsedActionValue
              } : undefined,
              requester: r.requester ?? undefined,
              reason: parsedReason ?? undefined,
              votes: parsedVotes ?? undefined,
              voteBefore: r.vote_before ?? undefined,
              targetEffectiveAt: r.target_effective_at ?? undefined,
              trackingCid: r.tracking_cid ?? undefined,
              dso: r.dso ?? undefined
            } satisfies Record<string, unknown>;
            return {
              event_id: r.event_id,
              event_type: r.is_closed ? "archived" : "created",
              contract_id: r.contract_id,
              template_id: r.template_id ?? "Splice:DsoRules:VoteRequest",
              effective_at: r.effective_at ?? undefined,
              timestamp: r.effective_at ?? undefined,
              payload
            } satisfies GovernanceEvent;
          }) : (stryCov_9fa48("2033"), (stryMutAct_9fa48("2036") ? response.data && [] : stryMutAct_9fa48("2035") ? false : stryMutAct_9fa48("2034") ? true : (stryCov_9fa48("2034", "2035", "2036"), response.data || (stryMutAct_9fa48("2037") ? ["Stryker was here"] : (stryCov_9fa48("2037"), [])))).filter(r => {
            if (stryMutAct_9fa48("2038")) {
              {}
            } else {
              stryCov_9fa48("2038");
              if (stryMutAct_9fa48("2041") ? false : stryMutAct_9fa48("2040") ? true : stryMutAct_9fa48("2039") ? r?.event_id : (stryCov_9fa48("2039", "2040", "2041"), !(stryMutAct_9fa48("2042") ? r.event_id : (stryCov_9fa48("2042"), r?.event_id)))) return stryMutAct_9fa48("2043") ? true : (stryCov_9fa48("2043"), false);
              if (stryMutAct_9fa48("2045") ? false : stryMutAct_9fa48("2044") ? true : (stryCov_9fa48("2044", "2045"), seen.has(r.event_id))) return stryMutAct_9fa48("2046") ? true : (stryCov_9fa48("2046"), false);
              seen.add(r.event_id);
              return stryMutAct_9fa48("2047") ? false : (stryCov_9fa48("2047"), true);
            }
          }).map(r => {
            if (stryMutAct_9fa48("2048")) {
              {}
            } else {
              stryCov_9fa48("2048");
              // Helper to safely parse JSON if it's a string
              const safeJsonParse = (val: unknown): unknown => {
                if (stryMutAct_9fa48("2049")) {
                  {}
                } else {
                  stryCov_9fa48("2049");
                  if (stryMutAct_9fa48("2052") ? val === null && val === undefined : stryMutAct_9fa48("2051") ? false : stryMutAct_9fa48("2050") ? true : (stryCov_9fa48("2050", "2051", "2052"), (stryMutAct_9fa48("2054") ? val !== null : stryMutAct_9fa48("2053") ? false : (stryCov_9fa48("2053", "2054"), val === null)) || (stryMutAct_9fa48("2056") ? val !== undefined : stryMutAct_9fa48("2055") ? false : (stryCov_9fa48("2055", "2056"), val === undefined)))) return null;
                  if (stryMutAct_9fa48("2059") ? typeof val === 'string' : stryMutAct_9fa48("2058") ? false : stryMutAct_9fa48("2057") ? true : (stryCov_9fa48("2057", "2058", "2059"), typeof val !== 'string')) return val;
                  try {
                    if (stryMutAct_9fa48("2061")) {
                      {}
                    } else {
                      stryCov_9fa48("2061");
                      return JSON.parse(val);
                    }
                  } catch {
                    if (stryMutAct_9fa48("2062")) {
                      {}
                    } else {
                      stryCov_9fa48("2062");
                      return val;
                    }
                  }
                }
              };

              // Use full payload if available (new indexer stores complete JSON)
              // Parse it if it's a string (DuckDB may return VARCHAR as string)
              // Fall back to reconstructed payload for backwards compatibility
              const rawPayload = (r as any).payload;
              const parsedPayload = safeJsonParse(rawPayload);

              // Also parse votes and reason if they are strings
              const parsedVotes = safeJsonParse(r.votes);
              const parsedReason = safeJsonParse(r.reason);
              const parsedActionValue = safeJsonParse(r.action_value);
              const payload = (stryMutAct_9fa48("2065") ? parsedPayload || typeof parsedPayload === 'object' : stryMutAct_9fa48("2064") ? false : stryMutAct_9fa48("2063") ? true : (stryCov_9fa48("2063", "2064", "2065"), parsedPayload && (stryMutAct_9fa48("2067") ? typeof parsedPayload !== 'object' : stryMutAct_9fa48("2066") ? true : (stryCov_9fa48("2066", "2067"), typeof parsedPayload === 'object')))) ? parsedPayload as Record<string, unknown> : {
                action: r.action_tag ? {
                  tag: r.action_tag,
                  value: parsedActionValue
                } : undefined,
                requester: stryMutAct_9fa48("2071") ? r.requester && undefined : (stryCov_9fa48("2071"), r.requester ?? undefined),
                reason: stryMutAct_9fa48("2072") ? parsedReason && undefined : (stryCov_9fa48("2072"), parsedReason ?? undefined),
                votes: stryMutAct_9fa48("2073") ? parsedVotes && undefined : (stryCov_9fa48("2073"), parsedVotes ?? undefined),
                voteBefore: stryMutAct_9fa48("2074") ? r.vote_before && undefined : (stryCov_9fa48("2074"), r.vote_before ?? undefined),
                targetEffectiveAt: stryMutAct_9fa48("2075") ? r.target_effective_at && undefined : (stryCov_9fa48("2075"), r.target_effective_at ?? undefined),
                trackingCid: stryMutAct_9fa48("2076") ? r.tracking_cid && undefined : (stryCov_9fa48("2076"), r.tracking_cid ?? undefined),
                dso: stryMutAct_9fa48("2077") ? r.dso && undefined : (stryCov_9fa48("2077"), r.dso ?? undefined)
              } satisfies Record<string, unknown>;
              return {
                event_id: r.event_id,
                event_type: r.is_closed ? "archived" : "created",
                contract_id: r.contract_id,
                template_id: stryMutAct_9fa48("2081") ? r.template_id && "Splice:DsoRules:VoteRequest" : (stryCov_9fa48("2081"), r.template_id ?? "Splice:DsoRules:VoteRequest"),
                effective_at: stryMutAct_9fa48("2083") ? r.effective_at && undefined : (stryCov_9fa48("2083"), r.effective_at ?? undefined),
                timestamp: stryMutAct_9fa48("2084") ? r.effective_at && undefined : (stryCov_9fa48("2084"), r.effective_at ?? undefined),
                payload
              } satisfies GovernanceEvent;
            }
          }));
          return {
            events: mapped,
            source: stryMutAct_9fa48("2088") ? response.source && null : stryMutAct_9fa48("2087") ? false : stryMutAct_9fa48("2086") ? true : (stryCov_9fa48("2086", "2087", "2088"), response.source || null),
            fromIndex: stryMutAct_9fa48("2091") ? response._debug?.fromIndex && response.source === 'duckdb-index' : stryMutAct_9fa48("2090") ? false : stryMutAct_9fa48("2089") ? true : (stryCov_9fa48("2089", "2090", "2091"), (stryMutAct_9fa48("2092") ? response._debug.fromIndex : (stryCov_9fa48("2092"), response._debug?.fromIndex)) || (stryMutAct_9fa48("2094") ? response.source !== 'duckdb-index' : stryMutAct_9fa48("2093") ? false : (stryCov_9fa48("2093", "2094"), response.source === 'duckdb-index'))),
            indexedAt: stryMutAct_9fa48("2098") ? response._debug?.indexedAt && null : stryMutAct_9fa48("2097") ? false : stryMutAct_9fa48("2096") ? true : (stryCov_9fa48("2096", "2097", "2098"), (stryMutAct_9fa48("2099") ? response._debug.indexedAt : (stryCov_9fa48("2099"), response._debug?.indexedAt)) || null),
            totalIndexed: stryMutAct_9fa48("2102") ? response._debug?.totalIndexed && null : stryMutAct_9fa48("2101") ? false : stryMutAct_9fa48("2100") ? true : (stryCov_9fa48("2100", "2101", "2102"), (stryMutAct_9fa48("2103") ? response._debug.totalIndexed : (stryCov_9fa48("2103"), response._debug?.totalIndexed)) || null)
          };
        }
      },
      staleTime: 30_000
    });
  }
}
export function useRewardClaimEvents() {
  if (stryMutAct_9fa48("2104")) {
    {}
  } else {
    stryCov_9fa48("2104");
    return useQuery({
      queryKey: stryMutAct_9fa48("2106") ? [] : (stryCov_9fa48("2106"), ["rewardClaimEvents"]),
      queryFn: async (): Promise<GovernanceEvent[]> => {
        if (stryMutAct_9fa48("2108")) {
          {}
        } else {
          stryCov_9fa48("2108");
          // Fetch reward claim events from DuckDB
          const response = await apiFetch<EventsResponse<GovernanceEvent>>("/api/events/rewards");
          return stryMutAct_9fa48("2112") ? response.data && [] : stryMutAct_9fa48("2111") ? false : stryMutAct_9fa48("2110") ? true : (stryCov_9fa48("2110", "2111", "2112"), response.data || (stryMutAct_9fa48("2113") ? ["Stryker was here"] : (stryCov_9fa48("2113"), [])));
        }
      },
      staleTime: 30_000
    });
  }
}