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
export interface GovernanceHistoryEvent {
  event_id: string;
  event_type: 'created' | 'archived';
  contract_id: string;
  template_id: string;
  effective_at: string;
  timestamp: string;
  action_tag: string | null;
  requester: string | null;
  reason: {
    url?: string;
    body?: string;
  } | null;
  votes: Array<[string, {
    accept?: boolean;
    reason?: {
      url?: string;
      body?: string;
    };
  }]>;
  vote_before: string | null;
}
interface HistoryResponse {
  data: GovernanceHistoryEvent[];
  count: number;
  hasMore?: boolean;
  source?: string;
}

// Processed governance action for display
export interface GovernanceAction {
  id: string;
  type: 'vote_completed' | 'rule_change' | 'confirmation';
  actionTag: string;
  templateType: 'VoteRequest' | 'DsoRules' | 'AmuletRules' | 'Confirmation';
  status: 'passed' | 'failed' | 'expired' | 'executed';
  effectiveAt: string;
  requester: string | null;
  reason: string | null;
  reasonUrl: string | null;
  votesFor: number;
  votesAgainst: number;
  totalVotes: number;
  contractId: string;
  cipReference: string | null;
}

// Extract CIP reference from reason text
const extractCipReference = (reason: {
  url?: string;
  body?: string;
} | null): string | null => {
  if (stryMutAct_9fa48("2114")) {
    {}
  } else {
    stryCov_9fa48("2114");
    if (stryMutAct_9fa48("2117") ? false : stryMutAct_9fa48("2116") ? true : stryMutAct_9fa48("2115") ? reason : (stryCov_9fa48("2115", "2116", "2117"), !reason)) return null;
    const text = `${stryMutAct_9fa48("2121") ? reason.body && '' : stryMutAct_9fa48("2120") ? false : stryMutAct_9fa48("2119") ? true : (stryCov_9fa48("2119", "2120", "2121"), reason.body || '')} ${stryMutAct_9fa48("2125") ? reason.url && '' : stryMutAct_9fa48("2124") ? false : stryMutAct_9fa48("2123") ? true : (stryCov_9fa48("2123", "2124", "2125"), reason.url || '')}`;
    const match = text.match(stryMutAct_9fa48("2132") ? /CIP[#\-\s]?0*(\D+)/i : stryMutAct_9fa48("2131") ? /CIP[#\-\s]?0*(\d)/i : stryMutAct_9fa48("2130") ? /CIP[#\-\s]?0(\d+)/i : stryMutAct_9fa48("2129") ? /CIP[#\-\S]?0*(\d+)/i : stryMutAct_9fa48("2128") ? /CIP[^#\-\s]?0*(\d+)/i : stryMutAct_9fa48("2127") ? /CIP[#\-\s]0*(\d+)/i : (stryCov_9fa48("2127", "2128", "2129", "2130", "2131", "2132"), /CIP[#\-\s]?0*(\d+)/i));
    return match ? match[1].padStart(4, '0') : null;
  }
};

// Parse votes array to count for/against
const parseVotes = (votes: GovernanceHistoryEvent['votes']): {
  votesFor: number;
  votesAgainst: number;
} => {
  if (stryMutAct_9fa48("2134")) {
    {}
  } else {
    stryCov_9fa48("2134");
    let votesFor = 0;
    let votesAgainst = 0;
    for (const vote of stryMutAct_9fa48("2137") ? votes && [] : stryMutAct_9fa48("2136") ? false : stryMutAct_9fa48("2135") ? true : (stryCov_9fa48("2135", "2136", "2137"), votes || (stryMutAct_9fa48("2138") ? ["Stryker was here"] : (stryCov_9fa48("2138"), [])))) {
      if (stryMutAct_9fa48("2139")) {
        {}
      } else {
        stryCov_9fa48("2139");
        const [, voteData] = Array.isArray(vote) ? vote : stryMutAct_9fa48("2140") ? [] : (stryCov_9fa48("2140"), ['', vote]);
        const isAccept = stryMutAct_9fa48("2144") ? voteData?.accept === true && (voteData as any)?.Accept === true : stryMutAct_9fa48("2143") ? false : stryMutAct_9fa48("2142") ? true : (stryCov_9fa48("2142", "2143", "2144"), (stryMutAct_9fa48("2146") ? voteData?.accept !== true : stryMutAct_9fa48("2145") ? false : (stryCov_9fa48("2145", "2146"), (stryMutAct_9fa48("2147") ? voteData.accept : (stryCov_9fa48("2147"), voteData?.accept)) === (stryMutAct_9fa48("2148") ? false : (stryCov_9fa48("2148"), true)))) || (stryMutAct_9fa48("2150") ? (voteData as any)?.Accept !== true : stryMutAct_9fa48("2149") ? false : (stryCov_9fa48("2149", "2150"), (stryMutAct_9fa48("2151") ? (voteData as any).Accept : (stryCov_9fa48("2151"), (voteData as any)?.Accept)) === (stryMutAct_9fa48("2152") ? false : (stryCov_9fa48("2152"), true)))));
        if (stryMutAct_9fa48("2154") ? false : stryMutAct_9fa48("2153") ? true : (stryCov_9fa48("2153", "2154"), isAccept)) stryMutAct_9fa48("2155") ? votesFor-- : (stryCov_9fa48("2155"), votesFor++);else stryMutAct_9fa48("2156") ? votesAgainst-- : (stryCov_9fa48("2156"), votesAgainst++);
      }
    }
    return {
      votesFor,
      votesAgainst
    };
  }
};

// Get template type from template_id
const getTemplateType = (templateId: string): GovernanceAction['templateType'] => {
  if (stryMutAct_9fa48("2158")) {
    {}
  } else {
    stryCov_9fa48("2158");
    if (stryMutAct_9fa48("2160") ? false : stryMutAct_9fa48("2159") ? true : (stryCov_9fa48("2159", "2160"), templateId.includes('VoteRequest'))) return 'VoteRequest';
    if (stryMutAct_9fa48("2164") ? false : stryMutAct_9fa48("2163") ? true : (stryCov_9fa48("2163", "2164"), templateId.includes('DsoRules'))) return 'DsoRules';
    if (stryMutAct_9fa48("2168") ? false : stryMutAct_9fa48("2167") ? true : (stryCov_9fa48("2167", "2168"), templateId.includes('AmuletRules'))) return 'AmuletRules';
    if (stryMutAct_9fa48("2172") ? false : stryMutAct_9fa48("2171") ? true : (stryCov_9fa48("2171", "2172"), templateId.includes('Confirmation'))) return 'Confirmation';
    return 'VoteRequest';
  }
};
export function useGovernanceHistory(limit = 500) {
  if (stryMutAct_9fa48("2176")) {
    {}
  } else {
    stryCov_9fa48("2176");
    return useQuery({
      queryKey: stryMutAct_9fa48("2178") ? [] : (stryCov_9fa48("2178"), ["governanceHistory", limit]),
      queryFn: async (): Promise<GovernanceAction[]> => {
        if (stryMutAct_9fa48("2180")) {
          {}
        } else {
          stryCov_9fa48("2180");
          const response = await apiFetch<HistoryResponse>(`/api/events/governance-history?limit=${limit}`);
          const events = stryMutAct_9fa48("2184") ? response.data && [] : stryMutAct_9fa48("2183") ? false : stryMutAct_9fa48("2182") ? true : (stryCov_9fa48("2182", "2183", "2184"), response.data || (stryMutAct_9fa48("2185") ? ["Stryker was here"] : (stryCov_9fa48("2185"), [])));

          // Process events into governance actions
          // Focus on archived VoteRequests (completed votes) and created DsoRules/AmuletRules
          const actions: GovernanceAction[] = stryMutAct_9fa48("2186") ? ["Stryker was here"] : (stryCov_9fa48("2186"), []);
          const seenContracts = new Set<string>();
          for (const event of events) {
            if (stryMutAct_9fa48("2187")) {
              {}
            } else {
              stryCov_9fa48("2187");
              const templateType = getTemplateType(event.template_id);

              // For VoteRequests, we care about archived events (completed votes)
              if (stryMutAct_9fa48("2190") ? templateType === 'VoteRequest' || event.event_type === 'archived' : stryMutAct_9fa48("2189") ? false : stryMutAct_9fa48("2188") ? true : (stryCov_9fa48("2188", "2189", "2190"), (stryMutAct_9fa48("2192") ? templateType !== 'VoteRequest' : stryMutAct_9fa48("2191") ? true : (stryCov_9fa48("2191", "2192"), templateType === 'VoteRequest')) && (stryMutAct_9fa48("2195") ? event.event_type !== 'archived' : stryMutAct_9fa48("2194") ? true : (stryCov_9fa48("2194", "2195"), event.event_type === 'archived')))) {
                if (stryMutAct_9fa48("2197")) {
                  {}
                } else {
                  stryCov_9fa48("2197");
                  if (stryMutAct_9fa48("2199") ? false : stryMutAct_9fa48("2198") ? true : (stryCov_9fa48("2198", "2199"), seenContracts.has(event.contract_id))) continue;
                  seenContracts.add(event.contract_id);
                  const {
                    votesFor,
                    votesAgainst
                  } = parseVotes(event.votes);
                  const totalVotes = stryMutAct_9fa48("2200") ? votesFor - votesAgainst : (stryCov_9fa48("2200"), votesFor + votesAgainst);
                  const threshold = 10; // Standard threshold

                  let status: GovernanceAction['status'] = 'failed';
                  if (stryMutAct_9fa48("2205") ? votesFor < threshold : stryMutAct_9fa48("2204") ? votesFor > threshold : stryMutAct_9fa48("2203") ? false : stryMutAct_9fa48("2202") ? true : (stryCov_9fa48("2202", "2203", "2204", "2205"), votesFor >= threshold)) status = 'passed';else if (stryMutAct_9fa48("2209") ? totalVotes !== 0 : stryMutAct_9fa48("2208") ? false : stryMutAct_9fa48("2207") ? true : (stryCov_9fa48("2207", "2208", "2209"), totalVotes === 0)) status = 'expired';
                  actions.push({
                    id: stryMutAct_9fa48("2214") ? event.event_id && event.contract_id : stryMutAct_9fa48("2213") ? false : stryMutAct_9fa48("2212") ? true : (stryCov_9fa48("2212", "2213", "2214"), event.event_id || event.contract_id),
                    type: 'vote_completed',
                    actionTag: stryMutAct_9fa48("2218") ? event.action_tag && 'Unknown' : stryMutAct_9fa48("2217") ? false : stryMutAct_9fa48("2216") ? true : (stryCov_9fa48("2216", "2217", "2218"), event.action_tag || 'Unknown'),
                    templateType,
                    status,
                    effectiveAt: stryMutAct_9fa48("2222") ? event.effective_at && event.timestamp : stryMutAct_9fa48("2221") ? false : stryMutAct_9fa48("2220") ? true : (stryCov_9fa48("2220", "2221", "2222"), event.effective_at || event.timestamp),
                    requester: event.requester,
                    reason: stryMutAct_9fa48("2225") ? event.reason?.body && null : stryMutAct_9fa48("2224") ? false : stryMutAct_9fa48("2223") ? true : (stryCov_9fa48("2223", "2224", "2225"), (stryMutAct_9fa48("2226") ? event.reason.body : (stryCov_9fa48("2226"), event.reason?.body)) || null),
                    reasonUrl: stryMutAct_9fa48("2229") ? event.reason?.url && null : stryMutAct_9fa48("2228") ? false : stryMutAct_9fa48("2227") ? true : (stryCov_9fa48("2227", "2228", "2229"), (stryMutAct_9fa48("2230") ? event.reason.url : (stryCov_9fa48("2230"), event.reason?.url)) || null),
                    votesFor,
                    votesAgainst,
                    totalVotes,
                    contractId: event.contract_id,
                    cipReference: extractCipReference(event.reason)
                  });
                }
              }

              // For DsoRules/AmuletRules, we care about created events (rule changes)
              if (stryMutAct_9fa48("2233") ? templateType === 'DsoRules' || templateType === 'AmuletRules' || event.event_type === 'created' : stryMutAct_9fa48("2232") ? false : stryMutAct_9fa48("2231") ? true : (stryCov_9fa48("2231", "2232", "2233"), (stryMutAct_9fa48("2235") ? templateType === 'DsoRules' && templateType === 'AmuletRules' : stryMutAct_9fa48("2234") ? true : (stryCov_9fa48("2234", "2235"), (stryMutAct_9fa48("2237") ? templateType !== 'DsoRules' : stryMutAct_9fa48("2236") ? false : (stryCov_9fa48("2236", "2237"), templateType === 'DsoRules')) || (stryMutAct_9fa48("2240") ? templateType !== 'AmuletRules' : stryMutAct_9fa48("2239") ? false : (stryCov_9fa48("2239", "2240"), templateType === 'AmuletRules')))) && (stryMutAct_9fa48("2243") ? event.event_type !== 'created' : stryMutAct_9fa48("2242") ? true : (stryCov_9fa48("2242", "2243"), event.event_type === 'created')))) {
                if (stryMutAct_9fa48("2245")) {
                  {}
                } else {
                  stryCov_9fa48("2245");
                  if (stryMutAct_9fa48("2247") ? false : stryMutAct_9fa48("2246") ? true : (stryCov_9fa48("2246", "2247"), seenContracts.has(event.contract_id))) continue;
                  seenContracts.add(event.contract_id);
                  actions.push({
                    id: stryMutAct_9fa48("2251") ? event.event_id && event.contract_id : stryMutAct_9fa48("2250") ? false : stryMutAct_9fa48("2249") ? true : (stryCov_9fa48("2249", "2250", "2251"), event.event_id || event.contract_id),
                    type: 'rule_change',
                    actionTag: (stryMutAct_9fa48("2255") ? templateType !== 'DsoRules' : stryMutAct_9fa48("2254") ? false : stryMutAct_9fa48("2253") ? true : (stryCov_9fa48("2253", "2254", "2255"), templateType === 'DsoRules')) ? 'DSO Rules Update' : 'Amulet Rules Update',
                    templateType,
                    status: 'executed',
                    effectiveAt: stryMutAct_9fa48("2262") ? event.effective_at && event.timestamp : stryMutAct_9fa48("2261") ? false : stryMutAct_9fa48("2260") ? true : (stryCov_9fa48("2260", "2261", "2262"), event.effective_at || event.timestamp),
                    requester: null,
                    reason: null,
                    reasonUrl: null,
                    votesFor: 0,
                    votesAgainst: 0,
                    totalVotes: 0,
                    contractId: event.contract_id,
                    cipReference: null
                  });
                }
              }

              // For Confirmations (executed actions)
              if (stryMutAct_9fa48("2265") ? templateType === 'Confirmation' || event.event_type === 'created' : stryMutAct_9fa48("2264") ? false : stryMutAct_9fa48("2263") ? true : (stryCov_9fa48("2263", "2264", "2265"), (stryMutAct_9fa48("2267") ? templateType !== 'Confirmation' : stryMutAct_9fa48("2266") ? true : (stryCov_9fa48("2266", "2267"), templateType === 'Confirmation')) && (stryMutAct_9fa48("2270") ? event.event_type !== 'created' : stryMutAct_9fa48("2269") ? true : (stryCov_9fa48("2269", "2270"), event.event_type === 'created')))) {
                if (stryMutAct_9fa48("2272")) {
                  {}
                } else {
                  stryCov_9fa48("2272");
                  if (stryMutAct_9fa48("2274") ? false : stryMutAct_9fa48("2273") ? true : (stryCov_9fa48("2273", "2274"), seenContracts.has(event.contract_id))) continue;
                  seenContracts.add(event.contract_id);
                  actions.push({
                    id: stryMutAct_9fa48("2278") ? event.event_id && event.contract_id : stryMutAct_9fa48("2277") ? false : stryMutAct_9fa48("2276") ? true : (stryCov_9fa48("2276", "2277", "2278"), event.event_id || event.contract_id),
                    type: 'confirmation',
                    actionTag: stryMutAct_9fa48("2282") ? event.action_tag && 'Confirmation' : stryMutAct_9fa48("2281") ? false : stryMutAct_9fa48("2280") ? true : (stryCov_9fa48("2280", "2281", "2282"), event.action_tag || 'Confirmation'),
                    templateType,
                    status: 'executed',
                    effectiveAt: stryMutAct_9fa48("2287") ? event.effective_at && event.timestamp : stryMutAct_9fa48("2286") ? false : stryMutAct_9fa48("2285") ? true : (stryCov_9fa48("2285", "2286", "2287"), event.effective_at || event.timestamp),
                    requester: event.requester,
                    reason: stryMutAct_9fa48("2290") ? event.reason?.body && null : stryMutAct_9fa48("2289") ? false : stryMutAct_9fa48("2288") ? true : (stryCov_9fa48("2288", "2289", "2290"), (stryMutAct_9fa48("2291") ? event.reason.body : (stryCov_9fa48("2291"), event.reason?.body)) || null),
                    reasonUrl: stryMutAct_9fa48("2294") ? event.reason?.url && null : stryMutAct_9fa48("2293") ? false : stryMutAct_9fa48("2292") ? true : (stryCov_9fa48("2292", "2293", "2294"), (stryMutAct_9fa48("2295") ? event.reason.url : (stryCov_9fa48("2295"), event.reason?.url)) || null),
                    votesFor: 0,
                    votesAgainst: 0,
                    totalVotes: 0,
                    contractId: event.contract_id,
                    cipReference: extractCipReference(event.reason)
                  });
                }
              }
            }
          }

          // Sort by effective date descending
          return stryMutAct_9fa48("2296") ? actions : (stryCov_9fa48("2296"), actions.sort(stryMutAct_9fa48("2297") ? () => undefined : (stryCov_9fa48("2297"), (a, b) => stryMutAct_9fa48("2298") ? new Date(b.effectiveAt).getTime() + new Date(a.effectiveAt).getTime() : (stryCov_9fa48("2298"), new Date(b.effectiveAt).getTime() - new Date(a.effectiveAt).getTime()))));
        }
      },
      staleTime: 60_000 // 1 minute
    });
  }
}