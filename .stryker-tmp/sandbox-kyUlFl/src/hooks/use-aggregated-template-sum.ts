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
interface AggregationResult {
  sum: number;
  count: number;
  templateCount: number;
}
interface AggregateResponse {
  sum: number;
  count: number;
  templateCount?: number;
}

/**
 * Aggregates template data using DuckDB backend.
 * The pickFn is no longer used client-side - aggregation happens server-side.
 */
export function useAggregatedTemplateSum(snapshotId: string | undefined, templateSuffix: string, pickFn: (obj: any) => number, enabled: boolean = stryMutAct_9fa48("1032") ? false : (stryCov_9fa48("1032"), true)) {
  if (stryMutAct_9fa48("1033")) {
    {}
  } else {
    stryCov_9fa48("1033");
    return useQuery<AggregationResult, Error>({
      queryKey: stryMutAct_9fa48("1035") ? [] : (stryCov_9fa48("1035"), ["aggregated-template-sum", templateSuffix]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1037")) {
          {}
        } else {
          stryCov_9fa48("1037");
          // Use DuckDB aggregation endpoint
          const response = await apiFetch<AggregateResponse>(`/api/acs/aggregate?template=${encodeURIComponent(templateSuffix)}`);
          return {
            sum: stryMutAct_9fa48("1042") ? response.sum && 0 : stryMutAct_9fa48("1041") ? false : stryMutAct_9fa48("1040") ? true : (stryCov_9fa48("1040", "1041", "1042"), response.sum || 0),
            count: stryMutAct_9fa48("1045") ? response.count && 0 : stryMutAct_9fa48("1044") ? false : stryMutAct_9fa48("1043") ? true : (stryCov_9fa48("1043", "1044", "1045"), response.count || 0),
            templateCount: stryMutAct_9fa48("1048") ? response.templateCount && 1 : stryMutAct_9fa48("1047") ? false : stryMutAct_9fa48("1046") ? true : (stryCov_9fa48("1046", "1047", "1048"), response.templateCount || 1)
          };
        }
      },
      enabled: stryMutAct_9fa48("1051") ? enabled || !!templateSuffix : stryMutAct_9fa48("1050") ? false : stryMutAct_9fa48("1049") ? true : (stryCov_9fa48("1049", "1050", "1051"), enabled && (stryMutAct_9fa48("1052") ? !templateSuffix : (stryCov_9fa48("1052"), !(stryMutAct_9fa48("1053") ? templateSuffix : (stryCov_9fa48("1053"), !templateSuffix))))),
      staleTime: stryMutAct_9fa48("1054") ? 5 * 60 / 1000 : (stryCov_9fa48("1054"), (stryMutAct_9fa48("1055") ? 5 / 60 : (stryCov_9fa48("1055"), 5 * 60)) * 1000)
    });
  }
}