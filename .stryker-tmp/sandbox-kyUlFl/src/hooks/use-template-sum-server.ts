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
export interface ServerAggregationResult {
  sum: number;
  count: number;
  templateCount: number;
}
interface AggregateResponse {
  sum: number;
  count: number;
  templateCount?: number;
}
export function useTemplateSumServer(snapshotId: string | undefined, templateSuffix: string, mode: "circulating" | "locked", enabled: boolean = stryMutAct_9fa48("2888") ? false : (stryCov_9fa48("2888"), true)) {
  if (stryMutAct_9fa48("2889")) {
    {}
  } else {
    stryCov_9fa48("2889");
    return useQuery<ServerAggregationResult, Error>({
      queryKey: stryMutAct_9fa48("2891") ? [] : (stryCov_9fa48("2891"), ["server-template-sum", templateSuffix, mode]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2893")) {
          {}
        } else {
          stryCov_9fa48("2893");
          // Use DuckDB aggregation endpoint
          const response = await apiFetch<AggregateResponse>(`/api/acs/aggregate?template=${encodeURIComponent(templateSuffix)}&mode=${mode}`);
          return {
            sum: stryMutAct_9fa48("2898") ? response.sum && 0 : stryMutAct_9fa48("2897") ? false : stryMutAct_9fa48("2896") ? true : (stryCov_9fa48("2896", "2897", "2898"), response.sum || 0),
            count: stryMutAct_9fa48("2901") ? response.count && 0 : stryMutAct_9fa48("2900") ? false : stryMutAct_9fa48("2899") ? true : (stryCov_9fa48("2899", "2900", "2901"), response.count || 0),
            templateCount: stryMutAct_9fa48("2904") ? response.templateCount && 1 : stryMutAct_9fa48("2903") ? false : stryMutAct_9fa48("2902") ? true : (stryCov_9fa48("2902", "2903", "2904"), response.templateCount || 1)
          };
        }
      },
      enabled: stryMutAct_9fa48("2907") ? enabled || !!templateSuffix : stryMutAct_9fa48("2906") ? false : stryMutAct_9fa48("2905") ? true : (stryCov_9fa48("2905", "2906", "2907"), enabled && (stryMutAct_9fa48("2908") ? !templateSuffix : (stryCov_9fa48("2908"), !(stryMutAct_9fa48("2909") ? templateSuffix : (stryCov_9fa48("2909"), !templateSuffix))))),
      staleTime: stryMutAct_9fa48("2910") ? 5 * 60 / 1000 : (stryCov_9fa48("2910"), (stryMutAct_9fa48("2911") ? 5 / 60 : (stryCov_9fa48("2911"), 5 * 60)) * 1000)
    });
  }
}