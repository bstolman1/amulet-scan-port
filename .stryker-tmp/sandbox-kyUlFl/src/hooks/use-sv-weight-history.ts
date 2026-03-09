/**
 * Hook for fetching SV weight history from local DuckDB API
 */
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
interface SvWeightEntry {
  timestamp: string;
  effectiveUntil: string | null;
  svCount: number;
  svParties: string[];
  contractId: string;
}
interface DailySvData {
  date: string;
  svCount: number;
  svParties: string[];
  timestamp: string;
}
interface StackedSvData {
  date: string;
  timestamp: string;
  total: number;
  [svName: string]: string | number; // Dynamic SV name keys
}
interface SvWeightHistoryResponse {
  data: SvWeightEntry[];
  dailyData: DailySvData[];
  stackedData: StackedSvData[];
  svNames: string[];
  totalRules: number;
}
export function useSvWeightHistory(limit = 100) {
  if (stryMutAct_9fa48("2882")) {
    {}
  } else {
    stryCov_9fa48("2882");
    return useQuery({
      queryKey: stryMutAct_9fa48("2884") ? [] : (stryCov_9fa48("2884"), ["sv-weight-history", limit]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2886")) {
          {}
        } else {
          stryCov_9fa48("2886");
          const response = await apiFetch<SvWeightHistoryResponse>(`/api/stats/sv-weight-history?limit=${limit}`);
          return response;
        }
      },
      staleTime: 60_000,
      retry: 1
    });
  }
}