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
import { getACSContracts as getLocalACSContracts } from "@/lib/duckdb-api-client";

/**
 * Fetch and aggregate data across all templates matching a suffix
 * Uses DuckDB/Parquet backend exclusively
 */
export function useAggregatedTemplateData(snapshotId: string | undefined, templateSuffix: string, enabled: boolean = stryMutAct_9fa48("985") ? false : (stryCov_9fa48("985"), true)) {
  if (stryMutAct_9fa48("986")) {
    {}
  } else {
    stryCov_9fa48("986");
    return useQuery({
      queryKey: stryMutAct_9fa48("988") ? [] : (stryCov_9fa48("988"), ["aggregated-template-data", templateSuffix]),
      queryFn: async () => {
        if (stryMutAct_9fa48("990")) {
          {}
        } else {
          stryCov_9fa48("990");
          if (stryMutAct_9fa48("993") ? false : stryMutAct_9fa48("992") ? true : stryMutAct_9fa48("991") ? templateSuffix : (stryCov_9fa48("991", "992", "993"), !templateSuffix)) {
            if (stryMutAct_9fa48("994")) {
              {}
            } else {
              stryCov_9fa48("994");
              throw new Error("Missing templateSuffix");
            }
          }

          // Use DuckDB for all template data queries
          console.log(`[useAggregatedTemplateData] Fetching template=${templateSuffix}`);
          try {
            if (stryMutAct_9fa48("997")) {
              {}
            } else {
              stryCov_9fa48("997");
              const response = await getLocalACSContracts({
                template: templateSuffix,
                limit: 100000
              });
              const totalCount = stryMutAct_9fa48("999") ? (response.count ?? response.data?.length) && 0 : (stryCov_9fa48("999"), (stryMutAct_9fa48("1000") ? response.count && response.data?.length : (stryCov_9fa48("1000"), response.count ?? (stryMutAct_9fa48("1001") ? response.data.length : (stryCov_9fa48("1001"), response.data?.length)))) ?? 0);
              console.log(`[useAggregatedTemplateData] DuckDB returned ${stryMutAct_9fa48("1005") ? response.data?.length && 0 : stryMutAct_9fa48("1004") ? false : stryMutAct_9fa48("1003") ? true : (stryCov_9fa48("1003", "1004", "1005"), (stryMutAct_9fa48("1006") ? response.data.length : (stryCov_9fa48("1006"), response.data?.length)) || 0)} contracts (total: ${totalCount}) for template=${templateSuffix}`);
              if (stryMutAct_9fa48("1009") ? !response.data && response.data.length === 0 : stryMutAct_9fa48("1008") ? false : stryMutAct_9fa48("1007") ? true : (stryCov_9fa48("1007", "1008", "1009"), (stryMutAct_9fa48("1010") ? response.data : (stryCov_9fa48("1010"), !response.data)) || (stryMutAct_9fa48("1012") ? response.data.length !== 0 : stryMutAct_9fa48("1011") ? false : (stryCov_9fa48("1011", "1012"), response.data.length === 0)))) {
                if (stryMutAct_9fa48("1013")) {
                  {}
                } else {
                  stryCov_9fa48("1013");
                  console.warn(`[useAggregatedTemplateData] No contracts found for template=${templateSuffix}`);
                }
              }
              return {
                data: stryMutAct_9fa48("1018") ? response.data && [] : stryMutAct_9fa48("1017") ? false : stryMutAct_9fa48("1016") ? true : (stryCov_9fa48("1016", "1017", "1018"), response.data || (stryMutAct_9fa48("1019") ? ["Stryker was here"] : (stryCov_9fa48("1019"), []))),
                templateCount: 1,
                totalContracts: totalCount,
                templateIds: stryMutAct_9fa48("1020") ? [] : (stryCov_9fa48("1020"), [templateSuffix]),
                source: "duckdb"
              };
            }
          } catch (error) {
            if (stryMutAct_9fa48("1022")) {
              {}
            } else {
              stryCov_9fa48("1022");
              console.error(`[useAggregatedTemplateData] Error fetching template=${templateSuffix}:`, error);
              throw error;
            }
          }
        }
      },
      enabled: stryMutAct_9fa48("1026") ? enabled || !!templateSuffix : stryMutAct_9fa48("1025") ? false : stryMutAct_9fa48("1024") ? true : (stryCov_9fa48("1024", "1025", "1026"), enabled && (stryMutAct_9fa48("1027") ? !templateSuffix : (stryCov_9fa48("1027"), !(stryMutAct_9fa48("1028") ? templateSuffix : (stryCov_9fa48("1028"), !templateSuffix))))),
      staleTime: stryMutAct_9fa48("1029") ? 5 * 60 / 1000 : (stryCov_9fa48("1029"), (stryMutAct_9fa48("1030") ? 5 / 60 : (stryCov_9fa48("1030"), 5 * 60)) * 1000),
      // 5 minutes
      retry: stryMutAct_9fa48("1031") ? true : (stryCov_9fa48("1031"), false)
    });
  }
}