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
import { getACSSnapshots, getLatestACSSnapshot, getACSTemplates, getACSContracts, getACSStats, getACSSupply, getACSStatus, isApiAvailable, type ACSSnapshot, type ACSTemplateStats, type ACSStats, type ACSStatusResponse } from "@/lib/duckdb-api-client";

/**
 * Hook to get ACS availability status (for graceful degradation during snapshots)
 */
export function useACSStatus() {
  if (stryMutAct_9fa48("2506")) {
    {}
  } else {
    stryCov_9fa48("2506");
    return useQuery({
      queryKey: stryMutAct_9fa48("2508") ? [] : (stryCov_9fa48("2508"), ["acsStatus"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2510")) {
          {}
        } else {
          stryCov_9fa48("2510");
          try {
            if (stryMutAct_9fa48("2511")) {
              {}
            } else {
              stryCov_9fa48("2511");
              return await getACSStatus();
            }
          } catch (err) {
            if (stryMutAct_9fa48("2512")) {
              {}
            } else {
              stryCov_9fa48("2512");
              return {
                available: false,
                snapshotInProgress: false,
                completeSnapshotCount: 0,
                inProgressSnapshotCount: 0,
                latestComplete: null,
                message: 'Unable to check ACS status',
                error: err instanceof Error ? err.message : 'Unknown error'
              } as ACSStatusResponse;
            }
          }
        }
      },
      staleTime: 10_000,
      // Check status more frequently
      refetchInterval: 15_000,
      // Auto-refresh every 15 seconds when snapshot in progress
      retry: 1
    });
  }
}

/**
 * Hook to check if DuckDB API is available for ACS data
 * Now uses the status endpoint for better accuracy during snapshots
 */
export function useLocalACSAvailable() {
  if (stryMutAct_9fa48("2513")) {
    {}
  } else {
    stryCov_9fa48("2513");
    return useQuery({
      queryKey: stryMutAct_9fa48("2515") ? [] : (stryCov_9fa48("2515"), ["localACSAvailable"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2517")) {
          {}
        } else {
          stryCov_9fa48("2517");
          try {
            if (stryMutAct_9fa48("2518")) {
              {}
            } else {
              stryCov_9fa48("2518");
              // First check if API is reachable at all
              const available = await isApiAvailable();
              if (stryMutAct_9fa48("2521") ? false : stryMutAct_9fa48("2520") ? true : stryMutAct_9fa48("2519") ? available : (stryCov_9fa48("2519", "2520", "2521"), !available)) return {
                available: stryMutAct_9fa48("2523") ? true : (stryCov_9fa48("2523"), false),
                reason: 'api_unreachable'
              };

              // Use status endpoint to check for complete snapshots
              const status = await getACSStatus();
              if (stryMutAct_9fa48("2526") ? false : stryMutAct_9fa48("2525") ? true : (stryCov_9fa48("2525", "2526"), status.available)) {
                if (stryMutAct_9fa48("2527")) {
                  {}
                } else {
                  stryCov_9fa48("2527");
                  return {
                    available: stryMutAct_9fa48("2529") ? false : (stryCov_9fa48("2529"), true),
                    reason: 'complete_snapshot_available'
                  };
                }
              }
              if (stryMutAct_9fa48("2532") ? false : stryMutAct_9fa48("2531") ? true : (stryCov_9fa48("2531", "2532"), status.snapshotInProgress)) {
                if (stryMutAct_9fa48("2533")) {
                  {}
                } else {
                  stryCov_9fa48("2533");
                  return {
                    available: stryMutAct_9fa48("2535") ? true : (stryCov_9fa48("2535"), false),
                    reason: 'snapshot_in_progress',
                    message: status.message
                  };
                }
              }
              return {
                available: stryMutAct_9fa48("2538") ? true : (stryCov_9fa48("2538"), false),
                reason: 'no_data'
              };
            }
          } catch {
            if (stryMutAct_9fa48("2540")) {
              {}
            } else {
              stryCov_9fa48("2540");
              return {
                available: stryMutAct_9fa48("2542") ? true : (stryCov_9fa48("2542"), false),
                reason: 'error'
              };
            }
          }
        }
      },
      staleTime: 10_000,
      retry: stryMutAct_9fa48("2544") ? true : (stryCov_9fa48("2544"), false),
      refetchOnWindowFocus: stryMutAct_9fa48("2545") ? true : (stryCov_9fa48("2545"), false)
    });
  }
}

/**
 * Hook to fetch ACS snapshots from local DuckDB
 */
export function useLocalACSSnapshots() {
  if (stryMutAct_9fa48("2546")) {
    {}
  } else {
    stryCov_9fa48("2546");
    return useQuery({
      queryKey: stryMutAct_9fa48("2548") ? [] : (stryCov_9fa48("2548"), ["localACSSnapshots"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2550")) {
          {}
        } else {
          stryCov_9fa48("2550");
          const response = await getACSSnapshots();
          return response.data;
        }
      },
      staleTime: 30_000
    });
  }
}

/**
 * Hook to fetch the latest ACS snapshot from local DuckDB
 */
export function useLocalLatestACSSnapshot() {
  if (stryMutAct_9fa48("2551")) {
    {}
  } else {
    stryCov_9fa48("2551");
    return useQuery({
      queryKey: stryMutAct_9fa48("2553") ? [] : (stryCov_9fa48("2553"), ["localLatestACSSnapshot"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2555")) {
          {}
        } else {
          stryCov_9fa48("2555");
          const response = await getLatestACSSnapshot();
          return response.data;
        }
      },
      staleTime: 30_000
    });
  }
}

/**
 * Hook to fetch ACS template statistics from local DuckDB
 */
export function useLocalACSTemplates(limit = 100) {
  if (stryMutAct_9fa48("2556")) {
    {}
  } else {
    stryCov_9fa48("2556");
    return useQuery({
      queryKey: stryMutAct_9fa48("2558") ? [] : (stryCov_9fa48("2558"), ["localACSTemplates", limit]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2560")) {
          {}
        } else {
          stryCov_9fa48("2560");
          const response = await getACSTemplates(limit);
          return response.data;
        }
      },
      staleTime: 60_000
    });
  }
}

/**
 * Hook to fetch ACS contracts by template from local DuckDB
 */
export function useLocalACSContracts(params: {
  template?: string;
  entity?: string;
  limit?: number;
  offset?: number;
  enabled?: boolean;
}) {
  if (stryMutAct_9fa48("2561")) {
    {}
  } else {
    stryCov_9fa48("2561");
    const {
      enabled = stryMutAct_9fa48("2562") ? false : (stryCov_9fa48("2562"), true),
      ...queryParams
    } = params;
    return useQuery({
      queryKey: stryMutAct_9fa48("2564") ? [] : (stryCov_9fa48("2564"), ["localACSContracts", queryParams]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2566")) {
          {}
        } else {
          stryCov_9fa48("2566");
          const response = await getACSContracts(queryParams);
          return response.data;
        }
      },
      enabled,
      staleTime: 60_000
    });
  }
}

/**
 * Hook to fetch ACS overview statistics from local DuckDB
 */
export function useLocalACSStats() {
  if (stryMutAct_9fa48("2567")) {
    {}
  } else {
    stryCov_9fa48("2567");
    return useQuery({
      queryKey: stryMutAct_9fa48("2569") ? [] : (stryCov_9fa48("2569"), ["localACSStats"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2571")) {
          {}
        } else {
          stryCov_9fa48("2571");
          const response = await getACSStats();
          return response.data;
        }
      },
      staleTime: 30_000
    });
  }
}

/**
 * Hook to fetch ACS supply data from local DuckDB
 */
export function useLocalACSSupply() {
  if (stryMutAct_9fa48("2572")) {
    {}
  } else {
    stryCov_9fa48("2572");
    return useQuery({
      queryKey: stryMutAct_9fa48("2574") ? [] : (stryCov_9fa48("2574"), ["localACSSupply"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2576")) {
          {}
        } else {
          stryCov_9fa48("2576");
          const response = await getACSSupply();
          return response.data;
        }
      },
      staleTime: 30_000
    });
  }
}
export type { ACSSnapshot, ACSTemplateStats, ACSStats, ACSStatusResponse };