/**
 * Hook for fetching events from local DuckDB API
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
import { getLatestEvents, getEventsByTemplate } from "@/lib/duckdb-api-client";
export interface LocalEvent {
  event_id: string;
  update_id?: string;
  event_type: string;
  synchronizer_id?: string;
  timestamp: string;
  effective_at?: string;
  contract_id: string;
  party?: string;
  template_id: string;
  payload?: any;
}
export function useLocalEvents(limit = 100, offset = 0) {
  if (stryMutAct_9fa48("2577")) {
    {}
  } else {
    stryCov_9fa48("2577");
    return useQuery({
      queryKey: stryMutAct_9fa48("2579") ? [] : (stryCov_9fa48("2579"), ["local-events", limit, offset]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2581")) {
          {}
        } else {
          stryCov_9fa48("2581");
          const response = await getLatestEvents(limit, offset);
          return response.data as unknown as LocalEvent[];
        }
      },
      staleTime: 30000,
      retry: 1
    });
  }
}
export function useLocalEventsByTemplate(templateFilter: string, limit = 100) {
  if (stryMutAct_9fa48("2582")) {
    {}
  } else {
    stryCov_9fa48("2582");
    return useQuery({
      queryKey: stryMutAct_9fa48("2584") ? [] : (stryCov_9fa48("2584"), ["local-events-by-template", templateFilter, limit]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2586")) {
          {}
        } else {
          stryCov_9fa48("2586");
          const response = await getEventsByTemplate(templateFilter, limit);
          return response.data as unknown as LocalEvent[];
        }
      },
      staleTime: 30000,
      retry: 1,
      enabled: stryMutAct_9fa48("2587") ? !templateFilter : (stryCov_9fa48("2587"), !(stryMutAct_9fa48("2588") ? templateFilter : (stryCov_9fa48("2588"), !templateFilter)))
    });
  }
}
export function useLocalTransactions(limit = 100, offset = 0) {
  if (stryMutAct_9fa48("2589")) {
    {}
  } else {
    stryCov_9fa48("2589");
    return useQuery({
      queryKey: stryMutAct_9fa48("2591") ? [] : (stryCov_9fa48("2591"), ["local-transactions", limit, offset]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2593")) {
          {}
        } else {
          stryCov_9fa48("2593");
          // Fetch all events, not just Amulet-related
          const response = await getLatestEvents(limit, offset);
          return response.data as unknown as LocalEvent[];
        }
      },
      staleTime: 30000,
      retry: 1
    });
  }
}