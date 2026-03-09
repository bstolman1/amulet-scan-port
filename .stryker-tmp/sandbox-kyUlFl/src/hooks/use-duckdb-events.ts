/**
 * DuckDB Events Hooks
 * 
 * React Query hooks for fetching ledger events from the DuckDB API.
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
import { getLatestEvents, getEventsByType, getEventsByTemplate, getEventsCount, getOverviewStats, getTemplatesList, searchEvents, type LedgerEvent, type TemplateInfo, type OverviewStats, type SearchParams } from "@/lib/duckdb-api-client";
import { checkDuckDBConnection } from "@/lib/backend-config";

/**
 * Check if DuckDB API is available
 */
export function useDuckDBHealth() {
  if (stryMutAct_9fa48("1885")) {
    {}
  } else {
    stryCov_9fa48("1885");
    return useQuery({
      queryKey: stryMutAct_9fa48("1887") ? [] : (stryCov_9fa48("1887"), ["duckdb-health"]),
      queryFn: checkDuckDBConnection,
      staleTime: 30_000,
      refetchInterval: 60_000
    });
  }
}

/**
 * Fetch latest ledger events from DuckDB
 */
export function useDuckDBLatestEvents(limit = 100, offset = 0) {
  if (stryMutAct_9fa48("1889")) {
    {}
  } else {
    stryCov_9fa48("1889");
    return useQuery({
      queryKey: stryMutAct_9fa48("1891") ? [] : (stryCov_9fa48("1891"), ["duckdb-events-latest", limit, offset]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1893")) {
          {}
        } else {
          stryCov_9fa48("1893");
          const response = await getLatestEvents(limit, offset);
          return response.data;
        }
      },
      staleTime: 5_000
    });
  }
}

/**
 * Fetch events by type from DuckDB
 */
export function useDuckDBEventsByType(type: string, limit = 100) {
  if (stryMutAct_9fa48("1894")) {
    {}
  } else {
    stryCov_9fa48("1894");
    return useQuery({
      queryKey: stryMutAct_9fa48("1896") ? [] : (stryCov_9fa48("1896"), ["duckdb-events-by-type", type, limit]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1898")) {
          {}
        } else {
          stryCov_9fa48("1898");
          const response = await getEventsByType(type, limit);
          return response.data;
        }
      },
      enabled: stryMutAct_9fa48("1899") ? !type : (stryCov_9fa48("1899"), !(stryMutAct_9fa48("1900") ? type : (stryCov_9fa48("1900"), !type))),
      staleTime: 10_000
    });
  }
}

/**
 * Fetch events by template from DuckDB
 */
export function useDuckDBEventsByTemplate(templateId: string, limit = 100) {
  if (stryMutAct_9fa48("1901")) {
    {}
  } else {
    stryCov_9fa48("1901");
    return useQuery({
      queryKey: stryMutAct_9fa48("1903") ? [] : (stryCov_9fa48("1903"), ["duckdb-events-by-template", templateId, limit]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1905")) {
          {}
        } else {
          stryCov_9fa48("1905");
          const response = await getEventsByTemplate(templateId, limit);
          return response.data;
        }
      },
      enabled: stryMutAct_9fa48("1906") ? !templateId : (stryCov_9fa48("1906"), !(stryMutAct_9fa48("1907") ? templateId : (stryCov_9fa48("1907"), !templateId))),
      staleTime: 10_000
    });
  }
}

/**
 * Get total event count from DuckDB
 */
export function useDuckDBEventsCount() {
  if (stryMutAct_9fa48("1908")) {
    {}
  } else {
    stryCov_9fa48("1908");
    return useQuery({
      queryKey: stryMutAct_9fa48("1910") ? [] : (stryCov_9fa48("1910"), ["duckdb-events-count"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1912")) {
          {}
        } else {
          stryCov_9fa48("1912");
          const response = await getEventsCount();
          return response.count;
        }
      },
      staleTime: 30_000
    });
  }
}

/**
 * Get overview stats from DuckDB
 */
export function useDuckDBOverviewStats() {
  if (stryMutAct_9fa48("1913")) {
    {}
  } else {
    stryCov_9fa48("1913");
    return useQuery({
      queryKey: stryMutAct_9fa48("1915") ? [] : (stryCov_9fa48("1915"), ["duckdb-overview-stats"]),
      queryFn: getOverviewStats,
      staleTime: 30_000
    });
  }
}

/**
 * Get templates list from DuckDB
 */
export function useDuckDBTemplatesList() {
  if (stryMutAct_9fa48("1917")) {
    {}
  } else {
    stryCov_9fa48("1917");
    return useQuery({
      queryKey: stryMutAct_9fa48("1919") ? [] : (stryCov_9fa48("1919"), ["duckdb-templates-list"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1921")) {
          {}
        } else {
          stryCov_9fa48("1921");
          const response = await getTemplatesList();
          return response.data;
        }
      },
      staleTime: 60_000
    });
  }
}

/**
 * Search events in DuckDB
 */
export function useDuckDBSearch(params: SearchParams, enabled = stryMutAct_9fa48("1922") ? false : (stryCov_9fa48("1922"), true)) {
  if (stryMutAct_9fa48("1923")) {
    {}
  } else {
    stryCov_9fa48("1923");
    return useQuery({
      queryKey: stryMutAct_9fa48("1925") ? [] : (stryCov_9fa48("1925"), ["duckdb-search", params]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1927")) {
          {}
        } else {
          stryCov_9fa48("1927");
          const response = await searchEvents(params);
          return response.data;
        }
      },
      enabled: stryMutAct_9fa48("1930") ? enabled || !!params.q || !!params.type || !!params.template || !!params.party : stryMutAct_9fa48("1929") ? false : stryMutAct_9fa48("1928") ? true : (stryCov_9fa48("1928", "1929", "1930"), enabled && (stryMutAct_9fa48("1932") ? (!!params.q || !!params.type || !!params.template) && !!params.party : stryMutAct_9fa48("1931") ? true : (stryCov_9fa48("1931", "1932"), (stryMutAct_9fa48("1934") ? (!!params.q || !!params.type) && !!params.template : stryMutAct_9fa48("1933") ? false : (stryCov_9fa48("1933", "1934"), (stryMutAct_9fa48("1936") ? !!params.q && !!params.type : stryMutAct_9fa48("1935") ? false : (stryCov_9fa48("1935", "1936"), (stryMutAct_9fa48("1937") ? !params.q : (stryCov_9fa48("1937"), !(stryMutAct_9fa48("1938") ? params.q : (stryCov_9fa48("1938"), !params.q)))) || (stryMutAct_9fa48("1939") ? !params.type : (stryCov_9fa48("1939"), !(stryMutAct_9fa48("1940") ? params.type : (stryCov_9fa48("1940"), !params.type)))))) || (stryMutAct_9fa48("1941") ? !params.template : (stryCov_9fa48("1941"), !(stryMutAct_9fa48("1942") ? params.template : (stryCov_9fa48("1942"), !params.template)))))) || (stryMutAct_9fa48("1943") ? !params.party : (stryCov_9fa48("1943"), !(stryMutAct_9fa48("1944") ? params.party : (stryCov_9fa48("1944"), !params.party))))))),
      staleTime: 10_000
    });
  }
}
export type { LedgerEvent, TemplateInfo, OverviewStats, SearchParams };