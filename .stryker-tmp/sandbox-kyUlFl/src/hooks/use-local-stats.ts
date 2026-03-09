/**
 * Hook for fetching dashboard stats from local DuckDB API
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
import { useQuery } from '@tanstack/react-query';
import * as duckdbApi from '@/lib/duckdb-api-client';
import { checkDuckDBConnection } from '@/lib/backend-config';
export interface LocalDashboardStats {
  total_events: number;
  unique_contracts: number;
  unique_templates: number;
  earliest_event: string | null;
  latest_event: string | null;
  data_source: string;
}
export interface DailyStats {
  date: string;
  event_count: number;
  contract_count: number;
}
export interface TemplateStats {
  template_id: string;
  event_count: number;
  contract_count: number;
  first_seen: string;
  last_seen: string;
}
export interface TypeStats {
  event_type: string;
  count: number;
}

/**
 * Check if local DuckDB API is available
 */
export function useLocalApiAvailable() {
  if (stryMutAct_9fa48("2594")) {
    {}
  } else {
    stryCov_9fa48("2594");
    return useQuery({
      queryKey: stryMutAct_9fa48("2596") ? [] : (stryCov_9fa48("2596"), ['local-api-available']),
      queryFn: async () => {
        if (stryMutAct_9fa48("2598")) {
          {}
        } else {
          stryCov_9fa48("2598");
          return await checkDuckDBConnection();
        }
      },
      staleTime: 30000,
      // Cache for 30 seconds
      retry: stryMutAct_9fa48("2599") ? true : (stryCov_9fa48("2599"), false)
    });
  }
}

/**
 * Fetch overview stats from local DuckDB API
 */
export function useLocalOverviewStats() {
  if (stryMutAct_9fa48("2600")) {
    {}
  } else {
    stryCov_9fa48("2600");
    return useQuery({
      queryKey: stryMutAct_9fa48("2602") ? [] : (stryCov_9fa48("2602"), ['local-overview-stats']),
      queryFn: async () => {
        if (stryMutAct_9fa48("2604")) {
          {}
        } else {
          stryCov_9fa48("2604");
          const stats = await duckdbApi.getOverviewStats();
          return stats as LocalDashboardStats;
        }
      },
      staleTime: 60000,
      // Cache for 1 minute
      retry: 2
    });
  }
}

/**
 * Fetch daily stats from local DuckDB API
 */
export function useLocalDailyStats(days = 30) {
  if (stryMutAct_9fa48("2605")) {
    {}
  } else {
    stryCov_9fa48("2605");
    return useQuery({
      queryKey: stryMutAct_9fa48("2607") ? [] : (stryCov_9fa48("2607"), ['local-daily-stats', days]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2609")) {
          {}
        } else {
          stryCov_9fa48("2609");
          const response = await duckdbApi.getDailyStats(days);
          return response.data as DailyStats[];
        }
      },
      staleTime: 60000,
      retry: 2
    });
  }
}

/**
 * Fetch stats by template from local DuckDB API
 */
export function useLocalTemplateStats(limit = 50) {
  if (stryMutAct_9fa48("2610")) {
    {}
  } else {
    stryCov_9fa48("2610");
    return useQuery({
      queryKey: stryMutAct_9fa48("2612") ? [] : (stryCov_9fa48("2612"), ['local-template-stats', limit]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2614")) {
          {}
        } else {
          stryCov_9fa48("2614");
          const response = await duckdbApi.getStatsByTemplate(limit);
          return response.data as TemplateStats[];
        }
      },
      staleTime: 60000,
      retry: 2
    });
  }
}

/**
 * Fetch stats by type from local DuckDB API
 */
export function useLocalTypeStats() {
  if (stryMutAct_9fa48("2615")) {
    {}
  } else {
    stryCov_9fa48("2615");
    return useQuery({
      queryKey: stryMutAct_9fa48("2617") ? [] : (stryCov_9fa48("2617"), ['local-type-stats']),
      queryFn: async () => {
        if (stryMutAct_9fa48("2619")) {
          {}
        } else {
          stryCov_9fa48("2619");
          const response = await duckdbApi.getStatsByType();
          return response.data as TypeStats[];
        }
      },
      staleTime: 60000,
      retry: 2
    });
  }
}

/**
 * Fetch hourly stats from local DuckDB API
 */
export function useLocalHourlyStats() {
  if (stryMutAct_9fa48("2620")) {
    {}
  } else {
    stryCov_9fa48("2620");
    return useQuery({
      queryKey: stryMutAct_9fa48("2622") ? [] : (stryCov_9fa48("2622"), ['local-hourly-stats']),
      queryFn: async () => {
        if (stryMutAct_9fa48("2624")) {
          {}
        } else {
          stryCov_9fa48("2624");
          const response = await duckdbApi.getHourlyStats();
          return response.data;
        }
      },
      staleTime: 60000,
      retry: 2
    });
  }
}

/**
 * Fetch burn stats from local DuckDB API
 */
export function useLocalBurnStats() {
  if (stryMutAct_9fa48("2625")) {
    {}
  } else {
    stryCov_9fa48("2625");
    return useQuery({
      queryKey: stryMutAct_9fa48("2627") ? [] : (stryCov_9fa48("2627"), ['local-burn-stats']),
      queryFn: async () => {
        if (stryMutAct_9fa48("2629")) {
          {}
        } else {
          stryCov_9fa48("2629");
          const response = await duckdbApi.getBurnStats();
          return response.data;
        }
      },
      staleTime: 60000,
      retry: 2
    });
  }
}