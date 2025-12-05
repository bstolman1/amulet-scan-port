/**
 * DuckDB Events Hooks
 * 
 * React Query hooks for fetching ledger events from the DuckDB API.
 */

import { useQuery } from "@tanstack/react-query";
import {
  getLatestEvents,
  getEventsByType,
  getEventsByTemplate,
  getEventsCount,
  getOverviewStats,
  getTemplatesList,
  searchEvents,
  type LedgerEvent,
  type TemplateInfo,
  type OverviewStats,
  type SearchParams,
} from "@/lib/duckdb-api-client";
import { checkDuckDBConnection } from "@/lib/backend-config";

/**
 * Check if DuckDB API is available
 */
export function useDuckDBHealth() {
  return useQuery({
    queryKey: ["duckdb-health"],
    queryFn: checkDuckDBConnection,
    staleTime: 30_000,
    refetchInterval: 60_000,
  });
}

/**
 * Fetch latest ledger events from DuckDB
 */
export function useDuckDBLatestEvents(limit = 100, offset = 0) {
  return useQuery({
    queryKey: ["duckdb-events-latest", limit, offset],
    queryFn: async () => {
      const response = await getLatestEvents(limit, offset);
      return response.data;
    },
    staleTime: 5_000,
  });
}

/**
 * Fetch events by type from DuckDB
 */
export function useDuckDBEventsByType(type: string, limit = 100) {
  return useQuery({
    queryKey: ["duckdb-events-by-type", type, limit],
    queryFn: async () => {
      const response = await getEventsByType(type, limit);
      return response.data;
    },
    enabled: !!type,
    staleTime: 10_000,
  });
}

/**
 * Fetch events by template from DuckDB
 */
export function useDuckDBEventsByTemplate(templateId: string, limit = 100) {
  return useQuery({
    queryKey: ["duckdb-events-by-template", templateId, limit],
    queryFn: async () => {
      const response = await getEventsByTemplate(templateId, limit);
      return response.data;
    },
    enabled: !!templateId,
    staleTime: 10_000,
  });
}

/**
 * Get total event count from DuckDB
 */
export function useDuckDBEventsCount() {
  return useQuery({
    queryKey: ["duckdb-events-count"],
    queryFn: async () => {
      const response = await getEventsCount();
      return response.count;
    },
    staleTime: 30_000,
  });
}

/**
 * Get overview stats from DuckDB
 */
export function useDuckDBOverviewStats() {
  return useQuery({
    queryKey: ["duckdb-overview-stats"],
    queryFn: getOverviewStats,
    staleTime: 30_000,
  });
}

/**
 * Get templates list from DuckDB
 */
export function useDuckDBTemplatesList() {
  return useQuery({
    queryKey: ["duckdb-templates-list"],
    queryFn: async () => {
      const response = await getTemplatesList();
      return response.data;
    },
    staleTime: 60_000,
  });
}

/**
 * Search events in DuckDB
 */
export function useDuckDBSearch(params: SearchParams, enabled = true) {
  return useQuery({
    queryKey: ["duckdb-search", params],
    queryFn: async () => {
      const response = await searchEvents(params);
      return response.data;
    },
    enabled: enabled && (!!params.q || !!params.type || !!params.template || !!params.party),
    staleTime: 10_000,
  });
}

export type { LedgerEvent, TemplateInfo, OverviewStats, SearchParams };
