/**
 * Hook for fetching dashboard stats from local DuckDB API
 */

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
  return useQuery({
    queryKey: ['local-api-available'],
    queryFn: async () => {
      return await checkDuckDBConnection();
    },
    staleTime: 30000, // Cache for 30 seconds
    retry: false,
  });
}

/**
 * Fetch overview stats from local DuckDB API
 */
export function useLocalOverviewStats() {
  return useQuery({
    queryKey: ['local-overview-stats'],
    queryFn: async () => {
      const stats = await duckdbApi.getOverviewStats();
      return stats as LocalDashboardStats;
    },
    staleTime: 60000, // Cache for 1 minute
    retry: 2,
  });
}

/**
 * Fetch daily stats from local DuckDB API
 */
export function useLocalDailyStats(days = 30) {
  return useQuery({
    queryKey: ['local-daily-stats', days],
    queryFn: async () => {
      const response = await duckdbApi.getDailyStats(days);
      return response.data as DailyStats[];
    },
    staleTime: 60000,
    retry: 2,
  });
}

/**
 * Fetch stats by template from local DuckDB API
 */
export function useLocalTemplateStats(limit = 50) {
  return useQuery({
    queryKey: ['local-template-stats', limit],
    queryFn: async () => {
      const response = await duckdbApi.getStatsByTemplate(limit);
      return response.data as TemplateStats[];
    },
    staleTime: 60000,
    retry: 2,
  });
}

/**
 * Fetch stats by type from local DuckDB API
 */
export function useLocalTypeStats() {
  return useQuery({
    queryKey: ['local-type-stats'],
    queryFn: async () => {
      const response = await duckdbApi.getStatsByType();
      return response.data as TypeStats[];
    },
    staleTime: 60000,
    retry: 2,
  });
}

/**
 * Fetch hourly stats from local DuckDB API
 */
export function useLocalHourlyStats() {
  return useQuery({
    queryKey: ['local-hourly-stats'],
    queryFn: async () => {
      const response = await duckdbApi.getHourlyStats();
      return response.data;
    },
    staleTime: 60000,
    retry: 2,
  });
}

/**
 * Fetch burn stats from local DuckDB API
 */
export function useLocalBurnStats() {
  return useQuery({
    queryKey: ['local-burn-stats'],
    queryFn: async () => {
      const response = await duckdbApi.getBurnStats();
      return response.data;
    },
    staleTime: 60000,
    retry: 2,
  });
}
