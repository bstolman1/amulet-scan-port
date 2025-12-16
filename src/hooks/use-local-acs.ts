import { useQuery } from "@tanstack/react-query";
import {
  getACSSnapshots,
  getLatestACSSnapshot,
  getACSTemplates,
  getACSContracts,
  getACSStats,
  getACSSupply,
  isApiAvailable,
  type ACSSnapshot,
  type ACSTemplateStats,
  type ACSStats,
} from "@/lib/duckdb-api-client";

/**
 * Hook to check if DuckDB API is available for ACS data
 */
export function useLocalACSAvailable() {
  return useQuery({
    queryKey: ["localACSAvailable"],
    queryFn: async () => {
      try {
        // First check if API is reachable at all
        const available = await isApiAvailable();
        if (!available) return false;
        
        // Then verify ACS data actually exists
        const stats = await getACSStats();
        return stats?.data?.total_contracts > 0;
      } catch {
        return false;
      }
    },
    staleTime: 60_000,
    retry: false,
    refetchOnWindowFocus: false,
  });
}

/**
 * Hook to fetch ACS snapshots from local DuckDB
 */
export function useLocalACSSnapshots() {
  return useQuery({
    queryKey: ["localACSSnapshots"],
    queryFn: async () => {
      const response = await getACSSnapshots();
      return response.data;
    },
    staleTime: 30_000,
  });
}

/**
 * Hook to fetch the latest ACS snapshot from local DuckDB
 */
export function useLocalLatestACSSnapshot() {
  return useQuery({
    queryKey: ["localLatestACSSnapshot"],
    queryFn: async () => {
      const response = await getLatestACSSnapshot();
      return response.data;
    },
    staleTime: 30_000,
  });
}

/**
 * Hook to fetch ACS template statistics from local DuckDB
 */
export function useLocalACSTemplates(limit = 100) {
  return useQuery({
    queryKey: ["localACSTemplates", limit],
    queryFn: async () => {
      const response = await getACSTemplates(limit);
      return response.data;
    },
    staleTime: 60_000,
  });
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
  const { enabled = true, ...queryParams } = params;
  
  return useQuery({
    queryKey: ["localACSContracts", queryParams],
    queryFn: async () => {
      const response = await getACSContracts(queryParams);
      return response.data;
    },
    enabled,
    staleTime: 60_000,
  });
}

/**
 * Hook to fetch ACS overview statistics from local DuckDB
 */
export function useLocalACSStats() {
  return useQuery({
    queryKey: ["localACSStats"],
    queryFn: async () => {
      const response = await getACSStats();
      return response.data;
    },
    staleTime: 30_000,
  });
}

/**
 * Hook to fetch ACS supply data from local DuckDB
 */
export function useLocalACSSupply() {
  return useQuery({
    queryKey: ["localACSSupply"],
    queryFn: async () => {
      const response = await getACSSupply();
      return response.data;
    },
    staleTime: 30_000,
  });
}

export type { ACSSnapshot, ACSTemplateStats, ACSStats };
