import { useQuery } from "@tanstack/react-query";
import {
  getACSSnapshots,
  getLatestACSSnapshot,
  getACSTemplates,
  getACSContracts,
  getACSStats,
  getACSSupply,
  getACSStatus,
  isApiAvailable,
  type ACSSnapshot,
  type ACSTemplateStats,
  type ACSStats,
  type ACSStatusResponse,
} from "@/lib/duckdb-api-client";

/**
 * Hook to get ACS availability status (for graceful degradation during snapshots)
 */
export function useACSStatus() {
  return useQuery({
    queryKey: ["acsStatus"],
    queryFn: async () => {
      try {
        return await getACSStatus();
      } catch (err) {
        return {
          available: false,
          snapshotInProgress: false,
          completeSnapshotCount: 0,
          inProgressSnapshotCount: 0,
          latestComplete: null,
          message: 'DuckDB ACS not available - using live Scan API',
          error: err instanceof Error ? err.message : 'Unknown error',
        } as ACSStatusResponse;
      }
    },
    staleTime: 60_000, // Check less frequently since DuckDB is optional
    refetchInterval: false, // Disable auto-refresh - DuckDB is optional
    retry: false, // Don't retry failed requests
  });
}

/**
 * Hook to check if DuckDB API is available for ACS data
 * Now uses the status endpoint for better accuracy during snapshots
 */
export function useLocalACSAvailable() {
  return useQuery({
    queryKey: ["localACSAvailable"],
    queryFn: async () => {
      try {
        // First check if API is reachable at all
        const available = await isApiAvailable();
        if (!available) return { available: false, reason: 'api_unreachable' };
        
        // Use status endpoint to check for complete snapshots
        const status = await getACSStatus();
        if (status.available) {
          return { available: true, reason: 'complete_snapshot_available' };
        }
        
        if (status.snapshotInProgress) {
          return { available: false, reason: 'snapshot_in_progress', message: status.message };
        }
        
        return { available: false, reason: 'no_data' };
      } catch {
        return { available: false, reason: 'error' };
      }
    },
    staleTime: 10_000,
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

export type { ACSSnapshot, ACSTemplateStats, ACSStats, ACSStatusResponse };
