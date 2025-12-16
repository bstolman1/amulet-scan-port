import { useQuery } from "@tanstack/react-query";
import {
  getACSSnapshots,
  getLatestACSSnapshot,
  getACSTemplates,
  getACSContracts,
  getACSStats,
  getACSSupply,
  isApiAvailable,
  getActiveContractsByTemplate,
  getTemplatesList,
  getOverviewStats,
  type ACSSnapshot,
  type ACSTemplateStats,
  type ACSStats,
} from "@/lib/duckdb-api-client";

/**
 * Hook to check if DuckDB API is available for data
 * Primary check: updates data availability (not ACS)
 */
export function useLocalACSAvailable() {
  return useQuery({
    queryKey: ["localDataAvailable"],
    queryFn: async () => {
      try {
        // First check if API is reachable at all
        const available = await isApiAvailable();
        if (!available) return false;
        
        // Check if updates data exists (primary source)
        try {
          const stats = await getOverviewStats();
          if (stats?.total_events > 0) return true;
        } catch {}
        
        // Fallback: check ACS data (redundancy)
        try {
          const acsStats = await getACSStats();
          return acsStats?.data?.total_contracts > 0;
        } catch {}
        
        return false;
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
 * Hook to fetch active contracts by template from updates (created - archived)
 * This is the PRIMARY source for contract data
 */
export function useActiveContracts(templateSuffix: string, limit = 1000, enabled = true) {
  return useQuery({
    queryKey: ["activeContracts", templateSuffix, limit],
    queryFn: async () => {
      const response = await getActiveContractsByTemplate(templateSuffix, limit);
      return response.data;
    },
    enabled: enabled && !!templateSuffix,
    staleTime: 60_000,
  });
}

/**
 * Hook to fetch template list from updates data
 */
export function useTemplatesList() {
  return useQuery({
    queryKey: ["templatesList"],
    queryFn: async () => {
      const response = await getTemplatesList();
      return response.data;
    },
    staleTime: 60_000,
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// ACS-based hooks (for redundancy/fallback only)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Hook to fetch ACS snapshots from local DuckDB (redundancy only)
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
 * Hook to fetch the latest ACS snapshot from local DuckDB (redundancy only)
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
 * Hook to fetch ACS template statistics from local DuckDB (redundancy only)
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
 * Hook to fetch ACS contracts by template from local DuckDB (redundancy only)
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
 * Hook to fetch ACS overview statistics from local DuckDB (redundancy only)
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
 * Hook to fetch ACS supply data from local DuckDB (redundancy only)
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
