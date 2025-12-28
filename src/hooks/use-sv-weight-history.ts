/**
 * Hook for fetching SV weight history from local DuckDB API
 */

import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

interface SvWeightEntry {
  timestamp: string;
  effectiveUntil: string | null;
  svCount: number;
  svParties: string[];
  contractId: string;
}

interface DailySvData {
  date: string;
  svCount: number;
  timestamp: string;
}

interface SvWeightHistoryResponse {
  data: SvWeightEntry[];
  dailyData: DailySvData[];
  totalRules: number;
}

export function useSvWeightHistory(limit = 100) {
  return useQuery({
    queryKey: ["sv-weight-history", limit],
    queryFn: async () => {
      const response = await apiFetch<SvWeightHistoryResponse>(
        `/api/stats/sv-weight-history?limit=${limit}`
      );
      return response;
    },
    staleTime: 60_000,
    retry: 1,
  });
}
