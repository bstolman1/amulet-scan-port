import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

interface AggregationResult {
  sum: number;
  count: number;
  templateCount: number;
}

interface AggregateResponse {
  sum: number;
  count: number;
  templateCount?: number;
}

/**
 * Aggregates template data using DuckDB backend.
 * The pickFn is no longer used client-side - aggregation happens server-side.
 */
export function useAggregatedTemplateSum(
  snapshotId: string | undefined,
  templateSuffix: string,
  pickFn: (obj: any) => number,
  enabled: boolean = true,
) {
  return useQuery<AggregationResult, Error>({
    queryKey: ["aggregated-template-sum", templateSuffix],
    queryFn: async () => {
      // Use DuckDB aggregation endpoint
      const response = await apiFetch<AggregateResponse>(
        `/api/acs/aggregate?template=${encodeURIComponent(templateSuffix)}`
      );
      return {
        sum: response.sum || 0,
        count: response.count || 0,
        templateCount: response.templateCount || 1,
      };
    },
    enabled: enabled && !!templateSuffix,
    staleTime: 5 * 60 * 1000,
  });
}
