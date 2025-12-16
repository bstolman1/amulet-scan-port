import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

export interface ServerAggregationResult {
  sum: number;
  count: number;
  templateCount: number;
}

interface AggregateResponse {
  sum: number;
  count: number;
  templateCount?: number;
}

export function useTemplateSumServer(
  snapshotId: string | undefined,
  templateSuffix: string,
  mode: "circulating" | "locked",
  enabled: boolean = true,
) {
  return useQuery<ServerAggregationResult, Error>({
    queryKey: ["server-template-sum", templateSuffix, mode],
    queryFn: async () => {
      // Use DuckDB aggregation endpoint
      const response = await apiFetch<AggregateResponse>(
        `/api/acs/aggregate?template=${encodeURIComponent(templateSuffix)}&mode=${mode}`
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
