import { useQuery } from "@tanstack/react-query";
import { getACSContracts as getLocalACSContracts } from "@/lib/duckdb-api-client";

/**
 * Fetch and aggregate data across all templates matching a suffix
 * Uses DuckDB/Parquet backend exclusively
 */
export function useAggregatedTemplateData(
  snapshotId: string | undefined,
  templateSuffix: string,
  enabled: boolean = true,
) {
  return useQuery({
    queryKey: ["aggregated-template-data", templateSuffix],
    queryFn: async () => {
      if (!templateSuffix) {
        throw new Error("Missing templateSuffix");
      }

      // Use DuckDB for all template data queries
      console.log(`[useAggregatedTemplateData] Using DuckDB for template=${templateSuffix}`);
      
      const response = await getLocalACSContracts({ 
        template: templateSuffix,
        limit: 100000 
      });
      
      const totalCount = response.count ?? response.data?.length ?? 0;
      
      console.log(`[useAggregatedTemplateData] DuckDB returned ${response.data?.length || 0} contracts (total: ${totalCount}) for template=${templateSuffix}`);
      
      return {
        data: response.data || [],
        templateCount: 1,
        totalContracts: totalCount,
        templateIds: [templateSuffix],
        source: "duckdb",
      };
    },
    enabled: enabled && !!templateSuffix,
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
}
