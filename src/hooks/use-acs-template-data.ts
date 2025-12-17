import { useQuery } from "@tanstack/react-query";
import { getACSTemplates as getLocalACSTemplates, getACSContracts as getLocalACSContracts } from "@/lib/duckdb-api-client";

interface TemplateDataMetadata {
  template_id: string;
  snapshot_timestamp: string;
  entry_count: number;
}

interface TemplateDataResponse<T = any> {
  metadata: TemplateDataMetadata;
  data: T[];
}

/**
 * Fetch template data from local DuckDB for a given snapshot
 */
export function useACSTemplateData<T = any>(
  snapshotId: string | undefined,
  templateId: string,
  enabled: boolean = true,
) {
  return useQuery({
    queryKey: ["acs-template-data", templateId],
    queryFn: async (): Promise<TemplateDataResponse<T>> => {
      if (!templateId) {
        throw new Error("Missing templateId");
      }

      console.log(`[useACSTemplateData] Fetching from DuckDB: ${templateId}`);
      const response = await getLocalACSContracts({ template: templateId, limit: 100 });
      
      return {
        metadata: {
          template_id: templateId,
          snapshot_timestamp: new Date().toISOString(),
          entry_count: response.data.length,
        },
        data: response.data as T[],
      };
    },
    enabled: enabled && !!templateId,
    staleTime: 5 * 60 * 1000,
    retry: false,
  });
}

/**
 * Get all available templates for a snapshot
 */
export function useACSTemplates(snapshotId: string | undefined) {
  return useQuery({
    queryKey: ["acs-templates"],
    queryFn: async () => {
      console.log("[useACSTemplates] Fetching from DuckDB");
      const response = await getLocalACSTemplates(500);
      
      return response.data.map(t => ({
        template_id: t.template_id,
        contract_count: t.contract_count,
        storage_path: null,
        entity_name: t.entity_name,
        module_name: t.module_name,
      }));
    },
    enabled: true,
    staleTime: 5 * 60 * 1000,
    retry: false,
  });
}
