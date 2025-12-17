import { useQuery } from "@tanstack/react-query";
import { checkDuckDBConnection } from "@/lib/backend-config";
import { getACSTemplates as getLocalACSTemplates, getACSContracts as getLocalACSContracts } from "@/lib/duckdb-api-client";
// Cached DuckDB availability check
let duckDBAvailable: boolean | null = null;
let duckDBCheckTime = 0;
async function isDuckDBAvailable(): Promise<boolean> {
  const now = Date.now();
  if (duckDBAvailable !== null && now - duckDBCheckTime < 30_000) {
    return duckDBAvailable;
  }
  duckDBAvailable = await checkDuckDBConnection();
  duckDBCheckTime = now;
  return duckDBAvailable;
}

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
 * Fetch template data from Supabase Storage or local DuckDB for a given snapshot
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

      const isAvailable = await isDuckDBAvailable();
      if (!isAvailable) {
        throw new Error("Local DuckDB server is not available. Start with: cd server && npm start");
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
 * Get all available templates for a snapshot (with DuckDB fallback)
 */
export function useACSTemplates(snapshotId: string | undefined) {
  return useQuery({
    queryKey: ["acs-templates"],
    queryFn: async () => {
      const isAvailable = await isDuckDBAvailable();
      if (!isAvailable) {
        throw new Error("Local DuckDB server is not available. Start with: cd server && npm start");
      }

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
