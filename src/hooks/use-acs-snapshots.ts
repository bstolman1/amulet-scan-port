import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import {
  getACSSnapshots as getLocalACSSnapshots,
  getLatestACSSnapshot as getLocalLatestACSSnapshot,
  getACSTemplates as getLocalACSTemplates,
  apiFetch,
  isApiAvailable,
} from "@/lib/duckdb-api-client";

export interface ACSSnapshot {
  id: string;
  timestamp: string;
  migration_id: number;
  record_time: string;
  sv_url?: string;
  canonical_package?: string | null;
  amulet_total?: number;
  locked_total?: number;
  circulating_supply?: number;
  entry_count: number;
  status: "processing" | "completed" | "failed" | string;
  error_message?: string | null;
  created_at?: string;
  updated_at?: string;
  source?: string;
}

export interface ACSTemplateStats {
  id?: string;
  snapshot_id?: string;
  template_id: string;
  contract_count: number;
  field_sums?: Record<string, string> | null;
  status_tallies?: Record<string, number> | null;
  storage_path?: string | null;
  created_at?: string;
  entity_name?: string;
  module_name?: string;
}

export function useACSSnapshots() {
  return useQuery({
    queryKey: ["acsSnapshots"],
    queryFn: async (): Promise<ACSSnapshot[]> => {
      const available = await isApiAvailable();
      if (!available) {
        throw new Error("Local DuckDB server is not available. Start with: cd server && npm start");
      }
      const response = await getLocalACSSnapshots();
      return (response.data as ACSSnapshot[]) || [];
    },
    staleTime: 30_000,
    retry: false,
  });
}

export function useLatestACSSnapshot() {
  return useQuery({
    queryKey: ["latestAcsSnapshot"],
    queryFn: async (): Promise<ACSSnapshot | null> => {
      const available = await isApiAvailable();
      if (!available) {
        throw new Error("Local DuckDB server is not available. Start with: cd server && npm start");
      }
      const response = await getLocalLatestACSSnapshot();
      return (response.data as ACSSnapshot) || null;
    },
    staleTime: 30_000,
    retry: false,
  });
}

// Hook that returns the latest snapshot regardless of status
export function useActiveSnapshot() {
  return useQuery({
    queryKey: ["activeAcsSnapshot"],
    queryFn: async () => {
      const available = await isApiAvailable();
      if (!available) {
        throw new Error("Local DuckDB server is not available");
      }
      const response = await getLocalLatestACSSnapshot();
      if (response.data) {
        return { 
          snapshot: response.data as ACSSnapshot, 
          isProcessing: response.data.status === "processing" 
        };
      }
      return { snapshot: null, isProcessing: false };
    },
    staleTime: 30_000,
    retry: false,
  });
}

export function useTemplateStats(snapshotId: string | undefined) {
  return useQuery({
    queryKey: ["acsTemplateStats", snapshotId],
    queryFn: async (): Promise<ACSTemplateStats[]> => {
      const response = await getLocalACSTemplates(100);
      return response.data.map(t => ({
        id: t.template_id,
        snapshot_id: snapshotId,
        template_id: t.template_id,
        contract_count: t.contract_count,
        entity_name: t.entity_name,
        module_name: t.module_name,
      })) as ACSTemplateStats[];
    },
    enabled: !!snapshotId,
    staleTime: 60_000,
  });
}

export function useTriggerACSSnapshot() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async () => {
      // Trigger snapshot via the local API
      const response = await apiFetch<{ success: boolean; message: string }>("/api/acs/trigger-snapshot", {
        method: "POST",
      });
      return response;
    },
    onSuccess: (data) => {
      toast.success("ACS snapshot triggered", {
        description: data.message || "Snapshot process started",
      });
      queryClient.invalidateQueries({ queryKey: ["acsSnapshots"] });
    },
    onError: (error: Error) => {
      toast.error("Failed to trigger ACS snapshot", {
        description: error.message,
      });
    },
  });
}
