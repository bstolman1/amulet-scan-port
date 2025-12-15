import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { toast } from "sonner";
import { useDuckDBForLedger, checkDuckDBConnection } from "@/lib/backend-config";
import {
  getACSSnapshots as getLocalACSSnapshots,
  getLatestACSSnapshot as getLocalLatestACSSnapshot,
  getACSTemplates as getLocalACSTemplates,
} from "@/lib/duckdb-api-client";

// Cached DuckDB availability check to avoid repeated failed fetches
let duckDBAvailable: boolean | null = null;
let duckDBCheckTime = 0;
async function isDuckDBAvailable(): Promise<boolean> {
  const now = Date.now();
  // Re-check every 30 seconds
  if (duckDBAvailable !== null && now - duckDBCheckTime < 30_000) {
    return duckDBAvailable;
  }
  duckDBAvailable = await checkDuckDBConnection();
  duckDBCheckTime = now;
  console.log(`[Backend] DuckDB available: ${duckDBAvailable}`);
  return duckDBAvailable;
}

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
  const useDuckDB = useDuckDBForLedger();

  return useQuery({
    queryKey: ["acsSnapshots", useDuckDB ? "duckdb" : "supabase"],
    queryFn: async () => {
      // Only try DuckDB if configured AND server is available
      if (useDuckDB && await isDuckDBAvailable()) {
        try {
          const response = await getLocalACSSnapshots();
          return response.data as ACSSnapshot[];
        } catch (error) {
          console.warn("DuckDB ACS fetch failed, falling back to Supabase:", error);
        }
      }

      const { data, error } = await supabase
        .from("acs_snapshots")
        .select("*")
        .order("timestamp", { ascending: false })
        .limit(10);

      if (error) throw error;
      return data as ACSSnapshot[];
    },
    staleTime: 30_000,
  });
}

export function useLatestACSSnapshot() {
  const useDuckDB = useDuckDBForLedger();

  return useQuery({
    queryKey: ["latestAcsSnapshot", useDuckDB ? "duckdb" : "supabase"],
    queryFn: async () => {
      // Only try DuckDB if configured AND server is available
      if (useDuckDB && await isDuckDBAvailable()) {
        try {
          const response = await getLocalLatestACSSnapshot();
          return response.data as ACSSnapshot | null;
        } catch (error) {
          console.warn("DuckDB ACS fetch failed, falling back to Supabase:", error);
        }
      }

      const { data, error } = await supabase
        .from("acs_snapshots")
        .select("*")
        .eq("status", "completed")
        .order("timestamp", { ascending: false })
        .limit(1)
        .maybeSingle();

      if (error) throw error;
      return data as ACSSnapshot | null;
    },
    staleTime: 30_000,
  });
}

// Hook that returns the latest snapshot regardless of status (for pages that need data even from processing snapshots)
export function useActiveSnapshot() {
  const useDuckDB = useDuckDBForLedger();

  return useQuery({
    queryKey: ["activeAcsSnapshot", useDuckDB ? "duckdb" : "supabase"],
    queryFn: async () => {
      // Only try DuckDB if configured AND server is available
      if (useDuckDB && await isDuckDBAvailable()) {
        try {
          const response = await getLocalLatestACSSnapshot();
          if (response.data) {
            return { snapshot: response.data as ACSSnapshot, isProcessing: false };
          }
        } catch (error) {
          console.warn("DuckDB ACS fetch failed, falling back to Supabase:", error);
        }
      }

      // First try to get latest completed snapshot
      const { data: completed, error: completedError } = await supabase
        .from("acs_snapshots")
        .select("*")
        .eq("status", "completed")
        .order("timestamp", { ascending: false })
        .limit(1)
        .maybeSingle();

      if (completedError) throw completedError;

      // If we have a completed snapshot, return it
      if (completed) {
        return { snapshot: completed as ACSSnapshot, isProcessing: false };
      }

      // Otherwise, fall back to most recent snapshot regardless of status
      const { data: latest, error: latestError } = await supabase
        .from("acs_snapshots")
        .select("*")
        .order("timestamp", { ascending: false })
        .limit(1)
        .maybeSingle();

      if (latestError) throw latestError;

      return {
        snapshot: latest as ACSSnapshot | null,
        isProcessing: latest?.status === "processing",
      };
    },
    staleTime: 30_000,
  });
}

export function useTemplateStats(snapshotId: string | undefined) {
  const useDuckDB = useDuckDBForLedger();

  return useQuery({
    queryKey: ["acsTemplateStats", snapshotId, useDuckDB ? "duckdb" : "supabase"],
    queryFn: async () => {
      // Only try DuckDB if configured AND server is available
      if (useDuckDB && await isDuckDBAvailable()) {
        try {
          const response = await getLocalACSTemplates(100);
          return response.data.map(t => ({
            id: t.template_id,
            snapshot_id: snapshotId,
            template_id: t.template_id,
            contract_count: t.contract_count,
            entity_name: t.entity_name,
            module_name: t.module_name,
          })) as ACSTemplateStats[];
        } catch (error) {
          console.warn("DuckDB ACS fetch failed, falling back to Supabase:", error);
        }
      }

      if (!snapshotId) return [];

      const { data, error } = await supabase
        .from("acs_template_stats")
        .select("*")
        .eq("snapshot_id", snapshotId)
        .order("contract_count", { ascending: false });

      if (error) throw error;
      return data as ACSTemplateStats[];
    },
    enabled: !!snapshotId,
    staleTime: 60_000,
  });
}

export function useTriggerACSSnapshot() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async () => {
      const { data, error } = await supabase.functions.invoke("fetch-acs-snapshot");

      if (error) throw error;
      return data;
    },
    onSuccess: (data) => {
      toast.success("ACS snapshot started", {
        description: `Snapshot ID: ${data.snapshot_id}`,
      });
      queryClient.invalidateQueries({ queryKey: ["acsSnapshots"] });
    },
    onError: (error: Error) => {
      toast.error("Failed to start ACS snapshot", {
        description: error.message,
      });
    },
  });
}
