import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { toast } from "sonner";

export interface ACSSnapshot {
  id: string;
  timestamp: string;
  migration_id: number;
  record_time: string;
  sv_url: string;
  canonical_package: string | null;
  amulet_total: number;
  locked_total: number;
  circulating_supply: number;
  entry_count: number;
  status: "processing" | "completed" | "failed";
  error_message: string | null;
  created_at: string;
  updated_at: string;
}

export interface ACSTemplateStats {
  id: string;
  snapshot_id: string;
  template_id: string;
  contract_count: number;
  field_sums: Record<string, string> | null;
  status_tallies: Record<string, number> | null;
  storage_path: string | null;
  created_at: string;
}

export function useACSSnapshots() {
  return useQuery({
    queryKey: ["acsSnapshots"],
    queryFn: async () => {
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
  return useQuery({
    queryKey: ["latestAcsSnapshot"],
    queryFn: async () => {
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
  return useQuery({
    queryKey: ["activeAcsSnapshot"],
    queryFn: async () => {
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
  return useQuery({
    queryKey: ["acsTemplateStats", snapshotId],
    queryFn: async () => {
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
