import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

export interface LedgerUpdate {
  update_id: string;
  migration_id: number | null;
  synchronizer_id: string | null;
  record_time: string | null;
  effective_at: string | null;
  offset: string | null;
  workflow_id: string | null;
  kind: string | null;
  raw: any;
  created_at: string;
}

export function useLedgerUpdates(limit: number = 50) {
  return useQuery({
    queryKey: ["ledgerUpdates", limit],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("ledger_updates")
        .select("*")
        .order("created_at", { ascending: false })
        .limit(limit);

      if (error) throw error;
      return data as LedgerUpdate[];
    },
    staleTime: 5_000,
  });
}

export function useLedgerUpdatesByMigration(migrationId: number | undefined, limit: number = 50) {
  return useQuery({
    queryKey: ["ledgerUpdates", migrationId, limit],
    queryFn: async () => {
      if (!migrationId) return [];

      const { data, error } = await supabase
        .from("ledger_updates")
        .select("*")
        .eq("migration_id", migrationId)
        .order("created_at", { ascending: false })
        .limit(limit);

      if (error) throw error;
      return data as LedgerUpdate[];
    },
    enabled: !!migrationId,
    staleTime: 5_000,
  });
}
