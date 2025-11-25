import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

export interface LedgerUpdate {
  id: string;
  timestamp: string;
  update_type: string;
  update_data: any;
  created_at: string;
  migration_id?: number | null;
  synchronizer_id?: string | null;
  update_id?: string | null;
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

export function useLedgerUpdatesByTimestamp(timestamp: string | undefined, limit: number = 50) {
  return useQuery({
    queryKey: ["ledgerUpdates", timestamp, limit],
    queryFn: async () => {
      if (!timestamp) return [];

      const { data, error } = await supabase
        .from("ledger_updates")
        .select("*")
        .gte("created_at", timestamp)
        .order("created_at", { ascending: false })
        .limit(limit);

      if (error) throw error;
      return data as LedgerUpdate[];
    },
    enabled: !!timestamp,
    staleTime: 5_000,
  });
}
