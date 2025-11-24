import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

export interface LedgerUpdate {
  id: string;
  round: number;
  timestamp: string;
  update_type: string;
  update_data: any;
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

export function useLedgerUpdatesByRound(round: number | undefined, limit: number = 50) {
  return useQuery({
    queryKey: ["ledgerUpdates", round, limit],
    queryFn: async () => {
      if (!round) return [];

      const { data, error } = await supabase
        .from("ledger_updates")
        .select("*")
        .eq("round", round)
        .order("created_at", { ascending: false })
        .limit(limit);

      if (error) throw error;
      return data as LedgerUpdate[];
    },
    enabled: !!round,
    staleTime: 5_000,
  });
}
