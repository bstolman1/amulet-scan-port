import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { useDuckDBForLedger } from "@/lib/backend-config";
import { getLatestEvents, type LedgerEvent as DuckDBEvent } from "@/lib/duckdb-api-client";

export interface LedgerUpdate {
  id: string;
  timestamp: string;
  effective_at?: string;
  update_type: string;
  update_data: any;
  created_at: string;
  migration_id?: number | null;
  synchronizer_id?: string | null;
  update_id?: string | null;
}

/**
 * Fetch ledger updates - automatically uses DuckDB or Supabase based on config
 */
export function useLedgerUpdates(limit: number = 50) {
  const useDuckDB = useDuckDBForLedger();

  return useQuery({
    queryKey: ["ledgerUpdates", limit, useDuckDB ? "duckdb" : "supabase"],
    queryFn: async () => {
      if (useDuckDB) {
        // Use DuckDB API
        try {
          const response = await getLatestEvents(limit, 0);
          // Transform DuckDB events to LedgerUpdate format
          return response.data.map((event: DuckDBEvent) => ({
            id: event.event_id,
            timestamp: event.timestamp,
            effective_at: event.effective_at,
            update_type: event.event_type,
            update_data: event.payload,
            created_at: event.timestamp,
            migration_id: null,
            synchronizer_id: null,
            update_id: event.event_id,
          })) as LedgerUpdate[];
        } catch (error) {
          console.warn("DuckDB API unavailable, falling back to Supabase:", error);
          // Fallback to Supabase
          const { data, error: sbError } = await supabase
            .from("ledger_updates")
            .select("*")
            .order("created_at", { ascending: false })
            .limit(limit);
          if (sbError) throw sbError;
          return data as LedgerUpdate[];
        }
      }

      // Use Supabase
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
