import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { useDuckDBForLedger } from "@/lib/backend-config";
import { getLatestUpdates, type LedgerUpdateRecord as DuckDBUpdate } from "@/lib/duckdb-api-client";

export interface LedgerUpdateRecord {
  update_id: string;
  update_type: string;
  migration_id?: number | null;
  synchronizer_id?: string | null;
  record_time?: string | null;
  effective_at?: string | null;
  timestamp?: string | null;
  workflow_id?: string | null;
  command_id?: string | null;
  kind?: string | null;
  offset?: string | number | null;
  root_event_ids?: string[] | null;
  event_count?: number | null;
  update_data?: any;
}

export function useLatestUpdates(limit: number = 100) {
  const useDuckDB = useDuckDBForLedger();

  return useQuery({
    queryKey: ["latestUpdates", limit, useDuckDB ? "duckdb" : "supabase"],
    queryFn: async () => {
      if (useDuckDB) {
        const res = await getLatestUpdates(limit, 0);
        return res.data as DuckDBUpdate[];
      }

      const { data, error } = await supabase
        .from("ledger_updates")
        .select("*")
        .order("created_at", { ascending: false })
        .limit(limit);

      if (error) throw error;
      return (data ?? []) as LedgerUpdateRecord[];
    },
    staleTime: 5_000,
    refetchInterval: 10_000,
  });
}
