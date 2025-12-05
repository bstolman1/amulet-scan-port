import { useQuery } from "@tanstack/react-query";
import { getBackfillCursors, getBackfillStats, isApiAvailable } from "@/lib/duckdb-api-client";
import { supabase } from "@/integrations/supabase/client";

export interface BackfillCursor {
  id: string;
  cursor_name: string;
  last_processed_round: number;
  updated_at: string;
  complete?: boolean | null;
  min_time?: string | null;
  max_time?: string | null;
  migration_id?: number | null;
  synchronizer_id?: string | null;
  last_before?: string | null;
}

export interface BackfillStats {
  totalUpdates: number;
  totalEvents: number;
  activeMigrations: number;
  totalCursors: number;
  completedCursors: number;
}

export function useBackfillCursors() {
  return useQuery({
    queryKey: ["backfillCursors"],
    queryFn: async () => {
      // Try DuckDB API first (reads from local cursor file)
      const duckdbAvailable = await isApiAvailable();
      
      if (duckdbAvailable) {
        try {
          const result = await getBackfillCursors();
          return result.data as BackfillCursor[];
        } catch (e) {
          console.warn("DuckDB API cursor fetch failed, falling back to Supabase:", e);
        }
      }

      // Fallback to Supabase
      const { data, error } = await supabase
        .from("backfill_cursors")
        .select("*")
        .order("updated_at", { ascending: false });

      if (error) throw error;
      return data as BackfillCursor[];
    },
    staleTime: 10_000,
  });
}

export function useBackfillStats() {
  return useQuery({
    queryKey: ["backfillStats"],
    queryFn: async () => {
      // Try DuckDB API first (reads from Parquet files)
      const duckdbAvailable = await isApiAvailable();
      
      if (duckdbAvailable) {
        try {
          return await getBackfillStats();
        } catch (e) {
          console.warn("DuckDB API stats fetch failed, falling back to Supabase:", e);
        }
      }

      // Fallback to Supabase
      const [updatesCount, eventsCount, migrationsResult] = await Promise.all([
        supabase.from("ledger_updates").select("*", { count: "exact", head: true }),
        supabase.from("ledger_events").select("*", { count: "exact", head: true }),
        supabase.from("ledger_updates").select("migration_id").not("migration_id", "is", null),
      ]);

      const uniqueMigrations = new Set(migrationsResult.data?.map(row => row.migration_id) || []);

      return {
        totalUpdates: updatesCount.count || 0,
        totalEvents: eventsCount.count || 0,
        activeMigrations: uniqueMigrations.size,
        totalCursors: 0,
        completedCursors: 0,
      } as BackfillStats;
    },
    staleTime: 10_000,
    refetchInterval: 30_000, // Auto-refresh every 30s
  });
}

export function useBackfillCursorByName(cursorName: string | undefined) {
  return useQuery({
    queryKey: ["backfillCursors", cursorName],
    queryFn: async () => {
      if (!cursorName) return null;

      const { data, error } = await supabase
        .from("backfill_cursors")
        .select("*")
        .eq("cursor_name", cursorName)
        .maybeSingle();

      if (error) throw error;
      return data as BackfillCursor | null;
    },
    enabled: !!cursorName,
    staleTime: 10_000,
  });
}
