import { useQuery } from "@tanstack/react-query";
import { getBackfillCursors, getBackfillStats, getWriteActivity, isApiAvailable } from "@/lib/duckdb-api-client";
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
  total_updates?: number | null;
  total_events?: number | null;
  started_at?: string | null;
  pending_writes?: number | null;
  buffered_records?: number | null;
  is_recently_updated?: boolean | null;
  error?: string | null;
}

export interface BackfillStats {
  totalUpdates: number;
  totalEvents: number;
  activeMigrations: number;
  totalCursors: number;
  completedCursors: number;
}

export interface WriteActivityState {
  isWriting: boolean;
  eventFiles: number;
  updateFiles: number;
  message: string;
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
      const duckdbAvailable = await isApiAvailable();

      // If DuckDB API is available, still also fetch Supabase cursors.
      // This prevents the UI from "missing" migrations that exist in Supabase but
      // haven't been written to local cursor files yet (common during transitions).
      if (duckdbAvailable) {
        const [duckdbResult, supabaseResult] = await Promise.all([
          getBackfillCursors().catch((e) => {
            console.warn("DuckDB API cursor fetch failed:", e);
            return { data: [] as BackfillCursor[] } as any;
          }),
          (async () => {
            try {
              const { data, error } = await supabase
                .from("backfill_cursors")
                .select("*")
                .order("updated_at", { ascending: false });
              if (error) throw error;
              return (data || []) as BackfillCursor[];
            } catch (e) {
              console.warn("Supabase cursor fetch failed:", e);
              return [] as BackfillCursor[];
            }
          })(),
        ]);

        const duckdbCursors = (duckdbResult?.data || []) as BackfillCursor[];
        const supabaseCursors = supabaseResult as BackfillCursor[];

        // Merge by a stable key (cursor_name is consistent across backends)
        const byName = new Map<string, BackfillCursor>();
        for (const c of supabaseCursors) {
          if (c?.cursor_name) byName.set(c.cursor_name, c);
        }
        for (const c of duckdbCursors) {
          if (c?.cursor_name) byName.set(c.cursor_name, { ...byName.get(c.cursor_name), ...c });
        }

        const merged = Array.from(byName.values());
        merged.sort((a, b) => {
          const at = a.updated_at ? new Date(a.updated_at).getTime() : 0;
          const bt = b.updated_at ? new Date(b.updated_at).getTime() : 0;
          return bt - at;
        });

        return merged;
      }

      // DuckDB API not available: Supabase-only
      const { data, error } = await supabase
        .from("backfill_cursors")
        .select("*")
        .order("updated_at", { ascending: false });

      if (error) throw error;
      return data as BackfillCursor[];
    },
    staleTime: 5_000,
    refetchInterval: 10_000, // Auto-refresh every 10s
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

export function useWriteActivity() {
  return useQuery({
    queryKey: ["writeActivity"],
    queryFn: async () => {
      const duckdbAvailable = await isApiAvailable();
      
      if (duckdbAvailable) {
        try {
          const result = await getWriteActivity();
          return {
            isWriting: result.isWriting,
            eventFiles: result.currentCounts.events,
            updateFiles: result.currentCounts.updates,
            message: result.message,
          } as WriteActivityState;
        } catch (e) {
          console.warn("Write activity check failed:", e);
        }
      }
      
      return { isWriting: false, eventFiles: 0, updateFiles: 0, message: "API unavailable" } as WriteActivityState;
    },
    staleTime: 5_000,
    refetchInterval: 5_000, // Check every 5 seconds
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
