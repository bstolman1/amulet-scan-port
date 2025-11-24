import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

export interface BackfillCursor {
  id: string;
  migration_id: number;
  synchronizer_id: string;
  min_time: string | null;
  max_time: string | null;
  last_before: string | null;
  complete: boolean;
  created_at: string;
  updated_at: string;
}

export function useBackfillCursors() {
  return useQuery({
    queryKey: ["backfillCursors"],
    queryFn: async () => {
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

export function useBackfillCursorsByMigration(migrationId: number | undefined) {
  return useQuery({
    queryKey: ["backfillCursors", migrationId],
    queryFn: async () => {
      if (!migrationId) return [];

      const { data, error } = await supabase
        .from("backfill_cursors")
        .select("*")
        .eq("migration_id", migrationId)
        .order("updated_at", { ascending: false });

      if (error) throw error;
      return data as BackfillCursor[];
    },
    enabled: !!migrationId,
    staleTime: 10_000,
  });
}
