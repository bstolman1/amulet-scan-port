import { useQuery } from "@tanstack/react-query";
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
