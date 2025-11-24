import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

export function useBackfillCursors() {
  return useQuery({
    queryKey: ["backfill-cursors"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("backfill_cursors")
        .select("*")
        .order("cursor_name", { ascending: true });

      if (error) throw error;
      return data;
    },
  });
}
