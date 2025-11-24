import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

interface UseLedgerUpdatesOptions {
  limit?: number;
  offset?: number;
}

export function useLedgerUpdates(options: UseLedgerUpdatesOptions = {}) {
  const { limit = 50, offset = 0 } = options;

  return useQuery({
    queryKey: ["ledger-updates", limit, offset],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("ledger_updates")
        .select("*")
        .order("round", { ascending: false })
        .range(offset, offset + limit - 1);

      if (error) throw error;
      return data;
    },
  });
}
