import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

interface UseAcsSnapshotsOptions {
  limit?: number;
  offset?: number;
}

export function useAcsSnapshots(options: UseAcsSnapshotsOptions = {}) {
  const { limit = 10, offset = 0 } = options;

  return useQuery({
    queryKey: ["acs-snapshots", limit, offset],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("acs_snapshots")
        .select("*")
        .order("round", { ascending: false })
        .range(offset, offset + limit - 1);

      if (error) throw error;
      return data;
    },
  });
}
