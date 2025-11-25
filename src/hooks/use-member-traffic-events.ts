import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

export function useMemberTrafficEvents() {
  return useQuery({
    queryKey: ["memberTrafficEvents"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("ledger_events")
        .select("*")
        .like("template_id", "%MemberTraffic%")
        .order("created_at", { ascending: false })
        .limit(200);

      if (error) throw error;
      return data;
    },
    staleTime: 30_000,
  });
}
