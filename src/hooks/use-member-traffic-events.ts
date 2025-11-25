import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

export function useMemberTrafficEvents() {
  return useQuery({
    queryKey: ["memberTrafficEvents"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("ledger_events")
        .select("*")
        .or(
          "template_id.ilike.%MemberTraffic%,event_type.ilike.%traffic%,event_type.ilike.%synchronizer%"
        )
        .order("created_at", { ascending: false })
        .limit(200);

      if (error) throw error;
      return data;
    },
    staleTime: 30_000,
  });
}
