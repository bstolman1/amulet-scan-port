import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

export function useGovernanceEvents() {
  return useQuery({
    queryKey: ["governanceEvents"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("ledger_events")
        .select("*")
        .like("template_id", "%Confirmation%")
        .order("created_at", { ascending: false })
        .limit(200);

      if (error) throw error;
      return data;
    },
    staleTime: 30_000,
  });
}

export function useRewardClaimEvents() {
  return useQuery({
    queryKey: ["rewardClaimEvents"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("ledger_events")
        .select("*")
        .like("template_id", "%RewardCoupon%")
        .order("created_at", { ascending: false })
        .limit(500);

      if (error) throw error;
      return data;
    },
    staleTime: 30_000,
  });
}
