import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

export function useGovernanceEvents() {
  return useQuery({
    queryKey: ["governanceEvents"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("ledger_events")
        .select("*")
        .or(
          "event_type.ilike.%vote%,event_type.ilike.%proposal%,template_id.ilike.%VoteRequest%,template_id.ilike.%Confirmation%"
        )
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
        .or(
          "event_type.ilike.%reward%,template_id.ilike.%RewardCoupon%,template_id.ilike.%ClaimReward%"
        )
        .order("created_at", { ascending: false })
        .limit(500);

      if (error) throw error;
      return data;
    },
    staleTime: 30_000,
  });
}
