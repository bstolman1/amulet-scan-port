import { useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";
import { 
  getRealtimeSupply, 
  getACSMiningRounds, 
  getRealtimeRichList,
  getACSAllocations,
  isApiAvailable 
} from "@/lib/duckdb-api-client";

/**
 * Prefetch hook for instant page loads
 * Modern explorers prefetch data on link hover so pages load instantly
 */
export function usePrefetch() {
  const queryClient = useQueryClient();

  const prefetchSupply = useCallback(async () => {
    const available = await isApiAvailable();
    if (!available) return;

    // Prefetch all supply page data in parallel
    await Promise.all([
      queryClient.prefetchQuery({
        queryKey: ["realtime-supply"],
        queryFn: () => getRealtimeSupply(),
        staleTime: 5 * 60 * 1000,
      }),
      queryClient.prefetchQuery({
        queryKey: ["mining-rounds"],
        queryFn: () => getACSMiningRounds({ closedLimit: 20 }),
        staleTime: 5 * 60 * 1000,
      }),
      queryClient.prefetchQuery({
        queryKey: ["allocations", "", 1],
        queryFn: () => getACSAllocations({ limit: 20, offset: 0 }),
        staleTime: 5 * 60 * 1000,
      }),
    ]);
  }, [queryClient]);

  const prefetchRichList = useCallback(async () => {
    const available = await isApiAvailable();
    if (!available) return;

    await queryClient.prefetchQuery({
      queryKey: ["acs-realtime-rich-list", ""],
      queryFn: () => getRealtimeRichList({ limit: 100 }),
      staleTime: 5 * 60 * 1000,
    });
  }, [queryClient]);

  const prefetchRoundStats = useCallback(async () => {
    const available = await isApiAvailable();
    if (!available) return;

    await queryClient.prefetchQuery({
      queryKey: ["localMiningRounds"],
      queryFn: () => getACSMiningRounds({ closedLimit: 20 }),
      staleTime: 5 * 60 * 1000,
    });
  }, [queryClient]);

  const prefetchDashboard = useCallback(async () => {
    const available = await isApiAvailable();
    if (!available) return;

    await queryClient.prefetchQuery({
      queryKey: ["dashboard-realtime-supply"],
      queryFn: () => getRealtimeSupply(),
      staleTime: 5 * 60 * 1000,
    });
  }, [queryClient]);

  return {
    prefetchSupply,
    prefetchRichList,
    prefetchRoundStats,
    prefetchDashboard,
  };
}

/**
 * Map of routes to prefetch functions
 */
export const routePrefetchMap: Record<string, keyof ReturnType<typeof usePrefetch>> = {
  "/supply": "prefetchSupply",
  "/rich-list": "prefetchRichList",
  "/round-stats": "prefetchRoundStats",
  "/": "prefetchDashboard",
};
