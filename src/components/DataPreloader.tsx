import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { 
  getRealtimeSupply, 
  getACSMiningRounds, 
  getRealtimeRichList,
  isApiAvailable 
} from "@/lib/duckdb-api-client";
import { scanApi } from "@/lib/api-client";

/**
 * Preloads critical data on app startup for instant navigation
 * Like Etherscan - data is ready before user clicks
 */
export function DataPreloader() {
  const queryClient = useQueryClient();

  useEffect(() => {
    const preloadData = async () => {
      // Check API availability first
      const available = await isApiAvailable();
      
      // Preload all critical data in parallel
      const promises: Promise<void>[] = [];

      // Always preload scan API data (external, fast CDN)
      promises.push(
        queryClient.prefetchQuery({
          queryKey: ["latestRound"],
          queryFn: () => scanApi.fetchLatestRound(),
          staleTime: 60_000,
        })
      );

      if (available) {
        // Preload local ACS data
        promises.push(
          queryClient.prefetchQuery({
            queryKey: ["dashboard-realtime-supply"],
            queryFn: () => getRealtimeSupply(),
            staleTime: 5 * 60_000,
          }),
          queryClient.prefetchQuery({
            queryKey: ["realtime-supply"],
            queryFn: () => getRealtimeSupply(),
            staleTime: 5 * 60_000,
          }),
          queryClient.prefetchQuery({
            queryKey: ["mining-rounds"],
            queryFn: () => getACSMiningRounds({ closedLimit: 20 }),
            staleTime: 5 * 60_000,
          }),
          queryClient.prefetchQuery({
            queryKey: ["acs-realtime-rich-list", ""],
            queryFn: () => getRealtimeRichList({ limit: 100 }),
            staleTime: 5 * 60_000,
          })
        );
      }

      // Fire all prefetches in parallel
      await Promise.allSettled(promises);
    };

    // Start preloading immediately
    preloadData();
  }, [queryClient]);

  // Render nothing - this is just for side effects
  return null;
}
