import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";

/**
 * Default voting threshold if we can't fetch from DSO.
 * This is a fallback - the actual threshold should come from the network.
 */
const DEFAULT_VOTING_THRESHOLD = 10;

/**
 * Hook to fetch the current voting threshold from the DSO.
 *
 * The voting threshold is the minimum number of "accept" votes required
 * for a governance proposal to pass. This is typically ~2/3 of the SV count.
 *
 * This hook provides a single source of truth for the threshold, ensuring
 * consistency across all governance-related pages.
 */
export function useVotingThreshold() {
  const {
    data: dsoInfo,
    isLoading,
    error,
  } = useQuery({
    queryKey: ["dsoInfo"],
    queryFn: () => scanApi.fetchDsoInfo(),
    retry: 1,
    staleTime: 60 * 1000, // Cache for 1 minute
  });

  // Extract SV count from dso_rules if available
  const dsoRules = dsoInfo?.dso_rules?.contract?.payload;
  const svs = dsoRules?.svs || {};
  const svCount = Object.keys(svs).length;

  // Use the explicit voting_threshold from DSO, or calculate from SV count
  // If neither is available, fall back to default
  const threshold =
    dsoInfo?.voting_threshold ||
    (svCount > 0 ? Math.ceil(svCount * 0.67) : DEFAULT_VOTING_THRESHOLD);

  return {
    threshold,
    svCount,
    isLoading,
    error,
    // Expose the raw dsoInfo for components that need more details
    dsoInfo,
  };
}

/**
 * Get the voting threshold synchronously from provided dsoInfo.
 * Useful when you already have dsoInfo and don't need the hook.
 */
export function getVotingThreshold(
  dsoInfo: { voting_threshold?: number; dso_rules?: any } | null | undefined,
  defaultThreshold = DEFAULT_VOTING_THRESHOLD
): number {
  if (!dsoInfo) return defaultThreshold;

  // Use explicit threshold if available
  if (dsoInfo.voting_threshold) {
    return dsoInfo.voting_threshold;
  }

  // Calculate from SV count
  const dsoRules = dsoInfo.dso_rules?.contract?.payload;
  const svs = dsoRules?.svs || {};
  const svCount = Object.keys(svs).length;

  if (svCount > 0) {
    return Math.ceil(svCount * 0.67);
  }

  return defaultThreshold;
}
