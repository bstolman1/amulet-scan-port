import { useQuery } from "@tanstack/react-query";
import { useAcsSnapshots } from "./use-acs-snapshots";

interface UsageStats {
  total_contracts: number;
  total_templates: number;
}

export function useUsageStats() {
  const { data: snapshots, isLoading: snapshotsLoading } = useAcsSnapshots({ limit: 1 });

  return useQuery({
    queryKey: ["usage-stats", snapshots?.[0]?.id],
    queryFn: async () => {
      const latestSnapshot = snapshots?.[0];
      if (!latestSnapshot) {
        return { total_contracts: 0, total_templates: 0 };
      }

      const templates = latestSnapshot.snapshot_data as any;
      const stats: UsageStats = {
        total_contracts: 0,
        total_templates: Object.keys(templates || {}).length,
      };

      Object.values(templates || {}).forEach((instances) => {
        if (Array.isArray(instances)) {
          stats.total_contracts += instances.length;
        }
      });

      return stats;
    },
    enabled: !snapshotsLoading && !!snapshots?.[0],
  });
}
