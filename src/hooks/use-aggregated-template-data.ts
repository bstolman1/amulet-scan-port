import { useQuery } from "@tanstack/react-query";
import { useAcsSnapshots } from "./use-acs-snapshots";

export function useAggregatedTemplateData() {
  const { data: snapshots, isLoading: snapshotsLoading } = useAcsSnapshots({ limit: 1 });

  return useQuery({
    queryKey: ["aggregated-template-data", snapshots?.[0]?.id],
    queryFn: async () => {
      const latestSnapshot = snapshots?.[0];
      if (!latestSnapshot) return [];

      const templates = latestSnapshot.snapshot_data as any;
      const aggregated = Object.entries(templates || {}).map(([templateName, instances]) => ({
        template_name: templateName,
        instance_count: Array.isArray(instances) ? instances.length : 0,
        round: latestSnapshot.round,
      }));

      return aggregated;
    },
    enabled: !snapshotsLoading && !!snapshots?.[0],
  });
}
