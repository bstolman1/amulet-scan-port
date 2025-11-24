import { useQuery } from "@tanstack/react-query";
import { useAcsSnapshots } from "./use-acs-snapshots";
import { pickAmount } from "@/lib/amount-utils";

interface AggregatedTemplateSum {
  template_name: string;
  total_amount: number;
  instance_count: number;
}

export function useAggregatedTemplateSum(templateSuffix: string) {
  const { data: snapshots, isLoading: snapshotsLoading } = useAcsSnapshots({ limit: 1 });

  return useQuery({
    queryKey: ["aggregated-template-sum", templateSuffix, snapshots?.[0]?.id],
    queryFn: async () => {
      const latestSnapshot = snapshots?.[0];
      if (!latestSnapshot) return null;

      const templates = latestSnapshot.snapshot_data as any;
      const matchingTemplates = Object.entries(templates || {}).filter(([key]) =>
        key.endsWith(templateSuffix)
      );

      const result: AggregatedTemplateSum = {
        template_name: templateSuffix,
        total_amount: 0,
        instance_count: 0,
      };

      matchingTemplates.forEach(([, instances]) => {
        if (Array.isArray(instances)) {
          result.instance_count += instances.length;
          result.total_amount += instances.reduce(
            (sum: number, contract: any) => sum + pickAmount(contract),
            0
          );
        }
      });

      return result;
    },
    enabled: !snapshotsLoading && !!snapshots?.[0],
  });
}
