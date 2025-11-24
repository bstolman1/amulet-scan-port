import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

export interface ServerAggregationResult {
  sum: number;
  count: number;
  templateCount: number;
}

export function useTemplateSumServer(
  snapshotId: string | undefined,
  templateSuffix: string,
  mode: "circulating" | "locked",
  enabled: boolean = true,
) {
  return useQuery<ServerAggregationResult, Error>({
    queryKey: ["server-template-sum", snapshotId, templateSuffix, mode],
    queryFn: async () => {
      if (!snapshotId) throw new Error("Snapshot ID required");
      const { data, error } = await supabase.functions.invoke("aggregate-template-sum", {
        body: { snapshot_id: snapshotId, template_suffix: templateSuffix, mode },
      });
      if (error) throw error as any;
      return data as ServerAggregationResult;
    },
    enabled: enabled && !!snapshotId && !!templateSuffix,
    staleTime: 5 * 60 * 1000,
  });
}
