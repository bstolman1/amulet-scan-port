import { useQuery } from "@tanstack/react-query";
import { getDuckDBApiUrl } from "@/lib/backend-config";

interface TemplateDelta {
  template_suffix: string;
  created_count: number;
  archived_count: number;
  net_change: number;
  since: string;
}

/**
 * Hook to fetch event deltas for a template since the latest snapshot
 */
export function useTemplateEventDelta(
  snapshotRecordTime: string | undefined,
  templateSuffix: string | undefined
) {
  return useQuery({
    queryKey: ["templateEventDelta", snapshotRecordTime, templateSuffix],
    queryFn: async (): Promise<TemplateDelta | null> => {
      if (!snapshotRecordTime || !templateSuffix) return null;
      
      try {
        const baseUrl = getDuckDBApiUrl();
        const params = new URLSearchParams({
          since: snapshotRecordTime,
          template: templateSuffix,
        });
        
        const response = await fetch(`${baseUrl}/api/events/delta?${params}`);
        if (!response.ok) return null;
        
        const data = await response.json();
        return data.data as TemplateDelta;
      } catch {
        return null;
      }
    },
    enabled: !!snapshotRecordTime && !!templateSuffix,
    staleTime: 30_000,
    retry: false,
  });
}

/**
 * Hook to fetch aggregated event deltas for multiple templates
 */
export function useMultiTemplateEventDelta(
  snapshotRecordTime: string | undefined,
  templateSuffixes: string[]
) {
  return useQuery({
    queryKey: ["multiTemplateEventDelta", snapshotRecordTime, templateSuffixes],
    queryFn: async (): Promise<Record<string, TemplateDelta>> => {
      if (!snapshotRecordTime || templateSuffixes.length === 0) return {};
      
      try {
        const baseUrl = getDuckDBApiUrl();
        const params = new URLSearchParams({
          since: snapshotRecordTime,
          templates: templateSuffixes.join(","),
        });
        
        const response = await fetch(`${baseUrl}/api/events/multi-delta?${params}`);
        if (!response.ok) return {};
        
        const data = await response.json();
        return data.data || {};
      } catch {
        return {};
      }
    },
    enabled: !!snapshotRecordTime && templateSuffixes.length > 0,
    staleTime: 30_000,
    retry: false,
  });
}
