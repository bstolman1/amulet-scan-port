import { useQuery } from "@tanstack/react-query";
import { fetchCDNFiles, type CDNFile } from "@/utils/cdnService";

/**
 * Hook to fetch dashboard files from CDN
 */
export const useCDNDashboards = () => {
  return useQuery<CDNFile[]>({
    queryKey: ["cdn-dashboards"],
    queryFn: fetchCDNFiles,
    staleTime: 5 * 60 * 1000, // Cache for 5 minutes
    retry: 2,
  });
};
