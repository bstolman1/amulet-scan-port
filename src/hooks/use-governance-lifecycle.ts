import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

export interface GovernanceLifecycleTopic {
  id: string;
  subject: string;
  date: string;
  sourceUrl: string;
  groupName: string;
  groupLabel: string;
  stage: string;
  flow: string;
  identifiers: {
    cipNumber: string | null;
    appName: string | null;
    validatorName: string | null;
    entityName: string | null;
    network: string | null;
    keywords: string[];
  };
  effectiveStage: string;
}

export interface GovernanceLifecycleItem {
  id: string;
  primaryId: string;
  type: "featured-app" | "validator" | "cip" | "other";
  network: string | null;
  stages: Record<string, GovernanceLifecycleTopic[]>;
  topics: GovernanceLifecycleTopic[];
  firstDate: string;
  lastDate: string;
  currentStage: string;
}

interface LifecycleResponse {
  data: GovernanceLifecycleItem[];
  count: number;
  cached: boolean;
}

export function useGovernanceLifecycle(type?: string) {
  return useQuery({
    queryKey: ["governance-lifecycle", type],
    queryFn: async (): Promise<GovernanceLifecycleItem[]> => {
      const url = type 
        ? `/api/governance-lifecycle?type=${encodeURIComponent(type)}`
        : "/api/governance-lifecycle";
      const response = await apiFetch<LifecycleResponse>(url);
      return response.data || [];
    },
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
}

// Helper to find lifecycle items matching an app name
export function findLifecycleForApp(
  lifecycleItems: GovernanceLifecycleItem[] | undefined,
  appName: string | undefined
): GovernanceLifecycleItem | undefined {
  if (!lifecycleItems || !appName) return undefined;
  
  const normalizedAppName = appName.toLowerCase().trim();
  
  return lifecycleItems.find((item) => {
    // Match by primaryId (exact or partial)
    const primaryIdMatch = item.primaryId?.toLowerCase().trim() === normalizedAppName ||
      item.primaryId?.toLowerCase().includes(normalizedAppName) ||
      normalizedAppName.includes(item.primaryId?.toLowerCase() || "");
    
    if (primaryIdMatch) return true;
    
    // Match by appName in any topic's identifiers
    return item.topics?.some((topic) => {
      const topicAppName = topic.identifiers?.appName?.toLowerCase().trim();
      return topicAppName === normalizedAppName ||
        topicAppName?.includes(normalizedAppName) ||
        normalizedAppName.includes(topicAppName || "");
    });
  });
}

// Get stage badge color
export function getStageColor(stage: string): string {
  switch (stage) {
    case "tokenomics":
      return "bg-blue-500/10 text-blue-500 border-blue-500/30";
    case "tokenomics-announce":
      return "bg-purple-500/10 text-purple-500 border-purple-500/30";
    case "sv-announce":
      return "bg-green-500/10 text-green-500 border-green-500/30";
    case "cip-discuss":
      return "bg-yellow-500/10 text-yellow-500 border-yellow-500/30";
    case "cip-vote":
      return "bg-orange-500/10 text-orange-500 border-orange-500/30";
    case "cip-announce":
      return "bg-teal-500/10 text-teal-500 border-teal-500/30";
    default:
      return "bg-muted text-muted-foreground";
  }
}

// Get human-readable stage label
export function getStageLabel(stage: string): string {
  switch (stage) {
    case "tokenomics":
      return "Discussion";
    case "tokenomics-announce":
      return "Announced";
    case "sv-announce":
      return "Approved";
    case "cip-discuss":
      return "CIP Discussion";
    case "cip-vote":
      return "CIP Vote";
    case "cip-announce":
      return "CIP Announced";
    default:
      return stage;
  }
}
