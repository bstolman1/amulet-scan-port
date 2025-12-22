import { useQuery } from "@tanstack/react-query";
import { getDuckDBApiUrl } from "@/lib/backend-config";

export interface GovernanceStageItem {
  id: string;
  subject: string;
  date: string;
  sourceUrl: string;
  stage: string;
  effectiveStage: string;
  groupLabel: string;
  messageCount: number;
  identifiers: {
    cipNumber?: string;
    appName?: string;
    validatorName?: string;
    entityName?: string;
    network?: string;
  };
}

export interface GovernanceLifecycleItem {
  id: string;
  primaryId: string;
  type: "cip" | "featured-app" | "validator" | "protocol-upgrade" | "outcome" | "other";
  network: string | null;
  currentStage: string;
  stages: Record<string, GovernanceStageItem[]>;
  topics: GovernanceStageItem[];
}

export interface GovernanceLifecycleResponse {
  lifecycleItems: GovernanceLifecycleItem[];
  summary?: {
    total: number;
    byType: Record<string, number>;
    byCurrentStage: Record<string, number>;
  };
}

// Stage display configuration
export const STAGE_CONFIG: Record<string, { label: string; color: string; order: number }> = {
  "tokenomics": { label: "Discussion", color: "bg-blue-500/20 text-blue-400 border-blue-500/30", order: 1 },
  "tokenomics-announce": { label: "Announced", color: "bg-yellow-500/20 text-yellow-400 border-yellow-500/30", order: 2 },
  "sv-announce": { label: "SV Announced", color: "bg-green-500/20 text-green-400 border-green-500/30", order: 3 },
  "cip-discuss": { label: "CIP Discussion", color: "bg-purple-500/20 text-purple-400 border-purple-500/30", order: 1 },
  "cip-vote": { label: "CIP Vote", color: "bg-orange-500/20 text-orange-400 border-orange-500/30", order: 2 },
  "cip-announce": { label: "CIP Announced", color: "bg-green-500/20 text-green-400 border-green-500/30", order: 3 },
};

// Get the current/latest stage from a lifecycle item
export function getCurrentStage(item: GovernanceLifecycleItem): string {
  const stageOrder = ["sv-announce", "tokenomics-announce", "tokenomics", "cip-announce", "cip-vote", "cip-discuss"];
  for (const stage of stageOrder) {
    if (item.stages[stage]?.length > 0) {
      return stage;
    }
  }
  return "unknown";
}

// Get the most recent topic from a lifecycle item
export function getLatestTopic(item: GovernanceLifecycleItem): GovernanceStageItem | null {
  if (item.topics?.length > 0) {
    return item.topics.reduce((latest, topic) => {
      return new Date(topic.date) > new Date(latest.date) ? topic : latest;
    });
  }
  return null;
}

export function useGovernanceLifecycle(type?: string) {
  return useQuery<GovernanceLifecycleResponse>({
    queryKey: ["governance-lifecycle", type],
    queryFn: async () => {
      const baseUrl = getDuckDBApiUrl();
      const url = new URL("/api/governance-lifecycle", baseUrl);
      if (type) {
        url.searchParams.set("type", type);
      }
      const response = await fetch(url.toString());
      if (!response.ok) {
        throw new Error(`Failed to fetch governance lifecycle: ${response.status}`);
      }
      return response.json();
    },
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
}

// Hook specifically for featured app governance items
export function useFeaturedAppGovernance() {
  return useGovernanceLifecycle("featured-app");
}
