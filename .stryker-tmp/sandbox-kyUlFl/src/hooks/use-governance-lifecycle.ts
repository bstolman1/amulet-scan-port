// @ts-nocheck
function stryNS_9fa48() {
  var g = typeof globalThis === 'object' && globalThis && globalThis.Math === Math && globalThis || new Function("return this")();
  var ns = g.__stryker__ || (g.__stryker__ = {});
  if (ns.activeMutant === undefined && g.process && g.process.env && g.process.env.__STRYKER_ACTIVE_MUTANT__) {
    ns.activeMutant = g.process.env.__STRYKER_ACTIVE_MUTANT__;
  }
  function retrieveNS() {
    return ns;
  }
  stryNS_9fa48 = retrieveNS;
  return retrieveNS();
}
stryNS_9fa48();
function stryCov_9fa48() {
  var ns = stryNS_9fa48();
  var cov = ns.mutantCoverage || (ns.mutantCoverage = {
    static: {},
    perTest: {}
  });
  function cover() {
    var c = cov.static;
    if (ns.currentTestId) {
      c = cov.perTest[ns.currentTestId] = cov.perTest[ns.currentTestId] || {};
    }
    var a = arguments;
    for (var i = 0; i < a.length; i++) {
      c[a[i]] = (c[a[i]] || 0) + 1;
    }
  }
  stryCov_9fa48 = cover;
  cover.apply(null, arguments);
}
function stryMutAct_9fa48(id) {
  var ns = stryNS_9fa48();
  function isActive(id) {
    if (ns.activeMutant === id) {
      if (ns.hitCount !== void 0 && ++ns.hitCount > ns.hitLimit) {
        throw new Error('Stryker: Hit count limit reached (' + ns.hitCount + ')');
      }
      return true;
    }
    return false;
  }
  stryMutAct_9fa48 = isActive;
  return isActive(id);
}
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
export const STAGE_CONFIG: Record<string, {
  label: string;
  color: string;
  order: number;
}> = {
  "tokenomics": {
    label: "Discussion",
    color: "bg-blue-500/20 text-blue-400 border-blue-500/30",
    order: 1
  },
  "tokenomics-announce": {
    label: "Announced",
    color: "bg-yellow-500/20 text-yellow-400 border-yellow-500/30",
    order: 2
  },
  "sv-announce": {
    label: "SV Announced",
    color: "bg-green-500/20 text-green-400 border-green-500/30",
    order: 3
  },
  "cip-discuss": {
    label: "CIP Discussion",
    color: "bg-purple-500/20 text-purple-400 border-purple-500/30",
    order: 1
  },
  "cip-vote": {
    label: "CIP Vote",
    color: "bg-orange-500/20 text-orange-400 border-orange-500/30",
    order: 2
  },
  "cip-announce": {
    label: "CIP Announced",
    color: "bg-green-500/20 text-green-400 border-green-500/30",
    order: 3
  }
};

// Get the current/latest stage from a lifecycle item
export function getCurrentStage(item: GovernanceLifecycleItem): string {
  if (stryMutAct_9fa48("2318")) {
    {}
  } else {
    stryCov_9fa48("2318");
    const stageOrder = stryMutAct_9fa48("2319") ? [] : (stryCov_9fa48("2319"), ["sv-announce", "tokenomics-announce", "tokenomics", "cip-announce", "cip-vote", "cip-discuss"]);
    for (const stage of stageOrder) {
      if (stryMutAct_9fa48("2326")) {
        {}
      } else {
        stryCov_9fa48("2326");
        if (stryMutAct_9fa48("2330") ? item.stages[stage]?.length <= 0 : stryMutAct_9fa48("2329") ? item.stages[stage]?.length >= 0 : stryMutAct_9fa48("2328") ? false : stryMutAct_9fa48("2327") ? true : (stryCov_9fa48("2327", "2328", "2329", "2330"), (stryMutAct_9fa48("2331") ? item.stages[stage].length : (stryCov_9fa48("2331"), item.stages[stage]?.length)) > 0)) {
          if (stryMutAct_9fa48("2332")) {
            {}
          } else {
            stryCov_9fa48("2332");
            return stage;
          }
        }
      }
    }
    return "unknown";
  }
}

// Get the most recent topic from a lifecycle item
export function getLatestTopic(item: GovernanceLifecycleItem): GovernanceStageItem | null {
  if (stryMutAct_9fa48("2334")) {
    {}
  } else {
    stryCov_9fa48("2334");
    if (stryMutAct_9fa48("2338") ? item.topics?.length <= 0 : stryMutAct_9fa48("2337") ? item.topics?.length >= 0 : stryMutAct_9fa48("2336") ? false : stryMutAct_9fa48("2335") ? true : (stryCov_9fa48("2335", "2336", "2337", "2338"), (stryMutAct_9fa48("2339") ? item.topics.length : (stryCov_9fa48("2339"), item.topics?.length)) > 0)) {
      if (stryMutAct_9fa48("2340")) {
        {}
      } else {
        stryCov_9fa48("2340");
        return item.topics.reduce((latest, topic) => {
          if (stryMutAct_9fa48("2341")) {
            {}
          } else {
            stryCov_9fa48("2341");
            return (stryMutAct_9fa48("2345") ? new Date(topic.date) <= new Date(latest.date) : stryMutAct_9fa48("2344") ? new Date(topic.date) >= new Date(latest.date) : stryMutAct_9fa48("2343") ? false : stryMutAct_9fa48("2342") ? true : (stryCov_9fa48("2342", "2343", "2344", "2345"), new Date(topic.date) > new Date(latest.date))) ? topic : latest;
          }
        });
      }
    }
    return null;
  }
}
export function useGovernanceLifecycle(type?: string) {
  if (stryMutAct_9fa48("2346")) {
    {}
  } else {
    stryCov_9fa48("2346");
    return useQuery<GovernanceLifecycleResponse>({
      queryKey: stryMutAct_9fa48("2348") ? [] : (stryCov_9fa48("2348"), ["governance-lifecycle", type]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2350")) {
          {}
        } else {
          stryCov_9fa48("2350");
          const baseUrl = getDuckDBApiUrl();
          const url = new URL("/api/governance-lifecycle", baseUrl);
          if (stryMutAct_9fa48("2353") ? false : stryMutAct_9fa48("2352") ? true : (stryCov_9fa48("2352", "2353"), type)) {
            if (stryMutAct_9fa48("2354")) {
              {}
            } else {
              stryCov_9fa48("2354");
              url.searchParams.set("type", type);
            }
          }
          const response = await fetch(url.toString());
          if (stryMutAct_9fa48("2358") ? false : stryMutAct_9fa48("2357") ? true : stryMutAct_9fa48("2356") ? response.ok : (stryCov_9fa48("2356", "2357", "2358"), !response.ok)) {
            if (stryMutAct_9fa48("2359")) {
              {}
            } else {
              stryCov_9fa48("2359");
              throw new Error(`Failed to fetch governance lifecycle: ${response.status}`);
            }
          }
          return response.json();
        }
      },
      staleTime: stryMutAct_9fa48("2361") ? 5 * 60 / 1000 : (stryCov_9fa48("2361"), (stryMutAct_9fa48("2362") ? 5 / 60 : (stryCov_9fa48("2362"), 5 * 60)) * 1000) // 5 minutes
    });
  }
}

// Hook specifically for featured app governance items
export function useFeaturedAppGovernance() {
  if (stryMutAct_9fa48("2363")) {
    {}
  } else {
    stryCov_9fa48("2363");
    return useGovernanceLifecycle("featured-app");
  }
}