import { useQuery } from "@tanstack/react-query";
import { scanApi, CreatedEvent } from "@/lib/api-client";
import { pickAmountAsCC, pickLockedAmount, toCC } from "@/lib/amount-utils";

export interface RichListHolder {
  owner: string;
  amount: number;      // unlocked balance (after holding fees)
  locked: number;      // locked balance (after holding fees)
  total: number;       // total available balance
}

export interface RichListDebug {
  contractsFetched: number;
  pagesLoaded: number;
  templatesQueried: string[];
  sampleContracts: Array<{
    template_id: string;
    owner: string | undefined;
    rawPayload: any;
    parsedBalance: number;
  }>;
  uniqueTemplates: string[];
}

export interface RichListData {
  data: RichListHolder[];
  totalSupply: number;
  holderCount: number;
  recordTime: string;
  round: number;
  debug: RichListDebug;
}

function pickOwner(payload: any, fallback?: string): string | undefined {
  return (
    payload?.owner ||
    payload?.amulet?.owner ||
    payload?.state?.owner ||
    payload?.create_arguments?.owner ||
    fallback
  );
}

function pickBalanceCC(payload: any, isLocked: boolean): number {
  // Ledger contracts often store amount in raw 10-decimal units.
  // Use shared parsing utilities (strict parsing + correct decimals).
  if (isLocked) return toCC(pickLockedAmount(payload));
  return pickAmountAsCC(payload);
}

/**
 * Fetches all amulet holdings and aggregates them into a rich list
 * Uses /v0/state/acs to get all Amulet contracts and aggregates by owner
 */
export function useRichList(limit: number = 100) {
  return useQuery({
    queryKey: ["scan-api", "rich-list", limit],
    queryFn: async (): Promise<RichListData> => {
      // Get current round and record time
      const latestRound = await scanApi.fetchLatestRound();
      const snapshot = await scanApi.fetchAcsSnapshotTimestamp(latestRound.effectiveAt, 0);
      
      // Fetch all Amulet and LockedAmulet contracts using /v0/state/acs
      const holdings: CreatedEvent[] = [];
      let nextPage: number | undefined = undefined;
      const pageSize = 1000; // Max page size for efficiency
      let pagesLoaded = 0;
      
      // Templates for Amulet holdings (format: package:module:entity)
      // Try without template filter first to see what's available
      const templates: string[] = [];
      
      // Fetch up to 10 pages (10k contracts) to avoid infinite loops
      for (let i = 0; i < 10; i++) {
        const response = await scanApi.fetchStateAcs({
          migration_id: 0,
          record_time: snapshot.record_time,
          record_time_match: "exact",
          page_size: pageSize,
          after: nextPage,
          templates: templates.length > 0 ? templates : undefined,
        });
        
        pagesLoaded++;
        holdings.push(...response.created_events);
        
        if (!response.next_page_token) break;
        nextPage = response.next_page_token;
      }
      
      // Debug: collect unique templates and sample AMULET contracts specifically
      const uniqueTemplates = [...new Set(holdings.map(e => e.template_id || "unknown"))];
      const amuletSamples = holdings
        .filter(e => e.template_id?.includes("Amulet") && !e.template_id?.includes("AmuletRules"))
        .slice(0, 5);
      const sampleContracts = amuletSamples.map(event => {
        const payload = event.create_arguments as any;
        const isLocked = event.template_id?.includes("LockedAmulet") || !!payload?.lock;
        return {
          template_id: event.template_id || "unknown",
          owner: pickOwner(payload, event.signatories?.[0]),
          rawPayload: payload,
          parsedBalance: pickBalanceCC(payload, isLocked),
          rawAmount: payload?.amount?.initialAmount || payload?.amulet?.amount?.initialAmount || "not found",
        };
      });
      
      // Filter to only Amulet contracts for aggregation
      const amuletHoldings = holdings.filter(e => 
        e.template_id?.includes("Amulet") && !e.template_id?.includes("AmuletRules")
      );
      
      // Aggregate holdings by owner
      const holderMap = new Map<string, { unlocked: number; locked: number }>();
      
      for (const event of amuletHoldings) {
        const payload = event.create_arguments as any;
        const owner = pickOwner(payload, event.signatories?.[0]);
        if (!owner) continue;

        // Check if this is a locked amulet
        const isLocked = event.template_id?.includes("LockedAmulet") || !!payload?.lock;

        const balance = pickBalanceCC(payload, isLocked);
        if (balance <= 0) continue;
        
        const existing = holderMap.get(owner) || { unlocked: 0, locked: 0 };
        if (isLocked) {
          existing.locked += balance;
        } else {
          existing.unlocked += balance;
        }
        holderMap.set(owner, existing);
      }
      
      // Convert to sorted array
      const holders: RichListHolder[] = Array.from(holderMap.entries())
        .map(([owner, { unlocked, locked }]) => ({
          owner,
          amount: unlocked,
          locked,
          total: unlocked + locked,
        }))
        .sort((a, b) => b.total - a.total)
        .slice(0, limit);
      
      // Calculate totals
      const totalSupply = holders.reduce((sum, h) => sum + h.total, 0);
      
      return {
        data: holders,
        totalSupply,
        holderCount: holderMap.size,
        recordTime: snapshot.record_time,
        round: latestRound.round,
        debug: {
          contractsFetched: holdings.length,
          pagesLoaded,
          templatesQueried: templates.length > 0 ? templates : ["(no filter - fetching all)"],
          sampleContracts,
          uniqueTemplates,
        },
      };
    },
    staleTime: 60_000, // 1 minute - holdings don't change rapidly
    gcTime: 5 * 60_000,
    retry: 2,
  });
}
