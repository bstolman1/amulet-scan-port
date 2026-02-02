import { useQuery } from "@tanstack/react-query";
import { scanApi, CreatedEvent } from "@/lib/api-client";
import { pickAmountAsCC, pickLockedAmount, toCC } from "@/lib/amount-utils";

export interface RichListHolder {
  owner: string;
  amount: number;      // unlocked balance (after holding fees)
  locked: number;      // locked balance (after holding fees)
  total: number;       // total available balance
}

export interface RichListData {
  data: RichListHolder[];
  totalSupply: number;
  holderCount: number;
  recordTime: string;
  round: number;
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
      
      // Templates for Amulet holdings (format: package:module:entity)
      // NOTE: package name here includes a dot (as documented), so we still end up with 3 parts.
      const templates = ["Splice.Amulet:Amulet:Amulet", "Splice.Amulet:Amulet:LockedAmulet"];
      
      // Fetch up to 10 pages (10k contracts) to avoid infinite loops
      for (let i = 0; i < 10; i++) {
        const response = await scanApi.fetchStateAcs({
          migration_id: 0,
          record_time: snapshot.record_time,
          record_time_match: "exact",
          page_size: pageSize,
          after: nextPage,
          templates,
        });
        
        holdings.push(...response.created_events);
        
        if (!response.next_page_token) break;
        nextPage = response.next_page_token;
      }
      
      // Aggregate holdings by owner
      const holderMap = new Map<string, { unlocked: number; locked: number }>();
      
      for (const event of holdings) {
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
      };
    },
    staleTime: 60_000, // 1 minute - holdings don't change rapidly
    gcTime: 5 * 60_000,
    retry: 2,
  });
}
