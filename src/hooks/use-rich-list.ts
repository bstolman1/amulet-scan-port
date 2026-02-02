import { useQuery } from "@tanstack/react-query";
import { scanApi, CreatedEvent } from "@/lib/api-client";

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

/**
 * Calculates the effective balance after holding fees
 */
function calculateEffectiveBalance(
  initialAmount: string,
  createdAtMicros: string,
  ratePerRound: string,
  currentRound: number,
  roundDuration: number = 600_000_000 // ~10 minutes in microseconds
): number {
  const initial = parseFloat(initialAmount);
  const createdAt = BigInt(createdAtMicros);
  const rate = parseFloat(ratePerRound);
  
  // Approximate rounds held based on creation time
  const microsecondsPerRound = BigInt(roundDuration);
  const elapsedMicros = BigInt(Date.now() * 1000) - createdAt;
  const roundsHeld = Math.max(0, Number(elapsedMicros / microsecondsPerRound));
  
  // Holding fee reduces the balance over time
  const holdingFee = initial * rate * roundsHeld;
  return Math.max(0, initial - holdingFee);
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
      
      // Templates for Amulet holdings
      const templates = [
        "Splice.Amulet:Amulet",
        "Splice.Amulet:LockedAmulet"
      ];
      
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
        const owner = payload?.owner || event.signatories?.[0];
        if (!owner) continue;
        
        // Parse amount from payload
        let balance = 0;
        const amount = payload?.amount;
        if (amount?.initialAmount) {
          balance = calculateEffectiveBalance(
            amount.initialAmount,
            amount.createdAt?.microseconds || "0",
            amount.ratePerRound?.rate || "0",
            latestRound.round
          );
        }
        
        // Check if this is a locked amulet
        const isLocked = event.template_id?.includes("LockedAmulet") || !!payload?.lock;
        
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
