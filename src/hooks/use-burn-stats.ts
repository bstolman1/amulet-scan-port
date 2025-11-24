import { useQuery } from "@tanstack/react-query";
import { useLedgerUpdates } from "./use-ledger-updates";

interface BurnStats {
  total_burned: number;
  burn_count: number;
}

export function useBurnStats() {
  const { data: updates, isLoading: updatesLoading } = useLedgerUpdates({ limit: 1000 });

  return useQuery({
    queryKey: ["burn-stats", updates?.length],
    queryFn: async () => {
      if (!updates) {
        return { total_burned: 0, burn_count: 0 };
      }

      const burnEvents = updates.filter(
        (update) => update.update_type === "ArchiveEvent" || update.update_type.includes("Burn")
      );

      const stats: BurnStats = {
        total_burned: 0,
        burn_count: burnEvents.length,
      };

      return stats;
    },
    enabled: !updatesLoading && !!updates,
  });
}
