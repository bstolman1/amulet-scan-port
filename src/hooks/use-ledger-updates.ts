import { useQuery } from "@tanstack/react-query";
import { getLatestEvents, type LedgerEvent as DuckDBEvent } from "@/lib/duckdb-api-client";

export interface LedgerUpdate {
  id: string;
  timestamp: string;
  effective_at?: string;
  update_type: string;
  update_data: any;
  created_at: string;
  migration_id?: number | null;
  synchronizer_id?: string | null;
  update_id?: string | null;
  contract_id?: string | null;
  template_id?: string | null;
}

/**
 * Fetch ledger updates from DuckDB
 */
export function useLedgerUpdates(limit: number = 50) {
  return useQuery({
    queryKey: ["ledgerUpdates", limit],
    queryFn: async (): Promise<LedgerUpdate[]> => {
      const response = await getLatestEvents(limit, 0);
      return response.data.map((event: DuckDBEvent) => {
        const fullData = event as any;
        return {
          id: event.event_id,
          timestamp: event.timestamp,
          effective_at: event.effective_at,
          update_type: event.event_type,
          update_data: fullData,
          created_at: event.timestamp,
          migration_id: fullData.migration_id ?? null,
          synchronizer_id: fullData.synchronizer_id ?? null,
          update_id: fullData.update_id ?? null,
          contract_id: event.contract_id,
          template_id: event.template_id,
        };
      });
    },
    staleTime: 5_000,
    refetchInterval: 10_000,
  });
}

export function useLedgerUpdatesByTimestamp(timestamp: string | undefined, limit: number = 50) {
  return useQuery({
    queryKey: ["ledgerUpdates", timestamp, limit],
    queryFn: async (): Promise<LedgerUpdate[]> => {
      if (!timestamp) return [];
      // For now, just get latest events - timestamp filtering can be added to API if needed
      const response = await getLatestEvents(limit, 0);
      return response.data
        .filter((event: DuckDBEvent) => event.timestamp >= timestamp)
        .map((event: DuckDBEvent) => {
          const fullData = event as any;
          return {
            id: event.event_id,
            timestamp: event.timestamp,
            effective_at: event.effective_at,
            update_type: event.event_type,
            update_data: fullData,
            created_at: event.timestamp,
            migration_id: fullData.migration_id ?? null,
            synchronizer_id: fullData.synchronizer_id ?? null,
            update_id: fullData.update_id ?? null,
            contract_id: event.contract_id,
            template_id: event.template_id,
          };
        });
    },
    enabled: !!timestamp,
    staleTime: 5_000,
  });
}
