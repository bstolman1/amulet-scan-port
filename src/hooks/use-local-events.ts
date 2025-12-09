/**
 * Hook for fetching events from local DuckDB API
 */

import { useQuery } from "@tanstack/react-query";
import { getLatestEvents, getEventsByTemplate } from "@/lib/duckdb-api-client";

export interface LocalEvent {
  event_id: string;
  update_id?: string;
  event_type: string;
  synchronizer_id?: string;
  timestamp: string;
  effective_at?: string;
  contract_id: string;
  party?: string;
  template_id: string;
  payload?: any;
}

export function useLocalEvents(limit = 100, offset = 0) {
  return useQuery({
    queryKey: ["local-events", limit, offset],
    queryFn: async () => {
      const response = await getLatestEvents(limit, offset);
      return response.data as unknown as LocalEvent[];
    },
    staleTime: 30000,
    retry: 1,
  });
}

export function useLocalEventsByTemplate(templateFilter: string, limit = 100) {
  return useQuery({
    queryKey: ["local-events-by-template", templateFilter, limit],
    queryFn: async () => {
      const response = await getEventsByTemplate(templateFilter, limit);
      return response.data as unknown as LocalEvent[];
    },
    staleTime: 30000,
    retry: 1,
    enabled: !!templateFilter,
  });
}

export function useLocalTransactions(limit = 100) {
  return useQuery({
    queryKey: ["local-transactions", limit],
    queryFn: async () => {
      // Fetch events that are likely transactions (Amulet-related)
      const response = await getEventsByTemplate("Amulet", limit);
      return response.data as unknown as LocalEvent[];
    },
    staleTime: 30000,
    retry: 1,
  });
}
