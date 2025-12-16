import { useQuery } from "@tanstack/react-query";
import {
  getBackfillCursors,
  getBackfillStats,
  getWriteActivity,
  getBackfillDebugInfo,
  apiFetch,
  type BackfillDebugInfo,
} from "@/lib/duckdb-api-client";

export interface BackfillCursor {
  id: string;
  cursor_name: string;
  last_processed_round: number;
  updated_at: string;
  complete?: boolean | null;
  min_time?: string | null;
  max_time?: string | null;
  migration_id?: number | null;
  synchronizer_id?: string | null;
  last_before?: string | null;
  total_updates?: number | null;
  total_events?: number | null;
  started_at?: string | null;
  pending_writes?: number | null;
  buffered_records?: number | null;
  is_recently_updated?: boolean | null;
  error?: string | null;
}

export interface BackfillStats {
  totalUpdates: number;
  totalEvents: number;
  activeMigrations: number;
  migrationsFromDirs?: number[];
  totalCursors: number;
  completedCursors: number;
  rawFileCounts?: {
    events: number;
    updates: number;
  };
}

export interface WriteActivityState {
  isWriting: boolean;
  eventFiles: number;
  updateFiles: number;
  message: string;
}

export function useBackfillCursors() {
  return useQuery({
    queryKey: ["backfillCursors"],
    queryFn: async (): Promise<BackfillCursor[]> => {
      const result = await getBackfillCursors();
      return (result.data as BackfillCursor[]) || [];
    },
    staleTime: 5_000,
    refetchInterval: 10_000,
  });
}

export function useBackfillStats() {
  return useQuery({
    queryKey: ["backfillStats"],
    queryFn: async (): Promise<BackfillStats> => {
      return await getBackfillStats();
    },
    staleTime: 10_000,
    refetchInterval: 30_000,
  });
}

export function useWriteActivity() {
  return useQuery({
    queryKey: ["writeActivity"],
    queryFn: async (): Promise<WriteActivityState> => {
      try {
        const result = await getWriteActivity();
        return {
          isWriting: result.isWriting,
          eventFiles: result.currentCounts.events,
          updateFiles: result.currentCounts.updates,
          message: result.message,
        };
      } catch (e) {
        console.warn("Write activity check failed:", e);
        return { isWriting: false, eventFiles: 0, updateFiles: 0, message: "API unavailable" };
      }
    },
    staleTime: 5_000,
    refetchInterval: 5_000,
  });
}

export function useBackfillCursorByName(cursorName: string | undefined) {
  return useQuery({
    queryKey: ["backfillCursors", cursorName],
    queryFn: async (): Promise<BackfillCursor | null> => {
      if (!cursorName) return null;
      
      // Get all cursors and filter by name
      const result = await getBackfillCursors();
      const cursors = (result.data as BackfillCursor[]) || [];
      return cursors.find(c => c.cursor_name === cursorName) || null;
    },
    enabled: !!cursorName,
    staleTime: 10_000,
  });
}

export function useBackfillDebugInfo() {
  return useQuery({
    queryKey: ["backfillDebugInfo"],
    queryFn: async () => {
      return await getBackfillDebugInfo();
    },
    staleTime: 5_000,
    refetchInterval: 10_000,
  });
}
