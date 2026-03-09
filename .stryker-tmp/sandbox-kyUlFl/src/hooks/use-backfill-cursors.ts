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
import { getBackfillCursors, getBackfillStats, getWriteActivity, getBackfillDebugInfo, apiFetch, type BackfillDebugInfo } from "@/lib/duckdb-api-client";
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
  if (stryMutAct_9fa48("1056")) {
    {}
  } else {
    stryCov_9fa48("1056");
    return useQuery({
      queryKey: stryMutAct_9fa48("1058") ? [] : (stryCov_9fa48("1058"), ["backfillCursors"]),
      queryFn: async (): Promise<BackfillCursor[]> => {
        if (stryMutAct_9fa48("1060")) {
          {}
        } else {
          stryCov_9fa48("1060");
          const result = await getBackfillCursors();
          return stryMutAct_9fa48("1063") ? result.data as BackfillCursor[] && [] : stryMutAct_9fa48("1062") ? false : stryMutAct_9fa48("1061") ? true : (stryCov_9fa48("1061", "1062", "1063"), result.data as BackfillCursor[] || (stryMutAct_9fa48("1064") ? ["Stryker was here"] : (stryCov_9fa48("1064"), [])));
        }
      },
      staleTime: 5_000,
      refetchInterval: 10_000
    });
  }
}
export function useBackfillStats() {
  if (stryMutAct_9fa48("1065")) {
    {}
  } else {
    stryCov_9fa48("1065");
    return useQuery({
      queryKey: stryMutAct_9fa48("1067") ? [] : (stryCov_9fa48("1067"), ["backfillStats"]),
      queryFn: async (): Promise<BackfillStats> => {
        if (stryMutAct_9fa48("1069")) {
          {}
        } else {
          stryCov_9fa48("1069");
          return await getBackfillStats();
        }
      },
      staleTime: 10_000,
      refetchInterval: 30_000
    });
  }
}
export function useWriteActivity() {
  if (stryMutAct_9fa48("1070")) {
    {}
  } else {
    stryCov_9fa48("1070");
    return useQuery({
      queryKey: stryMutAct_9fa48("1072") ? [] : (stryCov_9fa48("1072"), ["writeActivity"]),
      queryFn: async (): Promise<WriteActivityState> => {
        if (stryMutAct_9fa48("1074")) {
          {}
        } else {
          stryCov_9fa48("1074");
          try {
            if (stryMutAct_9fa48("1075")) {
              {}
            } else {
              stryCov_9fa48("1075");
              const result = await getWriteActivity();
              return {
                isWriting: result.isWriting,
                eventFiles: result.currentCounts.events,
                updateFiles: result.currentCounts.updates,
                message: result.message
              };
            }
          } catch (e) {
            if (stryMutAct_9fa48("1077")) {
              {}
            } else {
              stryCov_9fa48("1077");
              console.warn("Write activity check failed:", e);
              return {
                isWriting: stryMutAct_9fa48("1080") ? true : (stryCov_9fa48("1080"), false),
                eventFiles: 0,
                updateFiles: 0,
                message: "API unavailable"
              };
            }
          }
        }
      },
      staleTime: 5_000,
      refetchInterval: 5_000
    });
  }
}
export function useBackfillCursorByName(cursorName: string | undefined) {
  if (stryMutAct_9fa48("1082")) {
    {}
  } else {
    stryCov_9fa48("1082");
    return useQuery({
      queryKey: stryMutAct_9fa48("1084") ? [] : (stryCov_9fa48("1084"), ["backfillCursors", cursorName]),
      queryFn: async (): Promise<BackfillCursor | null> => {
        if (stryMutAct_9fa48("1086")) {
          {}
        } else {
          stryCov_9fa48("1086");
          if (stryMutAct_9fa48("1089") ? false : stryMutAct_9fa48("1088") ? true : stryMutAct_9fa48("1087") ? cursorName : (stryCov_9fa48("1087", "1088", "1089"), !cursorName)) return null;

          // Get all cursors and filter by name
          const result = await getBackfillCursors();
          const cursors = stryMutAct_9fa48("1092") ? result.data as BackfillCursor[] && [] : stryMutAct_9fa48("1091") ? false : stryMutAct_9fa48("1090") ? true : (stryCov_9fa48("1090", "1091", "1092"), result.data as BackfillCursor[] || (stryMutAct_9fa48("1093") ? ["Stryker was here"] : (stryCov_9fa48("1093"), [])));
          return stryMutAct_9fa48("1096") ? cursors.find(c => c.cursor_name === cursorName) && null : stryMutAct_9fa48("1095") ? false : stryMutAct_9fa48("1094") ? true : (stryCov_9fa48("1094", "1095", "1096"), cursors.find(stryMutAct_9fa48("1097") ? () => undefined : (stryCov_9fa48("1097"), c => stryMutAct_9fa48("1100") ? c.cursor_name !== cursorName : stryMutAct_9fa48("1099") ? false : stryMutAct_9fa48("1098") ? true : (stryCov_9fa48("1098", "1099", "1100"), c.cursor_name === cursorName))) || null);
        }
      },
      enabled: stryMutAct_9fa48("1101") ? !cursorName : (stryCov_9fa48("1101"), !(stryMutAct_9fa48("1102") ? cursorName : (stryCov_9fa48("1102"), !cursorName))),
      staleTime: 10_000
    });
  }
}
export function useBackfillDebugInfo() {
  if (stryMutAct_9fa48("1103")) {
    {}
  } else {
    stryCov_9fa48("1103");
    return useQuery({
      queryKey: stryMutAct_9fa48("1105") ? [] : (stryCov_9fa48("1105"), ["backfillDebugInfo"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1107")) {
          {}
        } else {
          stryCov_9fa48("1107");
          return await getBackfillDebugInfo();
        }
      },
      staleTime: 5_000,
      refetchInterval: 10_000
    });
  }
}