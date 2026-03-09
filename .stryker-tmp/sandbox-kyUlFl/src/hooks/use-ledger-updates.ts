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
  if (stryMutAct_9fa48("2474")) {
    {}
  } else {
    stryCov_9fa48("2474");
    return useQuery({
      queryKey: stryMutAct_9fa48("2476") ? [] : (stryCov_9fa48("2476"), ["ledgerUpdates", limit]),
      queryFn: async (): Promise<LedgerUpdate[]> => {
        if (stryMutAct_9fa48("2478")) {
          {}
        } else {
          stryCov_9fa48("2478");
          const response = await getLatestEvents(limit, 0);
          return response.data.map((event: DuckDBEvent) => {
            if (stryMutAct_9fa48("2479")) {
              {}
            } else {
              stryCov_9fa48("2479");
              const fullData = event as any;
              return {
                id: event.event_id,
                timestamp: event.timestamp,
                effective_at: event.effective_at,
                update_type: event.event_type,
                update_data: fullData,
                created_at: event.timestamp,
                migration_id: stryMutAct_9fa48("2481") ? fullData.migration_id && null : (stryCov_9fa48("2481"), fullData.migration_id ?? null),
                synchronizer_id: stryMutAct_9fa48("2482") ? fullData.synchronizer_id && null : (stryCov_9fa48("2482"), fullData.synchronizer_id ?? null),
                update_id: stryMutAct_9fa48("2483") ? fullData.update_id && null : (stryCov_9fa48("2483"), fullData.update_id ?? null),
                contract_id: event.contract_id,
                template_id: event.template_id
              };
            }
          });
        }
      },
      staleTime: 5_000,
      refetchInterval: 10_000
    });
  }
}
export function useLedgerUpdatesByTimestamp(timestamp: string | undefined, limit: number = 50) {
  if (stryMutAct_9fa48("2484")) {
    {}
  } else {
    stryCov_9fa48("2484");
    return useQuery({
      queryKey: stryMutAct_9fa48("2486") ? [] : (stryCov_9fa48("2486"), ["ledgerUpdates", timestamp, limit]),
      queryFn: async (): Promise<LedgerUpdate[]> => {
        if (stryMutAct_9fa48("2488")) {
          {}
        } else {
          stryCov_9fa48("2488");
          if (stryMutAct_9fa48("2491") ? false : stryMutAct_9fa48("2490") ? true : stryMutAct_9fa48("2489") ? timestamp : (stryCov_9fa48("2489", "2490", "2491"), !timestamp)) return stryMutAct_9fa48("2492") ? ["Stryker was here"] : (stryCov_9fa48("2492"), []);
          // For now, just get latest events - timestamp filtering can be added to API if needed
          const response = await getLatestEvents(limit, 0);
          return stryMutAct_9fa48("2493") ? response.data.map((event: DuckDBEvent) => {
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
              template_id: event.template_id
            };
          }) : (stryCov_9fa48("2493"), response.data.filter(stryMutAct_9fa48("2494") ? () => undefined : (stryCov_9fa48("2494"), (event: DuckDBEvent) => stryMutAct_9fa48("2498") ? event.timestamp < timestamp : stryMutAct_9fa48("2497") ? event.timestamp > timestamp : stryMutAct_9fa48("2496") ? false : stryMutAct_9fa48("2495") ? true : (stryCov_9fa48("2495", "2496", "2497", "2498"), event.timestamp >= timestamp))).map((event: DuckDBEvent) => {
            if (stryMutAct_9fa48("2499")) {
              {}
            } else {
              stryCov_9fa48("2499");
              const fullData = event as any;
              return {
                id: event.event_id,
                timestamp: event.timestamp,
                effective_at: event.effective_at,
                update_type: event.event_type,
                update_data: fullData,
                created_at: event.timestamp,
                migration_id: stryMutAct_9fa48("2501") ? fullData.migration_id && null : (stryCov_9fa48("2501"), fullData.migration_id ?? null),
                synchronizer_id: stryMutAct_9fa48("2502") ? fullData.synchronizer_id && null : (stryCov_9fa48("2502"), fullData.synchronizer_id ?? null),
                update_id: stryMutAct_9fa48("2503") ? fullData.update_id && null : (stryCov_9fa48("2503"), fullData.update_id ?? null),
                contract_id: event.contract_id,
                template_id: event.template_id
              };
            }
          }));
        }
      },
      enabled: stryMutAct_9fa48("2504") ? !timestamp : (stryCov_9fa48("2504"), !(stryMutAct_9fa48("2505") ? timestamp : (stryCov_9fa48("2505"), !timestamp))),
      staleTime: 5_000
    });
  }
}