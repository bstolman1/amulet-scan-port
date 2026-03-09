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
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { getACSSnapshots as getLocalACSSnapshots, getLatestACSSnapshot as getLocalLatestACSSnapshot, getACSTemplates as getLocalACSTemplates, apiFetch } from "@/lib/duckdb-api-client";
export interface ACSSnapshot {
  id: string;
  timestamp: string;
  migration_id: number;
  record_time: string;
  sv_url?: string;
  canonical_package?: string | null;
  amulet_total?: number;
  locked_total?: number;
  circulating_supply?: number;
  entry_count: number;
  status: "processing" | "completed" | "failed" | string;
  error_message?: string | null;
  created_at?: string;
  updated_at?: string;
  source?: string;
}
export interface ACSTemplateStats {
  id?: string;
  snapshot_id?: string;
  template_id: string;
  contract_count: number;
  field_sums?: Record<string, string> | null;
  status_tallies?: Record<string, number> | null;
  storage_path?: string | null;
  created_at?: string;
  entity_name?: string;
  module_name?: string;
}
export function useACSSnapshots() {
  if (stryMutAct_9fa48("889")) {
    {}
  } else {
    stryCov_9fa48("889");
    return useQuery({
      queryKey: stryMutAct_9fa48("891") ? [] : (stryCov_9fa48("891"), ["acsSnapshots"]),
      queryFn: async (): Promise<ACSSnapshot[]> => {
        if (stryMutAct_9fa48("893")) {
          {}
        } else {
          stryCov_9fa48("893");
          const response = await getLocalACSSnapshots();
          return stryMutAct_9fa48("896") ? response.data as ACSSnapshot[] && [] : stryMutAct_9fa48("895") ? false : stryMutAct_9fa48("894") ? true : (stryCov_9fa48("894", "895", "896"), response.data as ACSSnapshot[] || (stryMutAct_9fa48("897") ? ["Stryker was here"] : (stryCov_9fa48("897"), [])));
        }
      },
      staleTime: 30_000,
      retry: stryMutAct_9fa48("898") ? true : (stryCov_9fa48("898"), false)
    });
  }
}
export function useLatestACSSnapshot() {
  if (stryMutAct_9fa48("899")) {
    {}
  } else {
    stryCov_9fa48("899");
    return useQuery({
      queryKey: stryMutAct_9fa48("901") ? [] : (stryCov_9fa48("901"), ["latestAcsSnapshot"]),
      queryFn: async (): Promise<ACSSnapshot | null> => {
        if (stryMutAct_9fa48("903")) {
          {}
        } else {
          stryCov_9fa48("903");
          const response = await getLocalLatestACSSnapshot();
          return stryMutAct_9fa48("906") ? response.data as ACSSnapshot && null : stryMutAct_9fa48("905") ? false : stryMutAct_9fa48("904") ? true : (stryCov_9fa48("904", "905", "906"), response.data as ACSSnapshot || null);
        }
      },
      staleTime: 30_000,
      retry: stryMutAct_9fa48("907") ? true : (stryCov_9fa48("907"), false)
    });
  }
}

// Hook that returns the latest snapshot regardless of status
export function useActiveSnapshot() {
  if (stryMutAct_9fa48("908")) {
    {}
  } else {
    stryCov_9fa48("908");
    return useQuery({
      queryKey: stryMutAct_9fa48("910") ? [] : (stryCov_9fa48("910"), ["activeAcsSnapshot"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("912")) {
          {}
        } else {
          stryCov_9fa48("912");
          const response = await getLocalLatestACSSnapshot();
          if (stryMutAct_9fa48("914") ? false : stryMutAct_9fa48("913") ? true : (stryCov_9fa48("913", "914"), response.data)) {
            if (stryMutAct_9fa48("915")) {
              {}
            } else {
              stryCov_9fa48("915");
              return {
                snapshot: response.data as ACSSnapshot,
                isProcessing: stryMutAct_9fa48("919") ? response.data.status !== "processing" : stryMutAct_9fa48("918") ? false : stryMutAct_9fa48("917") ? true : (stryCov_9fa48("917", "918", "919"), response.data.status === "processing")
              };
            }
          }
          return {
            snapshot: null,
            isProcessing: stryMutAct_9fa48("922") ? true : (stryCov_9fa48("922"), false)
          };
        }
      },
      staleTime: 30_000,
      retry: stryMutAct_9fa48("923") ? true : (stryCov_9fa48("923"), false)
    });
  }
}
export function useTemplateStats(snapshotId: string | undefined) {
  if (stryMutAct_9fa48("924")) {
    {}
  } else {
    stryCov_9fa48("924");
    return useQuery({
      queryKey: stryMutAct_9fa48("926") ? [] : (stryCov_9fa48("926"), ["acsTemplateStats", snapshotId]),
      queryFn: async (): Promise<ACSTemplateStats[]> => {
        if (stryMutAct_9fa48("928")) {
          {}
        } else {
          stryCov_9fa48("928");
          const response = await getLocalACSTemplates(100);
          return response.data.map(t => ({
            id: t.template_id,
            snapshot_id: snapshotId,
            template_id: t.template_id,
            contract_count: t.contract_count,
            entity_name: t.entity_name,
            module_name: t.module_name
          })) as ACSTemplateStats[];
        }
      },
      enabled: stryMutAct_9fa48("929") ? !snapshotId : (stryCov_9fa48("929"), !(stryMutAct_9fa48("930") ? snapshotId : (stryCov_9fa48("930"), !snapshotId))),
      staleTime: 60_000
    });
  }
}
export function useTriggerACSSnapshot() {
  if (stryMutAct_9fa48("931")) {
    {}
  } else {
    stryCov_9fa48("931");
    const queryClient = useQueryClient();
    return useMutation({
      mutationFn: async () => {
        if (stryMutAct_9fa48("933")) {
          {}
        } else {
          stryCov_9fa48("933");
          // Trigger snapshot via the local API
          const response = await apiFetch<{
            success: boolean;
            message: string;
          }>("/api/acs/trigger-snapshot", {
            method: "POST"
          });
          return response;
        }
      },
      onSuccess: data => {
        if (stryMutAct_9fa48("937")) {
          {}
        } else {
          stryCov_9fa48("937");
          toast.success("ACS snapshot triggered", {
            description: stryMutAct_9fa48("942") ? data.message && "Snapshot process started" : stryMutAct_9fa48("941") ? false : stryMutAct_9fa48("940") ? true : (stryCov_9fa48("940", "941", "942"), data.message || "Snapshot process started")
          });
          queryClient.invalidateQueries({
            queryKey: stryMutAct_9fa48("945") ? [] : (stryCov_9fa48("945"), ["acsSnapshots"])
          });
        }
      },
      onError: (error: Error) => {
        if (stryMutAct_9fa48("947")) {
          {}
        } else {
          stryCov_9fa48("947");
          toast.error("Failed to trigger ACS snapshot", {
            description: error.message
          });
        }
      }
    });
  }
}