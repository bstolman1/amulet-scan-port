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
import { getACSTemplates as getLocalACSTemplates, getACSContracts as getLocalACSContracts } from "@/lib/duckdb-api-client";
interface TemplateDataMetadata {
  template_id: string;
  snapshot_timestamp: string;
  entry_count: number;
}
interface TemplateDataResponse<T = any> {
  metadata: TemplateDataMetadata;
  data: T[];
}

/**
 * Fetch template data from local DuckDB for a given snapshot
 */
export function useACSTemplateData<T = any>(snapshotId: string | undefined, templateId: string, enabled: boolean = stryMutAct_9fa48("950") ? false : (stryCov_9fa48("950"), true)) {
  if (stryMutAct_9fa48("951")) {
    {}
  } else {
    stryCov_9fa48("951");
    return useQuery({
      queryKey: stryMutAct_9fa48("953") ? [] : (stryCov_9fa48("953"), ["acs-template-data", templateId]),
      queryFn: async (): Promise<TemplateDataResponse<T>> => {
        if (stryMutAct_9fa48("955")) {
          {}
        } else {
          stryCov_9fa48("955");
          if (stryMutAct_9fa48("958") ? false : stryMutAct_9fa48("957") ? true : stryMutAct_9fa48("956") ? templateId : (stryCov_9fa48("956", "957", "958"), !templateId)) {
            if (stryMutAct_9fa48("959")) {
              {}
            } else {
              stryCov_9fa48("959");
              throw new Error("Missing templateId");
            }
          }
          console.log(`[useACSTemplateData] Fetching from DuckDB: ${templateId}`);
          const response = await getLocalACSContracts({
            template: templateId,
            limit: 100
          });
          return {
            metadata: {
              template_id: templateId,
              snapshot_timestamp: new Date().toISOString(),
              entry_count: response.data.length
            },
            data: response.data as T[]
          };
        }
      },
      enabled: stryMutAct_9fa48("967") ? enabled || !!templateId : stryMutAct_9fa48("966") ? false : stryMutAct_9fa48("965") ? true : (stryCov_9fa48("965", "966", "967"), enabled && (stryMutAct_9fa48("968") ? !templateId : (stryCov_9fa48("968"), !(stryMutAct_9fa48("969") ? templateId : (stryCov_9fa48("969"), !templateId))))),
      staleTime: stryMutAct_9fa48("970") ? 5 * 60 / 1000 : (stryCov_9fa48("970"), (stryMutAct_9fa48("971") ? 5 / 60 : (stryCov_9fa48("971"), 5 * 60)) * 1000),
      retry: stryMutAct_9fa48("972") ? true : (stryCov_9fa48("972"), false)
    });
  }
}

/**
 * Get all available templates for a snapshot
 */
export function useACSTemplates(snapshotId: string | undefined) {
  if (stryMutAct_9fa48("973")) {
    {}
  } else {
    stryCov_9fa48("973");
    return useQuery({
      queryKey: stryMutAct_9fa48("975") ? [] : (stryCov_9fa48("975"), ["acs-templates"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("977")) {
          {}
        } else {
          stryCov_9fa48("977");
          console.log("[useACSTemplates] Fetching from DuckDB");
          const response = await getLocalACSTemplates(500);
          return response.data.map(stryMutAct_9fa48("979") ? () => undefined : (stryCov_9fa48("979"), t => ({
            template_id: t.template_id,
            contract_count: t.contract_count,
            storage_path: null,
            entity_name: t.entity_name,
            module_name: t.module_name
          })));
        }
      },
      enabled: stryMutAct_9fa48("981") ? false : (stryCov_9fa48("981"), true),
      staleTime: stryMutAct_9fa48("982") ? 5 * 60 / 1000 : (stryCov_9fa48("982"), (stryMutAct_9fa48("983") ? 5 / 60 : (stryCov_9fa48("983"), 5 * 60)) * 1000),
      retry: stryMutAct_9fa48("984") ? true : (stryCov_9fa48("984"), false)
    });
  }
}