/**
 * Backend Configuration
 * 
 * DuckDB API backend for all ledger data queries.
 */
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
interface BackendConfig {
  /** DuckDB API base URL */
  duckdbApiUrl: string;
}

// Configuration
// - Local dev: frontend on http://localhost:<vitePort> talks to API on http://localhost:3001
// - Non-local (Lovable preview / deployed): localhost is not reachable, so API features will be unavailable.
const DEFAULT_DUCKDB_PORT = 3001;

// Remote server URL for access from non-local environments (set to empty string to disable)
const REMOTE_SERVER_URL = 'http://34.56.191.157:3001';
function computeDuckDbApiUrl(): string {
  if (stryMutAct_9fa48("4266")) {
    {}
  } else {
    stryCov_9fa48("4266");
    if (stryMutAct_9fa48("4269") ? typeof window !== 'undefined' : stryMutAct_9fa48("4268") ? false : stryMutAct_9fa48("4267") ? true : (stryCov_9fa48("4267", "4268", "4269"), typeof window === 'undefined')) return `http://localhost:${DEFAULT_DUCKDB_PORT}`;
    const host = window.location.hostname;
    const protocol = window.location.protocol;
    const isLocalHost = stryMutAct_9fa48("4274") ? host === 'localhost' && host === '127.0.0.1' : stryMutAct_9fa48("4273") ? false : stryMutAct_9fa48("4272") ? true : (stryCov_9fa48("4272", "4273", "4274"), (stryMutAct_9fa48("4276") ? host !== 'localhost' : stryMutAct_9fa48("4275") ? false : (stryCov_9fa48("4275", "4276"), host === 'localhost')) || (stryMutAct_9fa48("4279") ? host !== '127.0.0.1' : stryMutAct_9fa48("4278") ? false : (stryCov_9fa48("4278", "4279"), host === '127.0.0.1')));

    // When running locally, always use localhost backend
    if (stryMutAct_9fa48("4282") ? false : stryMutAct_9fa48("4281") ? true : (stryCov_9fa48("4281", "4282"), isLocalHost)) {
      if (stryMutAct_9fa48("4283")) {
        {}
      } else {
        stryCov_9fa48("4283");
        return `http://localhost:${DEFAULT_DUCKDB_PORT}`;
      }
    }

    // For non-local environments, use remote server if configured
    if (stryMutAct_9fa48("4286") ? false : stryMutAct_9fa48("4285") ? true : (stryCov_9fa48("4285", "4286"), REMOTE_SERVER_URL)) {
      if (stryMutAct_9fa48("4287")) {
        {}
      } else {
        stryCov_9fa48("4287");
        return REMOTE_SERVER_URL;
      }
    }

    // Fallback: same host with DuckDB port
    const baseProtocol = (stryMutAct_9fa48("4290") ? protocol !== 'https:' : stryMutAct_9fa48("4289") ? false : stryMutAct_9fa48("4288") ? true : (stryCov_9fa48("4288", "4289", "4290"), protocol === 'https:')) ? 'https' : 'http';
    return `${baseProtocol}://${host}:${DEFAULT_DUCKDB_PORT}`;
  }
}
const config: BackendConfig = {
  duckdbApiUrl: computeDuckDbApiUrl()
};
export function getBackendConfig(): BackendConfig {
  if (stryMutAct_9fa48("4296")) {
    {}
  } else {
    stryCov_9fa48("4296");
    // Recompute each time so it stays correct if host changes (rare, but safe).
    return {
      ...config,
      duckdbApiUrl: computeDuckDbApiUrl()
    };
  }
}
export function useDuckDBForLedger(): boolean {
  if (stryMutAct_9fa48("4298")) {
    {}
  } else {
    stryCov_9fa48("4298");
    return stryMutAct_9fa48("4299") ? false : (stryCov_9fa48("4299"), true);
  }
}
export function getDuckDBApiUrl(): string {
  if (stryMutAct_9fa48("4300")) {
    {}
  } else {
    stryCov_9fa48("4300");
    return computeDuckDbApiUrl();
  }
}

/**
 * Check if DuckDB API is available
 */
export async function checkDuckDBConnection(): Promise<boolean> {
  if (stryMutAct_9fa48("4301")) {
    {}
  } else {
    stryCov_9fa48("4301");
    try {
      if (stryMutAct_9fa48("4302")) {
        {}
      } else {
        stryCov_9fa48("4302");
        const baseUrl = computeDuckDbApiUrl();
        console.log('[DuckDB] Checking connection to:', baseUrl);
        const response = await fetch(`${baseUrl}/health`, {
          method: 'GET',
          signal: AbortSignal.timeout(5000) // Increased timeout
        });
        console.log('[DuckDB] Health check response:', response.ok);
        return response.ok;
      }
    } catch (err) {
      if (stryMutAct_9fa48("4308")) {
        {}
      } else {
        stryCov_9fa48("4308");
        console.warn('[DuckDB] Health check failed:', err);
        return stryMutAct_9fa48("4310") ? true : (stryCov_9fa48("4310"), false);
      }
    }
  }
}