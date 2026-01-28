/**
 * Backend Configuration
 * 
 * DuckDB API backend for all ledger data queries.
 */

interface BackendConfig {
  /** DuckDB API base URL */
  duckdbApiUrl: string;
}

// Configuration
// - Local dev: frontend on http://localhost:<vitePort> talks to API on http://localhost:3001
// - Production: use relative "/api" path - nginx proxies to localhost:3001
const DEFAULT_DUCKDB_PORT = 3001;

function computeDuckDbApiUrl(): string {
  if (typeof window === 'undefined') return `http://localhost:${DEFAULT_DUCKDB_PORT}`;

  const host = window.location.hostname;
  const isLocalHost = host === 'localhost' || host === '127.0.0.1';

  // When running locally, use localhost backend directly
  if (isLocalHost) {
    return `http://localhost:${DEFAULT_DUCKDB_PORT}`;
  }

  // For all deployed environments, use relative /api path
  // nginx proxies /api/* to localhost:3001
  return '';
}

const config: BackendConfig = {
  duckdbApiUrl: computeDuckDbApiUrl(),
};

export function getBackendConfig(): BackendConfig {
  // Recompute each time so it stays correct if host changes (rare, but safe).
  return { ...config, duckdbApiUrl: computeDuckDbApiUrl() };
}

export function useDuckDBForLedger(): boolean {
  return true;
}

export function getDuckDBApiUrl(): string {
  return computeDuckDbApiUrl();
}

/**
 * Check if DuckDB API is available
 */
export async function checkDuckDBConnection(): Promise<boolean> {
  try {
    const baseUrl = computeDuckDbApiUrl();
    console.log('[DuckDB] Checking connection to:', baseUrl);
    const response = await fetch(`${baseUrl}/health`, {
      method: 'GET',
      signal: AbortSignal.timeout(5000), // Increased timeout
    });
    console.log('[DuckDB] Health check response:', response.ok);
    return response.ok;
  } catch (err) {
    console.warn('[DuckDB] Health check failed:', err);
    return false;
  }
}
