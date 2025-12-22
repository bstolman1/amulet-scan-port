/**
 * Backend Configuration
 * 
 * Toggle between Supabase and DuckDB API backends.
 * For internal use, prefer DuckDB for heavy ledger data queries.
 */

export type BackendType = 'supabase' | 'duckdb';

interface BackendConfig {
  /** Primary backend for ledger updates/events (heavy data) */
  ledgerBackend: BackendType;
  /** Backend for metadata (cursors, snapshots, etc.) */
  metadataBackend: BackendType;
  /** DuckDB API base URL */
  duckdbApiUrl: string;
}

// Configuration
// - Local dev: frontend on http://localhost:<vitePort> talks to API on http://localhost:3001
// - Non-local (Lovable preview / deployed): localhost is not reachable, so API features will be unavailable.
const DEFAULT_DUCKDB_PORT = 3001;

function computeDuckDbApiUrl(): string {
  if (typeof window === 'undefined') return `http://localhost:${DEFAULT_DUCKDB_PORT}`;

  const host = window.location.hostname;
  const isLocalHost = host === 'localhost' || host === '127.0.0.1';

  if (isLocalHost) {
    return `http://localhost:${DEFAULT_DUCKDB_PORT}`;
  }

  // If you run the UI from another host on your LAN (e.g. http://192.168.x.x:5173)
  // this will automatically target that same host on :3001.
  return `http://${host}:${DEFAULT_DUCKDB_PORT}`;
}

const config: BackendConfig = {
  ledgerBackend: 'duckdb',
  metadataBackend: 'supabase',
  duckdbApiUrl: computeDuckDbApiUrl(),
};

export function getBackendConfig(): BackendConfig {
  // Recompute each time so it stays correct if host changes (rare, but safe).
  return { ...config, duckdbApiUrl: computeDuckDbApiUrl() };
}

export function useDuckDBForLedger(): boolean {
  return config.ledgerBackend === 'duckdb';
}

export function useSupabaseForLedger(): boolean {
  return config.ledgerBackend === 'supabase';
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
    const response = await fetch(`${baseUrl}/health`, {
      method: 'GET',
      signal: AbortSignal.timeout(3000),
    });
    return response.ok;
  } catch {
    return false;
  }
}
