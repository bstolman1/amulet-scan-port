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
  const protocol = window.location.protocol; // "http:" | "https:"
  const isLocalHost = host === 'localhost' || host === '127.0.0.1';

  if (isLocalHost) {
    return `http://localhost:${DEFAULT_DUCKDB_PORT}`;
  }

  // In Lovable preview / deployed environments the UI is typically served over HTTPS.
  // Using the same protocol avoids mixed-content blocks (HTTPS page calling HTTP API).
  // Note: This assumes the DuckDB API is reachable on the same host + port.
  const baseProtocol = protocol === 'https:' ? 'https' : 'http';
  return `${baseProtocol}://${host}:${DEFAULT_DUCKDB_PORT}`;
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
