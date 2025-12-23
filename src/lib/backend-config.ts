/**
 * Backend Configuration
 * 
 * Toggle between Supabase and DuckDB API backends.
 * For internal use, prefer DuckDB for heavy ledger data queries.
 * 
 * Uses relative /api paths that work across all environments:
 * - Local: Vite proxies /api → localhost:3001
 * - Cloudflare tunnel: Same origin, tunneled to Vite → proxy → backend
 * - Production: Same origin or configured backend
 */

export type BackendType = 'supabase' | 'duckdb';

interface BackendConfig {
  /** Primary backend for ledger updates/events (heavy data) */
  ledgerBackend: BackendType;
  /** Backend for metadata (cursors, snapshots, etc.) */
  metadataBackend: BackendType;
  /** DuckDB API base URL (relative path for same-origin requests) */
  duckdbApiUrl: string;
}

// Use relative /api path - works in all environments via Vite proxy or same-origin
const API_BASE_PATH = '/api';

const config: BackendConfig = {
  ledgerBackend: 'duckdb',
  metadataBackend: 'supabase',
  duckdbApiUrl: API_BASE_PATH,
};

export function getBackendConfig(): BackendConfig {
  return { ...config };
}

export function useDuckDBForLedger(): boolean {
  return config.ledgerBackend === 'duckdb';
}

export function useSupabaseForLedger(): boolean {
  return config.ledgerBackend === 'supabase';
}

export function getDuckDBApiUrl(): string {
  return API_BASE_PATH;
}

/**
 * Check if DuckDB API is available
 */
export async function checkDuckDBConnection(): Promise<boolean> {
  try {
    const response = await fetch(`${API_BASE_PATH}/health`, {
      method: 'GET',
      signal: AbortSignal.timeout(3000),
    });
    return response.ok;
  } catch {
    return false;
  }
}
