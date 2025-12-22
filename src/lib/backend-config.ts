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

// Configuration - using Cloudflare tunnel for DuckDB backend
const config: BackendConfig = {
  ledgerBackend: 'duckdb',
  metadataBackend: 'supabase', // Keep Supabase for small metadata tables  
  duckdbApiUrl: 'https://visual-sox-face-generator.trycloudflare.com',
};

export function getBackendConfig(): BackendConfig {
  return config;
}

export function useDuckDBForLedger(): boolean {
  return config.ledgerBackend === 'duckdb';
}

export function useSupabaseForLedger(): boolean {
  return config.ledgerBackend === 'supabase';
}

export function getDuckDBApiUrl(): string {
  return config.duckdbApiUrl;
}

/**
 * Check if DuckDB API is available
 */
export async function checkDuckDBConnection(): Promise<boolean> {
  try {
    const response = await fetch(`${config.duckdbApiUrl}/health`, {
      method: 'GET',
      signal: AbortSignal.timeout(3000),
    });
    return response.ok;
  } catch {
    return false;
  }
}
