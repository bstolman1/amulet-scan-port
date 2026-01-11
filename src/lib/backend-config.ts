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
// - Non-local (Lovable preview / deployed): localhost is not reachable, so API features will be unavailable.
const DEFAULT_DUCKDB_PORT = 3001;

// Cloudflare tunnel URL for remote access (set to empty string to use default localhost behavior)
const CLOUDFLARE_TUNNEL_URL = '';

function computeDuckDbApiUrl(): string {
  // If Cloudflare tunnel is configured, always use it (works from any environment)
  if (CLOUDFLARE_TUNNEL_URL) {
    return CLOUDFLARE_TUNNEL_URL;
  }
  
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
