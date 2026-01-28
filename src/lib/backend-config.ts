/**
 * Backend Configuration
 * 
 * All frontend API calls go through relative paths.
 * nginx proxies /api/* → backend:3001
 */

// Frontend API base - nginx handles routing
// NEVER use localhost, IPs, or ports directly
export const API_BASE = "/api";

/**
 * @deprecated Use API_BASE and apiFetch from duckdb-api-client.ts instead.
 * This function is kept for backwards compatibility during migration.
 */
export function getDuckDBApiUrl(): string {
  // For local development with Vite, we need direct connection to backend
  if (typeof window !== 'undefined') {
    const host = window.location.hostname;
    if (host === 'localhost' || host === '127.0.0.1') {
      return 'http://localhost:3001';
    }
  }
  // Production: return empty string so paths are relative
  return '';
}

/**
 * @deprecated Legacy compatibility
 */
export function getBackendConfig() {
  return { duckdbApiUrl: getDuckDBApiUrl() };
}

/**
 * @deprecated Legacy compatibility
 */
export function useDuckDBForLedger(): boolean {
  return true;
}

/**
 * Check if backend API is available
 */
export async function checkDuckDBConnection(): Promise<boolean> {
  try {
    const response = await fetch(`${API_BASE.replace('/api', '')}/health`, {
      method: 'GET',
      signal: AbortSignal.timeout(5000),
    });
    return response.ok;
  } catch {
    return false;
  }
}
