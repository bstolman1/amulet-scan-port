/**
 * DuckDB API Client
 * 
 * Client for the local DuckDB API server.
 * Replace Supabase calls with this when using the Parquet/DuckDB backend.
 */

// Configure your API URL
// Local development: http://localhost:3001
// Cloudflare Tunnel: https://your-tunnel.trycloudflare.com
const API_BASE_URL = import.meta.env.VITE_DUCKDB_API_URL || 'http://localhost:3001';

interface ApiResponse<T> {
  data: T;
  count?: number;
  error?: string;
}

/**
 * Generic fetch wrapper with error handling
 */
async function apiFetch<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${endpoint}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Unknown error' }));
    throw new Error(error.error || `API error: ${response.status}`);
  }

  return response.json();
}

// ─────────────────────────────────────────────────────────────────────────────
// Events API
// ─────────────────────────────────────────────────────────────────────────────

export interface LedgerEvent {
  event_id: string;
  event_type: string;
  contract_id: string;
  template_id: string;
  package_name: string;
  timestamp: string;
  signatories: string[];
  observers: string[];
  payload: any;
}

export async function getLatestEvents(limit = 100, offset = 0): Promise<ApiResponse<LedgerEvent[]>> {
  return apiFetch(`/api/events/latest?limit=${limit}&offset=${offset}`);
}

export async function getEventsByType(type: string, limit = 100): Promise<ApiResponse<LedgerEvent[]>> {
  return apiFetch(`/api/events/by-type/${encodeURIComponent(type)}?limit=${limit}`);
}

export async function getEventsByTemplate(templateId: string, limit = 100): Promise<ApiResponse<LedgerEvent[]>> {
  return apiFetch(`/api/events/by-template/${encodeURIComponent(templateId)}?limit=${limit}`);
}

export async function getEventsByDateRange(start: string, end: string, limit = 1000): Promise<ApiResponse<LedgerEvent[]>> {
  return apiFetch(`/api/events/by-date?start=${start}&end=${end}&limit=${limit}`);
}

export async function getEventsCount(): Promise<{ count: number }> {
  return apiFetch('/api/events/count');
}

// ─────────────────────────────────────────────────────────────────────────────
// Party API
// ─────────────────────────────────────────────────────────────────────────────

export async function getPartyEvents(partyId: string, limit = 100): Promise<ApiResponse<LedgerEvent[]>> {
  return apiFetch(`/api/party/${encodeURIComponent(partyId)}?limit=${limit}`);
}

export interface PartySummary {
  event_type: string;
  count: number;
  first_seen: string;
  last_seen: string;
}

export async function getPartySummary(partyId: string): Promise<ApiResponse<PartySummary[]>> {
  return apiFetch(`/api/party/${encodeURIComponent(partyId)}/summary`);
}

export async function getAllParties(limit = 1000): Promise<ApiResponse<string[]>> {
  return apiFetch(`/api/party/list/all?limit=${limit}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Contracts API
// ─────────────────────────────────────────────────────────────────────────────

export async function getContractLifecycle(contractId: string): Promise<ApiResponse<LedgerEvent[]>> {
  return apiFetch(`/api/contracts/${encodeURIComponent(contractId)}`);
}

export async function getActiveContractsByTemplate(templateSuffix: string, limit = 100): Promise<ApiResponse<any[]>> {
  return apiFetch(`/api/contracts/active/by-template/${encodeURIComponent(templateSuffix)}?limit=${limit}`);
}

export interface TemplateInfo {
  template_id: string;
  event_count: number;
  contract_count: number;
}

export async function getTemplatesList(): Promise<ApiResponse<TemplateInfo[]>> {
  return apiFetch('/api/contracts/templates/list');
}

// ─────────────────────────────────────────────────────────────────────────────
// Stats API
// ─────────────────────────────────────────────────────────────────────────────

export interface OverviewStats {
  total_events: number;
  unique_contracts: number;
  unique_templates: number;
  earliest_event: string;
  latest_event: string;
}

export async function getOverviewStats(): Promise<OverviewStats> {
  return apiFetch('/api/stats/overview');
}

export interface DailyStats {
  date: string;
  event_count: number;
  contract_count: number;
}

export async function getDailyStats(days = 30): Promise<ApiResponse<DailyStats[]>> {
  return apiFetch(`/api/stats/daily?days=${days}`);
}

export interface TypeStats {
  event_type: string;
  count: number;
}

export async function getStatsByType(): Promise<ApiResponse<TypeStats[]>> {
  return apiFetch('/api/stats/by-type');
}

export interface TemplateStats {
  template_id: string;
  event_count: number;
  contract_count: number;
  first_seen: string;
  last_seen: string;
}

export async function getStatsByTemplate(limit = 50): Promise<ApiResponse<TemplateStats[]>> {
  return apiFetch(`/api/stats/by-template?limit=${limit}`);
}

export interface HourlyStats {
  hour: string;
  event_count: number;
}

export async function getHourlyStats(): Promise<ApiResponse<HourlyStats[]>> {
  return apiFetch('/api/stats/hourly');
}

export interface BurnStats {
  date: string;
  burn_amount: number;
}

export async function getBurnStats(): Promise<ApiResponse<BurnStats[]>> {
  return apiFetch('/api/stats/burn');
}

// ─────────────────────────────────────────────────────────────────────────────
// Search API
// ─────────────────────────────────────────────────────────────────────────────

export interface SearchParams {
  q?: string;
  type?: string;
  template?: string;
  party?: string;
  limit?: number;
}

export async function searchEvents(params: SearchParams): Promise<ApiResponse<LedgerEvent[]>> {
  const queryParams = new URLSearchParams();
  if (params.q) queryParams.set('q', params.q);
  if (params.type) queryParams.set('type', params.type);
  if (params.template) queryParams.set('template', params.template);
  if (params.party) queryParams.set('party', params.party);
  if (params.limit) queryParams.set('limit', params.limit.toString());

  return apiFetch(`/api/search?${queryParams}`);
}

export async function searchContractById(idPrefix: string): Promise<ApiResponse<any[]>> {
  return apiFetch(`/api/search/contract/${encodeURIComponent(idPrefix)}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Health Check
// ─────────────────────────────────────────────────────────────────────────────

export async function checkHealth(): Promise<{ status: string; timestamp: string }> {
  return apiFetch('/health');
}

/**
 * Check if the DuckDB API is available
 */
export async function isApiAvailable(): Promise<boolean> {
  try {
    await checkHealth();
    return true;
  } catch {
    return false;
  }
}
