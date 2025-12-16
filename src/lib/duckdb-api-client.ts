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
  effective_at?: string;
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
// Backfill API
// ─────────────────────────────────────────────────────────────────────────────

export interface BackfillCursor {
  id: string;
  cursor_name: string;
  migration_id: number;
  synchronizer_id: string;
  min_time: string;
  max_time: string;
  last_before: string | null;
  complete: boolean;
  last_processed_round: number;
  updated_at: string;
  started_at?: string;
  total_updates?: number;
  total_events?: number;
  pending_writes?: number;
  buffered_records?: number;
  is_recently_updated?: boolean;
  error?: string;
}

export interface BackfillStats {
  totalUpdates: number;
  totalEvents: number;
  activeMigrations: number;
  totalCursors: number;
  completedCursors: number;
}

export interface WriteActivity {
  isWriting: boolean;
  currentCounts: { events: number; updates: number };
  delta: { events: number; updates: number; seconds: number };
  message: string;
}

export interface BackfillDebugInfo {
  cursorDir: string;
  cursorDirExists: boolean;
  cursorFiles: string[];
  rawFileCounts: { events: number; updates: number };
  dataDir: string;
}

export async function getBackfillCursors(): Promise<ApiResponse<BackfillCursor[]>> {
  return apiFetch('/api/backfill/cursors');
}

export async function getBackfillStats(): Promise<BackfillStats> {
  return apiFetch('/api/backfill/stats');
}

export async function getWriteActivity(): Promise<WriteActivity> {
  return apiFetch('/api/backfill/write-activity');
}

export async function getBackfillDebugInfo(): Promise<BackfillDebugInfo> {
  return apiFetch('/api/backfill/debug');
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
// ACS (Active Contract Set) API
// ─────────────────────────────────────────────────────────────────────────────

export interface ACSSnapshot {
  id: string;
  timestamp: string;
  migration_id: number;
  record_time: string;
  entry_count: number;
  template_count?: number;
  status: string;
  source: string;
}

export interface ACSTemplateStats {
  template_id: string;
  entity_name: string;
  module_name: string;
  contract_count: number;
  unique_contracts: number;
}

export interface ACSStats {
  total_contracts: number;
  total_templates: number;
  total_snapshots: number;
  latest_snapshot: string | null;
  latest_record_time: string | null;
}

export async function getACSSnapshots(): Promise<ApiResponse<ACSSnapshot[]>> {
  return apiFetch('/api/acs/snapshots');
}

export async function getLatestACSSnapshot(): Promise<{ data: ACSSnapshot | null }> {
  return apiFetch('/api/acs/latest');
}

export async function getACSTemplates(limit = 100): Promise<ApiResponse<ACSTemplateStats[]>> {
  return apiFetch(`/api/acs/templates?limit=${limit}`);
}

export async function getACSContracts(params: { template?: string; entity?: string; limit?: number; offset?: number }): Promise<ApiResponse<any[]>> {
  const queryParams = new URLSearchParams();
  if (params.template) queryParams.set('template', params.template);
  if (params.entity) queryParams.set('entity', params.entity);
  if (params.limit) queryParams.set('limit', params.limit.toString());
  if (params.offset) queryParams.set('offset', params.offset.toString());
  return apiFetch(`/api/acs/contracts?${queryParams}`);
}

export async function getACSStats(): Promise<{ data: ACSStats }> {
  return apiFetch('/api/acs/stats');
}

export async function getACSSupply(): Promise<{ data: any }> {
  return apiFetch('/api/acs/supply');
}

export interface RichListHolder {
  owner: string;
  amount: number;
  locked: number;
  total: number;
}

export interface RichListResponse {
  data: RichListHolder[];
  totalSupply: number;
  unlockedSupply: number;
  lockedSupply: number;
  holderCount: number;
}

export async function getACSRichList(params: { limit?: number; search?: string } = {}): Promise<RichListResponse> {
  const queryParams = new URLSearchParams();
  if (params.limit) queryParams.set('limit', params.limit.toString());
  if (params.search) queryParams.set('search', params.search);
  return apiFetch(`/api/acs/rich-list?${queryParams}`);
}

// Allocations
export interface AllocationData {
  contract_id: string;
  executor: string;
  sender: string;
  receiver: string;
  amount: number;
  requested_at: string;
  transfer_leg_id: string;
  payload: any;
}

export interface AllocationsResponse {
  data: AllocationData[];
  totalCount: number;
  totalAmount: number;
  uniqueExecutors: number;
}

export async function getACSAllocations(params: { limit?: number; offset?: number; search?: string } = {}): Promise<AllocationsResponse> {
  const queryParams = new URLSearchParams();
  if (params.limit) queryParams.set('limit', params.limit.toString());
  if (params.offset) queryParams.set('offset', params.offset.toString());
  if (params.search) queryParams.set('search', params.search);
  return apiFetch(`/api/acs/allocations?${queryParams}`);
}

// Mining Rounds
export interface MiningRound {
  contract_id: string;
  round_number: string;
  opens_at: string;
  target_closes_at: string;
  amulet_price?: string;
  issuance_per_sv_reward?: string;
  issuance_per_validator_reward?: string;
  payload: any;
}

export interface MiningRoundsResponse {
  openRounds: MiningRound[];
  issuingRounds: MiningRound[];
  closedRounds: MiningRound[];
  counts: {
    open: number;
    issuing: number;
    closed: number;
  };
}

export async function getACSMiningRounds(params: { closedLimit?: number } = {}): Promise<MiningRoundsResponse> {
  const queryParams = new URLSearchParams();
  if (params.closedLimit) queryParams.set('closedLimit', params.closedLimit.toString());
  return apiFetch(`/api/acs/mining-rounds?${queryParams}`);
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
