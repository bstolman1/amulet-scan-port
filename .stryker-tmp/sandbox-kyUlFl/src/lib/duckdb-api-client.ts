/**
 * DuckDB API Client
 * 
 * Client for the local DuckDB API server.
 */
// @ts-nocheck


// NOTE: In Lovable preview, localhost is not reachable. When running locally, this resolves
// to http://localhost:3001 (or http://<your-host>:3001 if hosting the UI on your LAN).
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
import { getDuckDBApiUrl } from "@/lib/backend-config";
interface ApiResponse<T> {
  data: T;
  count?: number;
  error?: string;
}
function getApiBaseUrl() {
  if (stryMutAct_9fa48("4843")) {
    {}
  } else {
    stryCov_9fa48("4843");
    return getDuckDBApiUrl();
  }
}

/**
 * Generic fetch wrapper with error handling
 */
export async function apiFetch<T>(endpoint: string, options?: RequestInit): Promise<T> {
  if (stryMutAct_9fa48("4844")) {
    {}
  } else {
    stryCov_9fa48("4844");
    const API_BASE_URL = getApiBaseUrl();
    const response = await fetch(`${API_BASE_URL}${endpoint}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...(stryMutAct_9fa48("4849") ? options.headers : (stryCov_9fa48("4849"), options?.headers))
      }
    });
    if (stryMutAct_9fa48("4852") ? false : stryMutAct_9fa48("4851") ? true : stryMutAct_9fa48("4850") ? response.ok : (stryCov_9fa48("4850", "4851", "4852"), !response.ok)) {
      if (stryMutAct_9fa48("4853")) {
        {}
      } else {
        stryCov_9fa48("4853");
        const error = await response.json().catch(stryMutAct_9fa48("4854") ? () => undefined : (stryCov_9fa48("4854"), () => ({
          error: 'Unknown error'
        })));
        throw new Error(stryMutAct_9fa48("4859") ? error.error && `API error: ${response.status}` : stryMutAct_9fa48("4858") ? false : stryMutAct_9fa48("4857") ? true : (stryCov_9fa48("4857", "4858", "4859"), error.error || `API error: ${response.status}`));
      }
    }
    return response.json();
  }
}

/**
 * Invalidate server-side ACS cache (useful right after new snapshots are written).
 */
export async function invalidateAcsCache(prefix = 'acs:') {
  if (stryMutAct_9fa48("4862")) {
    {}
  } else {
    stryCov_9fa48("4862");
    return apiFetch('/api/acs/cache/invalidate', {
      method: 'POST',
      body: JSON.stringify({
        prefix
      })
    });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Events API
// ─────────────────────────────────────────────────────────────────────────────

export interface LedgerEvent {
  event_id: string;
  update_id?: string;
  event_type: string;
  event_type_original?: string;
  contract_id: string;
  template_id: string;
  package_name: string;
  timestamp: string;
  effective_at?: string;
  created_at_ts?: string;
  synchronizer_id?: string;
  migration_id?: number;
  signatories: string[];
  observers: string[];
  acting_parties?: string[];
  witness_parties?: string[];
  choice?: string | null;
  consuming?: boolean;
  payload: any;
  raw?: any;
}
export async function getLatestEvents(limit = 100, offset = 0): Promise<ApiResponse<LedgerEvent[]>> {
  if (stryMutAct_9fa48("4867")) {
    {}
  } else {
    stryCov_9fa48("4867");
    return apiFetch(`/api/events/latest?limit=${limit}&offset=${offset}`);
  }
}
export async function getEventsByType(type: string, limit = 100): Promise<ApiResponse<LedgerEvent[]>> {
  if (stryMutAct_9fa48("4869")) {
    {}
  } else {
    stryCov_9fa48("4869");
    return apiFetch(`/api/events/by-type/${encodeURIComponent(type)}?limit=${limit}`);
  }
}
export async function getEventsByTemplate(templateId: string, limit = 100): Promise<ApiResponse<LedgerEvent[]>> {
  if (stryMutAct_9fa48("4871")) {
    {}
  } else {
    stryCov_9fa48("4871");
    return apiFetch(`/api/events/by-template/${encodeURIComponent(templateId)}?limit=${limit}`);
  }
}
export async function getEventsByDateRange(start: string, end: string, limit = 1000): Promise<ApiResponse<LedgerEvent[]>> {
  if (stryMutAct_9fa48("4873")) {
    {}
  } else {
    stryCov_9fa48("4873");
    return apiFetch(`/api/events/by-date?start=${start}&end=${end}&limit=${limit}`);
  }
}
export async function getEventsCount(): Promise<{
  count: number;
}> {
  if (stryMutAct_9fa48("4875")) {
    {}
  } else {
    stryCov_9fa48("4875");
    return apiFetch('/api/events/count');
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Updates API
// ─────────────────────────────────────────────────────────────────────────────

export interface LedgerUpdateRecord {
  update_id: string;
  update_type: string;
  migration_id?: number | null;
  synchronizer_id?: string | null;
  record_time?: string | null;
  effective_at?: string | null;
  timestamp?: string | null;
  workflow_id?: string | null;
  command_id?: string | null;
  kind?: string | null;
  offset?: string | number | null;
  root_event_ids?: string[] | null;
  event_count?: number | null;
  update_data?: any;
}
export async function getLatestUpdates(limit = 100, offset = 0): Promise<ApiResponse<LedgerUpdateRecord[]>> {
  if (stryMutAct_9fa48("4877")) {
    {}
  } else {
    stryCov_9fa48("4877");
    return apiFetch(`/api/updates/latest?limit=${limit}&offset=${offset}`);
  }
}
export async function getUpdatesCount(): Promise<{
  count: number;
}> {
  if (stryMutAct_9fa48("4879")) {
    {}
  } else {
    stryCov_9fa48("4879");
    return apiFetch('/api/updates/count');
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Party API
// ─────────────────────────────────────────────────────────────────────────────

export async function getPartyEvents(partyId: string, limit = 100): Promise<ApiResponse<LedgerEvent[]>> {
  if (stryMutAct_9fa48("4881")) {
    {}
  } else {
    stryCov_9fa48("4881");
    return apiFetch(`/api/party/${encodeURIComponent(partyId)}?limit=${limit}`);
  }
}
export interface PartySummary {
  event_type: string;
  count: number;
  first_seen: string;
  last_seen: string;
}
export async function getPartySummary(partyId: string): Promise<ApiResponse<PartySummary[]>> {
  if (stryMutAct_9fa48("4883")) {
    {}
  } else {
    stryCov_9fa48("4883");
    return apiFetch(`/api/party/${encodeURIComponent(partyId)}/summary`);
  }
}
export async function getAllParties(limit = 1000): Promise<ApiResponse<string[]>> {
  if (stryMutAct_9fa48("4885")) {
    {}
  } else {
    stryCov_9fa48("4885");
    return apiFetch(`/api/party/list/all?limit=${limit}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Contracts API
// ─────────────────────────────────────────────────────────────────────────────

export async function getContractLifecycle(contractId: string): Promise<ApiResponse<LedgerEvent[]>> {
  if (stryMutAct_9fa48("4887")) {
    {}
  } else {
    stryCov_9fa48("4887");
    return apiFetch(`/api/contracts/${encodeURIComponent(contractId)}`);
  }
}
export async function getActiveContractsByTemplate(templateSuffix: string, limit = 100): Promise<ApiResponse<any[]>> {
  if (stryMutAct_9fa48("4889")) {
    {}
  } else {
    stryCov_9fa48("4889");
    return apiFetch(`/api/contracts/active/by-template/${encodeURIComponent(templateSuffix)}?limit=${limit}`);
  }
}
export interface TemplateInfo {
  template_id: string;
  event_count: number;
  contract_count: number;
}
export async function getTemplatesList(): Promise<ApiResponse<TemplateInfo[]>> {
  if (stryMutAct_9fa48("4891")) {
    {}
  } else {
    stryCov_9fa48("4891");
    return apiFetch('/api/contracts/templates/list');
  }
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
  if (stryMutAct_9fa48("4893")) {
    {}
  } else {
    stryCov_9fa48("4893");
    return apiFetch('/api/stats/overview');
  }
}
export interface DailyStats {
  date: string;
  event_count: number;
  contract_count: number;
}
export async function getDailyStats(days = 30): Promise<ApiResponse<DailyStats[]>> {
  if (stryMutAct_9fa48("4895")) {
    {}
  } else {
    stryCov_9fa48("4895");
    return apiFetch(`/api/stats/daily?days=${days}`);
  }
}
export interface TypeStats {
  event_type: string;
  count: number;
}
export async function getStatsByType(): Promise<ApiResponse<TypeStats[]>> {
  if (stryMutAct_9fa48("4897")) {
    {}
  } else {
    stryCov_9fa48("4897");
    return apiFetch('/api/stats/by-type');
  }
}
export interface TemplateStats {
  template_id: string;
  event_count: number;
  contract_count: number;
  first_seen: string;
  last_seen: string;
}
export async function getStatsByTemplate(limit = 50): Promise<ApiResponse<TemplateStats[]>> {
  if (stryMutAct_9fa48("4899")) {
    {}
  } else {
    stryCov_9fa48("4899");
    return apiFetch(`/api/stats/by-template?limit=${limit}`);
  }
}
export interface HourlyStats {
  hour: string;
  event_count: number;
}
export async function getHourlyStats(): Promise<ApiResponse<HourlyStats[]>> {
  if (stryMutAct_9fa48("4901")) {
    {}
  } else {
    stryCov_9fa48("4901");
    return apiFetch('/api/stats/hourly');
  }
}
export interface BurnStats {
  date: string;
  burn_amount: number;
}
export async function getBurnStats(): Promise<ApiResponse<BurnStats[]>> {
  if (stryMutAct_9fa48("4903")) {
    {}
  } else {
    stryCov_9fa48("4903");
    return apiFetch('/api/stats/burn');
  }
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
  currentCounts: {
    events: number;
    updates: number;
  };
  delta: {
    events: number;
    updates: number;
    seconds: number;
  };
  message: string;
}
export interface BackfillDebugInfo {
  cursorDir: string;
  cursorDirExists: boolean;
  cursorFiles: string[];
  rawFileCounts: {
    events: number;
    updates: number;
  };
  dataDir: string;
}
export async function getBackfillCursors(): Promise<ApiResponse<BackfillCursor[]>> {
  if (stryMutAct_9fa48("4905")) {
    {}
  } else {
    stryCov_9fa48("4905");
    return apiFetch('/api/backfill/cursors');
  }
}
export async function getBackfillStats(): Promise<BackfillStats> {
  if (stryMutAct_9fa48("4907")) {
    {}
  } else {
    stryCov_9fa48("4907");
    return apiFetch('/api/backfill/stats');
  }
}
export async function getWriteActivity(): Promise<WriteActivity> {
  if (stryMutAct_9fa48("4909")) {
    {}
  } else {
    stryCov_9fa48("4909");
    return apiFetch('/api/backfill/write-activity');
  }
}
export async function getBackfillDebugInfo(): Promise<BackfillDebugInfo> {
  if (stryMutAct_9fa48("4911")) {
    {}
  } else {
    stryCov_9fa48("4911");
    return apiFetch('/api/backfill/debug');
  }
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
  if (stryMutAct_9fa48("4913")) {
    {}
  } else {
    stryCov_9fa48("4913");
    const queryParams = new URLSearchParams();
    if (stryMutAct_9fa48("4915") ? false : stryMutAct_9fa48("4914") ? true : (stryCov_9fa48("4914", "4915"), params.q)) queryParams.set('q', params.q);
    if (stryMutAct_9fa48("4918") ? false : stryMutAct_9fa48("4917") ? true : (stryCov_9fa48("4917", "4918"), params.type)) queryParams.set('type', params.type);
    if (stryMutAct_9fa48("4921") ? false : stryMutAct_9fa48("4920") ? true : (stryCov_9fa48("4920", "4921"), params.template)) queryParams.set('template', params.template);
    if (stryMutAct_9fa48("4924") ? false : stryMutAct_9fa48("4923") ? true : (stryCov_9fa48("4923", "4924"), params.party)) queryParams.set('party', params.party);
    if (stryMutAct_9fa48("4927") ? false : stryMutAct_9fa48("4926") ? true : (stryCov_9fa48("4926", "4927"), params.limit)) queryParams.set('limit', params.limit.toString());
    return apiFetch(`/api/search?${queryParams}`);
  }
}
export async function searchContractById(idPrefix: string): Promise<ApiResponse<any[]>> {
  if (stryMutAct_9fa48("4930")) {
    {}
  } else {
    stryCov_9fa48("4930");
    return apiFetch(`/api/search/contract/${encodeURIComponent(idPrefix)}`);
  }
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
  if (stryMutAct_9fa48("4932")) {
    {}
  } else {
    stryCov_9fa48("4932");
    return apiFetch('/api/acs/snapshots');
  }
}
export async function getLatestACSSnapshot(): Promise<{
  data: ACSSnapshot | null;
}> {
  if (stryMutAct_9fa48("4934")) {
    {}
  } else {
    stryCov_9fa48("4934");
    return apiFetch('/api/acs/latest');
  }
}
export async function getACSTemplates(limit = 100): Promise<ApiResponse<ACSTemplateStats[]>> {
  if (stryMutAct_9fa48("4936")) {
    {}
  } else {
    stryCov_9fa48("4936");
    return apiFetch(`/api/acs/templates?limit=${limit}`);
  }
}
export async function getACSContracts(params: {
  template?: string;
  entity?: string;
  limit?: number;
  offset?: number;
}): Promise<ApiResponse<any[]>> {
  if (stryMutAct_9fa48("4938")) {
    {}
  } else {
    stryCov_9fa48("4938");
    const queryParams = new URLSearchParams();
    if (stryMutAct_9fa48("4940") ? false : stryMutAct_9fa48("4939") ? true : (stryCov_9fa48("4939", "4940"), params.template)) queryParams.set('template', params.template);
    if (stryMutAct_9fa48("4943") ? false : stryMutAct_9fa48("4942") ? true : (stryCov_9fa48("4942", "4943"), params.entity)) queryParams.set('entity', params.entity);
    if (stryMutAct_9fa48("4946") ? false : stryMutAct_9fa48("4945") ? true : (stryCov_9fa48("4945", "4946"), params.limit)) queryParams.set('limit', params.limit.toString());
    if (stryMutAct_9fa48("4949") ? false : stryMutAct_9fa48("4948") ? true : (stryCov_9fa48("4948", "4949"), params.offset)) queryParams.set('offset', params.offset.toString());
    return apiFetch(`/api/acs/contracts?${queryParams}`);
  }
}
export async function getACSStats(): Promise<{
  data: ACSStats;
}> {
  if (stryMutAct_9fa48("4952")) {
    {}
  } else {
    stryCov_9fa48("4952");
    return apiFetch('/api/acs/stats');
  }
}
export async function getACSSupply(): Promise<{
  data: any;
}> {
  if (stryMutAct_9fa48("4954")) {
    {}
  } else {
    stryCov_9fa48("4954");
    return apiFetch('/api/acs/supply');
  }
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
export async function getACSRichList(params: {
  limit?: number;
  search?: string;
} = {}): Promise<RichListResponse> {
  if (stryMutAct_9fa48("4956")) {
    {}
  } else {
    stryCov_9fa48("4956");
    const queryParams = new URLSearchParams();
    if (stryMutAct_9fa48("4958") ? false : stryMutAct_9fa48("4957") ? true : (stryCov_9fa48("4957", "4958"), params.limit)) queryParams.set('limit', params.limit.toString());
    if (stryMutAct_9fa48("4961") ? false : stryMutAct_9fa48("4960") ? true : (stryCov_9fa48("4960", "4961"), params.search)) queryParams.set('search', params.search);
    return apiFetch(`/api/acs/rich-list?${queryParams}`);
  }
}

// ANS (Amulet Name Service) search via ACS
export interface AnsEntry {
  contract_id: string;
  name: string;
  user: string;
  expires_at: string;
  payload: any;
}
export async function searchAnsEntries(search: string, limit = 25): Promise<ApiResponse<AnsEntry[]>> {
  if (stryMutAct_9fa48("4964")) {
    {}
  } else {
    stryCov_9fa48("4964");
    const queryParams = new URLSearchParams();
    queryParams.set('template', 'AnsEntry');
    queryParams.set('limit', limit.toString());
    if (stryMutAct_9fa48("4969") ? false : stryMutAct_9fa48("4968") ? true : (stryCov_9fa48("4968", "4969"), search)) queryParams.set('search', search);
    return apiFetch(`/api/acs/contracts?${queryParams}`);
  }
}

// Real-time supply (snapshot + v2/updates delta)
export interface RealtimeSupplyData {
  snapshot: {
    timestamp: string;
    migration_id: number;
    record_time: string;
    unlocked: number;
    locked: number;
    total: number;
  };
  delta: {
    since: string;
    unlocked: number;
    locked: number;
    total: number;
    events: {
      created: number;
      archived: number;
    };
  };
  realtime: {
    unlocked: number;
    locked: number;
    total: number;
    circulating: number;
  };
  calculated_at: string;
}
export async function getRealtimeSupply(): Promise<{
  data: RealtimeSupplyData | null;
}> {
  if (stryMutAct_9fa48("4972")) {
    {}
  } else {
    stryCov_9fa48("4972");
    return apiFetch('/api/acs/realtime-supply');
  }
}

// Real-time rich list (snapshot + v2/updates delta)
export interface RealtimeRichListResponse extends RichListResponse {
  snapshotRecordTime: string;
  isRealtime: boolean;
}
export async function getRealtimeRichList(params: {
  limit?: number;
  search?: string;
} = {}): Promise<RealtimeRichListResponse> {
  if (stryMutAct_9fa48("4974")) {
    {}
  } else {
    stryCov_9fa48("4974");
    const queryParams = new URLSearchParams();
    if (stryMutAct_9fa48("4976") ? false : stryMutAct_9fa48("4975") ? true : (stryCov_9fa48("4975", "4976"), params.limit)) queryParams.set('limit', params.limit.toString());
    if (stryMutAct_9fa48("4979") ? false : stryMutAct_9fa48("4978") ? true : (stryCov_9fa48("4978", "4979"), params.search)) queryParams.set('search', params.search);
    return apiFetch(`/api/acs/realtime-rich-list?${queryParams}`);
  }
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
export async function getACSAllocations(params: {
  limit?: number;
  offset?: number;
  search?: string;
} = {}): Promise<AllocationsResponse> {
  if (stryMutAct_9fa48("4982")) {
    {}
  } else {
    stryCov_9fa48("4982");
    const queryParams = new URLSearchParams();
    if (stryMutAct_9fa48("4984") ? false : stryMutAct_9fa48("4983") ? true : (stryCov_9fa48("4983", "4984"), params.limit)) queryParams.set('limit', params.limit.toString());
    if (stryMutAct_9fa48("4987") ? false : stryMutAct_9fa48("4986") ? true : (stryCov_9fa48("4986", "4987"), params.offset)) queryParams.set('offset', params.offset.toString());
    if (stryMutAct_9fa48("4990") ? false : stryMutAct_9fa48("4989") ? true : (stryCov_9fa48("4989", "4990"), params.search)) queryParams.set('search', params.search);
    return apiFetch(`/api/acs/allocations?${queryParams}`);
  }
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
export async function getACSMiningRounds(params: {
  closedLimit?: number;
} = {}): Promise<MiningRoundsResponse> {
  if (stryMutAct_9fa48("4993")) {
    {}
  } else {
    stryCov_9fa48("4993");
    const queryParams = new URLSearchParams();
    if (stryMutAct_9fa48("4995") ? false : stryMutAct_9fa48("4994") ? true : (stryCov_9fa48("4994", "4995"), params.closedLimit)) queryParams.set('closedLimit', params.closedLimit.toString());
    return apiFetch(`/api/acs/mining-rounds?${queryParams}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Live Status API
// ─────────────────────────────────────────────────────────────────────────────

export interface LiveCursor {
  migration_id: number;
  record_time: string;
  updated_at: string;
  mode: string;
}
export interface LiveStatus {
  mode: 'live' | 'backfill' | 'unknown';
  status: 'running' | 'idle' | 'stopped';
  live_cursor: LiveCursor | null;
  backfill_cursors: Array<{
    file: string;
    migration_id: number;
    min_time?: string;
    max_time?: string;
    complete?: boolean;
  }>;
  all_backfill_complete: boolean;
  latest_file_write: string | null;
  earliest_file_write: string | null;
  current_record_time: string | null;
  suggestion: string | null;
}
export async function getLiveStatus(): Promise<LiveStatus> {
  if (stryMutAct_9fa48("4998")) {
    {}
  } else {
    stryCov_9fa48("4998");
    return apiFetch('/api/stats/live-status');
  }
}
export async function purgeLiveCursor(): Promise<{
  success: boolean;
  message: string;
  deleted_file: string | null;
}> {
  if (stryMutAct_9fa48("5000")) {
    {}
  } else {
    stryCov_9fa48("5000");
    return apiFetch('/api/stats/live-cursor', {
      method: 'DELETE'
    });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Health Check
// ─────────────────────────────────────────────────────────────────────────────

export async function checkHealth(): Promise<{
  status: string;
  timestamp: string;
}> {
  if (stryMutAct_9fa48("5004")) {
    {}
  } else {
    stryCov_9fa48("5004");
    return apiFetch('/health');
  }
}

/**
 * Check if the DuckDB API is available
 */
export async function isApiAvailable(): Promise<boolean> {
  if (stryMutAct_9fa48("5006")) {
    {}
  } else {
    stryCov_9fa48("5006");
    try {
      if (stryMutAct_9fa48("5007")) {
        {}
      } else {
        stryCov_9fa48("5007");
        await checkHealth();
        return stryMutAct_9fa48("5008") ? false : (stryCov_9fa48("5008"), true);
      }
    } catch {
      if (stryMutAct_9fa48("5009")) {
        {}
      } else {
        stryCov_9fa48("5009");
        return stryMutAct_9fa48("5010") ? true : (stryCov_9fa48("5010"), false);
      }
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ACS Status API (for graceful degradation during snapshots)
// ─────────────────────────────────────────────────────────────────────────────

export interface ACSStatusResponse {
  available: boolean;
  snapshotInProgress: boolean;
  completeSnapshotCount: number;
  inProgressSnapshotCount: number;
  latestComplete: {
    migrationId: number;
    snapshotTime: string;
  } | null;
  message: string;
  error?: string;
}
export async function getACSStatus(): Promise<ACSStatusResponse> {
  if (stryMutAct_9fa48("5011")) {
    {}
  } else {
    stryCov_9fa48("5011");
    return apiFetch('/api/acs/status');
  }
}