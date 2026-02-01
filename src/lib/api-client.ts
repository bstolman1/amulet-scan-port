// SCANTON API Client — COMPLETE FILE
// ------------------------------------------------------------------
// All Scan API calls are proxied through our backend to avoid CORS and rate-limit issues.
// Rule: Browser → our API → Scan API (never browser → Scan directly)
// CRITICAL: Frontend must NEVER know the real Scan URL — only /api/scan-proxy

// Backend proxy base URL - all Scan API calls go through here
// Hard-coded to prevent any env variable leakage (e.g., VITE_SCAN_API_URL)
const API_BASE = "/api/scan-proxy";

/* =========================
 *    CORE SCAN HELPERS
 * ========================= */

/**
 * Centralized POST helper for SCAN endpoints that require POST.
 * Use for: endpoints that accept a request body
 */
async function scanPost<T>(path: string, body: unknown = {}): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`SCAN POST ${path} failed (${res.status}): ${text}`);
  }
  return res.json();
}

/**
 * GET helper for SCAN endpoints that are explicitly GET-only.
 * Use for: metadata endpoints, lookups by path param, simple queries
 */
async function scanGet<T>(path: string, queryParams?: Record<string, string | number>): Promise<T> {
  let url = `${API_BASE}${path}`;
  if (queryParams && Object.keys(queryParams).length > 0) {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(queryParams)) {
      params.append(k, String(v));
    }
    url += `?${params.toString()}`;
  }
  const res = await fetch(url, {
    method: "GET",
    headers: { Accept: "application/json" },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`SCAN GET ${path} failed (${res.status}): ${text}`);
  }
  return res.json();
}

/* =========================
 *         TYPES
 * ========================= */

export interface UpdateHistoryRequest {
  after?: {
    after_migration_id: number;
    after_record_time: string;
  };
  page_size: number;
  daml_value_encoding?: "compact_json" | "protobuf_json";
}

export interface UpdateHistoryResponse {
  transactions: Array<Transaction | Reassignment>;
}

export interface Transaction {
  update_id: string;
  migration_id: number;
  workflow_id: string;
  record_time: string;
  synchronizer_id: string;
  effective_at: string;
  root_event_ids: string[];
  events_by_id: Record<string, TreeEvent>;
}

export interface Reassignment {
  update_id: string;
  offset: string;
  record_time: string;
  event: AssignmentEvent | UnassignmentEvent;
}

export interface AssignmentEvent {
  submitter: string;
  source_synchronizer: string;
  target_synchronizer: string;
  migration_id: number;
  unassign_id: string;
  created_event: CreatedEvent;
  reassignment_counter: number;
}

export interface UnassignmentEvent {
  submitter: string;
  source_synchronizer: string;
  migration_id: number;
  target_synchronizer: string;
  unassign_id: string;
  reassignment_counter: number;
  contract_id: string;
}

export interface TreeEvent {
  event_type: "created_event" | "exercised_event";
  event_id: string;
  contract_id: string;
  template_id: string;
  package_name: string;
  [key: string]: any;
}

export interface CreatedEvent extends TreeEvent {
  event_type: "created_event";
  create_arguments: any;
  created_at: string;
  signatories: string[];
  observers: string[];
}

export interface ExercisedEvent extends TreeEvent {
  event_type: "exercised_event";
  choice: string;
  choice_argument: any;
  child_event_ids: string[];
  exercise_result: any;
  consuming: boolean;
  acting_parties: string[];
  interface_id?: string;
}

export interface TransactionHistoryRequest {
  page_end_event_id?: string;
  sort_order?: "asc" | "desc";
  page_size: number;
}

export interface TransactionHistoryResponse {
  transactions: TransactionHistoryItem[];
}

export interface TransactionHistoryItem {
  transaction_type: string;
  event_id: string;
  offset?: string;
  date: string;
  domain_id: string;
  round?: number;
  amulet_price?: string;
  transfer?: TransferData;
  mint?: AmuletAmount;
  tap?: AmuletAmount;
  abort_transfer_instruction?: AbortTransferInstruction;
}

export interface TransferData {
  provider: string;
  sender: SenderAmount;
  receivers: ReceiverAmount[];
  balance_changes: BalanceChange[];
  description?: string;
  transferInstructionReceiver?: string;
  transferInstructionAmount?: string;
  transferInstructionCid?: string;
  transfer_kind?: string;
}

export interface SenderAmount {
  party: string;
  input_amulet_amount?: string;
  input_app_reward_amount?: string;
  input_validator_reward_amount?: string;
  input_sv_reward_amount?: string;
  input_validator_faucet_amount?: string;
  sender_change_fee: string;
  sender_change_amount: string;
  sender_fee: string;
  holding_fees: string;
}

export interface ReceiverAmount {
  party: string;
  amount: string;
  receiver_fee: string;
}

export interface BalanceChange {
  party: string;
  change_to_initial_amount_as_of_round_zero: string;
  change_to_holding_fees_rate: string;
}

export interface AmuletAmount {
  amulet_owner: string;
  amulet_amount: string;
}

export interface AbortTransferInstruction {
  abort_kind: string;
  transfer_instruction_cid: string;
}

export interface ListRoundTotalsRequest {
  start_round: number;
  end_round: number;
}

export interface ListRoundTotalsResponse {
  entries: RoundTotals[];
}

export interface RoundTotals {
  closed_round: number;
  closed_round_effective_at: string;
  app_rewards: string;
  validator_rewards: string;
  change_to_initial_amount_as_of_round_zero?: string;
  change_to_holding_fees_rate?: string;
  cumulative_app_rewards: string;
  cumulative_validator_rewards: string;
  cumulative_change_to_initial_amount_as_of_round_zero?: string;
  cumulative_change_to_holding_fees_rate?: string;
  total_amulet_balance: string;
}

export interface GetTopValidatorsByValidatorRewardsResponse {
  validatorsAndRewards: PartyAndRewards[];
}

export interface GetTopProvidersByAppRewardsResponse {
  providersAndRewards: PartyAndRewards[];
}

export interface PartyAndRewards {
  provider: string;
  rewards: string;
  firstCollectedInRound?: number;
}

export interface GetOpenAndIssuingMiningRoundsRequest {
  cached_open_mining_round_contract_ids?: string[];
  cached_issuing_round_contract_ids?: string[];
}

export interface GetOpenAndIssuingMiningRoundsResponse {
  time_to_live_in_microseconds: number;
  open_mining_rounds: Record<string, ContractWithState>;
  issuing_mining_rounds: Record<string, ContractWithState>;
}

export interface ContractWithState {
  contract: Contract;
  domain_id?: string;
}

export interface Contract {
  template_id: string;
  contract_id: string;
  payload: any;
  created_event_blob: string;
  created_at: string;
}

export interface GetClosedRoundsResponse {
  rounds: ClosedRound[];
}

export interface ClosedRound {
  contract: Contract;
  domain_id: string;
}

export interface GetRoundOfLatestDataResponse {
  round: number;
  effectiveAt: string;
}

export interface GetTotalAmuletBalanceResponse {
  total_balance: string;
}

export interface ValidatorLivenessResponse {
  validatorsReceivedFaucets: ValidatorFaucetInfo[];
}

export interface ValidatorFaucetInfo {
  validator: string;
  numRoundsCollected: number;
  numRoundsMissed: number;
  firstCollectedInRound: number;
  lastCollectedInRound: number;
}

export interface DsoInfoResponse {
  sv_user?: string;
  sv_party_id?: string;
  dso_party_id?: string;
  voting_threshold?: number;
  latest_mining_round?: ContractWithState;
  amulet_rules?: ContractWithState;
  dso_rules?: ContractWithState;
  sv_node_states?: ContractWithState[];
  initial_round?: string;
}

export interface ScansResponse {
  scans: ScanGroup[];
}

export interface ScanGroup {
  domainId: string;
  scans: ScanInfo[];
}

export interface ScanInfo {
  publicUrl: string;
  svName: string;
}

export interface ValidatorLicensesResponse {
  validator_licenses: Contract[];
  next_page_token?: number;
}

export interface DsoSequencersResponse {
  domainSequencers: DomainSequencerGroup[];
}

export interface DomainSequencerGroup {
  domainId: string;
  sequencers: SequencerInfo[];
}

export interface SequencerInfo {
  migrationId: number;
  id: string;
  url: string;
  svName: string;
  availableAfter: string;
}

export interface ParticipantIdResponse {
  participant_id: string;
}

export interface TrafficStatusResponse {
  traffic_status: {
    actual: { total_consumed: number; total_limit: number };
    target: { total_purchased: number };
  };
}

export interface AcsSnapshotTimestampResponse {
  record_time: string;
}

export interface StateAcsRequest {
  migration_id: number;
  record_time: string;
  record_time_match?: "exact" | "before";
  after?: number;
  page_size: number;
  party_ids?: string[];
  templates?: string[];
}

export interface StateAcsResponse {
  record_time: string;
  migration_id: number;
  created_events: CreatedEvent[];
  next_page_token?: number;
}

export interface HoldingsSummaryRequest {
  migration_id: number;
  record_time: string;
  record_time_match?: "exact" | "before";
  owner_party_ids: string[];
  as_of_round?: number;
}

export interface HoldingsSummaryResponse {
  record_time: string;
  migration_id: number;
  computed_as_of_round: number;
  summaries: AmuletSummary[];
}

export interface AmuletSummary {
  party_id: string;
  total_unlocked_coin: string;
  total_locked_coin: string;
  total_coin_holdings: string;
  accumulated_holding_fees_unlocked: string;
  accumulated_holding_fees_locked: string;
  accumulated_holding_fees_total: string;
  total_available_coin: string;
}

export interface AnsEntriesResponse {
  entries: AnsEntry[];
}

export interface AnsEntry {
  contract_id: string | null;
  user: string;
  name: string;
  url: string;
  description: string;
  expires_at: string | null;
}

export interface AnsEntryResponse {
  entry: AnsEntry;
}

export interface DsoPartyIdResponse {
  dso_party_id: string;
}

export interface FeaturedAppsResponse {
  featured_apps: Contract[];
}

export interface FeaturedAppResponse {
  featured_app_right?: Contract;
}

export interface TopValidatorsByFaucetsResponse {
  validatorsByReceivedFaucets: ValidatorFaucetInfo[];
}

export interface TransferPreapprovalResponse {
  transfer_preapproval: ContractWithState;
}

export interface TransferCommandCounterResponse {
  transfer_command_counter: ContractWithState;
}

export interface TransferCommandStatusResponse {
  transfer_commands_by_contract_id: Record<string, any>;
}

export interface MigrationScheduleResponse {
  time: string;
  migration_id: number;
}

export interface SpliceInstanceNamesResponse {
  network_name: string;
  network_favicon_url: string;
  amulet_name: string;
  amulet_name_acronym: string;
  name_service_name: string;
  name_service_name_acronym: string;
}

export interface UpdateByIdResponse {
  update_id: string;
  migration_id: number;
  workflow_id: string;
  record_time: string;
  synchronizer_id: string;
  effective_at: string;
  offset?: string;
  root_event_ids: string[];
  events_by_id: Record<string, TreeEvent>;
}

export interface AcsSnapshotResponse {
  acs_snapshot: string;
}

export interface AggregatedRoundsResponse {
  start: number;
  end: number;
}

export interface RoundPartyTotalsRequest {
  start_round: number;
  end_round: number;
}

export interface RoundPartyTotalsResponse {
  entries: RoundPartyTotal[];
}

export interface RoundPartyTotal {
  closed_round: number;
  party: string;
  app_rewards: string;
  validator_rewards: string;
  traffic_purchased: number;
  traffic_purchased_cc_spent: string;
  traffic_num_purchases: number;
  cumulative_app_rewards: string;
  cumulative_validator_rewards: string;
  cumulative_change_to_initial_amount_as_of_round_zero: string;
  cumulative_change_to_holding_fees_rate: string;
  cumulative_traffic_purchased: number;
  cumulative_traffic_purchased_cc_spent: string;
  cumulative_traffic_num_purchases: number;
}

export interface WalletBalanceResponse {
  wallet_balance: string;
}

export interface AmuletConfigForRoundResponse {
  amulet_config: any;
}

/* =========================
 *       API CLIENT
 * ========================= */

export const scanApi = {
  /* ==========================================================
   *  POST ENDPOINTS (accept request body)
   * ========================================================== */

  // POST /v2/updates
  async fetchUpdates(request: UpdateHistoryRequest): Promise<UpdateHistoryResponse> {
    return scanPost("/v2/updates", request);
  },

  // POST /v0/open-and-issuing-mining-rounds
  async fetchOpenAndIssuingRounds(
    request: GetOpenAndIssuingMiningRoundsRequest = {}
  ): Promise<GetOpenAndIssuingMiningRoundsResponse> {
    return scanPost("/v0/open-and-issuing-mining-rounds", request);
  },

  // POST /v0/state/acs
  async fetchStateAcs(request: StateAcsRequest): Promise<StateAcsResponse> {
    return scanPost("/v0/state/acs", request);
  },

  // POST /v0/holdings/summary
  async fetchHoldingsSummary(request: HoldingsSummaryRequest): Promise<HoldingsSummaryResponse> {
    return scanPost("/v0/holdings/summary", request);
  },

  // POST /v0/events
  async fetchEvents(request: { after?: { after_migration_id: number; after_record_time: string }; page_size: number }): Promise<any> {
    return scanPost("/v0/events", request);
  },

  // POST /v0/amulet-rules
  async fetchAmuletRules(cachedContractId?: string, cachedDomainId?: string): Promise<any> {
    return scanPost("/v0/amulet-rules", {
      cached_amulet_rules_contract_id: cachedContractId,
      cached_amulet_rules_domain_id: cachedDomainId,
    });
  },

  // POST /v0/external-party-amulet-rules
  async fetchExternalPartyAmuletRules(): Promise<{ external_party_amulet_rules_update: any }> {
    return scanPost("/v0/external-party-amulet-rules", {});
  },

  // POST /v0/ans-rules
  async fetchAnsRules(): Promise<any> {
    return scanPost("/v0/ans-rules", {});
  },

  // POST /v0/voterequest (batch lookup)
  async fetchVoteRequestsBatch(contractIds: string[]): Promise<{ vote_requests: Contract[] }> {
    return scanPost("/v0/voterequest", { vote_request_contract_ids: contractIds });
  },

  // POST /v0/admin/sv/voteresults
  async fetchVoteResults(request: {
    actionName?: string;
    accepted?: boolean;
    requester?: string;
    effectiveFrom?: string;
    effectiveTo?: string;
    limit?: number;
  } = {}): Promise<{ dso_rules_vote_results: any[] }> {
    return scanPost("/v0/admin/sv/voteresults", request);
  },

  // POST /v0/backfilling/migration-info
  async fetchMigrationInfo(migrationId: number): Promise<any> {
    return scanPost("/v0/backfilling/migration-info", { migration_id: migrationId });
  },

  // POST /v0/backfilling/updates-before
  async fetchBackfillUpdatesBefore(request: {
    migration_id: number;
    synchronizer_id: string;
    before: string;
    at_or_after?: string;
    count: number;
  }): Promise<UpdateHistoryResponse> {
    return scanPost("/v0/backfilling/updates-before", request);
  },

  // POST /v1/updates (deprecated but kept)
  async fetchUpdatesV1(request: UpdateHistoryRequest): Promise<UpdateHistoryResponse> {
    return scanPost("/v1/updates", request);
  },

  // POST /v0/state/acs/force (disabled in prod)
  async forceAcsSnapshot(): Promise<{ record_time: string; migration_id: number }> {
    return scanPost("/v0/state/acs/force", {});
  },

  // POST /v0/holdings/state
  async fetchHoldingsState(request: {
    migration_id: number;
    record_time: string;
    record_time_match?: "exact" | "before";
    after?: number;
    page_size: number;
    owner_party_ids?: string[];
  }): Promise<StateAcsResponse> {
    return scanPost("/v0/holdings/state", request);
  },

  /* ==========================================================
   *  GET ENDPOINTS (no body, use path/query params)
   * ========================================================== */

  // GET /v0/dso
  async fetchDsoInfo(): Promise<DsoInfoResponse> {
    return scanGet("/v0/dso");
  },

  // GET /v0/closed-rounds
  async fetchClosedRounds(): Promise<GetClosedRoundsResponse> {
    return scanGet("/v0/closed-rounds");
  },

  // GET /v0/scans
  async fetchScans(): Promise<ScansResponse> {
    return scanGet("/v0/scans");
  },

  // GET /v0/admin/validator/licenses
  async fetchValidatorLicenses(after?: number, limit: number = 1000): Promise<ValidatorLicensesResponse> {
    const params: Record<string, string | number> = { limit };
    if (after !== undefined) params.after = after;
    return scanGet("/v0/admin/validator/licenses", params);
  },

  // GET /v0/dso-sequencers
  async fetchDsoSequencers(): Promise<DsoSequencersResponse> {
    return scanGet("/v0/dso-sequencers");
  },

  // GET /v0/domains/{domain_id}/parties/{party_id}/participant-id
  async fetchParticipantId(domainId: string, partyId: string): Promise<ParticipantIdResponse> {
    return scanGet(`/v0/domains/${encodeURIComponent(domainId)}/parties/${encodeURIComponent(partyId)}/participant-id`);
  },

  // GET /v0/domains/{domain_id}/members/{member_id}/traffic-status
  async fetchTrafficStatus(domainId: string, memberId: string): Promise<TrafficStatusResponse> {
    return scanGet(`/v0/domains/${encodeURIComponent(domainId)}/members/${encodeURIComponent(memberId)}/traffic-status`);
  },

  // GET /v0/state/acs/snapshot-timestamp
  async fetchAcsSnapshotTimestamp(before: string, migrationId: number): Promise<AcsSnapshotTimestampResponse> {
    return scanGet("/v0/state/acs/snapshot-timestamp", { before, migration_id: migrationId });
  },

  // GET /v0/state/acs/snapshot-timestamp-after
  async fetchAcsSnapshotTimestampAfter(after: string, migrationId: number): Promise<AcsSnapshotTimestampResponse> {
    return scanGet("/v0/state/acs/snapshot-timestamp-after", { after, migration_id: migrationId });
  },

  // GET /v0/ans-entries
  async fetchAnsEntries(namePrefix?: string, pageSize: number = 100): Promise<AnsEntriesResponse> {
    const params: Record<string, string | number> = { page_size: pageSize };
    if (namePrefix) params.name_prefix = namePrefix;
    return scanGet("/v0/ans-entries", params);
  },

  // GET /v0/ans-entries/by-party/{party}
  async fetchAnsEntryByParty(party: string): Promise<AnsEntryResponse> {
    return scanGet(`/v0/ans-entries/by-party/${encodeURIComponent(party)}`);
  },

  // GET /v0/ans-entries/by-name/{name}
  async fetchAnsEntryByName(name: string): Promise<AnsEntryResponse> {
    return scanGet(`/v0/ans-entries/by-name/${encodeURIComponent(name)}`);
  },

  // GET /v0/dso-party-id
  async fetchDsoPartyId(): Promise<DsoPartyIdResponse> {
    return scanGet("/v0/dso-party-id");
  },

  // GET /v0/featured-apps
  async fetchFeaturedApps(): Promise<FeaturedAppsResponse> {
    return scanGet("/v0/featured-apps");
  },

  // GET /v0/featured-apps/{provider_party_id}
  async fetchFeaturedApp(providerPartyId: string): Promise<FeaturedAppResponse> {
    return scanGet(`/v0/featured-apps/${encodeURIComponent(providerPartyId)}`);
  },

  // GET /v0/top-validators-by-validator-faucets?limit=N
  async fetchTopValidatorsByFaucets(limit: number = 1000): Promise<TopValidatorsByFaucetsResponse> {
    return scanGet("/v0/top-validators-by-validator-faucets", { limit });
  },

  // GET /v0/validators/validator-faucets?validator_ids=...
  async fetchValidatorLiveness(validatorIds: string[]): Promise<ValidatorLivenessResponse> {
    // Query param is repeated for each ID: ?validator_ids=a&validator_ids=b
    const params = new URLSearchParams();
    for (const id of validatorIds) {
      params.append("validator_ids", id);
    }
    const url = `${API_BASE}/v0/validators/validator-faucets?${params.toString()}`;
    const res = await fetch(url, { method: "GET", headers: { Accept: "application/json" } });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`GET /v0/validators/validator-faucets failed (${res.status}): ${text}`);
    }
    return res.json();
  },

  // GET /v0/transfer-preapprovals/by-party/{party}
  async fetchTransferPreapprovalByParty(party: string): Promise<TransferPreapprovalResponse> {
    return scanGet(`/v0/transfer-preapprovals/by-party/${encodeURIComponent(party)}`);
  },

  // GET /v0/transfer-command-counter/{party}
  async fetchTransferCommandCounter(party: string): Promise<TransferCommandCounterResponse> {
    return scanGet(`/v0/transfer-command-counter/${encodeURIComponent(party)}`);
  },

  // GET /v0/transfer-command/status?sender=...&nonce=...
  async fetchTransferCommandStatus(sender: string, nonce: number): Promise<TransferCommandStatusResponse> {
    return scanGet("/v0/transfer-command/status", { sender, nonce });
  },

  // GET /v0/migrations/schedule
  async fetchMigrationSchedule(): Promise<MigrationScheduleResponse> {
    return scanGet("/v0/migrations/schedule");
  },

  // GET /v0/splice-instance-names
  async fetchSpliceInstanceNames(): Promise<SpliceInstanceNamesResponse> {
    return scanGet("/v0/splice-instance-names");
  },

  // GET /v0/admin/sv/voterequests
  async fetchActiveVoteRequests(): Promise<{ dso_rules_vote_requests: any[] }> {
    return scanGet("/v0/admin/sv/voterequests");
  },

  // GET /v0/voterequests/{vote_request_contract_id}
  async fetchVoteRequestById(contractId: string): Promise<{ dso_rules_vote_request: Contract }> {
    return scanGet(`/v0/voterequests/${encodeURIComponent(contractId)}`);
  },

  // GET /v0/unclaimed-development-fund-coupons
  async fetchUnclaimedDevFundCoupons(): Promise<{ "unclaimed-development-fund-coupons": ContractWithState[] }> {
    return scanGet("/v0/unclaimed-development-fund-coupons");
  },

  // GET /v0/backfilling/status
  async fetchBackfillStatus(): Promise<{ complete: boolean }> {
    return scanGet("/v0/backfilling/status");
  },

  // GET /v0/feature-support
  async fetchFeatureSupport(): Promise<{ no_holding_fees_on_transfers?: boolean }> {
    return scanGet("/v0/feature-support");
  },

  // GET /v2/updates/{update_id}
  async fetchUpdateByIdV2(updateId: string, damlValueEncoding?: string): Promise<UpdateByIdResponse> {
    const params: Record<string, string> = {};
    if (damlValueEncoding) params.daml_value_encoding = damlValueEncoding;
    return scanGet(`/v2/updates/${encodeURIComponent(updateId)}`, params);
  },

  // GET /v1/updates/{update_id}
  async fetchUpdateByIdV1(updateId: string, damlValueEncoding?: string): Promise<UpdateByIdResponse> {
    const params: Record<string, string> = {};
    if (damlValueEncoding) params.daml_value_encoding = damlValueEncoding;
    return scanGet(`/v1/updates/${encodeURIComponent(updateId)}`, params);
  },

  // GET /v0/events/{update_id}
  async fetchEventById(updateId: string, damlValueEncoding?: string): Promise<any> {
    const params: Record<string, string> = {};
    if (damlValueEncoding) params.daml_value_encoding = damlValueEncoding;
    return scanGet(`/v0/events/${encodeURIComponent(updateId)}`, params);
  },

  // GET /v0/acs/{party} (deprecated)
  async fetchAcsSnapshot(party: string, recordTime?: string): Promise<AcsSnapshotResponse> {
    const params: Record<string, string> = {};
    if (recordTime) params.record_time = recordTime;
    return scanGet(`/v0/acs/${encodeURIComponent(party)}`, params);
  },

  // GET /v0/synchronizer-identities/{domain_id_prefix}
  async fetchSynchronizerIdentities(domainIdPrefix: string): Promise<any> {
    return scanGet(`/v0/synchronizer-identities/${encodeURIComponent(domainIdPrefix)}`);
  },

  // GET /v0/synchronizer-bootstrapping-transactions/{domain_id_prefix}
  async fetchSynchronizerBootstrappingTransactions(domainIdPrefix: string): Promise<any> {
    return scanGet(`/v0/synchronizer-bootstrapping-transactions/${encodeURIComponent(domainIdPrefix)}`);
  },

  /* ==========================================================
   *  COMPOSITE / CONVENIENCE METHODS
   * ========================================================== */

  // fetchTopValidators: maps faucets to expected format
  async fetchTopValidators(): Promise<GetTopValidatorsByValidatorRewardsResponse> {
    const data = await this.fetchTopValidatorsByFaucets(1000);
    return {
      validatorsAndRewards: (data.validatorsByReceivedFaucets || []).map((v) => ({
        provider: v.validator,
        rewards: String(v.numRoundsCollected),
        firstCollectedInRound: v.firstCollectedInRound,
      })),
    };
  },

  // GET /v0/top-providers-by-app-rewards?round=N&limit=N (deprecated)
  async fetchTopProviders(limit: number = 1000): Promise<GetTopProvidersByAppRewardsResponse> {
    const latest = await this.fetchLatestRound();
    return scanGet("/v0/top-providers-by-app-rewards", { round: latest.round, limit });
  },

  // GET /v0/round-of-latest-data (deprecated)
  async fetchLatestRound(): Promise<GetRoundOfLatestDataResponse> {
    return scanGet("/v0/round-of-latest-data");
  },

  // fetchTransactions: POST /v0/transactions
  async fetchTransactions(request: TransactionHistoryRequest): Promise<TransactionHistoryResponse> {
    return scanPost("/v0/transactions", request);
  },

  // fetchTransactionsByParty: POST /v0/transactions/by-party
  async fetchTransactionsByParty(party: string, limit: number = 20): Promise<TransactionHistoryResponse> {
    return scanPost("/v0/transactions/by-party", { party, limit });
  },

  // fetchRoundTotals: POST /v0/round-totals
  async fetchRoundTotals(request: ListRoundTotalsRequest): Promise<ListRoundTotalsResponse> {
    return scanPost("/v0/round-totals", request);
  },

  // fetchTotalBalance: derived from round totals
  async fetchTotalBalance(): Promise<GetTotalAmuletBalanceResponse> {
    const latest = await this.fetchLatestRound();
    const totals = await this.fetchRoundTotals({ start_round: latest.round, end_round: latest.round });
    if (totals.entries.length === 0) throw new Error("No round totals for latest round");
    return { total_balance: totals.entries[0].total_amulet_balance };
  },

  // fetchActivities: POST /v0/activities
  async fetchActivities(request: Record<string, unknown> = { page_size: 50 }): Promise<unknown> {
    return scanPost("/v0/activities", request);
  },

  // GET /v0/aggregated-rounds (deprecated)
  async fetchAggregatedRounds(): Promise<AggregatedRoundsResponse> {
    return scanGet("/v0/aggregated-rounds");
  },

  // POST /v0/round-party-totals (deprecated)
  async fetchRoundPartyTotals(request: RoundPartyTotalsRequest): Promise<RoundPartyTotalsResponse> {
    return scanPost("/v0/round-party-totals", request);
  },

  // GET /v0/amulet-config-for-round?round=N (deprecated)
  async fetchAmuletConfigForRound(round: number): Promise<AmuletConfigForRoundResponse> {
    return scanGet("/v0/amulet-config-for-round", { round });
  },

  // GET /v0/rewards-collected?round=N (deprecated)
  async fetchRewardsCollected(round?: number): Promise<{ amount: string }> {
    const params: Record<string, number> = {};
    if (round !== undefined) params.round = round;
    return scanGet("/v0/rewards-collected", params);
  },

  // GET /v0/top-validators-by-validator-rewards?round=N&limit=N (deprecated)
  async fetchTopValidatorsByRewards(round: number, limit: number): Promise<GetTopValidatorsByValidatorRewardsResponse> {
    return scanGet("/v0/top-validators-by-validator-rewards", { round, limit });
  },

  // GET /v0/top-validators-by-purchased-traffic?round=N&limit=N (deprecated)
  async fetchTopValidatorsByPurchasedTraffic(round: number, limit: number): Promise<any> {
    return scanGet("/v0/top-validators-by-purchased-traffic", { round, limit });
  },

  // GET /v0/amulet-price/votes
  async fetchAmuletPriceVotes(): Promise<{ amulet_price_votes: Contract[] }> {
    return scanGet("/v0/amulet-price/votes");
  },

  // POST /v0/backfilling/import-updates
  async fetchBackfillImportUpdates(request: { migration_id: number; after_update_id?: string; limit: number }): Promise<UpdateHistoryResponse> {
    return scanPost("/v0/backfilling/import-updates", request);
  },

  // GET /v0/updates/{update_id} (deprecated, use v2)
  async fetchUpdateByIdV0(updateId: string, lossless?: boolean): Promise<UpdateByIdResponse> {
    const params: Record<string, string> = {};
    if (lossless) params.lossless = "true";
    return scanGet(`/v0/updates/${encodeURIComponent(updateId)}`, params);
  },

  // POST /v0/updates (deprecated, use v2)
  async fetchUpdatesV0(request: UpdateHistoryRequest & { lossless?: boolean }): Promise<UpdateHistoryResponse> {
    return scanPost("/v0/updates", request);
  },

  /* ==========================================================
   *  MINING ROUNDS HELPERS
   * ========================================================== */

  async fetchAllMiningRoundsFromUpdates(): Promise<{
    open_rounds: { contract_id: string; round_number?: number; opened_at?: string; payload?: any }[];
    issuing_rounds: { contract_id: string; round_number?: number; issued_at?: string; payload?: any }[];
    closed_rounds: { contract_id: string; round_number?: number; closed_at?: string; payload?: any }[];
  }> {
    try {
      const data = await this.fetchUpdates({ page_size: 1000 });
      const roundStates = new Map<number, {
        contract_id: string;
        round_number: number;
        state: "open" | "issuing" | "closed";
        timestamp: string;
        payload?: any;
      }>();

      for (const tx of data.transactions || []) {
        const events = (tx as Transaction).events_by_id || {};
        for (const ev of Object.values(events)) {
          const tid = (ev as TreeEvent).template_id || "";
          const createdEv = ev as CreatedEvent;
          const roundNum = createdEv.create_arguments?.round?.number;
          if (!roundNum) continue;

          let state: "open" | "issuing" | "closed" | null = null;
          if (tid.includes("OpenMiningRound")) state = "open";
          else if (tid.includes("IssuingMiningRound")) state = "issuing";
          else if (tid.includes("ClosedMiningRound")) state = "closed";

          if (state) {
            const existing = roundStates.get(roundNum);
            const timestamp = (tx as Transaction).record_time;
            if (!existing || new Date(timestamp) > new Date(existing.timestamp)) {
              roundStates.set(roundNum, {
                contract_id: ev.contract_id,
                round_number: roundNum,
                state,
                timestamp,
                payload: createdEv.create_arguments,
              });
            }
          }
        }
      }

      const open_rounds: any[] = [];
      const issuing_rounds: any[] = [];
      const closed_rounds: any[] = [];

      for (const round of roundStates.values()) {
        if (round.state === "open") {
          open_rounds.push({ contract_id: round.contract_id, round_number: round.round_number, opened_at: round.timestamp, payload: round.payload });
        } else if (round.state === "issuing") {
          issuing_rounds.push({ contract_id: round.contract_id, round_number: round.round_number, issued_at: round.timestamp, payload: round.payload });
        } else if (round.state === "closed") {
          closed_rounds.push({ contract_id: round.contract_id, round_number: round.round_number, closed_at: round.timestamp, payload: round.payload });
        }
      }

      const sortByRound = <T extends { round_number?: number }>(arr: T[]) =>
        arr.sort((a, b) => (b.round_number || 0) - (a.round_number || 0));

      return {
        open_rounds: sortByRound(open_rounds),
        issuing_rounds: sortByRound(issuing_rounds),
        closed_rounds: sortByRound(closed_rounds).slice(0, 10),
      };
    } catch (e) {
      // Fallback
      try {
        const [dso, closed] = await Promise.all([
          this.fetchDsoInfo().catch(() => null),
          this.fetchClosedRounds().catch(() => null),
        ]);

        const open_rounds = dso?.latest_mining_round?.contract?.contract_id
          ? [{
              contract_id: dso.latest_mining_round.contract.contract_id,
              round_number: dso.latest_mining_round.contract.payload?.round?.number,
              opened_at: dso.latest_mining_round.contract.created_at,
              payload: dso.latest_mining_round.contract.payload,
            }]
          : [];

        const closed_rounds = (closed?.rounds || []).slice(0, 10).map((r) => ({
          contract_id: r.contract.contract_id,
          round_number: r.contract.payload?.round?.number,
          closed_at: r.contract.created_at,
          payload: r.contract.payload,
        }));

        return { open_rounds, issuing_rounds: [], closed_rounds };
      } catch {
        return { open_rounds: [], issuing_rounds: [], closed_rounds: [] };
      }
    }
  },

  async fetchAllMiningRoundsCurrent(): Promise<{
    open_rounds: { contract_id: string; round_number?: number; opened_at?: string; payload?: any }[];
    issuing_rounds: { contract_id: string; round_number?: number; issued_at?: string; payload?: any }[];
    closed_rounds: { contract_id: string; round_number?: number; closed_at?: string; payload?: any }[];
  }> {
    try {
      const allActiveRounds: Array<{
        contract_id: string;
        round_number?: number;
        timestamp?: string;
        payload?: any;
        type: "open" | "issuing";
      }> = [];

      try {
        const latest = await this.fetchLatestRound();
        const snap = await this.fetchAcsSnapshotTimestamp(latest.effectiveAt, 0);
        const acs = await this.fetchStateAcs({
          migration_id: 0,
          record_time: snap.record_time,
          page_size: 2000,
          templates: ["Splice.Round:OpenMiningRound", "Splice.Round:IssuingMiningRound"],
        });

        for (const ev of acs.created_events || []) {
          const tid = ev.template_id || "";
          const rnd = (ev as any).create_arguments?.round?.number;
          if (tid.includes("OpenMiningRound")) {
            allActiveRounds.push({ contract_id: ev.contract_id, round_number: rnd, timestamp: (ev as any).created_at, payload: (ev as any).create_arguments, type: "open" });
          } else if (tid.includes("IssuingMiningRound")) {
            allActiveRounds.push({ contract_id: ev.contract_id, round_number: rnd, timestamp: (ev as any).created_at, payload: (ev as any).create_arguments, type: "issuing" });
          }
        }
      } catch {
        try {
          const current = await this.fetchOpenAndIssuingRounds();
          for (const v of Object.values(current.open_mining_rounds || {})) {
            const c = (v as any).contract;
            allActiveRounds.push({ contract_id: c.contract_id, round_number: c?.payload?.round?.number, timestamp: c?.created_at, payload: c?.payload, type: "open" });
          }
          for (const v of Object.values(current.issuing_mining_rounds || {})) {
            const c = (v as any).contract;
            allActiveRounds.push({ contract_id: c.contract_id, round_number: c?.payload?.round?.number, timestamp: c?.created_at, payload: c?.payload, type: "issuing" });
          }
        } catch {}
      }

      allActiveRounds.sort((a, b) => (b.round_number || 0) - (a.round_number || 0));
      const recentActiveRounds = allActiveRounds.slice(0, 5);

      const open_rounds = recentActiveRounds.filter((r) => r.type === "open").map((r) => ({ contract_id: r.contract_id, round_number: r.round_number, opened_at: r.timestamp, payload: r.payload }));
      const issuing_rounds = recentActiveRounds.filter((r) => r.type === "issuing").map((r) => ({ contract_id: r.contract_id, round_number: r.round_number, issued_at: r.timestamp, payload: r.payload }));

      let closed_rounds: any[] = [];
      try {
        const closed = await this.fetchClosedRounds();
        closed_rounds = (closed.rounds || []).slice(0, 10).map((r) => ({ contract_id: r.contract.contract_id, round_number: r.contract.payload?.round?.number, closed_at: r.contract.created_at, payload: r.contract.payload }));
      } catch {}

      return { open_rounds, issuing_rounds, closed_rounds };
    } catch {
      return this.fetchAllMiningRoundsFromUpdates();
    }
  },

  /* ==========================================================
   *  GOVERNANCE HELPER
   * ========================================================== */

  async fetchGovernanceProposals(): Promise<
    Array<{
      id: string;
      title: string;
      description: string;
      status: "pending" | "executed" | "rejected" | "expired";
      votesFor?: number;
      votesAgainst?: number;
      createdAt?: string;
    }>
  > {
    try {
      const dso = await this.fetchDsoInfo();
      const latest = await this.fetchLatestRound();
      const snap = await this.fetchAcsSnapshotTimestamp(latest.effectiveAt, 0);

      const acs = await this.fetchStateAcs({
        migration_id: 0,
        record_time: snap.record_time,
        page_size: 1000,
        templates: ["Splice.DsoRules:VoteRequest", "Splice.DsoRules:DsoRules_CloseVoteRequestResult"],
      });

      const proposals: any[] = [];
      const byId: Record<string, any> = {};

      for (const ev of acs.created_events) {
        const templateId = ev.template_id || "";
        const cid = ev.contract_id;
        const payload = ev.create_arguments || {};

        if (templateId.includes("VoteRequest")) {
          const votes = (payload as any).votes || {};
          const votesFor = Object.values(votes).filter((v: any) => v?.accept || v?.Accept).length;
          const votesAgainst = Object.values(votes).filter((v: any) => v?.reject || v?.Reject).length;
          const action = (payload as any).action || {};
          const key = Object.keys(action)[0];
          const title = key ? key.replace(/ARC_|_/g, " ") : "Governance Proposal";

          byId[cid] = {
            id: cid.slice(0, 12),
            title,
            description: "Vote request",
            status: "pending",
            votesFor,
            votesAgainst,
            createdAt: (ev as any).created_at,
          };
        }

        if (templateId.includes("CloseVoteRequestResult")) {
          const outcome = (payload as any).outcome || {};
          let status: "executed" | "rejected" | "expired" = "executed";
          if (outcome.VRO_Rejected) status = "rejected";
          if (outcome.VRO_Expired) status = "expired";

          const base = byId[cid] || { id: cid.slice(0, 12), title: "Governance Proposal" };
          byId[cid] = { ...base, status, createdAt: (ev as any).created_at };
        }
      }

      Object.values(byId).forEach((p) => proposals.push(p));

      // Fallback to SV onboardings
      if (proposals.length === 0 && (dso as any)?.dso_rules?.contract?.payload?.svs) {
        const svs = (dso as any).dso_rules.contract.payload.svs;
        svs.slice(0, 20).forEach(([svPartyId, svInfo]: [string, any]) => {
          proposals.push({
            id: svPartyId.slice(0, 12),
            title: `Super Validator Onboarding: ${svInfo.name}`,
            description: `${svInfo.name} approved at round ${svInfo.joinedAsOfRound?.number || 0}`,
            status: "executed",
            votesFor: (dso as any).voting_threshold,
            votesAgainst: 0,
            createdAt: (dso as any).dso_rules.contract.created_at,
          });
        });
      }

      proposals.sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
      return proposals;
    } catch (e) {
      console.error("Error fetching governance proposals:", e);
      return [];
    }
  },
};
