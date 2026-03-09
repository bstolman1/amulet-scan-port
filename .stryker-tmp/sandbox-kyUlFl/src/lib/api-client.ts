// @ts-nocheck
// SCANTON API Client — COMPLETE FILE
// ------------------------------------------------------------------
// Base URL: prefer env override, otherwise use hosted default
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
const DEFAULT_API_BASE = "https://scan.sv-1.global.canton.network.sync.global/api/scan";
const API_BASE = stryMutAct_9fa48("3560") ? import.meta.env.VITE_SCAN_API_URL && DEFAULT_API_BASE : stryMutAct_9fa48("3559") ? false : stryMutAct_9fa48("3558") ? true : (stryCov_9fa48("3558", "3559", "3560"), import.meta.env.VITE_SCAN_API_URL || DEFAULT_API_BASE);

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
  record_time: string; // ISO
  synchronizer_id: string;
  effective_at: string; // ISO
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
  provider: string; // provider or validator id
  rewards: string; // total or count as string
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
    actual: {
      total_consumed: number;
      total_limit: number;
    };
    target: {
      total_purchased: number;
    };
  };
}
export interface AcsSnapshotTimestampResponse {
  record_time: string;
}
export interface StateAcsRequest {
  migration_id: number;
  record_time: string;
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
  offset: string;
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
  /* ---------- v2 /updates ---------- */

  async fetchUpdates(request: UpdateHistoryRequest): Promise<UpdateHistoryResponse> {
    if (stryMutAct_9fa48("3562")) {
      {}
    } else {
      stryCov_9fa48("3562");
      const res = await fetch(`${API_BASE}/v2/updates`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(request)
      });
      if (stryMutAct_9fa48("3570") ? false : stryMutAct_9fa48("3569") ? true : stryMutAct_9fa48("3568") ? res.ok : (stryCov_9fa48("3568", "3569", "3570"), !res.ok)) throw new Error("Failed to fetch updates");
      return res.json();
    }
  },
  /**
   * Use /v2/updates to fetch open, issuing, and closed mining rounds.
   * Falls back to /v0/dso and /v0/closed-rounds if unavailable.
   */
  async fetchAllMiningRoundsFromUpdates(): Promise<{
    open_rounds: {
      contract_id: string;
      round_number?: number;
      opened_at?: string;
      payload?: any;
    }[];
    issuing_rounds: {
      contract_id: string;
      round_number?: number;
      issued_at?: string;
      payload?: any;
    }[];
    closed_rounds: {
      contract_id: string;
      round_number?: number;
      closed_at?: string;
      payload?: any;
    }[];
  }> {
    if (stryMutAct_9fa48("3572")) {
      {}
    } else {
      stryCov_9fa48("3572");
      try {
        if (stryMutAct_9fa48("3573")) {
          {}
        } else {
          stryCov_9fa48("3573");
          const res = await fetch(`${API_BASE}/v2/updates`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json"
            },
            body: JSON.stringify({
              page_size: 1000
            })
          });
          if (stryMutAct_9fa48("3582") ? false : stryMutAct_9fa48("3581") ? true : stryMutAct_9fa48("3580") ? res.ok : (stryCov_9fa48("3580", "3581", "3582"), !res.ok)) throw new Error(`v2/updates error ${res.status}`);
          const data: UpdateHistoryResponse = await res.json();

          // Track rounds by their round number to determine current state
          const roundStates = new Map<number, {
            contract_id: string;
            round_number: number;
            state: 'open' | 'issuing' | 'closed';
            timestamp: string;
            payload?: any;
          }>();
          for (const tx of stryMutAct_9fa48("3586") ? data.transactions && [] : stryMutAct_9fa48("3585") ? false : stryMutAct_9fa48("3584") ? true : (stryCov_9fa48("3584", "3585", "3586"), data.transactions || (stryMutAct_9fa48("3587") ? ["Stryker was here"] : (stryCov_9fa48("3587"), [])))) {
            if (stryMutAct_9fa48("3588")) {
              {}
            } else {
              stryCov_9fa48("3588");
              const events = stryMutAct_9fa48("3591") ? (tx as Transaction).events_by_id && {} : stryMutAct_9fa48("3590") ? false : stryMutAct_9fa48("3589") ? true : (stryCov_9fa48("3589", "3590", "3591"), (tx as Transaction).events_by_id || {});
              for (const [, ev] of Object.entries(events)) {
                if (stryMutAct_9fa48("3592")) {
                  {}
                } else {
                  stryCov_9fa48("3592");
                  const tid = stryMutAct_9fa48("3595") ? (ev as TreeEvent).template_id && "" : stryMutAct_9fa48("3594") ? false : stryMutAct_9fa48("3593") ? true : (stryCov_9fa48("3593", "3594", "3595"), (ev as TreeEvent).template_id || "");
                  const createdEv = ev as CreatedEvent;
                  const roundNum = stryMutAct_9fa48("3598") ? createdEv.create_arguments.round?.number : stryMutAct_9fa48("3597") ? createdEv.create_arguments?.round.number : (stryCov_9fa48("3597", "3598"), createdEv.create_arguments?.round?.number);
                  if (stryMutAct_9fa48("3601") ? false : stryMutAct_9fa48("3600") ? true : stryMutAct_9fa48("3599") ? roundNum : (stryCov_9fa48("3599", "3600", "3601"), !roundNum)) continue;
                  let state: 'open' | 'issuing' | 'closed' | null = null;
                  if (stryMutAct_9fa48("3603") ? false : stryMutAct_9fa48("3602") ? true : (stryCov_9fa48("3602", "3603"), tid.includes("OpenMiningRound"))) state = 'open';else if (stryMutAct_9fa48("3607") ? false : stryMutAct_9fa48("3606") ? true : (stryCov_9fa48("3606", "3607"), tid.includes("IssuingMiningRound"))) state = 'issuing';else if (stryMutAct_9fa48("3611") ? false : stryMutAct_9fa48("3610") ? true : (stryCov_9fa48("3610", "3611"), tid.includes("ClosedMiningRound"))) state = 'closed';
                  if (stryMutAct_9fa48("3615") ? false : stryMutAct_9fa48("3614") ? true : (stryCov_9fa48("3614", "3615"), state)) {
                    if (stryMutAct_9fa48("3616")) {
                      {}
                    } else {
                      stryCov_9fa48("3616");
                      const existing = roundStates.get(roundNum);
                      const timestamp = (tx as Transaction).record_time;

                      // Update if this is a newer state or first time seeing this round
                      if (stryMutAct_9fa48("3619") ? !existing && new Date(timestamp) > new Date(existing.timestamp) : stryMutAct_9fa48("3618") ? false : stryMutAct_9fa48("3617") ? true : (stryCov_9fa48("3617", "3618", "3619"), (stryMutAct_9fa48("3620") ? existing : (stryCov_9fa48("3620"), !existing)) || (stryMutAct_9fa48("3623") ? new Date(timestamp) <= new Date(existing.timestamp) : stryMutAct_9fa48("3622") ? new Date(timestamp) >= new Date(existing.timestamp) : stryMutAct_9fa48("3621") ? false : (stryCov_9fa48("3621", "3622", "3623"), new Date(timestamp) > new Date(existing.timestamp))))) {
                        if (stryMutAct_9fa48("3624")) {
                          {}
                        } else {
                          stryCov_9fa48("3624");
                          roundStates.set(roundNum, {
                            contract_id: ev.contract_id,
                            round_number: roundNum,
                            state,
                            timestamp,
                            payload: createdEv.create_arguments
                          });
                        }
                      }
                    }
                  }
                }
              }
            }
          }

          // Separate rounds by their current state
          const open_rounds: {
            contract_id: string;
            round_number?: number;
            opened_at?: string;
            payload?: any;
          }[] = stryMutAct_9fa48("3626") ? ["Stryker was here"] : (stryCov_9fa48("3626"), []);
          const issuing_rounds: {
            contract_id: string;
            round_number?: number;
            issued_at?: string;
            payload?: any;
          }[] = stryMutAct_9fa48("3627") ? ["Stryker was here"] : (stryCov_9fa48("3627"), []);
          const closed_rounds: {
            contract_id: string;
            round_number?: number;
            closed_at?: string;
            payload?: any;
          }[] = stryMutAct_9fa48("3628") ? ["Stryker was here"] : (stryCov_9fa48("3628"), []);
          for (const round of roundStates.values()) {
            if (stryMutAct_9fa48("3629")) {
              {}
            } else {
              stryCov_9fa48("3629");
              if (stryMutAct_9fa48("3632") ? round.state !== 'open' : stryMutAct_9fa48("3631") ? false : stryMutAct_9fa48("3630") ? true : (stryCov_9fa48("3630", "3631", "3632"), round.state === 'open')) {
                if (stryMutAct_9fa48("3634")) {
                  {}
                } else {
                  stryCov_9fa48("3634");
                  open_rounds.push({
                    contract_id: round.contract_id,
                    round_number: round.round_number,
                    opened_at: round.timestamp,
                    payload: round.payload
                  });
                }
              } else if (stryMutAct_9fa48("3638") ? round.state !== 'issuing' : stryMutAct_9fa48("3637") ? false : stryMutAct_9fa48("3636") ? true : (stryCov_9fa48("3636", "3637", "3638"), round.state === 'issuing')) {
                if (stryMutAct_9fa48("3640")) {
                  {}
                } else {
                  stryCov_9fa48("3640");
                  issuing_rounds.push({
                    contract_id: round.contract_id,
                    round_number: round.round_number,
                    issued_at: round.timestamp,
                    payload: round.payload
                  });
                }
              } else if (stryMutAct_9fa48("3644") ? round.state !== 'closed' : stryMutAct_9fa48("3643") ? false : stryMutAct_9fa48("3642") ? true : (stryCov_9fa48("3642", "3643", "3644"), round.state === 'closed')) {
                if (stryMutAct_9fa48("3646")) {
                  {}
                } else {
                  stryCov_9fa48("3646");
                  closed_rounds.push({
                    contract_id: round.contract_id,
                    round_number: round.round_number,
                    closed_at: round.timestamp,
                    payload: round.payload
                  });
                }
              }
            }
          }

          // Sort by round number descending
          const sortByRound = stryMutAct_9fa48("3648") ? () => undefined : (stryCov_9fa48("3648"), (() => {
            const sortByRound = <T extends {
              round_number?: number;
            },>(arr: T[]) => stryMutAct_9fa48("3649") ? arr : (stryCov_9fa48("3649"), arr.sort(stryMutAct_9fa48("3650") ? () => undefined : (stryCov_9fa48("3650"), (a, b) => stryMutAct_9fa48("3651") ? (b.round_number || 0) + (a.round_number || 0) : (stryCov_9fa48("3651"), (stryMutAct_9fa48("3654") ? b.round_number && 0 : stryMutAct_9fa48("3653") ? false : stryMutAct_9fa48("3652") ? true : (stryCov_9fa48("3652", "3653", "3654"), b.round_number || 0)) - (stryMutAct_9fa48("3657") ? a.round_number && 0 : stryMutAct_9fa48("3656") ? false : stryMutAct_9fa48("3655") ? true : (stryCov_9fa48("3655", "3656", "3657"), a.round_number || 0))))));
            return sortByRound;
          })());
          return {
            open_rounds: sortByRound(open_rounds),
            issuing_rounds: sortByRound(issuing_rounds),
            closed_rounds: stryMutAct_9fa48("3659") ? sortByRound(closed_rounds) : (stryCov_9fa48("3659"), sortByRound(closed_rounds).slice(0, 10)) // Only return last 10 closed rounds
          };
        }
      } catch (e) {
        if (stryMutAct_9fa48("3660")) {
          {}
        } else {
          stryCov_9fa48("3660");
          // Fallback to traditional endpoints
          try {
            if (stryMutAct_9fa48("3661")) {
              {}
            } else {
              stryCov_9fa48("3661");
              const [dso, closed] = await Promise.all(stryMutAct_9fa48("3662") ? [] : (stryCov_9fa48("3662"), [this.fetchDsoInfo().catch(stryMutAct_9fa48("3663") ? () => undefined : (stryCov_9fa48("3663"), () => null)), this.fetchClosedRounds().catch(stryMutAct_9fa48("3664") ? () => undefined : (stryCov_9fa48("3664"), () => null))]));
              const open_rounds = (stryMutAct_9fa48("3667") ? dso.latest_mining_round?.contract?.contract_id : stryMutAct_9fa48("3666") ? dso?.latest_mining_round.contract?.contract_id : stryMutAct_9fa48("3665") ? dso?.latest_mining_round?.contract.contract_id : (stryCov_9fa48("3665", "3666", "3667"), dso?.latest_mining_round?.contract?.contract_id)) ? stryMutAct_9fa48("3668") ? [] : (stryCov_9fa48("3668"), [{
                contract_id: dso.latest_mining_round.contract.contract_id,
                round_number: stryMutAct_9fa48("3671") ? dso.latest_mining_round.contract.payload.round?.number : stryMutAct_9fa48("3670") ? dso.latest_mining_round.contract.payload?.round.number : (stryCov_9fa48("3670", "3671"), dso.latest_mining_round.contract.payload?.round?.number),
                opened_at: dso.latest_mining_round.contract.created_at,
                payload: dso.latest_mining_round.contract.payload
              }]) : stryMutAct_9fa48("3672") ? ["Stryker was here"] : (stryCov_9fa48("3672"), []);
              const closed_rounds = stryMutAct_9fa48("3673") ? (closed?.rounds || []).map(r => ({
                contract_id: r.contract.contract_id,
                round_number: r.contract.payload?.round?.number,
                closed_at: r.contract.created_at,
                payload: r.contract.payload
              })) : (stryCov_9fa48("3673"), (stryMutAct_9fa48("3676") ? closed?.rounds && [] : stryMutAct_9fa48("3675") ? false : stryMutAct_9fa48("3674") ? true : (stryCov_9fa48("3674", "3675", "3676"), (stryMutAct_9fa48("3677") ? closed.rounds : (stryCov_9fa48("3677"), closed?.rounds)) || (stryMutAct_9fa48("3678") ? ["Stryker was here"] : (stryCov_9fa48("3678"), [])))).slice(0, 10).map(stryMutAct_9fa48("3679") ? () => undefined : (stryCov_9fa48("3679"), r => ({
                contract_id: r.contract.contract_id,
                round_number: stryMutAct_9fa48("3682") ? r.contract.payload.round?.number : stryMutAct_9fa48("3681") ? r.contract.payload?.round.number : (stryCov_9fa48("3681", "3682"), r.contract.payload?.round?.number),
                closed_at: r.contract.created_at,
                payload: r.contract.payload
              }))));
              return {
                open_rounds,
                issuing_rounds: stryMutAct_9fa48("3684") ? ["Stryker was here"] : (stryCov_9fa48("3684"), []),
                closed_rounds
              };
            }
          } catch {
            if (stryMutAct_9fa48("3685")) {
              {}
            } else {
              stryCov_9fa48("3685");
              return {
                open_rounds: stryMutAct_9fa48("3687") ? ["Stryker was here"] : (stryCov_9fa48("3687"), []),
                issuing_rounds: stryMutAct_9fa48("3688") ? ["Stryker was here"] : (stryCov_9fa48("3688"), []),
                closed_rounds: stryMutAct_9fa48("3689") ? ["Stryker was here"] : (stryCov_9fa48("3689"), [])
              };
            }
          }
        }
      }
    }
  },
  /* ---------- Current mining rounds via ACS snapshot ---------- */
  async fetchAllMiningRoundsCurrent(): Promise<{
    open_rounds: {
      contract_id: string;
      round_number?: number;
      opened_at?: string;
      payload?: any;
    }[];
    issuing_rounds: {
      contract_id: string;
      round_number?: number;
      issued_at?: string;
      payload?: any;
    }[];
    closed_rounds: {
      contract_id: string;
      round_number?: number;
      closed_at?: string;
      payload?: any;
    }[];
  }> {
    if (stryMutAct_9fa48("3690")) {
      {}
    } else {
      stryCov_9fa48("3690");
      try {
        if (stryMutAct_9fa48("3691")) {
          {}
        } else {
          stryCov_9fa48("3691");
          // Build active rounds with fallback: ACS snapshot -> live endpoint
          const allActiveRounds: Array<{
            contract_id: string;
            round_number?: number;
            timestamp?: string;
            payload?: any;
            type: 'open' | 'issuing';
          }> = stryMutAct_9fa48("3692") ? ["Stryker was here"] : (stryCov_9fa48("3692"), []);
          try {
            if (stryMutAct_9fa48("3693")) {
              {}
            } else {
              stryCov_9fa48("3693");
              // Primary: ACS snapshot (current active contracts)
              const latest = await this.fetchLatestRound();
              const snap = await this.fetchAcsSnapshotTimestamp(latest.effectiveAt, 0);
              const acs = await this.fetchStateAcs({
                migration_id: 0,
                record_time: snap.record_time,
                page_size: 2000,
                templates: stryMutAct_9fa48("3695") ? [] : (stryCov_9fa48("3695"), ['Splice.Round:OpenMiningRound', 'Splice.Round:IssuingMiningRound'])
              });
              for (const ev of stryMutAct_9fa48("3700") ? acs.created_events && [] : stryMutAct_9fa48("3699") ? false : stryMutAct_9fa48("3698") ? true : (stryCov_9fa48("3698", "3699", "3700"), acs.created_events || (stryMutAct_9fa48("3701") ? ["Stryker was here"] : (stryCov_9fa48("3701"), [])))) {
                if (stryMutAct_9fa48("3702")) {
                  {}
                } else {
                  stryCov_9fa48("3702");
                  const tid = stryMutAct_9fa48("3705") ? ev.template_id && '' : stryMutAct_9fa48("3704") ? false : stryMutAct_9fa48("3703") ? true : (stryCov_9fa48("3703", "3704", "3705"), ev.template_id || '');
                  const rnd = stryMutAct_9fa48("3708") ? (ev as any).create_arguments.round?.number : stryMutAct_9fa48("3707") ? (ev as any).create_arguments?.round.number : (stryCov_9fa48("3707", "3708"), (ev as any).create_arguments?.round?.number);
                  if (stryMutAct_9fa48("3710") ? false : stryMutAct_9fa48("3709") ? true : (stryCov_9fa48("3709", "3710"), tid.includes('OpenMiningRound'))) {
                    if (stryMutAct_9fa48("3712")) {
                      {}
                    } else {
                      stryCov_9fa48("3712");
                      allActiveRounds.push({
                        contract_id: ev.contract_id,
                        round_number: rnd,
                        timestamp: (ev as any).created_at,
                        payload: (ev as any).create_arguments,
                        type: 'open'
                      });
                    }
                  } else if (stryMutAct_9fa48("3716") ? false : stryMutAct_9fa48("3715") ? true : (stryCov_9fa48("3715", "3716"), tid.includes('IssuingMiningRound'))) {
                    if (stryMutAct_9fa48("3718")) {
                      {}
                    } else {
                      stryCov_9fa48("3718");
                      allActiveRounds.push({
                        contract_id: ev.contract_id,
                        round_number: rnd,
                        timestamp: (ev as any).created_at,
                        payload: (ev as any).create_arguments,
                        type: 'issuing'
                      });
                    }
                  }
                }
              }
            }
          } catch (_) {
            if (stryMutAct_9fa48("3721")) {
              {}
            } else {
              stryCov_9fa48("3721");
              try {
                if (stryMutAct_9fa48("3722")) {
                  {}
                } else {
                  stryCov_9fa48("3722");
                  // Secondary: live endpoint
                  const current = await this.fetchOpenAndIssuingRounds();
                  for (const v of Object.values(stryMutAct_9fa48("3725") ? current.open_mining_rounds && {} : stryMutAct_9fa48("3724") ? false : stryMutAct_9fa48("3723") ? true : (stryCov_9fa48("3723", "3724", "3725"), current.open_mining_rounds || {}))) {
                    if (stryMutAct_9fa48("3726")) {
                      {}
                    } else {
                      stryCov_9fa48("3726");
                      const c = (v as any).contract;
                      allActiveRounds.push({
                        contract_id: c.contract_id,
                        round_number: stryMutAct_9fa48("3730") ? c.payload?.round?.number : stryMutAct_9fa48("3729") ? c?.payload.round?.number : stryMutAct_9fa48("3728") ? c?.payload?.round.number : (stryCov_9fa48("3728", "3729", "3730"), c?.payload?.round?.number),
                        timestamp: stryMutAct_9fa48("3731") ? c.created_at : (stryCov_9fa48("3731"), c?.created_at),
                        payload: stryMutAct_9fa48("3732") ? c.payload : (stryCov_9fa48("3732"), c?.payload),
                        type: 'open'
                      });
                    }
                  }
                  for (const v of Object.values(stryMutAct_9fa48("3736") ? current.issuing_mining_rounds && {} : stryMutAct_9fa48("3735") ? false : stryMutAct_9fa48("3734") ? true : (stryCov_9fa48("3734", "3735", "3736"), current.issuing_mining_rounds || {}))) {
                    if (stryMutAct_9fa48("3737")) {
                      {}
                    } else {
                      stryCov_9fa48("3737");
                      const c = (v as any).contract;
                      allActiveRounds.push({
                        contract_id: c.contract_id,
                        round_number: stryMutAct_9fa48("3741") ? c.payload?.round?.number : stryMutAct_9fa48("3740") ? c?.payload.round?.number : stryMutAct_9fa48("3739") ? c?.payload?.round.number : (stryCov_9fa48("3739", "3740", "3741"), c?.payload?.round?.number),
                        timestamp: stryMutAct_9fa48("3742") ? c.created_at : (stryCov_9fa48("3742"), c?.created_at),
                        payload: stryMutAct_9fa48("3743") ? c.payload : (stryCov_9fa48("3743"), c?.payload),
                        type: 'issuing'
                      });
                    }
                  }
                }
              } catch (_) {
                // Tertiary fallback handled by outer catch
              }
            }
          }

          // Sort by round_number descending and take only the 5 most recent
          stryMutAct_9fa48("3745") ? allActiveRounds : (stryCov_9fa48("3745"), allActiveRounds.sort(stryMutAct_9fa48("3746") ? () => undefined : (stryCov_9fa48("3746"), (a, b) => stryMutAct_9fa48("3747") ? (b.round_number || 0) + (a.round_number || 0) : (stryCov_9fa48("3747"), (stryMutAct_9fa48("3750") ? b.round_number && 0 : stryMutAct_9fa48("3749") ? false : stryMutAct_9fa48("3748") ? true : (stryCov_9fa48("3748", "3749", "3750"), b.round_number || 0)) - (stryMutAct_9fa48("3753") ? a.round_number && 0 : stryMutAct_9fa48("3752") ? false : stryMutAct_9fa48("3751") ? true : (stryCov_9fa48("3751", "3752", "3753"), a.round_number || 0))))));
          const recentActiveRounds = stryMutAct_9fa48("3754") ? allActiveRounds : (stryCov_9fa48("3754"), allActiveRounds.slice(0, 5));

          // Separate back into open and issuing
          const open_rounds = stryMutAct_9fa48("3755") ? recentActiveRounds.map(r => ({
            contract_id: r.contract_id,
            round_number: r.round_number,
            opened_at: r.timestamp,
            payload: r.payload
          })) : (stryCov_9fa48("3755"), recentActiveRounds.filter(stryMutAct_9fa48("3756") ? () => undefined : (stryCov_9fa48("3756"), r => stryMutAct_9fa48("3759") ? r.type !== 'open' : stryMutAct_9fa48("3758") ? false : stryMutAct_9fa48("3757") ? true : (stryCov_9fa48("3757", "3758", "3759"), r.type === 'open'))).map(stryMutAct_9fa48("3761") ? () => undefined : (stryCov_9fa48("3761"), r => ({
            contract_id: r.contract_id,
            round_number: r.round_number,
            opened_at: r.timestamp,
            payload: r.payload
          }))));
          const issuing_rounds = stryMutAct_9fa48("3763") ? recentActiveRounds.map(r => ({
            contract_id: r.contract_id,
            round_number: r.round_number,
            issued_at: r.timestamp,
            payload: r.payload
          })) : (stryCov_9fa48("3763"), recentActiveRounds.filter(stryMutAct_9fa48("3764") ? () => undefined : (stryCov_9fa48("3764"), r => stryMutAct_9fa48("3767") ? r.type !== 'issuing' : stryMutAct_9fa48("3766") ? false : stryMutAct_9fa48("3765") ? true : (stryCov_9fa48("3765", "3766", "3767"), r.type === 'issuing'))).map(stryMutAct_9fa48("3769") ? () => undefined : (stryCov_9fa48("3769"), r => ({
            contract_id: r.contract_id,
            round_number: r.round_number,
            issued_at: r.timestamp,
            payload: r.payload
          }))));

          // Closed rounds: newest first, limit to 10
          let closed_rounds: {
            contract_id: string;
            round_number?: number;
            closed_at?: string;
            payload?: any;
          }[] = stryMutAct_9fa48("3771") ? ["Stryker was here"] : (stryCov_9fa48("3771"), []);
          try {
            if (stryMutAct_9fa48("3772")) {
              {}
            } else {
              stryCov_9fa48("3772");
              const closed = await this.fetchClosedRounds();
              closed_rounds = stryMutAct_9fa48("3773") ? (closed.rounds || []).map(r => ({
                contract_id: r.contract.contract_id,
                round_number: r.contract.payload?.round?.number,
                closed_at: r.contract.created_at,
                payload: r.contract.payload
              })) : (stryCov_9fa48("3773"), (stryMutAct_9fa48("3776") ? closed.rounds && [] : stryMutAct_9fa48("3775") ? false : stryMutAct_9fa48("3774") ? true : (stryCov_9fa48("3774", "3775", "3776"), closed.rounds || (stryMutAct_9fa48("3777") ? ["Stryker was here"] : (stryCov_9fa48("3777"), [])))).slice(0, 10).map(stryMutAct_9fa48("3778") ? () => undefined : (stryCov_9fa48("3778"), r => ({
                contract_id: r.contract.contract_id,
                round_number: stryMutAct_9fa48("3781") ? r.contract.payload.round?.number : stryMutAct_9fa48("3780") ? r.contract.payload?.round.number : (stryCov_9fa48("3780", "3781"), r.contract.payload?.round?.number),
                closed_at: r.contract.created_at,
                payload: r.contract.payload
              }))));
            }
          } catch (_) {
            if (stryMutAct_9fa48("3782")) {
              {}
            } else {
              stryCov_9fa48("3782");
              closed_rounds = stryMutAct_9fa48("3783") ? ["Stryker was here"] : (stryCov_9fa48("3783"), []);
            }
          }
          return {
            open_rounds,
            issuing_rounds,
            closed_rounds
          };
        }
      } catch (e) {
        if (stryMutAct_9fa48("3785")) {
          {}
        } else {
          stryCov_9fa48("3785");
          // Fallback to updates-based approach
          return this.fetchAllMiningRoundsFromUpdates();
        }
      }
    }
  },
  /* ---------- v0 transactions & helpers ---------- */

  async fetchTransactions(request: TransactionHistoryRequest): Promise<TransactionHistoryResponse> {
    if (stryMutAct_9fa48("3786")) {
      {}
    } else {
      stryCov_9fa48("3786");
      const controller = new AbortController();
      const timeout = setTimeout(stryMutAct_9fa48("3787") ? () => undefined : (stryCov_9fa48("3787"), () => controller.abort()), 30000);
      try {
        if (stryMutAct_9fa48("3788")) {
          {}
        } else {
          stryCov_9fa48("3788");
          const res = await fetch(`${API_BASE}/v0/transactions`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json"
            },
            body: JSON.stringify(request),
            signal: controller.signal
          });
          if (stryMutAct_9fa48("3796") ? false : stryMutAct_9fa48("3795") ? true : stryMutAct_9fa48("3794") ? res.ok : (stryCov_9fa48("3794", "3795", "3796"), !res.ok)) throw new Error("Failed to fetch transactions");
          return res.json();
        }
      } finally {
        if (stryMutAct_9fa48("3798")) {
          {}
        } else {
          stryCov_9fa48("3798");
          clearTimeout(timeout);
        }
      }
    }
  },
  async fetchTransactionsByParty(party: string, limit: number = 20): Promise<TransactionHistoryResponse> {
    if (stryMutAct_9fa48("3799")) {
      {}
    } else {
      stryCov_9fa48("3799");
      const params = new URLSearchParams();
      params.append("party", party);
      params.append("limit", limit.toString());
      const res = await fetch(`${API_BASE}/v0/transactions/by-party?${params.toString()}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("3807") ? false : stryMutAct_9fa48("3806") ? true : stryMutAct_9fa48("3805") ? res.ok : (stryCov_9fa48("3805", "3806", "3807"), !res.ok)) throw new Error("Failed to fetch transactions by party");
      return res.json();
    }
  },
  /* ---------- Leaderboards & stats ---------- */

  // Top validators via faucets; transformed to expected validatorsAndRewards
  async fetchTopValidators(): Promise<GetTopValidatorsByValidatorRewardsResponse> {
    if (stryMutAct_9fa48("3809")) {
      {}
    } else {
      stryCov_9fa48("3809");
      const res = await fetch(`${API_BASE}/v0/top-validators-by-validator-faucets?limit=1000`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("3815") ? false : stryMutAct_9fa48("3814") ? true : stryMutAct_9fa48("3813") ? res.ok : (stryCov_9fa48("3813", "3814", "3815"), !res.ok)) throw new Error("Failed to fetch top validators");
      const data: TopValidatorsByFaucetsResponse = await res.json();
      return {
        validatorsAndRewards: (stryMutAct_9fa48("3820") ? data.validatorsByReceivedFaucets && [] : stryMutAct_9fa48("3819") ? false : stryMutAct_9fa48("3818") ? true : (stryCov_9fa48("3818", "3819", "3820"), data.validatorsByReceivedFaucets || (stryMutAct_9fa48("3821") ? ["Stryker was here"] : (stryCov_9fa48("3821"), [])))).map(stryMutAct_9fa48("3822") ? () => undefined : (stryCov_9fa48("3822"), v => ({
          provider: v.validator,
          rewards: String(v.numRoundsCollected),
          firstCollectedInRound: v.firstCollectedInRound
        })))
      };
      // Note: rewards = collected rounds (count) so that your UI's growth logic works
    }
  },
  // Top providers by app rewards for latest round
  async fetchTopProviders(limit: number = 1000): Promise<GetTopProvidersByAppRewardsResponse> {
    if (stryMutAct_9fa48("3824")) {
      {}
    } else {
      stryCov_9fa48("3824");
      const latest = await this.fetchLatestRound();
      const params = new URLSearchParams({
        round: String(latest.round),
        limit: String(limit)
      });
      const res = await fetch(`${API_BASE}/v0/top-providers-by-app-rewards?${params.toString()}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("3831") ? false : stryMutAct_9fa48("3830") ? true : stryMutAct_9fa48("3829") ? res.ok : (stryCov_9fa48("3829", "3830", "3831"), !res.ok)) throw new Error("Failed to fetch top providers by app rewards");
      return res.json();
    }
  },
  async fetchRoundTotals(request: ListRoundTotalsRequest): Promise<ListRoundTotalsResponse> {
    if (stryMutAct_9fa48("3833")) {
      {}
    } else {
      stryCov_9fa48("3833");
      const controller = new AbortController();
      const timeout = setTimeout(stryMutAct_9fa48("3834") ? () => undefined : (stryCov_9fa48("3834"), () => controller.abort()), 10000);
      try {
        if (stryMutAct_9fa48("3835")) {
          {}
        } else {
          stryCov_9fa48("3835");
          const res = await fetch(`${API_BASE}/v0/round-totals`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json"
            },
            body: JSON.stringify(request),
            signal: controller.signal
          });
          if (stryMutAct_9fa48("3843") ? false : stryMutAct_9fa48("3842") ? true : stryMutAct_9fa48("3841") ? res.ok : (stryCov_9fa48("3841", "3842", "3843"), !res.ok)) throw new Error("Failed to fetch round totals");
          return res.json();
        }
      } finally {
        if (stryMutAct_9fa48("3845")) {
          {}
        } else {
          stryCov_9fa48("3845");
          clearTimeout(timeout);
        }
      }
    }
  },
  async fetchOpenAndIssuingRounds(request: GetOpenAndIssuingMiningRoundsRequest = {}): Promise<GetOpenAndIssuingMiningRoundsResponse> {
    if (stryMutAct_9fa48("3846")) {
      {}
    } else {
      stryCov_9fa48("3846");
      const res = await fetch(`${API_BASE}/v0/open-and-issuing-mining-rounds`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(request)
      });
      if (stryMutAct_9fa48("3854") ? false : stryMutAct_9fa48("3853") ? true : stryMutAct_9fa48("3852") ? res.ok : (stryCov_9fa48("3852", "3853", "3854"), !res.ok)) throw new Error("Failed to fetch mining rounds");
      return res.json();
    }
  },
  async fetchClosedRounds(): Promise<GetClosedRoundsResponse> {
    if (stryMutAct_9fa48("3856")) {
      {}
    } else {
      stryCov_9fa48("3856");
      const res = await fetch(`${API_BASE}/v0/closed-rounds`);
      if (stryMutAct_9fa48("3860") ? false : stryMutAct_9fa48("3859") ? true : stryMutAct_9fa48("3858") ? res.ok : (stryCov_9fa48("3858", "3859", "3860"), !res.ok)) throw new Error("Failed to fetch closed rounds");
      return res.json();
    }
  },
  async fetchLatestRound(): Promise<GetRoundOfLatestDataResponse> {
    if (stryMutAct_9fa48("3862")) {
      {}
    } else {
      stryCov_9fa48("3862");
      const controller = new AbortController();
      const timeout = setTimeout(stryMutAct_9fa48("3863") ? () => undefined : (stryCov_9fa48("3863"), () => controller.abort()), 8000);
      try {
        if (stryMutAct_9fa48("3864")) {
          {}
        } else {
          stryCov_9fa48("3864");
          const res = await fetch(`${API_BASE}/v0/round-of-latest-data`, {
            signal: controller.signal
          });
          if (stryMutAct_9fa48("3869") ? false : stryMutAct_9fa48("3868") ? true : stryMutAct_9fa48("3867") ? res.ok : (stryCov_9fa48("3867", "3868", "3869"), !res.ok)) throw new Error("Failed to fetch latest round");
          return res.json();
        }
      } finally {
        if (stryMutAct_9fa48("3871")) {
          {}
        } else {
          stryCov_9fa48("3871");
          clearTimeout(timeout);
        }
      }
    }
  },
  async fetchTotalBalance(): Promise<GetTotalAmuletBalanceResponse> {
    if (stryMutAct_9fa48("3872")) {
      {}
    } else {
      stryCov_9fa48("3872");
      const latest = await this.fetchLatestRound();
      const totals = await this.fetchRoundTotals({
        start_round: latest.round,
        end_round: latest.round
      });
      if (stryMutAct_9fa48("3876") ? totals.entries.length !== 0 : stryMutAct_9fa48("3875") ? false : stryMutAct_9fa48("3874") ? true : (stryCov_9fa48("3874", "3875", "3876"), totals.entries.length === 0)) throw new Error("No round totals for latest round");
      return {
        total_balance: totals.entries[0].total_amulet_balance
      };
    }
  },
  /* ---------- Validator health / liveness ---------- */

  async fetchValidatorLiveness(validator_ids: string[]): Promise<ValidatorLivenessResponse> {
    if (stryMutAct_9fa48("3879")) {
      {}
    } else {
      stryCov_9fa48("3879");
      const params = new URLSearchParams();
      for (const id of validator_ids) params.append("validator_ids", id);
      const res = await fetch(`${API_BASE}/v0/validators/validator-faucets?${params.toString()}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("3886") ? false : stryMutAct_9fa48("3885") ? true : stryMutAct_9fa48("3884") ? res.ok : (stryCov_9fa48("3884", "3885", "3886"), !res.ok)) throw new Error("Failed to fetch validator liveness");
      return res.json();
    }
  },
  /* ---------- DSO, scans, admin, misc ---------- */

  async fetchDsoInfo(): Promise<DsoInfoResponse> {
    if (stryMutAct_9fa48("3888")) {
      {}
    } else {
      stryCov_9fa48("3888");
      const controller = new AbortController();
      const timeout = setTimeout(stryMutAct_9fa48("3889") ? () => undefined : (stryCov_9fa48("3889"), () => controller.abort()), 10000);
      try {
        if (stryMutAct_9fa48("3890")) {
          {}
        } else {
          stryCov_9fa48("3890");
          const res = await fetch(`${API_BASE}/v0/dso`, {
            mode: "cors",
            signal: controller.signal
          });
          if (stryMutAct_9fa48("3896") ? false : stryMutAct_9fa48("3895") ? true : stryMutAct_9fa48("3894") ? res.ok : (stryCov_9fa48("3894", "3895", "3896"), !res.ok)) throw new Error("Failed to fetch DSO info");
          return res.json();
        }
      } finally {
        if (stryMutAct_9fa48("3898")) {
          {}
        } else {
          stryCov_9fa48("3898");
          clearTimeout(timeout);
        }
      }
    }
  },
  async fetchScans(): Promise<ScansResponse> {
    if (stryMutAct_9fa48("3899")) {
      {}
    } else {
      stryCov_9fa48("3899");
      const res = await fetch(`${API_BASE}/v0/scans`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("3905") ? false : stryMutAct_9fa48("3904") ? true : stryMutAct_9fa48("3903") ? res.ok : (stryCov_9fa48("3903", "3904", "3905"), !res.ok)) throw new Error("Failed to fetch scans");
      return res.json();
    }
  },
  async fetchValidatorLicenses(after?: number, limit: number = 1000): Promise<ValidatorLicensesResponse> {
    if (stryMutAct_9fa48("3907")) {
      {}
    } else {
      stryCov_9fa48("3907");
      const params = new URLSearchParams();
      if (stryMutAct_9fa48("3910") ? after === undefined : stryMutAct_9fa48("3909") ? false : stryMutAct_9fa48("3908") ? true : (stryCov_9fa48("3908", "3909", "3910"), after !== undefined)) params.append("after", String(after));
      params.append("limit", String(limit));
      const res = await fetch(`${API_BASE}/v0/admin/validator/licenses?${params.toString()}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("3918") ? false : stryMutAct_9fa48("3917") ? true : stryMutAct_9fa48("3916") ? res.ok : (stryCov_9fa48("3916", "3917", "3918"), !res.ok)) throw new Error("Failed to fetch validator licenses");
      return res.json();
    }
  },
  async fetchDsoSequencers(): Promise<DsoSequencersResponse> {
    if (stryMutAct_9fa48("3920")) {
      {}
    } else {
      stryCov_9fa48("3920");
      const res = await fetch(`${API_BASE}/v0/dso-sequencers`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("3926") ? false : stryMutAct_9fa48("3925") ? true : stryMutAct_9fa48("3924") ? res.ok : (stryCov_9fa48("3924", "3925", "3926"), !res.ok)) throw new Error("Failed to fetch DSO sequencers");
      return res.json();
    }
  },
  async fetchParticipantId(domainId: string, partyId: string): Promise<ParticipantIdResponse> {
    if (stryMutAct_9fa48("3928")) {
      {}
    } else {
      stryCov_9fa48("3928");
      const res = await fetch(`${API_BASE}/v0/domains/${domainId}/parties/${partyId}/participant-id`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("3934") ? false : stryMutAct_9fa48("3933") ? true : stryMutAct_9fa48("3932") ? res.ok : (stryCov_9fa48("3932", "3933", "3934"), !res.ok)) throw new Error("Failed to fetch participant ID");
      return res.json();
    }
  },
  async fetchTrafficStatus(domainId: string, memberId: string): Promise<TrafficStatusResponse> {
    if (stryMutAct_9fa48("3936")) {
      {}
    } else {
      stryCov_9fa48("3936");
      const res = await fetch(`${API_BASE}/v0/domains/${domainId}/members/${memberId}/traffic-status`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("3942") ? false : stryMutAct_9fa48("3941") ? true : stryMutAct_9fa48("3940") ? res.ok : (stryCov_9fa48("3940", "3941", "3942"), !res.ok)) throw new Error("Failed to fetch traffic status");
      return res.json();
    }
  },
  async fetchAcsSnapshotTimestamp(before: string, migrationId: number): Promise<AcsSnapshotTimestampResponse> {
    if (stryMutAct_9fa48("3944")) {
      {}
    } else {
      stryCov_9fa48("3944");
      const params = new URLSearchParams();
      params.append("before", before);
      params.append("migration_id", String(migrationId));
      const res = await fetch(`${API_BASE}/v0/state/acs/snapshot-timestamp?${params.toString()}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("3952") ? false : stryMutAct_9fa48("3951") ? true : stryMutAct_9fa48("3950") ? res.ok : (stryCov_9fa48("3950", "3951", "3952"), !res.ok)) throw new Error("Failed to fetch ACS snapshot timestamp");
      return res.json();
    }
  },
  async fetchStateAcs(request: StateAcsRequest): Promise<StateAcsResponse> {
    if (stryMutAct_9fa48("3954")) {
      {}
    } else {
      stryCov_9fa48("3954");
      const res = await fetch(`${API_BASE}/v0/state/acs`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(request),
        mode: "cors"
      });
      if (stryMutAct_9fa48("3963") ? false : stryMutAct_9fa48("3962") ? true : stryMutAct_9fa48("3961") ? res.ok : (stryCov_9fa48("3961", "3962", "3963"), !res.ok)) throw new Error("Failed to fetch state ACS");
      return res.json();
    }
  },
  async fetchHoldingsSummary(request: HoldingsSummaryRequest): Promise<HoldingsSummaryResponse> {
    if (stryMutAct_9fa48("3965")) {
      {}
    } else {
      stryCov_9fa48("3965");
      const res = await fetch(`${API_BASE}/v0/holdings/summary`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(request),
        mode: "cors"
      });
      if (stryMutAct_9fa48("3974") ? false : stryMutAct_9fa48("3973") ? true : stryMutAct_9fa48("3972") ? res.ok : (stryCov_9fa48("3972", "3973", "3974"), !res.ok)) throw new Error("Failed to fetch holdings summary");
      return res.json();
    }
  },
  async fetchAnsEntries(namePrefix?: string, pageSize: number = 100): Promise<AnsEntriesResponse> {
    if (stryMutAct_9fa48("3976")) {
      {}
    } else {
      stryCov_9fa48("3976");
      const params = new URLSearchParams();
      if (stryMutAct_9fa48("3978") ? false : stryMutAct_9fa48("3977") ? true : (stryCov_9fa48("3977", "3978"), namePrefix)) params.append("name_prefix", namePrefix);
      params.append("page_size", String(pageSize));
      const res = await fetch(`${API_BASE}/v0/ans-entries?${params.toString()}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("3986") ? false : stryMutAct_9fa48("3985") ? true : stryMutAct_9fa48("3984") ? res.ok : (stryCov_9fa48("3984", "3985", "3986"), !res.ok)) throw new Error("Failed to fetch ANS entries");
      return res.json();
    }
  },
  async fetchAnsEntryByParty(party: string): Promise<AnsEntryResponse> {
    if (stryMutAct_9fa48("3988")) {
      {}
    } else {
      stryCov_9fa48("3988");
      const res = await fetch(`${API_BASE}/v0/ans-entries/by-party/${party}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("3994") ? false : stryMutAct_9fa48("3993") ? true : stryMutAct_9fa48("3992") ? res.ok : (stryCov_9fa48("3992", "3993", "3994"), !res.ok)) throw new Error("Failed to fetch ANS entry by party");
      return res.json();
    }
  },
  async fetchAnsEntryByName(name: string): Promise<AnsEntryResponse> {
    if (stryMutAct_9fa48("3996")) {
      {}
    } else {
      stryCov_9fa48("3996");
      const res = await fetch(`${API_BASE}/v0/ans-entries/by-name/${name}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4002") ? false : stryMutAct_9fa48("4001") ? true : stryMutAct_9fa48("4000") ? res.ok : (stryCov_9fa48("4000", "4001", "4002"), !res.ok)) throw new Error("Failed to fetch ANS entry by name");
      return res.json();
    }
  },
  async fetchDsoPartyId(): Promise<DsoPartyIdResponse> {
    if (stryMutAct_9fa48("4004")) {
      {}
    } else {
      stryCov_9fa48("4004");
      const res = await fetch(`${API_BASE}/v0/dso-party-id`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4010") ? false : stryMutAct_9fa48("4009") ? true : stryMutAct_9fa48("4008") ? res.ok : (stryCov_9fa48("4008", "4009", "4010"), !res.ok)) throw new Error("Failed to fetch DSO party ID");
      return res.json();
    }
  },
  async fetchFeaturedApps(): Promise<FeaturedAppsResponse> {
    if (stryMutAct_9fa48("4012")) {
      {}
    } else {
      stryCov_9fa48("4012");
      const res = await fetch(`${API_BASE}/v0/featured-apps`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4018") ? false : stryMutAct_9fa48("4017") ? true : stryMutAct_9fa48("4016") ? res.ok : (stryCov_9fa48("4016", "4017", "4018"), !res.ok)) throw new Error("Failed to fetch featured apps");
      return res.json();
    }
  },
  async fetchFeaturedApp(providerPartyId: string): Promise<FeaturedAppResponse> {
    if (stryMutAct_9fa48("4020")) {
      {}
    } else {
      stryCov_9fa48("4020");
      const res = await fetch(`${API_BASE}/v0/featured-apps/${providerPartyId}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4026") ? false : stryMutAct_9fa48("4025") ? true : stryMutAct_9fa48("4024") ? res.ok : (stryCov_9fa48("4024", "4025", "4026"), !res.ok)) throw new Error("Failed to fetch featured app");
      return res.json();
    }
  },
  async fetchTopValidatorsByFaucets(limit: number): Promise<TopValidatorsByFaucetsResponse> {
    if (stryMutAct_9fa48("4028")) {
      {}
    } else {
      stryCov_9fa48("4028");
      const params = new URLSearchParams({
        limit: String(limit)
      });
      const res = await fetch(`${API_BASE}/v0/top-validators-by-validator-faucets?${params.toString()}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4035") ? false : stryMutAct_9fa48("4034") ? true : stryMutAct_9fa48("4033") ? res.ok : (stryCov_9fa48("4033", "4034", "4035"), !res.ok)) throw new Error("Failed to fetch top validators by faucets");
      return res.json();
    }
  },
  async fetchTransferPreapprovalByParty(party: string): Promise<TransferPreapprovalResponse> {
    if (stryMutAct_9fa48("4037")) {
      {}
    } else {
      stryCov_9fa48("4037");
      const res = await fetch(`${API_BASE}/v0/transfer-preapprovals/by-party/${party}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4043") ? false : stryMutAct_9fa48("4042") ? true : stryMutAct_9fa48("4041") ? res.ok : (stryCov_9fa48("4041", "4042", "4043"), !res.ok)) throw new Error("Failed to fetch transfer preapproval");
      return res.json();
    }
  },
  async fetchTransferCommandCounter(party: string): Promise<TransferCommandCounterResponse> {
    if (stryMutAct_9fa48("4045")) {
      {}
    } else {
      stryCov_9fa48("4045");
      const res = await fetch(`${API_BASE}/v0/transfer-command-counter/${party}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4051") ? false : stryMutAct_9fa48("4050") ? true : stryMutAct_9fa48("4049") ? res.ok : (stryCov_9fa48("4049", "4050", "4051"), !res.ok)) throw new Error("Failed to fetch transfer command counter");
      return res.json();
    }
  },
  async fetchTransferCommandStatus(sender: string, nonce: number): Promise<TransferCommandStatusResponse> {
    if (stryMutAct_9fa48("4053")) {
      {}
    } else {
      stryCov_9fa48("4053");
      const params = new URLSearchParams({
        sender,
        nonce: String(nonce)
      });
      const res = await fetch(`${API_BASE}/v0/transfer-command/status?${params.toString()}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4060") ? false : stryMutAct_9fa48("4059") ? true : stryMutAct_9fa48("4058") ? res.ok : (stryCov_9fa48("4058", "4059", "4060"), !res.ok)) throw new Error("Failed to fetch transfer command status");
      return res.json();
    }
  },
  async fetchMigrationSchedule(): Promise<MigrationScheduleResponse> {
    if (stryMutAct_9fa48("4062")) {
      {}
    } else {
      stryCov_9fa48("4062");
      const res = await fetch(`${API_BASE}/v0/migrations/schedule`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4068") ? false : stryMutAct_9fa48("4067") ? true : stryMutAct_9fa48("4066") ? res.ok : (stryCov_9fa48("4066", "4067", "4068"), !res.ok)) throw new Error("Failed to fetch migration schedule");
      return res.json();
    }
  },
  async fetchSpliceInstanceNames(): Promise<SpliceInstanceNamesResponse> {
    if (stryMutAct_9fa48("4070")) {
      {}
    } else {
      stryCov_9fa48("4070");
      const res = await fetch(`${API_BASE}/v0/splice-instance-names`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4076") ? false : stryMutAct_9fa48("4075") ? true : stryMutAct_9fa48("4074") ? res.ok : (stryCov_9fa48("4074", "4075", "4076"), !res.ok)) throw new Error("Failed to fetch splice instance names");
      return res.json();
    }
  },
  /* ---------- v1 helpers & v2 by id ---------- */

  async fetchUpdatesV1(request: UpdateHistoryRequest): Promise<UpdateHistoryResponse> {
    if (stryMutAct_9fa48("4078")) {
      {}
    } else {
      stryCov_9fa48("4078");
      const res = await fetch(`${API_BASE}/v1/updates`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(request),
        mode: "cors"
      });
      if (stryMutAct_9fa48("4087") ? false : stryMutAct_9fa48("4086") ? true : stryMutAct_9fa48("4085") ? res.ok : (stryCov_9fa48("4085", "4086", "4087"), !res.ok)) throw new Error("Failed to fetch v1 updates");
      return res.json();
    }
  },
  async fetchUpdateByIdV1(updateId: string, damlValueEncoding?: string): Promise<UpdateByIdResponse> {
    if (stryMutAct_9fa48("4089")) {
      {}
    } else {
      stryCov_9fa48("4089");
      const params = new URLSearchParams();
      if (stryMutAct_9fa48("4091") ? false : stryMutAct_9fa48("4090") ? true : (stryCov_9fa48("4090", "4091"), damlValueEncoding)) params.append("daml_value_encoding", damlValueEncoding);
      const url = params.toString() ? `${API_BASE}/v1/updates/${updateId}?${params.toString()}` : `${API_BASE}/v1/updates/${updateId}`;
      const res = await fetch(url, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4099") ? false : stryMutAct_9fa48("4098") ? true : stryMutAct_9fa48("4097") ? res.ok : (stryCov_9fa48("4097", "4098", "4099"), !res.ok)) throw new Error("Failed to fetch v1 update by ID");
      return res.json();
    }
  },
  async fetchUpdateByIdV2(updateId: string, damlValueEncoding?: string): Promise<UpdateByIdResponse> {
    if (stryMutAct_9fa48("4101")) {
      {}
    } else {
      stryCov_9fa48("4101");
      const params = new URLSearchParams();
      if (stryMutAct_9fa48("4103") ? false : stryMutAct_9fa48("4102") ? true : (stryCov_9fa48("4102", "4103"), damlValueEncoding)) params.append("daml_value_encoding", damlValueEncoding);
      const url = params.toString() ? `${API_BASE}/v2/updates/${updateId}?${params.toString()}` : `${API_BASE}/v2/updates/${updateId}`;
      const res = await fetch(url, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4111") ? false : stryMutAct_9fa48("4110") ? true : stryMutAct_9fa48("4109") ? res.ok : (stryCov_9fa48("4109", "4110", "4111"), !res.ok)) throw new Error("Failed to fetch v2 update by ID");
      return res.json();
    }
  },
  /* ---------- legacy / deprecated but kept for compatibility ---------- */

  async fetchAcsSnapshot(party: string, recordTime?: string): Promise<AcsSnapshotResponse> {
    if (stryMutAct_9fa48("4113")) {
      {}
    } else {
      stryCov_9fa48("4113");
      const params = new URLSearchParams();
      if (stryMutAct_9fa48("4115") ? false : stryMutAct_9fa48("4114") ? true : (stryCov_9fa48("4114", "4115"), recordTime)) params.append("record_time", recordTime);
      const url = params.toString() ? `${API_BASE}/v0/acs/${party}?${params.toString()}` : `${API_BASE}/v0/acs/${party}`;
      const res = await fetch(url, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4123") ? false : stryMutAct_9fa48("4122") ? true : stryMutAct_9fa48("4121") ? res.ok : (stryCov_9fa48("4121", "4122", "4123"), !res.ok)) throw new Error("Failed to fetch ACS snapshot");
      return res.json();
    }
  },
  async fetchAggregatedRounds(): Promise<AggregatedRoundsResponse> {
    if (stryMutAct_9fa48("4125")) {
      {}
    } else {
      stryCov_9fa48("4125");
      const res = await fetch(`${API_BASE}/v0/aggregated-rounds`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4131") ? false : stryMutAct_9fa48("4130") ? true : stryMutAct_9fa48("4129") ? res.ok : (stryCov_9fa48("4129", "4130", "4131"), !res.ok)) throw new Error("Failed to fetch aggregated rounds");
      return res.json();
    }
  },
  async fetchRoundPartyTotals(request: RoundPartyTotalsRequest): Promise<RoundPartyTotalsResponse> {
    if (stryMutAct_9fa48("4133")) {
      {}
    } else {
      stryCov_9fa48("4133");
      const controller = new AbortController();
      const timeout = setTimeout(stryMutAct_9fa48("4134") ? () => undefined : (stryCov_9fa48("4134"), () => controller.abort()), 15000);
      try {
        if (stryMutAct_9fa48("4135")) {
          {}
        } else {
          stryCov_9fa48("4135");
          const res = await fetch(`${API_BASE}/v0/round-party-totals`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json"
            },
            body: JSON.stringify(request),
            mode: "cors",
            signal: controller.signal
          });
          if (stryMutAct_9fa48("4144") ? false : stryMutAct_9fa48("4143") ? true : stryMutAct_9fa48("4142") ? res.ok : (stryCov_9fa48("4142", "4143", "4144"), !res.ok)) throw new Error("Failed to fetch round party totals");
          return res.json();
        }
      } finally {
        if (stryMutAct_9fa48("4146")) {
          {}
        } else {
          stryCov_9fa48("4146");
          clearTimeout(timeout);
        }
      }
    }
  },
  async fetchWalletBalance(partyId: string, asOfEndOfRound: number): Promise<WalletBalanceResponse> {
    if (stryMutAct_9fa48("4147")) {
      {}
    } else {
      stryCov_9fa48("4147");
      const params = new URLSearchParams({
        party_id: partyId,
        asOfEndOfRound: String(asOfEndOfRound)
      });
      const res = await fetch(`${API_BASE}/v0/wallet-balance?${params.toString()}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4154") ? false : stryMutAct_9fa48("4153") ? true : stryMutAct_9fa48("4152") ? res.ok : (stryCov_9fa48("4152", "4153", "4154"), !res.ok)) throw new Error("Failed to fetch wallet balance");
      return res.json();
    }
  },
  async fetchAmuletConfigForRound(round: number): Promise<AmuletConfigForRoundResponse> {
    if (stryMutAct_9fa48("4156")) {
      {}
    } else {
      stryCov_9fa48("4156");
      const params = new URLSearchParams({
        round: String(round)
      });
      const res = await fetch(`${API_BASE}/v0/amulet-config-for-round?${params.toString()}`, {
        mode: "cors"
      });
      if (stryMutAct_9fa48("4163") ? false : stryMutAct_9fa48("4162") ? true : stryMutAct_9fa48("4161") ? res.ok : (stryCov_9fa48("4161", "4162", "4163"), !res.ok)) throw new Error("Failed to fetch amulet config for round");
      return res.json();
    }
  },
  /* ---------- Governance helper (ACS snapshot approach) ---------- */

  async fetchGovernanceProposals(): Promise<Array<{
    id: string;
    title: string;
    description: string;
    status: "pending" | "executed" | "rejected" | "expired";
    votesFor?: number;
    votesAgainst?: number;
    createdAt?: string;
  }>> {
    if (stryMutAct_9fa48("4165")) {
      {}
    } else {
      stryCov_9fa48("4165");
      try {
        if (stryMutAct_9fa48("4166")) {
          {}
        } else {
          stryCov_9fa48("4166");
          const dso = await this.fetchDsoInfo();
          const latest = await this.fetchLatestRound();
          const snap = await this.fetchAcsSnapshotTimestamp(latest.effectiveAt, 0);
          const acs = await this.fetchStateAcs({
            migration_id: 0,
            record_time: snap.record_time,
            page_size: 1000,
            templates: stryMutAct_9fa48("4168") ? [] : (stryCov_9fa48("4168"), ["Splice.DsoRules:VoteRequest", "Splice.DsoRules:DsoRules_CloseVoteRequestResult"])
          });
          const proposals: any[] = stryMutAct_9fa48("4171") ? ["Stryker was here"] : (stryCov_9fa48("4171"), []);
          const byId: Record<string, any> = {};
          for (const ev of acs.created_events) {
            if (stryMutAct_9fa48("4172")) {
              {}
            } else {
              stryCov_9fa48("4172");
              const templateId = stryMutAct_9fa48("4175") ? ev.template_id && "" : stryMutAct_9fa48("4174") ? false : stryMutAct_9fa48("4173") ? true : (stryCov_9fa48("4173", "4174", "4175"), ev.template_id || "");
              const cid = ev.contract_id;
              const payload = stryMutAct_9fa48("4179") ? ev.create_arguments && {} : stryMutAct_9fa48("4178") ? false : stryMutAct_9fa48("4177") ? true : (stryCov_9fa48("4177", "4178", "4179"), ev.create_arguments || {});
              if (stryMutAct_9fa48("4181") ? false : stryMutAct_9fa48("4180") ? true : (stryCov_9fa48("4180", "4181"), templateId.includes("VoteRequest"))) {
                if (stryMutAct_9fa48("4183")) {
                  {}
                } else {
                  stryCov_9fa48("4183");
                  const votes = stryMutAct_9fa48("4186") ? (payload as any).votes && {} : stryMutAct_9fa48("4185") ? false : stryMutAct_9fa48("4184") ? true : (stryCov_9fa48("4184", "4185", "4186"), (payload as any).votes || {});
                  const votesFor = stryMutAct_9fa48("4187") ? Object.values(votes).length : (stryCov_9fa48("4187"), Object.values(votes).filter(stryMutAct_9fa48("4188") ? () => undefined : (stryCov_9fa48("4188"), (v: any) => stryMutAct_9fa48("4191") ? v?.accept && v?.Accept : stryMutAct_9fa48("4190") ? false : stryMutAct_9fa48("4189") ? true : (stryCov_9fa48("4189", "4190", "4191"), (stryMutAct_9fa48("4192") ? v.accept : (stryCov_9fa48("4192"), v?.accept)) || (stryMutAct_9fa48("4193") ? v.Accept : (stryCov_9fa48("4193"), v?.Accept))))).length);
                  const votesAgainst = stryMutAct_9fa48("4194") ? Object.values(votes).length : (stryCov_9fa48("4194"), Object.values(votes).filter(stryMutAct_9fa48("4195") ? () => undefined : (stryCov_9fa48("4195"), (v: any) => stryMutAct_9fa48("4198") ? v?.reject && v?.Reject : stryMutAct_9fa48("4197") ? false : stryMutAct_9fa48("4196") ? true : (stryCov_9fa48("4196", "4197", "4198"), (stryMutAct_9fa48("4199") ? v.reject : (stryCov_9fa48("4199"), v?.reject)) || (stryMutAct_9fa48("4200") ? v.Reject : (stryCov_9fa48("4200"), v?.Reject))))).length);
                  const action = stryMutAct_9fa48("4203") ? (payload as any).action && {} : stryMutAct_9fa48("4202") ? false : stryMutAct_9fa48("4201") ? true : (stryCov_9fa48("4201", "4202", "4203"), (payload as any).action || {});
                  const key = Object.keys(action)[0];
                  const title = key ? key.replace(/ARC_|_/g, " ") : "Governance Proposal";
                  byId[cid] = {
                    id: stryMutAct_9fa48("4207") ? cid : (stryCov_9fa48("4207"), cid.slice(0, 12)),
                    title,
                    description: "Vote request",
                    status: "pending",
                    votesFor,
                    votesAgainst,
                    createdAt: (ev as any).created_at
                  };
                }
              }
              if (stryMutAct_9fa48("4211") ? false : stryMutAct_9fa48("4210") ? true : (stryCov_9fa48("4210", "4211"), templateId.includes("CloseVoteRequestResult"))) {
                if (stryMutAct_9fa48("4213")) {
                  {}
                } else {
                  stryCov_9fa48("4213");
                  const outcome = stryMutAct_9fa48("4216") ? (payload as any).outcome && {} : stryMutAct_9fa48("4215") ? false : stryMutAct_9fa48("4214") ? true : (stryCov_9fa48("4214", "4215", "4216"), (payload as any).outcome || {});
                  let status: "executed" | "rejected" | "expired" = "executed";
                  if (stryMutAct_9fa48("4219") ? false : stryMutAct_9fa48("4218") ? true : (stryCov_9fa48("4218", "4219"), outcome.VRO_Rejected)) status = "rejected";
                  if (stryMutAct_9fa48("4222") ? false : stryMutAct_9fa48("4221") ? true : (stryCov_9fa48("4221", "4222"), outcome.VRO_Expired)) status = "expired";
                  const base = stryMutAct_9fa48("4226") ? byId[cid] && {
                    id: cid.slice(0, 12),
                    title: "Governance Proposal"
                  } : stryMutAct_9fa48("4225") ? false : stryMutAct_9fa48("4224") ? true : (stryCov_9fa48("4224", "4225", "4226"), byId[cid] || {
                    id: stryMutAct_9fa48("4228") ? cid : (stryCov_9fa48("4228"), cid.slice(0, 12)),
                    title: "Governance Proposal"
                  });
                  byId[cid] = {
                    ...base,
                    status,
                    createdAt: (ev as any).created_at
                  };
                }
              }
            }
          }
          Object.values(byId).forEach(stryMutAct_9fa48("4231") ? () => undefined : (stryCov_9fa48("4231"), p => proposals.push(p)));

          // Fallback: show SV onboardings as executed
          if (stryMutAct_9fa48("4234") ? proposals.length === 0 || (dso as any)?.dso_rules?.contract?.payload?.svs : stryMutAct_9fa48("4233") ? false : stryMutAct_9fa48("4232") ? true : (stryCov_9fa48("4232", "4233", "4234"), (stryMutAct_9fa48("4236") ? proposals.length !== 0 : stryMutAct_9fa48("4235") ? true : (stryCov_9fa48("4235", "4236"), proposals.length === 0)) && (stryMutAct_9fa48("4240") ? (dso as any).dso_rules?.contract?.payload?.svs : stryMutAct_9fa48("4239") ? (dso as any)?.dso_rules.contract?.payload?.svs : stryMutAct_9fa48("4238") ? (dso as any)?.dso_rules?.contract.payload?.svs : stryMutAct_9fa48("4237") ? (dso as any)?.dso_rules?.contract?.payload.svs : (stryCov_9fa48("4237", "4238", "4239", "4240"), (dso as any)?.dso_rules?.contract?.payload?.svs)))) {
            if (stryMutAct_9fa48("4241")) {
              {}
            } else {
              stryCov_9fa48("4241");
              const svs = (dso as any).dso_rules.contract.payload.svs;
              stryMutAct_9fa48("4242") ? svs.forEach(([svPartyId, svInfo]: [string, any]) => {
                proposals.push({
                  id: svPartyId.slice(0, 12),
                  title: `Super Validator Onboarding: ${svInfo.name}`,
                  description: `${svInfo.name} approved at round ${svInfo.joinedAsOfRound?.number || 0}`,
                  status: "executed",
                  votesFor: (dso as any).voting_threshold,
                  votesAgainst: 0,
                  createdAt: (dso as any).dso_rules.contract.created_at
                });
              }) : (stryCov_9fa48("4242"), svs.slice(0, 20).forEach(([svPartyId, svInfo]: [string, any]) => {
                if (stryMutAct_9fa48("4243")) {
                  {}
                } else {
                  stryCov_9fa48("4243");
                  proposals.push({
                    id: stryMutAct_9fa48("4245") ? svPartyId : (stryCov_9fa48("4245"), svPartyId.slice(0, 12)),
                    title: `Super Validator Onboarding: ${svInfo.name}`,
                    description: `${svInfo.name} approved at round ${stryMutAct_9fa48("4250") ? svInfo.joinedAsOfRound?.number && 0 : stryMutAct_9fa48("4249") ? false : stryMutAct_9fa48("4248") ? true : (stryCov_9fa48("4248", "4249", "4250"), (stryMutAct_9fa48("4251") ? svInfo.joinedAsOfRound.number : (stryCov_9fa48("4251"), svInfo.joinedAsOfRound?.number)) || 0)}`,
                    status: "executed",
                    votesFor: (dso as any).voting_threshold,
                    votesAgainst: 0,
                    createdAt: (dso as any).dso_rules.contract.created_at
                  });
                }
              }));
            }
          }
          stryMutAct_9fa48("4253") ? proposals : (stryCov_9fa48("4253"), proposals.sort(stryMutAct_9fa48("4254") ? () => undefined : (stryCov_9fa48("4254"), (a, b) => stryMutAct_9fa48("4255") ? new Date(b.createdAt || 0).getTime() + new Date(a.createdAt || 0).getTime() : (stryCov_9fa48("4255"), new Date(stryMutAct_9fa48("4258") ? b.createdAt && 0 : stryMutAct_9fa48("4257") ? false : stryMutAct_9fa48("4256") ? true : (stryCov_9fa48("4256", "4257", "4258"), b.createdAt || 0)).getTime() - new Date(stryMutAct_9fa48("4261") ? a.createdAt && 0 : stryMutAct_9fa48("4260") ? false : stryMutAct_9fa48("4259") ? true : (stryCov_9fa48("4259", "4260", "4261"), a.createdAt || 0)).getTime()))));
          return proposals;
        }
      } catch (e) {
        if (stryMutAct_9fa48("4262")) {
          {}
        } else {
          stryCov_9fa48("4262");
          console.error("Error fetching governance proposals:", e);
          return stryMutAct_9fa48("4264") ? ["Stryker was here"] : (stryCov_9fa48("4264"), []);
        }
      }
    }
  }
};