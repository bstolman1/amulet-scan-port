import { useQuery } from "@tanstack/react-query";
import { scanApi, AnsEntry, Contract, ValidatorFaucetInfo, ContractWithState } from "@/lib/api-client";

/**
 * Hooks that use Canton Scan API directly instead of ACS aggregates.
 * These provide real-time data from the Canton network.
 */

// ============ ANS Entries ============
export function useAnsEntries(namePrefix?: string, pageSize: number = 1000) {
  return useQuery({
    queryKey: ["scan-api", "ans-entries", namePrefix, pageSize],
    queryFn: async () => {
      const response = await scanApi.fetchAnsEntries(namePrefix, pageSize);
      return response.entries || [];
    },
    staleTime: 60 * 1000, // 1 minute
  });
}

export function useAnsEntryByParty(party: string | undefined) {
  return useQuery({
    queryKey: ["scan-api", "ans-entry-by-party", party],
    queryFn: async () => {
      if (!party) throw new Error("Party required");
      const response = await scanApi.fetchAnsEntryByParty(party);
      return response.entry;
    },
    enabled: !!party,
    staleTime: 60 * 1000,
  });
}

export function useAnsEntryByName(name: string | undefined) {
  return useQuery({
    queryKey: ["scan-api", "ans-entry-by-name", name],
    queryFn: async () => {
      if (!name) throw new Error("Name required");
      const response = await scanApi.fetchAnsEntryByName(name);
      return response.entry;
    },
    enabled: !!name,
    staleTime: 60 * 1000,
  });
}

// ============ Featured Apps ============
export function useFeaturedApps() {
  return useQuery({
    queryKey: ["scan-api", "featured-apps"],
    queryFn: async () => {
      const response = await scanApi.fetchFeaturedApps();
      return response.featured_apps || [];
    },
    staleTime: 60 * 1000,
  });
}

export function useFeaturedApp(providerPartyId: string | undefined) {
  return useQuery({
    queryKey: ["scan-api", "featured-app", providerPartyId],
    queryFn: async () => {
      if (!providerPartyId) throw new Error("Provider party ID required");
      const response = await scanApi.fetchFeaturedApp(providerPartyId);
      return response.featured_app_right;
    },
    enabled: !!providerPartyId,
    staleTime: 60 * 1000,
  });
}

// ============ Validator Licenses ============
export function useValidatorLicenses(limit: number = 1000) {
  return useQuery({
    queryKey: ["scan-api", "validator-licenses", limit],
    queryFn: async () => {
      // Fetch all pages
      const allLicenses: Contract[] = [];
      let after: number | undefined;
      
      while (true) {
        const response = await scanApi.fetchValidatorLicenses(after, limit);
        allLicenses.push(...(response.validator_licenses || []));
        
        if (!response.next_page_token) break;
        after = response.next_page_token;
      }
      
      return allLicenses;
    },
    staleTime: 60 * 1000,
  });
}

// ============ Top Validators ============
export function useTopValidatorsByFaucets(limit: number = 1000) {
  return useQuery({
    queryKey: ["scan-api", "top-validators-by-faucets", limit],
    queryFn: async () => {
      const response = await scanApi.fetchTopValidatorsByFaucets(limit);
      return response.validatorsByReceivedFaucets || [];
    },
    staleTime: 60 * 1000,
  });
}

export function useValidatorLiveness(validatorIds: string[]) {
  return useQuery({
    queryKey: ["scan-api", "validator-liveness", validatorIds],
    queryFn: async () => {
      if (validatorIds.length === 0) return { validatorsReceivedFaucets: [] };
      const response = await scanApi.fetchValidatorLiveness(validatorIds);
      return response;
    },
    enabled: validatorIds.length > 0,
    staleTime: 60 * 1000,
  });
}

// ============ DSO Info ============
export function useDsoInfo() {
  return useQuery({
    queryKey: ["scan-api", "dso-info"],
    queryFn: () => scanApi.fetchDsoInfo(),
    staleTime: 60 * 1000,
  });
}

export function useDsoPartyId() {
  return useQuery({
    queryKey: ["scan-api", "dso-party-id"],
    queryFn: async () => {
      const response = await scanApi.fetchDsoPartyId();
      return response.dso_party_id;
    },
    staleTime: 5 * 60 * 1000, // 5 minutes - rarely changes
  });
}

// ============ Mining Rounds ============
export function useClosedRounds() {
  return useQuery({
    queryKey: ["scan-api", "closed-rounds"],
    queryFn: async () => {
      const response = await scanApi.fetchClosedRounds();
      return response.rounds || [];
    },
    staleTime: 30 * 1000, // 30 seconds
  });
}

export function useOpenAndIssuingRounds() {
  return useQuery({
    queryKey: ["scan-api", "open-and-issuing-rounds"],
    queryFn: async () => {
      const response = await scanApi.fetchOpenAndIssuingRounds();
      return {
        openRounds: Object.values(response.open_mining_rounds || {}),
        issuingRounds: Object.values(response.issuing_mining_rounds || {}),
        ttl: response.time_to_live_in_microseconds,
      };
    },
    staleTime: 30 * 1000,
  });
}

export function useAllMiningRounds() {
  return useQuery({
    queryKey: ["scan-api", "all-mining-rounds"],
    queryFn: () => scanApi.fetchAllMiningRoundsCurrent(),
    staleTime: 30 * 1000,
  });
}

export function useLatestRound() {
  return useQuery({
    queryKey: ["scan-api", "latest-round"],
    queryFn: () => scanApi.fetchLatestRound(),
    staleTime: 30 * 1000,
  });
}

// ============ Scans & Sequencers ============
export function useScans() {
  return useQuery({
    queryKey: ["scan-api", "scans"],
    queryFn: async () => {
      const response = await scanApi.fetchScans();
      return response.scans || [];
    },
    staleTime: 5 * 60 * 1000,
  });
}

export function useDsoSequencers() {
  return useQuery({
    queryKey: ["scan-api", "dso-sequencers"],
    queryFn: async () => {
      const response = await scanApi.fetchDsoSequencers();
      return response.domainSequencers || [];
    },
    staleTime: 5 * 60 * 1000,
  });
}

// ============ Transfer APIs ============
export function useTransferPreapproval(party: string | undefined) {
  return useQuery({
    queryKey: ["scan-api", "transfer-preapproval", party],
    queryFn: async () => {
      if (!party) throw new Error("Party required");
      try {
        const response = await scanApi.fetchTransferPreapprovalByParty(party);
        return response.transfer_preapproval;
      } catch {
        return null;
      }
    },
    enabled: !!party,
    staleTime: 60 * 1000,
  });
}

export function useTransferCommandCounter(party: string | undefined) {
  return useQuery({
    queryKey: ["scan-api", "transfer-command-counter", party],
    queryFn: async () => {
      if (!party) throw new Error("Party required");
      try {
        const response = await scanApi.fetchTransferCommandCounter(party);
        return response.transfer_command_counter;
      } catch {
        return null;
      }
    },
    enabled: !!party,
    staleTime: 60 * 1000,
  });
}

// ============ Governance ============
export function useActiveVoteRequests() {
  return useQuery({
    queryKey: ["scan-api", "active-vote-requests"],
    queryFn: async () => {
      const response = await fetch(
        `${import.meta.env.VITE_SCAN_API_URL || "https://scan.sv-1.global.canton.network.sync.global/api/scan"}/v0/admin/sv/voterequests`,
        { mode: "cors" }
      );
      if (!response.ok) throw new Error("Failed to fetch vote requests");
      const data = await response.json();
      return data.dso_rules_vote_requests || [];
    },
    staleTime: 30 * 1000,
  });
}

export interface VoteResultsRequest {
  actionName?: string;
  accepted?: boolean;
  requester?: string;
  effectiveFrom?: string;
  effectiveTo?: string;
  limit?: number;
}

export function useVoteResults(request: VoteResultsRequest = {}) {
  return useQuery({
    queryKey: ["scan-api", "vote-results", request],
    queryFn: async () => {
      const response = await fetch(
        `${import.meta.env.VITE_SCAN_API_URL || "https://scan.sv-1.global.canton.network.sync.global/api/scan"}/v0/admin/sv/voteresults`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(request),
          mode: "cors",
        }
      );
      if (!response.ok) throw new Error("Failed to fetch vote results");
      const data = await response.json();
      return data.dso_rules_vote_results || [];
    },
    staleTime: 60 * 1000,
  });
}

// ============ Network Info ============
export function useSpliceInstanceNames() {
  return useQuery({
    queryKey: ["scan-api", "splice-instance-names"],
    queryFn: () => scanApi.fetchSpliceInstanceNames(),
    staleTime: 5 * 60 * 1000,
  });
}

export function useMigrationSchedule() {
  return useQuery({
    queryKey: ["scan-api", "migration-schedule"],
    queryFn: async () => {
      try {
        return await scanApi.fetchMigrationSchedule();
      } catch {
        return null; // No migration scheduled
      }
    },
    staleTime: 5 * 60 * 1000,
  });
}

// ============ ACS State (for data that doesn't have direct endpoints) ============
export function useStateAcs(templates: string[], pageSize: number = 1000) {
  return useQuery({
    queryKey: ["scan-api", "state-acs", templates, pageSize],
    queryFn: async () => {
      const latest = await scanApi.fetchLatestRound();
      const snap = await scanApi.fetchAcsSnapshotTimestamp(latest.effectiveAt, 0);
      const response = await scanApi.fetchStateAcs({
        migration_id: 0,
        record_time: snap.record_time,
        page_size: pageSize,
        templates,
      });
      return response.created_events || [];
    },
    staleTime: 60 * 1000,
    enabled: templates.length > 0,
  });
}

// ============ Holdings ============
export function useHoldingsSummary(partyIds: string[], asOfRound?: number) {
  return useQuery({
    queryKey: ["scan-api", "holdings-summary", partyIds, asOfRound],
    queryFn: async () => {
      if (partyIds.length === 0) return { summaries: [] };
      const latest = await scanApi.fetchLatestRound();
      const snap = await scanApi.fetchAcsSnapshotTimestamp(latest.effectiveAt, 0);
      const response = await scanApi.fetchHoldingsSummary({
        migration_id: 0,
        record_time: snap.record_time,
        owner_party_ids: partyIds,
        as_of_round: asOfRound,
      });
      return response;
    },
    enabled: partyIds.length > 0,
    staleTime: 60 * 1000,
  });
}

// ============ Amulet Rules ============
export function useAmuletRules() {
  return useQuery({
    queryKey: ["scan-api", "amulet-rules"],
    queryFn: async () => {
      const dsoInfo = await scanApi.fetchDsoInfo();
      return dsoInfo.amulet_rules;
    },
    staleTime: 60 * 1000,
  });
}

export function useExternalPartyAmuletRules() {
  return useQuery({
    queryKey: ["scan-api", "external-party-amulet-rules"],
    queryFn: async () => {
      const response = await fetch(
        `${import.meta.env.VITE_SCAN_API_URL || "https://scan.sv-1.global.canton.network.sync.global/api/scan"}/v0/external-party-amulet-rules`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({}),
          mode: "cors",
        }
      );
      if (!response.ok) throw new Error("Failed to fetch external party amulet rules");
      const data = await response.json();
      return data.external_party_amulet_rules_update;
    },
    staleTime: 60 * 1000,
  });
}

// ============ DSO State - SV Nodes ============
export function useSvNodeStates() {
  return useQuery({
    queryKey: ["scan-api", "sv-node-states"],
    queryFn: async () => {
      const dsoInfo = await scanApi.fetchDsoInfo();
      return dsoInfo.sv_node_states || [];
    },
    staleTime: 60 * 1000,
  });
}

export function useDsoRules() {
  return useQuery({
    queryKey: ["scan-api", "dso-rules"],
    queryFn: async () => {
      const dsoInfo = await scanApi.fetchDsoInfo();
      return dsoInfo.dso_rules;
    },
    staleTime: 60 * 1000,
  });
}

// ============ Traffic Status ============
export function useTrafficStatus(domainId: string | undefined, memberId: string | undefined) {
  return useQuery({
    queryKey: ["scan-api", "traffic-status", domainId, memberId],
    queryFn: async () => {
      if (!domainId || !memberId) throw new Error("Domain ID and Member ID required");
      return scanApi.fetchTrafficStatus(domainId, memberId);
    },
    enabled: !!domainId && !!memberId,
    staleTime: 30 * 1000,
  });
}
