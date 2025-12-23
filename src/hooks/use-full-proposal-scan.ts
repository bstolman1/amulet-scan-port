import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

interface Vote {
  svName: string;
  sv: string;
  accept: boolean;
  reasonUrl: string;
  reasonBody: string;
  castAt?: string;
}

interface Proposal {
  proposalKey: string;
  latestTimestamp: number;
  latestContractId: string;
  requester: string;
  actionType: string;
  actionDetails: any;
  reasonUrl: string;
  reasonBody: string;
  voteBefore: string;
  voteBeforeTimestamp: number;
  votes: Vote[];
  votesFor: number;
  votesAgainst: number;
  trackingCid: string | null;
  rawTimestamp: string;
}

interface Stats {
  total: number;
  byActionType: Record<string, number>;
  byStatus: {
    approved: number;
    rejected: number;
    pending: number;
  };
}

interface FullProposalScanResponse {
  summary: {
    filesScanned: number;
    totalFilesInDataset: number;
    totalVoteRequests: number;
    uniqueProposals: number;
  };
  stats: Stats;
  proposals: Proposal[];
}

export function useFullProposalScan(enabled: boolean = false, scanAll: boolean = false) {
  return useQuery<FullProposalScanResponse>({
    queryKey: ["full-proposal-scan", scanAll],
    queryFn: () => apiFetch<FullProposalScanResponse>(
      `/api/events/governance/proposals?files=${scanAll ? 'all' : '2000'}`
    ),
    enabled,
    staleTime: 5 * 60 * 1000, // 5 minutes
    gcTime: 10 * 60 * 1000, // 10 minutes
    retry: 1,
  });
}
