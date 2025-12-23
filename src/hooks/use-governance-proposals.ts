import { useQuery } from '@tanstack/react-query';
import { getDuckDBApiUrl } from '@/lib/backend-config';

export interface Vote {
  svName: string;
  sv: string;
  accept: boolean;
  reasonUrl: string;
  reasonBody: string;
  castAt?: string;
}

export interface Proposal {
  proposalKey: string;
  latestTimestamp: number;
  latestContractId: string;
  latestEventId: string;
  requester: string;
  actionType: string;
  actionDetails: unknown;
  reasonUrl: string;
  reasonBody: string;
  voteBefore: string;
  voteBeforeTimestamp: number;
  votes: Vote[];
  votesFor: number;
  votesAgainst: number;
  trackingCid: string | null;
  rawTimestamp: string;
  status: 'approved' | 'rejected' | 'pending' | 'expired';
}

export interface ProposalStats {
  total: number;
  byActionType: Record<string, number>;
  byStatus: {
    approved: number;
    rejected: number;
    pending: number;
    expired: number;
  };
}

export interface ProposalsResponse {
  proposals: Proposal[];
  pagination: {
    total: number;
    limit: number;
    offset: number;
    hasMore: boolean;
  };
}

export interface ProposalFilters {
  limit?: number;
  offset?: number;
  status?: 'approved' | 'rejected' | 'pending' | 'expired' | null;
  actionType?: string | null;
  requester?: string | null;
  search?: string | null;
  forceRefresh?: boolean;
}

/**
 * Fetch governance proposals with optional filters
 */
export function useGovernanceProposals(filters: ProposalFilters = {}) {
  const {
    limit = 100,
    offset = 0,
    status = null,
    actionType = null,
    requester = null,
    search = null,
    forceRefresh = false,
  } = filters;

  return useQuery({
    queryKey: ['governance-proposals', limit, offset, status, actionType, requester, search, forceRefresh],
    queryFn: async (): Promise<ProposalsResponse> => {
      const backendUrl = getDuckDBApiUrl();
      const params = new URLSearchParams();
      
      params.set('limit', String(limit));
      params.set('offset', String(offset));
      if (status) params.set('status', status);
      if (actionType) params.set('actionType', actionType);
      if (requester) params.set('requester', requester);
      if (search) params.set('search', search);
      if (forceRefresh) params.set('forceRefresh', 'true');
      
      const response = await fetch(`${backendUrl}/api/governance/proposals?${params}`);
      
      if (!response.ok) {
        throw new Error(`Failed to fetch proposals: ${response.statusText}`);
      }
      
      return response.json();
    },
    staleTime: 2 * 60 * 1000, // 2 minutes
    refetchOnWindowFocus: false,
  });
}

/**
 * Fetch proposal statistics
 */
export function useProposalStats() {
  return useQuery({
    queryKey: ['governance-proposal-stats'],
    queryFn: async (): Promise<ProposalStats> => {
      const backendUrl = getDuckDBApiUrl();
      const response = await fetch(`${backendUrl}/api/governance/proposals/stats`);
      
      if (!response.ok) {
        throw new Error(`Failed to fetch proposal stats: ${response.statusText}`);
      }
      
      return response.json();
    },
    staleTime: 2 * 60 * 1000,
    refetchOnWindowFocus: false,
  });
}

/**
 * Fetch a single proposal by key
 */
export function useProposalByKey(proposalKey: string | null) {
  return useQuery({
    queryKey: ['governance-proposal', proposalKey],
    queryFn: async (): Promise<Proposal | null> => {
      if (!proposalKey) return null;
      
      const backendUrl = getDuckDBApiUrl();
      const encodedKey = encodeURIComponent(proposalKey);
      const response = await fetch(`${backendUrl}/api/governance/proposals/by-key/${encodedKey}`);
      
      if (response.status === 404) {
        return null;
      }
      
      if (!response.ok) {
        throw new Error(`Failed to fetch proposal: ${response.statusText}`);
      }
      
      return response.json();
    },
    enabled: !!proposalKey,
    staleTime: 2 * 60 * 1000,
    refetchOnWindowFocus: false,
  });
}

/**
 * Fetch action types with counts
 */
export function useActionTypes() {
  return useQuery({
    queryKey: ['governance-action-types'],
    queryFn: async (): Promise<Array<{ type: string; count: number }>> => {
      const backendUrl = getDuckDBApiUrl();
      const response = await fetch(`${backendUrl}/api/governance/action-types`);
      
      if (!response.ok) {
        throw new Error(`Failed to fetch action types: ${response.statusText}`);
      }
      
      return response.json();
    },
    staleTime: 5 * 60 * 1000,
    refetchOnWindowFocus: false,
  });
}

/**
 * Get status badge color
 */
export function getStatusColor(status: Proposal['status']): string {
  switch (status) {
    case 'approved':
      return 'bg-green-500/20 text-green-400 border-green-500/30';
    case 'rejected':
      return 'bg-red-500/20 text-red-400 border-red-500/30';
    case 'pending':
      return 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30';
    case 'expired':
      return 'bg-gray-500/20 text-gray-400 border-gray-500/30';
    default:
      return 'bg-muted text-muted-foreground border-border';
  }
}

/**
 * Format action type for display
 */
export function formatActionType(actionType: string): string {
  if (!actionType) return 'Unknown';
  
  // Remove common prefixes
  let display = actionType
    .replace(/^SRARC_/, '')
    .replace(/^CRARC_/, '')
    .replace(/^ARC_/, '');
  
  // Add spaces before capitals
  display = display.replace(/([a-z])([A-Z])/g, '$1 $2');
  
  return display;
}
