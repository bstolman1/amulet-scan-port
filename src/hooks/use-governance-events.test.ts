/**
 * useGovernanceEvents - Comprehensive Unit Tests
 * Tests governance event fetching, transformation, and error handling
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ReactNode } from 'react';

// Mock the API client
vi.mock('@/lib/duckdb-api-client', () => ({
  apiFetch: vi.fn(),
}));

import { apiFetch } from '@/lib/duckdb-api-client';
import { useGovernanceEvents, useRewardClaimEvents } from './use-governance-events';

const createWrapper = () => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        gcTime: 0,
      },
    },
  });
  const Wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
  return Wrapper;
};

describe('useGovernanceEvents', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('successful data fetching', () => {
    it('returns governance events', async () => {
      const mockData = {
        data: [
          {
            event_id: 'event-1',
            contract_id: 'contract-1',
            template_id: 'Splice:DsoRules:VoteRequest',
            effective_at: '2024-12-01T00:00:00Z',
            is_closed: false,
            action_tag: 'ARC_DsoRules',
            requester: 'party-1',
          },
        ],
        source: 'duckdb-index',
        _debug: {
          fromIndex: true,
          indexedAt: '2024-12-15T00:00:00Z',
          totalIndexed: 100,
        },
      };

      (apiFetch as any).mockResolvedValueOnce(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      expect(result.current.data).toBeDefined();
      expect(result.current.data?.events).toHaveLength(1);
      expect(result.current.data?.source).toBe('duckdb-index');
      expect(result.current.data?.fromIndex).toBe(true);
    });

    it('deduplicates events by event_id', async () => {
      const mockData = {
        data: [
          { event_id: 'event-1', contract_id: 'c1' },
          { event_id: 'event-1', contract_id: 'c1' }, // Duplicate
          { event_id: 'event-2', contract_id: 'c2' },
        ],
        source: 'duckdb',
      };

      (apiFetch as any).mockResolvedValueOnce(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      expect(result.current.data?.events).toHaveLength(2);
    });

    it('filters out events without event_id', async () => {
      const mockData = {
        data: [
          { event_id: 'event-1', contract_id: 'c1' },
          { event_id: null, contract_id: 'c2' }, // No event_id
          { contract_id: 'c3' }, // Missing event_id
        ],
        source: 'duckdb',
      };

      (apiFetch as any).mockResolvedValueOnce(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      expect(result.current.data?.events).toHaveLength(1);
    });

    it('maps closed events to archived type', async () => {
      const mockData = {
        data: [
          { event_id: 'e1', contract_id: 'c1', is_closed: false },
          { event_id: 'e2', contract_id: 'c2', is_closed: true },
        ],
        source: 'duckdb',
      };

      (apiFetch as any).mockResolvedValueOnce(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      const events = result.current.data?.events;
      expect(events?.[0].event_type).toBe('created');
      expect(events?.[1].event_type).toBe('archived');
    });

    it('applies default template_id when missing', async () => {
      const mockData = {
        data: [
          { event_id: 'e1', contract_id: 'c1', template_id: null },
        ],
        source: 'duckdb',
      };

      (apiFetch as any).mockResolvedValueOnce(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      expect(result.current.data?.events[0].template_id).toBe('Splice:DsoRules:VoteRequest');
    });
  });

  describe('payload parsing', () => {
    it('parses JSON string payload', async () => {
      const mockData = {
        data: [
          {
            event_id: 'e1',
            contract_id: 'c1',
            payload: '{"action": {"tag": "ARC_DsoRules"}, "requester": "party-1"}',
          },
        ],
        source: 'duckdb',
      };

      (apiFetch as any).mockResolvedValueOnce(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      const payload = result.current.data?.events[0].payload as Record<string, unknown>;
      expect(payload).toHaveProperty('action');
      expect((payload.action as any).tag).toBe('ARC_DsoRules');
    });

    it('reconstructs payload from individual fields when payload is missing', async () => {
      const mockData = {
        data: [
          {
            event_id: 'e1',
            contract_id: 'c1',
            action_tag: 'ARC_AmuletRules',
            action_value: { newConfig: {} },
            requester: 'party-1',
            reason: { description: 'Test reason' },
            votes: [{ voter: 'party-2', accept: true }],
            vote_before: '2024-12-31T00:00:00Z',
            target_effective_at: '2025-01-01T00:00:00Z',
            tracking_cid: 'tracking-123',
            dso: 'DSO::1234',
          },
        ],
        source: 'duckdb',
      };

      (apiFetch as any).mockResolvedValueOnce(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      const payload = result.current.data?.events[0].payload as Record<string, unknown>;
      expect(payload).toHaveProperty('action');
      expect(payload).toHaveProperty('requester');
      expect(payload).toHaveProperty('votes');
      expect(payload.voteBefore).toBe('2024-12-31T00:00:00Z');
    });

    it('parses JSON string votes', async () => {
      const mockData = {
        data: [
          {
            event_id: 'e1',
            contract_id: 'c1',
            votes: '[{"voter": "party-1", "accept": true}]',
          },
        ],
        source: 'duckdb',
      };

      (apiFetch as any).mockResolvedValueOnce(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      const payload = result.current.data?.events[0].payload as Record<string, unknown>;
      expect(Array.isArray(payload.votes)).toBe(true);
    });

    it('handles invalid JSON gracefully', async () => {
      const mockData = {
        data: [
          {
            event_id: 'e1',
            contract_id: 'c1',
            payload: 'not-valid-json',
          },
        ],
        source: 'duckdb',
      };

      (apiFetch as any).mockResolvedValueOnce(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      // Should not throw, payload will be reconstructed
      expect(result.current.data?.events).toHaveLength(1);
    });
  });

  describe('debug info handling', () => {
    it('extracts debug info from response', async () => {
      const mockData = {
        data: [],
        source: 'duckdb-index',
        _debug: {
          fromIndex: true,
          indexedAt: '2024-12-15T12:00:00Z',
          totalIndexed: 500,
        },
      };

      (apiFetch as any).mockResolvedValueOnce(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      expect(result.current.data?.fromIndex).toBe(true);
      expect(result.current.data?.indexedAt).toBe('2024-12-15T12:00:00Z');
      expect(result.current.data?.totalIndexed).toBe(500);
    });

    it('handles missing debug info', async () => {
      const mockData = {
        data: [],
        source: 'duckdb',
      };

      (apiFetch as any).mockResolvedValueOnce(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      expect(result.current.data?.fromIndex).toBe(false);
      expect(result.current.data?.indexedAt).toBe(null);
      expect(result.current.data?.totalIndexed).toBe(null);
    });
  });

  describe('error handling', () => {
    it('handles API errors', async () => {
      (apiFetch as any).mockRejectedValueOnce(new Error('Network error'));

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isError).toBe(true));

      expect(result.current.error).toBeDefined();
    });

    it('handles empty response', async () => {
      const mockData = {
        data: null,
        source: 'duckdb',
      };

      (apiFetch as any).mockResolvedValueOnce(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      expect(result.current.data?.events).toEqual([]);
    });

    it('handles undefined data', async () => {
      const mockData = { source: 'duckdb' };

      (apiFetch as any).mockResolvedValueOnce(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      expect(result.current.data?.events).toEqual([]);
    });
  });

  describe('caching behavior', () => {
    it('uses staleTime of 30 seconds', async () => {
      const mockData = {
        data: [{ event_id: 'e1', contract_id: 'c1' }],
        source: 'duckdb',
      };

      (apiFetch as any).mockResolvedValue(mockData);

      const { result, rerender } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));

      // First call
      expect(apiFetch).toHaveBeenCalledTimes(1);

      // Rerender immediately
      rerender();

      // Should not refetch (stale time)
      expect(apiFetch).toHaveBeenCalledTimes(1);
    });
  });
});

describe('useRewardClaimEvents', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns reward events', async () => {
    const mockData = {
      data: [
        { event_id: 'reward-1', event_type: 'created', template_id: 'RewardCoupon' },
        { event_id: 'reward-2', event_type: 'exercised', template_id: 'SvRewardCoupon' },
      ],
    };

    (apiFetch as any).mockResolvedValueOnce(mockData);

    const { result } = renderHook(() => useRewardClaimEvents(), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(result.current.data).toHaveLength(2);
    expect(result.current.data?.[0].event_id).toBe('reward-1');
  });

  it('handles API errors', async () => {
    (apiFetch as any).mockRejectedValueOnce(new Error('Reward fetch failed'));

    const { result } = renderHook(() => useRewardClaimEvents(), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isError).toBe(true));

    expect(result.current.error).toBeDefined();
  });

  it('handles empty rewards', async () => {
    const mockData = { data: [] };

    (apiFetch as any).mockResolvedValueOnce(mockData);

    const { result } = renderHook(() => useRewardClaimEvents(), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(result.current.data).toEqual([]);
  });

  it('handles null data response', async () => {
    const mockData = { data: null };

    (apiFetch as any).mockResolvedValueOnce(mockData);

    const { result } = renderHook(() => useRewardClaimEvents(), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(result.current.data).toEqual([]);
  });
});
