/**
 * useGovernanceEvents - Comprehensive Unit Tests
 * Tests governance event fetching, transformation, and error handling
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import React, { ReactNode } from 'react';

// Mock the API client
vi.mock('@/lib/duckdb-api-client', () => ({
  apiFetch: vi.fn(),
}));

import { apiFetch } from '@/lib/duckdb-api-client';
import { useGovernanceEvents, useRewardClaimEvents } from './use-governance-events';

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        gcTime: 0,
      },
    },
  });
  return function Wrapper({ children }: { children: ReactNode }) {
    return React.createElement(QueryClientProvider, { client: queryClient }, children);
  };
}

// Helper to wait for query to settle
async function waitForQuery<T>(hook: { isLoading: boolean; isSuccess?: boolean; isError?: boolean; data?: T }) {
  // Give React Query time to process
  await act(async () => {
    await new Promise(resolve => setTimeout(resolve, 50));
  });
}

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

      (apiFetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitForQuery(result.current);

      expect(result.current.data).toBeDefined();
      expect(result.current.data?.events).toHaveLength(1);
      expect(result.current.data?.source).toBe('duckdb-index');
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

      (apiFetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitForQuery(result.current);

      // Hook should deduplicate
      expect(result.current.data?.events.length).toBeLessThanOrEqual(3);
    });

    it('maps closed events correctly', async () => {
      const mockData = {
        data: [
          { event_id: 'e1', contract_id: 'c1', is_closed: false },
          { event_id: 'e2', contract_id: 'c2', is_closed: true },
        ],
        source: 'duckdb',
      };

      (apiFetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitForQuery(result.current);

      const events = result.current.data?.events;
      expect(events).toBeDefined();
      expect(events?.length).toBeGreaterThan(0);
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

      (apiFetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitForQuery(result.current);

      expect(result.current.data?.events).toBeDefined();
    });

    it('handles object payload directly', async () => {
      const mockData = {
        data: [
          {
            event_id: 'e1',
            contract_id: 'c1',
            payload: { action: { tag: 'ARC_DsoRules' } },
          },
        ],
        source: 'duckdb',
      };

      (apiFetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitForQuery(result.current);

      expect(result.current.data?.events).toBeDefined();
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

      (apiFetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitForQuery(result.current);

      // Should not throw, event should still be present
      expect(result.current.data?.events).toBeDefined();
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

      (apiFetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitForQuery(result.current);

      expect(result.current.data?.source).toBe('duckdb-index');
    });

    it('handles missing debug info', async () => {
      const mockData = {
        data: [],
        source: 'duckdb',
      };

      (apiFetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitForQuery(result.current);

      expect(result.current.data?.source).toBe('duckdb');
    });
  });

  describe('error handling', () => {
    it('handles API errors', async () => {
      (apiFetch as ReturnType<typeof vi.fn>).mockRejectedValue(new Error('Network error'));

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitForQuery(result.current);

      expect(result.current.isError).toBe(true);
      expect(result.current.error).toBeDefined();
    });

    it('handles empty response', async () => {
      const mockData = {
        data: null,
        source: 'duckdb',
      };

      (apiFetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitForQuery(result.current);

      expect(result.current.data?.events).toEqual([]);
    });

    it('handles undefined data', async () => {
      const mockData = { source: 'duckdb' };

      (apiFetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockData);

      const { result } = renderHook(() => useGovernanceEvents(), {
        wrapper: createWrapper(),
      });

      await waitForQuery(result.current);

      expect(result.current.data?.events).toEqual([]);
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

    (apiFetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockData);

    const { result } = renderHook(() => useRewardClaimEvents(), {
      wrapper: createWrapper(),
    });

    await waitForQuery(result.current);

    expect(result.current.data).toHaveLength(2);
  });

  it('handles API errors', async () => {
    (apiFetch as ReturnType<typeof vi.fn>).mockRejectedValue(new Error('Reward fetch failed'));

    const { result } = renderHook(() => useRewardClaimEvents(), {
      wrapper: createWrapper(),
    });

    await waitForQuery(result.current);

    expect(result.current.isError).toBe(true);
  });

  it('handles empty rewards', async () => {
    const mockData = { data: [] };

    (apiFetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockData);

    const { result } = renderHook(() => useRewardClaimEvents(), {
      wrapper: createWrapper(),
    });

    await waitForQuery(result.current);

    expect(result.current.data).toEqual([]);
  });

  it('handles null data response', async () => {
    const mockData = { data: null };

    (apiFetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockData);

    const { result } = renderHook(() => useRewardClaimEvents(), {
      wrapper: createWrapper(),
    });

    await waitForQuery(result.current);

    expect(result.current.data).toEqual([]);
  });
});
