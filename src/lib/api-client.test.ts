/**
 * API Client Tests
 * 
 * Tests for the DuckDB API client functions.
 * These tests verify the actual client code behavior, not just mocks.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  apiFetch,
  getLatestEvents,
  getEventsByType,
  getEventsCount,
  getOverviewStats,
  getDailyStats,
  getStatsByType,
  searchEvents,
  getBackfillCursors,
  getACSSnapshots,
  searchAnsEntries,
} from './duckdb-api-client';

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

// Mock the backend config to use a known URL
vi.mock('@/lib/backend-config', () => ({
  getDuckDBApiUrl: () => 'http://test-api:3001',
}));

describe('DuckDB API Client', () => {
  beforeEach(() => {
    mockFetch.mockReset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('apiFetch', () => {
    it('should add Content-Type header automatically', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [] }),
      });

      await apiFetch('/api/test');

      expect(mockFetch).toHaveBeenCalledWith(
        'http://test-api:3001/api/test',
        expect.objectContaining({
          headers: expect.objectContaining({
            'Content-Type': 'application/json',
          }),
        })
      );
    });

    it('should throw on non-ok response with error message', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        json: async () => ({ error: 'Database connection failed' }),
      });

      await expect(apiFetch('/api/test')).rejects.toThrow('Database connection failed');
    });

    it('should throw generic error when response has no error field', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
        json: async () => ({}),
      });

      await expect(apiFetch('/api/test')).rejects.toThrow('API error: 404');
    });

    it('should handle JSON parse failure on error response', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        json: async () => { throw new Error('Invalid JSON'); },
      });

      await expect(apiFetch('/api/test')).rejects.toThrow('Unknown error');
    });

    it('should return parsed JSON on success', async () => {
      const expectedData = { data: [{ id: 1 }, { id: 2 }], count: 2 };
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => expectedData,
      });

      const result = await apiFetch('/api/events');
      expect(result).toEqual(expectedData);
    });
  });

  describe('getLatestEvents', () => {
    it('should call correct endpoint with limit and offset', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [], count: 0 }),
      });

      await getLatestEvents(50, 100);

      expect(mockFetch).toHaveBeenCalledWith(
        'http://test-api:3001/api/events/latest?limit=50&offset=100',
        expect.any(Object)
      );
    });

    it('should use default limit and offset', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [], count: 0 }),
      });

      await getLatestEvents();

      expect(mockFetch).toHaveBeenCalledWith(
        'http://test-api:3001/api/events/latest?limit=100&offset=0',
        expect.any(Object)
      );
    });

    it('should return response with data array', async () => {
      const mockEvents = [
        { event_id: '1', event_type: 'created', template_id: 'Test:Template' },
        { event_id: '2', event_type: 'archived', template_id: 'Test:Template' },
      ];
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: mockEvents, count: 2 }),
      });

      const result = await getLatestEvents();
      
      expect(result.data).toHaveLength(2);
      expect(result.count).toBe(2);
      expect(result.data[0].event_id).toBe('1');
    });
  });

  describe('getEventsByType', () => {
    it('should encode type parameter in URL', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [] }),
      });

      await getEventsByType('created', 25);

      expect(mockFetch).toHaveBeenCalledWith(
        'http://test-api:3001/api/events/by-type/created?limit=25',
        expect.any(Object)
      );
    });

    it('should handle special characters in type', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [] }),
      });

      await getEventsByType('type/with/slashes');

      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('type%2Fwith%2Fslashes'),
        expect.any(Object)
      );
    });
  });

  describe('getEventsCount', () => {
    it('should call count endpoint', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ count: 1234 }),
      });

      const result = await getEventsCount();

      expect(mockFetch).toHaveBeenCalledWith(
        'http://test-api:3001/api/events/count',
        expect.any(Object)
      );
      expect(result.count).toBe(1234);
    });
  });

  describe('getOverviewStats', () => {
    it('should return overview stats object', async () => {
      const mockStats = {
        total_events: 10000,
        unique_contracts: 500,
        unique_templates: 25,
        earliest_event: '2024-01-01T00:00:00Z',
        latest_event: '2025-01-10T12:00:00Z',
      };
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockStats,
      });

      const result = await getOverviewStats();

      expect(result.total_events).toBe(10000);
      expect(result.unique_contracts).toBe(500);
      expect(result.earliest_event).toBe('2024-01-01T00:00:00Z');
    });
  });

  describe('getDailyStats', () => {
    it('should pass days parameter', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [] }),
      });

      await getDailyStats(14);

      expect(mockFetch).toHaveBeenCalledWith(
        'http://test-api:3001/api/stats/daily?days=14',
        expect.any(Object)
      );
    });

    it('should use default days value', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [] }),
      });

      await getDailyStats();

      expect(mockFetch).toHaveBeenCalledWith(
        'http://test-api:3001/api/stats/daily?days=30',
        expect.any(Object)
      );
    });
  });

  describe('getStatsByType', () => {
    it('should return array of type stats', async () => {
      const mockData = {
        data: [
          { event_type: 'created', count: 5000 },
          { event_type: 'archived', count: 3000 },
        ],
      };
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockData,
      });

      const result = await getStatsByType();

      expect(result.data).toHaveLength(2);
      expect(result.data[0].event_type).toBe('created');
      expect(result.data[0].count).toBe(5000);
    });
  });

  describe('searchEvents', () => {
    it('should build query params correctly', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [] }),
      });

      await searchEvents({
        q: 'test query',
        type: 'created',
        template: 'Test:Template',
        limit: 50,
      });

      const calledUrl = mockFetch.mock.calls[0][0];
      expect(calledUrl).toContain('q=test+query');
      expect(calledUrl).toContain('type=created');
      expect(calledUrl).toContain('template=Test%3ATemplate');
      expect(calledUrl).toContain('limit=50');
    });

    it('should omit undefined params', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [] }),
      });

      await searchEvents({ q: 'test' });

      const calledUrl = mockFetch.mock.calls[0][0];
      expect(calledUrl).toContain('q=test');
      expect(calledUrl).not.toContain('type=');
      expect(calledUrl).not.toContain('template=');
    });
  });

  describe('getBackfillCursors', () => {
    it('should return cursor data', async () => {
      const mockCursors = {
        data: [
          { id: '1', cursor_name: 'migration-1', complete: false },
          { id: '2', cursor_name: 'migration-2', complete: true },
        ],
      };
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockCursors,
      });

      const result = await getBackfillCursors();

      expect(result.data).toHaveLength(2);
      expect(result.data[0].cursor_name).toBe('migration-1');
      expect(result.data[1].complete).toBe(true);
    });
  });

  describe('getACSSnapshots', () => {
    it('should call snapshots endpoint', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [] }),
      });

      await getACSSnapshots();

      expect(mockFetch).toHaveBeenCalledWith(
        'http://test-api:3001/api/acs/snapshots',
        expect.any(Object)
      );
    });
  });

  describe('searchAnsEntries', () => {
    it('should search with template=AnsEntry', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [] }),
      });

      await searchAnsEntries('testname', 10);

      const calledUrl = mockFetch.mock.calls[0][0];
      expect(calledUrl).toContain('template=AnsEntry');
      expect(calledUrl).toContain('search=testname');
      expect(calledUrl).toContain('limit=10');
    });

    it('should use default limit', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [] }),
      });

      await searchAnsEntries('test');

      const calledUrl = mockFetch.mock.calls[0][0];
      expect(calledUrl).toContain('limit=25');
    });
  });

  describe('Error handling', () => {
    it('should propagate network errors', async () => {
      mockFetch.mockRejectedValueOnce(new Error('Network error'));

      await expect(getLatestEvents()).rejects.toThrow('Network error');
    });

    it('should propagate API errors with message', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 400,
        json: async () => ({ error: 'Invalid limit parameter' }),
      });

      await expect(getLatestEvents(-1)).rejects.toThrow('Invalid limit parameter');
    });
  });

  describe('URL encoding', () => {
    it('should properly encode party IDs with special characters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [] }),
      });

      // Import the function that uses party ID
      const { getPartyEvents } = await import('./duckdb-api-client');
      await getPartyEvents('validator::1220abc');

      const calledUrl = mockFetch.mock.calls[0][0];
      expect(calledUrl).toContain('validator%3A%3A1220abc');
    });

    it('should properly encode template IDs with colons', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ data: [] }),
      });

      const { getEventsByTemplate } = await import('./duckdb-api-client');
      await getEventsByTemplate('Splice.Amulet:Amulet');

      const calledUrl = mockFetch.mock.calls[0][0];
      expect(calledUrl).toContain('Splice.Amulet%3AAmulet');
    });
  });
});
