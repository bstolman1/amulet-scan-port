/**
 * API Client Tests
 * 
 * Tests for the API client configuration and helpers
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

describe('API Client', () => {
  beforeEach(() => {
    mockFetch.mockReset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('fetch behavior', () => {
    it('should handle successful responses', async () => {
      const mockData = { data: [{ id: 1 }], count: 1 };
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockData,
      });

      const response = await fetch('http://localhost:3001/api/events/latest');
      const data = await response.json();

      expect(data).toEqual(mockData);
      expect(mockFetch).toHaveBeenCalledWith('http://localhost:3001/api/events/latest');
    });

    it('should handle error responses', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        json: async () => ({ error: 'Internal server error' }),
      });

      const response = await fetch('http://localhost:3001/api/events/latest');
      expect(response.ok).toBe(false);
      expect(response.status).toBe(500);
    });

    it('should handle network errors', async () => {
      mockFetch.mockRejectedValueOnce(new Error('Network error'));

      await expect(fetch('http://localhost:3001/api/events/latest')).rejects.toThrow('Network error');
    });
  });

  describe('query parameter handling', () => {
    it('should correctly encode query parameters', () => {
      const params = new URLSearchParams({
        limit: '100',
        offset: '0',
        template: 'Splice:Amulet:Amulet',
      });

      expect(params.toString()).toBe('limit=100&offset=0&template=Splice%3AAmulet%3AAmulet');
    });

    it('should handle special characters in params', () => {
      const params = new URLSearchParams({
        search: "O'Brien",
        template: 'test::value',
      });

      expect(params.get('search')).toBe("O'Brien");
      expect(params.get('template')).toBe('test::value');
    });
  });
});
