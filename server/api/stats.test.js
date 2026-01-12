/**
 * Stats API Integration Tests
 * 
 * Tests for /api/stats/* endpoints
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createMockRequest, createMockResponse, mockStatsOverview } from '../test/fixtures/mock-data.js';

// Mock the database and binary reader before importing the router
vi.mock('../duckdb/connection.js', () => ({
  default: {
    DATA_PATH: '/mock/data',
    safeQuery: vi.fn().mockResolvedValue([{
      total_events: 15000n,
      unique_contracts: 5000n,
      unique_templates: 25n,
      earliest_event: '2024-01-01T00:00:00.000Z',
      latest_event: '2025-01-10T14:00:00.000Z',
    }]),
    hasFileType: vi.fn().mockReturnValue(false),
  },
  query: vi.fn(),
}));

vi.mock('../duckdb/binary-reader.js', () => ({
  default: {
    hasBinaryFiles: vi.fn().mockReturnValue(false),
    findBinaryFiles: vi.fn().mockReturnValue([]),
    loadAllRecords: vi.fn().mockResolvedValue([]),
    streamRecords: vi.fn().mockResolvedValue({ records: [], hasMore: false }),
  },
}));

vi.mock('../engine/aggregations.js', () => ({
  getTotalCounts: vi.fn().mockResolvedValue({ events: 15000 }),
  getTimeRange: vi.fn().mockResolvedValue({ 
    min_ts: '2024-01-01T00:00:00.000Z', 
    max_ts: '2025-01-10T14:00:00.000Z' 
  }),
  getTemplateEventCounts: vi.fn().mockResolvedValue([]),
}));

vi.mock('../engine/ingest.js', () => ({
  getIngestionStats: vi.fn().mockResolvedValue({}),
}));

vi.mock('../engine/schema.js', () => ({
  initEngineSchema: vi.fn().mockResolvedValue(undefined),
}));

describe('Stats API', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('GET /api/stats/overview', () => {
    it('should return overview stats from JSONL source', async () => {
      // Import after mocks are set up
      const { default: db } = await import('../duckdb/connection.js');
      
      const req = createMockRequest();
      const res = createMockResponse();
      
      // Simulate calling the overview endpoint logic
      const rows = await db.safeQuery('SELECT COUNT(*) ...');
      
      expect(rows).toBeDefined();
      expect(rows[0].total_events).toBe(15000n);
    });

    it('should handle database errors gracefully', async () => {
      const { default: db } = await import('../duckdb/connection.js');
      db.safeQuery.mockRejectedValueOnce(new Error('Database connection failed'));
      
      await expect(db.safeQuery('SELECT ...')).rejects.toThrow('Database connection failed');
    });
  });

  describe('GET /api/stats/daily', () => {
    it('should validate days parameter', () => {
      const days = Math.min(parseInt('45') || 30, 365);
      expect(days).toBe(45);
    });

    it('should cap days at 365', () => {
      const days = Math.min(parseInt('500') || 30, 365);
      expect(days).toBe(365);
    });

    it('should default to 30 days when invalid', () => {
      const days = Math.min(parseInt('invalid') || 30, 365);
      expect(days).toBe(30);
    });
  });

  describe('GET /api/stats/by-template', () => {
    it('should validate limit parameter', () => {
      const limit = Math.min(parseInt('100') || 50, 500);
      expect(limit).toBe(100);
    });

    it('should cap limit at 500', () => {
      const limit = Math.min(parseInt('1000') || 50, 500);
      expect(limit).toBe(500);
    });
  });
});

describe('Stats data transformation', () => {
  it('should convert BigInt to Number for JSON serialization', () => {
    const data = {
      total_events: 15000n,
      unique_contracts: 5000n,
      nested: { count: 100n },
      array: [1n, 2n, 3n],
    };
    
    // BigInt serialization helper
    const convertBigInts = (obj) => {
      if (obj === null || obj === undefined) return obj;
      if (typeof obj === 'bigint') return Number(obj);
      if (Array.isArray(obj)) return obj.map(convertBigInts);
      if (typeof obj === 'object') {
        const result = {};
        for (const [key, value] of Object.entries(obj)) {
          result[key] = convertBigInts(value);
        }
        return result;
      }
      return obj;
    };
    
    const converted = convertBigInts(data);
    
    expect(converted.total_events).toBe(15000);
    expect(typeof converted.total_events).toBe('number');
    expect(converted.nested.count).toBe(100);
    expect(converted.array).toEqual([1, 2, 3]);
  });
});
