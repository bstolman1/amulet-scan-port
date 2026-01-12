import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock the connection module before importing aggregations
vi.mock('../duckdb/connection.js', () => ({
  safeQuery: vi.fn(),
  getPool: vi.fn(() => ({
    query: vi.fn(),
  })),
}));

// Now import the module under test
import * as aggregations from './aggregations.js';
import { safeQuery } from '../duckdb/connection.js';

describe('Aggregations Module', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('getLastFileId', () => {
    it('returns 0 when no aggregation state exists', async () => {
      safeQuery.mockResolvedValueOnce([]);
      
      const result = await aggregations.getLastFileId('test_agg');
      
      expect(result).toBe(0);
    });

    it('returns the stored file ID when state exists', async () => {
      safeQuery.mockResolvedValueOnce([{ last_file_id: 42n }]);
      
      const result = await aggregations.getLastFileId('event_type_counts');
      
      expect(result).toBe(42);
    });

    it('handles BigInt conversion correctly', async () => {
      safeQuery.mockResolvedValueOnce([{ last_file_id: 9999999999n }]);
      
      const result = await aggregations.getLastFileId('large_id_agg');
      
      expect(result).toBe(9999999999);
    });
  });

  describe('setLastFileId', () => {
    it('updates the last file ID for an aggregation', async () => {
      safeQuery.mockResolvedValueOnce([]);
      
      await aggregations.setLastFileId('test_agg', 100);
      
      expect(safeQuery).toHaveBeenCalled();
      const call = safeQuery.mock.calls[0][0];
      expect(call).toContain('test_agg');
      expect(call).toContain('100');
    });
  });

  describe('getMaxIngestedFileId', () => {
    it('returns 0 when no files are ingested', async () => {
      safeQuery.mockResolvedValueOnce([{ max_id: null }]);
      
      const result = await aggregations.getMaxIngestedFileId();
      
      expect(result).toBe(0);
    });

    it('returns the maximum file ID', async () => {
      safeQuery.mockResolvedValueOnce([{ max_id: 500n }]);
      
      const result = await aggregations.getMaxIngestedFileId();
      
      expect(result).toBe(500);
    });
  });

  describe('hasNewData', () => {
    it('returns true when there is new data to process', async () => {
      // First call: getLastFileId returns 10
      safeQuery.mockResolvedValueOnce([{ last_file_id: 10n }]);
      // Second call: getMaxIngestedFileId returns 20
      safeQuery.mockResolvedValueOnce([{ max_id: 20n }]);
      
      const result = await aggregations.hasNewData('test_agg');
      
      expect(result).toBe(true);
    });

    it('returns false when no new data exists', async () => {
      // Both return same value
      safeQuery.mockResolvedValueOnce([{ last_file_id: 50n }]);
      safeQuery.mockResolvedValueOnce([{ max_id: 50n }]);
      
      const result = await aggregations.hasNewData('test_agg');
      
      expect(result).toBe(false);
    });

    it('returns false when aggregation is ahead (edge case)', async () => {
      safeQuery.mockResolvedValueOnce([{ last_file_id: 100n }]);
      safeQuery.mockResolvedValueOnce([{ max_id: 50n }]);
      
      const result = await aggregations.hasNewData('test_agg');
      
      expect(result).toBe(false);
    });
  });

  describe('getTotalCounts', () => {
    it('returns event and update counts', async () => {
      safeQuery.mockResolvedValueOnce([
        { event_count: 10000n, update_count: 5000n },
      ]);
      
      const result = await aggregations.getTotalCounts();
      
      expect(result.eventCount).toBe(10000);
      expect(result.updateCount).toBe(5000);
    });

    it('handles missing counts gracefully', async () => {
      safeQuery.mockResolvedValueOnce([{}]);
      
      const result = await aggregations.getTotalCounts();
      
      expect(result.eventCount).toBe(0);
      expect(result.updateCount).toBe(0);
    });

    it('handles empty result set', async () => {
      safeQuery.mockResolvedValueOnce([]);
      
      const result = await aggregations.getTotalCounts();
      
      expect(result.eventCount).toBe(0);
      expect(result.updateCount).toBe(0);
    });
  });

  describe('getTimeRange', () => {
    it('returns min and max timestamps', async () => {
      safeQuery.mockResolvedValueOnce([{
        min_ts: '2024-01-01T00:00:00.000Z',
        max_ts: '2025-01-10T12:00:00.000Z',
      }]);
      
      const result = await aggregations.getTimeRange();
      
      expect(result.minTimestamp).toBe('2024-01-01T00:00:00.000Z');
      expect(result.maxTimestamp).toBe('2025-01-10T12:00:00.000Z');
    });

    it('returns null timestamps when no data exists', async () => {
      safeQuery.mockResolvedValueOnce([{ min_ts: null, max_ts: null }]);
      
      const result = await aggregations.getTimeRange();
      
      expect(result.minTimestamp).toBeNull();
      expect(result.maxTimestamp).toBeNull();
    });
  });

  describe('getTemplateEventCounts', () => {
    it('returns paginated event counts by template', async () => {
      safeQuery.mockResolvedValueOnce([
        { template_id: 'Splice.Amulet:Amulet', event_type: 'created', count: 5000n },
        { template_id: 'Splice.Amulet:Amulet', event_type: 'archived', count: 3000n },
        { template_id: 'Splice.DsoRules:VoteRequest', event_type: 'created', count: 150n },
      ]);
      
      const result = await aggregations.getTemplateEventCounts(100);
      
      expect(result).toHaveLength(3);
      expect(result[0].template_id).toBe('Splice.Amulet:Amulet');
      expect(result[0].count).toBe(5000);
    });

    it('respects limit parameter', async () => {
      safeQuery.mockResolvedValueOnce([
        { template_id: 'Template1', event_type: 'created', count: 100n },
      ]);
      
      await aggregations.getTemplateEventCounts(10);
      
      const query = safeQuery.mock.calls[0][0];
      expect(query).toContain('LIMIT 10');
    });
  });

  describe('updateAllAggregations', () => {
    it('returns results for all aggregation updates', async () => {
      // Mock multiple query calls for different aggregations
      safeQuery.mockResolvedValue([]);
      
      const result = await aggregations.updateAllAggregations();
      
      expect(result).toHaveProperty('eventTypeCounts');
    });

    it('handles errors gracefully', async () => {
      safeQuery.mockRejectedValueOnce(new Error('Database error'));
      
      const result = await aggregations.updateAllAggregations();
      
      // Should return error info instead of throwing
      expect(result.eventTypeCounts).toHaveProperty('error');
    });
  });

  describe('streamEvents generator', () => {
    it('yields events in pages', async () => {
      // First page
      safeQuery.mockResolvedValueOnce([
        { event_id: '1', template_id: 'Test', event_type: 'created' },
        { event_id: '2', template_id: 'Test', event_type: 'created' },
      ]);
      // Second page (empty, signals end)
      safeQuery.mockResolvedValueOnce([]);
      
      const events = [];
      for await (const event of aggregations.streamEvents({ pageSize: 2 })) {
        events.push(event);
      }
      
      expect(events).toHaveLength(2);
    });

    it('applies template filter when provided', async () => {
      safeQuery.mockResolvedValueOnce([]);
      
      const events = [];
      for await (const event of aggregations.streamEvents({ 
        template: 'Splice.Amulet:Amulet',
        pageSize: 10 
      })) {
        events.push(event);
      }
      
      const query = safeQuery.mock.calls[0][0];
      expect(query).toContain('Splice.Amulet:Amulet');
    });

    it('applies type filter when provided', async () => {
      safeQuery.mockResolvedValueOnce([]);
      
      const events = [];
      for await (const event of aggregations.streamEvents({ 
        type: 'exercised',
        pageSize: 10 
      })) {
        events.push(event);
      }
      
      const query = safeQuery.mock.calls[0][0];
      expect(query).toContain('exercised');
    });
  });
});

describe('Integration-like Scenarios', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('incremental update flow works correctly', async () => {
    // Simulate checking for new data
    safeQuery.mockResolvedValueOnce([{ last_file_id: 10n }]); // getLastFileId
    safeQuery.mockResolvedValueOnce([{ max_id: 15n }]); // getMaxIngestedFileId
    
    const hasNew = await aggregations.hasNewData('event_type_counts');
    expect(hasNew).toBe(true);
    
    // Simulate running the update
    safeQuery.mockResolvedValue([]);
    await aggregations.updateEventTypeCounts();
    
    // Verify update query was called
    expect(safeQuery).toHaveBeenCalled();
  });

  it('handles concurrent aggregation checks', async () => {
    // Set up different states for different aggregations
    safeQuery
      .mockResolvedValueOnce([{ last_file_id: 5n }])
      .mockResolvedValueOnce([{ max_id: 10n }])
      .mockResolvedValueOnce([{ last_file_id: 10n }])
      .mockResolvedValueOnce([{ max_id: 10n }]);
    
    const [hasNewA, hasNewB] = await Promise.all([
      aggregations.hasNewData('agg_a'),
      aggregations.hasNewData('agg_b'),
    ]);
    
    expect(hasNewA).toBe(true);
    expect(hasNewB).toBe(false);
  });
});
