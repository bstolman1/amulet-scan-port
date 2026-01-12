import { describe, it, expect, vi, beforeEach } from 'vitest';

// IMPORTANT:
// - We *partially* mock the DuckDB connection module so we don't erase real exports.
// - This prevents "No export is defined" errors and avoids connection lifecycle bugs.
vi.mock('../duckdb/connection.js', async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    query: vi.fn(),
    queryParallel: vi.fn(),
  };
});

import { query, queryParallel } from '../duckdb/connection.js';
import * as aggregations from './aggregations.js';

describe('server/engine/aggregations.js', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('hasNewData returns true when max ingested > last processed', async () => {
    // getLastFileId -> query
    (query as any)
      .mockResolvedValueOnce([{ last_file_id: 10n }])
      // getMaxIngestedFileId -> query
      .mockResolvedValueOnce([{ max_id: 20n }]);

    await expect(aggregations.hasNewData('event_type_counts')).resolves.toBe(true);
  });

  it('hasNewData returns false when no new data', async () => {
    (query as any)
      .mockResolvedValueOnce([{ last_file_id: 20n }])
      .mockResolvedValueOnce([{ max_id: 20n }]);

    await expect(aggregations.hasNewData('event_type_counts')).resolves.toBe(false);
  });

  it('updateEventTypeCounts returns null when nothing new', async () => {
    (query as any)
      .mockResolvedValueOnce([{ last_file_id: 20n }]) // lastFileId
      .mockResolvedValueOnce([{ max_id: 20n }]); // maxFileId

    const res = await aggregations.updateEventTypeCounts();
    expect(res).toBeNull();
  });

  it('updateEventTypeCounts aggregates and converts bigint counts to numbers', async () => {
    (query as any)
      // lastFileId
      .mockResolvedValueOnce([{ last_file_id: 10n }])
      // maxFileId
      .mockResolvedValueOnce([{ max_id: 12n }])
      // aggregation rows
      .mockResolvedValueOnce([
        { type: 'created', count: 3n },
        { type: 'archived', count: 2n },
      ])
      // setLastFileId write
      .mockResolvedValueOnce([]);

    const res = await aggregations.updateEventTypeCounts();

    expect(res).toEqual([
      { type: 'created', count: 3 },
      { type: 'archived', count: 2 },
    ]);
  });

  it('getTotalCounts returns events/updates counts (from queryParallel)', async () => {
    (queryParallel as any).mockResolvedValueOnce([[{ count: 100n }], [{ count: 50n }]]);

    const res = await aggregations.getTotalCounts();
    expect(res).toEqual({ events: 100, updates: 50 });
  });

  it('getTimeRange returns min/max fields as-is', async () => {
    (query as any).mockResolvedValueOnce([
      { min_ts: '2024-01-01T00:00:00.000Z', max_ts: '2025-01-10T12:00:00.000Z' },
    ]);

    const res = await aggregations.getTimeRange();
    expect(res).toEqual({
      min_ts: '2024-01-01T00:00:00.000Z',
      max_ts: '2025-01-10T12:00:00.000Z',
    });
  });

  it('getTemplateEventCounts converts bigint counts to numbers', async () => {
    (query as any).mockResolvedValueOnce([
      { template: 'T1', type: 'created', count: 5n },
      { template: 'T2', type: 'archived', count: 1n },
    ]);

    const res = await aggregations.getTemplateEventCounts(10);
    expect(res).toEqual([
      { template: 'T1', type: 'created', count: 5 },
      { template: 'T2', type: 'archived', count: 1 },
    ]);
  });

  it('streamEvents yields rows and stops when page is shorter than pageSize', async () => {
    (query as any)
      .mockResolvedValueOnce([
        { id: '1', template: 'T', type: 'created' },
        { id: '2', template: 'T', type: 'created' },
      ])
      .mockResolvedValueOnce([]);

    const out: any[] = [];
    for await (const row of aggregations.streamEvents({ pageSize: 2 })) {
      out.push(row);
    }

    expect(out).toHaveLength(2);
  });

  it('updateAllAggregations returns { eventTypeCounts, totals, timeRange }', async () => {
    // updateEventTypeCounts: 4 query calls
    (query as any)
      .mockResolvedValueOnce([{ last_file_id: 1n }])
      .mockResolvedValueOnce([{ max_id: 2n }])
      .mockResolvedValueOnce([{ type: 'created', count: 1n }])
      .mockResolvedValueOnce([])
      // getTimeRange: 1 query call
      .mockResolvedValueOnce([{ min_ts: null, max_ts: null }]);

    // getTotalCounts: queryParallel
    (queryParallel as any).mockResolvedValueOnce([[{ count: 1n }], [{ count: 1n }]]);

    const res = await aggregations.updateAllAggregations();

    expect(res).toEqual({
      eventTypeCounts: [{ type: 'created', count: 1 }],
      totals: { events: 1, updates: 1 },
      timeRange: { min_ts: null, max_ts: null },
    });
  });
});
